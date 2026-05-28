import {
  type PiSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
  ProviderDriverKind,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { makePiEnvironment, resolvePiHomePath } from "../Drivers/PiHome.ts";

const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const PROVIDER = ProviderDriverKind.make("piAgent");
const PI_PRESENTATION = {
  displayName: "Pi Agent",
  showInteractionModeToggle: true,
} as const;

/**
 * Capabilities for any model whose slug is not otherwise known. Used as a
 * safe fallback so callers never need to handle undefined.
 */
export function getPiModelCapabilities(_model: string | null | undefined): ModelCapabilities {
  // Model capabilities are now discovered dynamically from `pi --list-models`
  // and embedded directly in each `ServerProviderModel`. This function is
  // retained for callers that need a fallback when the model isn't found in
  // the live snapshot.
  return DEFAULT_PI_MODEL_CAPABILITIES;
}

const PI_LIST_MODELS_TIMEOUT_MS = 12_000;

/**
 * Parse the tabular output of `pi --list-models` into structured model rows.
 *
 * Expected header + data format:
 * ```
 * provider   model                       context  max-out  thinking  images
 * anthropic  claude-sonnet-4-6           1M       64K      yes       yes
 * cursor     claude-sonnet-4-6@1m        1M       16.4K    yes       yes
 * ```
 */
export function parsePiListModelsOutput(stdout: string): ReadonlyArray<{
  readonly provider: string;
  readonly model: string;
  readonly thinking: boolean;
}> {
  const lines = stdout.split("\n");
  const results: Array<{ provider: string; model: string; thinking: boolean }> = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !line.trim()) continue;
    // Columns are whitespace-separated; model names never contain spaces.
    const fields = line.trim().split(/\s+/);
    if (fields.length < 5) continue;
    const provider = fields[0];
    const model = fields[1];
    // "thinking" is the 5th column (index 4)
    const thinking = fields[4] === "yes";
    if (!provider || !model) continue;
    results.push({ provider, model, thinking });
  }

  return results;
}

/** Build the thinking-enabled `ModelCapabilities` descriptor for discovered models. */
function buildThinkingCapabilities(): ModelCapabilities {
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "thinking",
        label: "Thinking",
        options: [
          { value: "off", label: "Off" },
          { value: "low", label: "Low" },
          { value: "medium", label: "Medium", isDefault: true },
          { value: "high", label: "High" },
        ],
      }),
    ],
  });
}

/**
 * Discover Pi models dynamically by running `pi --list-models`. Returns every
 * model reported by the CLI using `provider/model` slugs (e.g.
 * `anthropic/claude-sonnet-4-6`, `cursor/claude-sonnet-4-6@1m`). The list is
 * authoritative — nothing is hardcoded in t3code.
 */
const discoverPiModels = Effect.fn("discoverPiModels")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ReadonlyArray<ServerProviderModel>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const result = yield* runPiCommand(piSettings, ["--list-models"], environment).pipe(
    Effect.timeoutOption(PI_LIST_MODELS_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(result) || Option.isNone(result.success)) {
    return [];
  }

  const commandResult = result.success.value;
  if (commandResult.code !== 0) return [];

  // `pi --list-models` writes its table to stderr, not stdout.
  const rows = parsePiListModelsOutput(commandResult.stderr);
  const thinkingCaps = buildThinkingCapabilities();

  return rows.map((row) => ({
    slug: `${row.provider}/${row.model}`,
    name: `${row.provider}/${row.model}`,
    isCustom: false,
    capabilities: row.thinking ? thinkingCaps : DEFAULT_PI_MODEL_CAPABILITIES,
  }));
});

/**
 * Background snapshot enrichment hook for the Pi Agent provider.
 *
 * Chains two passes:
 * 1. Version-advisory enrichment (checks npm/Homebrew for updates).
 * 2. Dynamic model discovery via `pi --list-models`, which surfaces cursor and
 *    other provider models that become available after login without restarting
 *    t3code.
 *
 * Mirrors the `enrichCursorSnapshot` pattern so both providers stay consistent.
 */
export const enrichPiSnapshot = (input: {
  readonly settings: PiSettings;
  readonly environment?: NodeJS.ProcessEnv;
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly stampIdentity?: (snapshot: ServerProvider) => ServerProvider;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<
  void,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> => {
  const { settings, snapshot, publishSnapshot } = input;
  const stampIdentity = input.stampIdentity ?? ((value: ServerProvider) => value);
  const environment = input.environment ?? process.env;

  const enrichVersionAdvisory = enrichProviderSnapshotWithVersionAdvisory(
    snapshot,
    input.maintenanceCapabilities,
  ).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) =>
      publishSnapshot(stampIdentity(enrichedSnapshot)).pipe(Effect.as(enrichedSnapshot)),
    ),
    Effect.catchCause((cause) =>
      Effect.logWarning("Pi version advisory enrichment failed", {
        cause: Cause.pretty(cause),
      }).pipe(Effect.as(snapshot)),
    ),
  );

  return enrichVersionAdvisory.pipe(
    Effect.flatMap((baseSnapshot) => {
      if (!settings.enabled || !baseSnapshot.installed) {
        return Effect.void;
      }

      return discoverPiModels(settings, environment).pipe(
        Effect.flatMap((discoveredModels) => {
          if (discoveredModels.length === 0) return Effect.void;

          const models = providerModelsFromSettings(
            discoveredModels,
            PROVIDER,
            settings.customModels,
            DEFAULT_PI_MODEL_CAPABILITIES,
          );

          return publishSnapshot(
            stampIdentity({
              ...baseSnapshot,
              models,
            }),
          );
        }),
        Effect.catchCause((cause) =>
          Effect.logWarning("Pi model discovery failed", {
            cause: Cause.pretty(cause),
          }).pipe(Effect.asVoid),
        ),
      );
    }),
  );
};

const runPiCommand = Effect.fn("runPiCommand")(function* (
  piSettings: PiSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const piEnvironment = yield* makePiEnvironment(piSettings, environment);
  const command = ChildProcess.make(piSettings.binaryPath || "pi", [...args], {
    env: piEnvironment,
    shell: false,
  });
  return yield* spawnAndCollect(piSettings.binaryPath || "pi", command);
});

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

function parseSkillFrontmatter(content: string): {
  name?: string;
  description?: string;
} {
  const match = /^---\s*\n([\s\S]*?)\n---/.exec(content);
  if (!match?.[1]) return {};
  const yaml = match[1];
  const result: { name?: string; description?: string } = {};
  for (const line of yaml.split("\n")) {
    const kvMatch = /^(\w[\w-]*):\s*(.+)$/.exec(line.trim());
    if (!kvMatch) continue;
    const key = kvMatch[1];
    const rawValue = kvMatch[2] ?? "";
    const value = rawValue.replace(/^["']|["']$/g, "").trim();
    if (key === "name" && value.length > 0) result.name = value;
    if (key === "description" && value.length > 0) result.description = value;
  }
  return result;
}

function skillNameFromPath(filePath: string, pathSep: string): string {
  const base = filePath.split(pathSep).pop() ?? filePath;
  return base.replace(/\.md$/i, "");
}

const scanSkillDir = Effect.fn("scanSkillDir")(function* (
  dir: string,
  scope: string,
): Effect.fn.Return<ServerProviderSkill[], never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const skills: ServerProviderSkill[] = [];

  const exists = yield* fs.exists(dir).pipe(Effect.orElseSucceed(() => false));
  if (!exists) return skills;

  const entries = yield* fs.readDirectory(dir).pipe(Effect.orElseSucceed(() => [] as string[]));

  for (const entry of entries) {
    const entryPath = path.join(dir, entry);
    const stat = yield* fs.stat(entryPath).pipe(Effect.orElseSucceed(() => undefined));
    if (!stat) continue;

    if (stat.type === "Directory") {
      const skillMdPath = path.join(entryPath, "SKILL.md");
      const hasSkillMd = yield* fs.exists(skillMdPath).pipe(Effect.orElseSucceed(() => false));
      if (hasSkillMd) {
        const content = yield* fs.readFileString(skillMdPath).pipe(Effect.orElseSucceed(() => ""));
        const frontmatter = parseSkillFrontmatter(content);
        const name = frontmatter.name ?? entry;
        skills.push({
          name,
          path: skillMdPath,
          enabled: true,
          scope,
          ...(frontmatter.description ? { description: frontmatter.description } : {}),
          ...(frontmatter.name ? { displayName: frontmatter.name } : {}),
        });
      }
      continue;
    }

    if (entry.endsWith(".md")) {
      const content = yield* fs.readFileString(entryPath).pipe(Effect.orElseSucceed(() => ""));
      const frontmatter = parseSkillFrontmatter(content);
      const name = frontmatter.name ?? skillNameFromPath(entry, path.sep);
      skills.push({
        name,
        path: entryPath,
        enabled: true,
        ...(scope ? { scope } : {}),
        ...(frontmatter.description ? { description: frontmatter.description } : {}),
        ...(frontmatter.name ? { displayName: frontmatter.name } : {}),
      });
    }
  }

  return skills;
});

const discoverPiSkills = Effect.fn("discoverPiSkills")(function* (
  cwd: string,
  piSettings: PiSettings,
): Effect.fn.Return<ReadonlyArray<ServerProviderSkill>, never, FileSystem.FileSystem | Path.Path> {
  const path = yield* Path.Path;
  const piHomePath = yield* resolvePiHomePath(piSettings);

  const dirs: Array<{ dir: string; scope: string }> = [
    { dir: path.join(cwd, ".pi", "skills"), scope: "project" },
    { dir: path.join(cwd, ".claude", "skills"), scope: "project" },
    { dir: path.join(piHomePath, ".pi", "agent", "skills"), scope: "user" },
  ];

  const results = yield* Effect.all(
    dirs.map(({ dir, scope }) => scanSkillDir(dir, scope)),
    { concurrency: "unbounded" },
  );

  const seen = new Set<string>();
  const deduped: ServerProviderSkill[] = [];
  for (const batch of results) {
    for (const skill of batch) {
      if (!seen.has(skill.name)) {
        seen.add(skill.name);
        deduped.push(skill);
      }
    }
  }
  return deduped;
});

export type PiCapabilitiesProbe = {
  readonly versionResult: Result.Result<
    Option.Option<{ code: number; stdout: string; stderr: string }>,
    { readonly message: string }
  >;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
};

export const probePiCapabilities = Effect.fn("probePiCapabilities")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<
  PiCapabilitiesProbe,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const [versionResult, skills] = yield* Effect.all(
    [
      runPiCommand(piSettings, ["--version"], environment).pipe(
        Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
        Effect.result,
      ),
      discoverPiSkills(cwd, piSettings).pipe(
        Effect.orElseSucceed(() => [] as ServerProviderSkill[]),
      ),
    ],
    { concurrency: "unbounded" },
  );
  return { versionResult, skills };
});

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  resolveProbe: () => Effect.Effect<PiCapabilitiesProbe>,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  // Models are populated by the background `enrichPiSnapshot` pass via
  // `pi --list-models`; use an empty list here to avoid stale hardcoded data.
  const customOnlyModels = providerModelsFromSettings(
    [],
    PROVIDER,
    piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: customOnlyModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi Agent is disabled in T3 Code settings.",
      },
    });
  }

  const { versionResult: versionProbe, skills } = yield* resolveProbe();

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: customOnlyModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Pi Agent CLI (`pi`) is not installed or not on PATH."
          : `Failed to execute Pi Agent CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: customOnlyModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi Agent CLI is installed but failed to run. Timed out while running command.",
      },
    });
  }

  const version = versionProbe.success.value;
  const parsedVersion = parseGenericCliVersion(`${version.stdout}\n${version.stderr}`);

  if (version.code !== 0) {
    const detail = detailFromResult(version);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: customOnlyModels,
      probe: {
        installed: true,
        version: parsedVersion,
        status: "error",
        auth: { status: "unknown" },
        message: detail ?? "Pi Agent CLI returned an error during health check.",
      },
    });
  }

  const slashCommands: ServerProviderSlashCommand[] = [
    { name: "compact", description: "Manually compact the session context" },
    ...skills.map((skill) => ({
      name: `skill:${skill.name}`,
      description: skill.description ?? `Run ${skill.displayName ?? skill.name} skill`,
    })),
  ];

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models: customOnlyModels,
    slashCommands,
    skills,
    probe: {
      installed: true,
      version: parsedVersion,
      status: "ready",
      auth: { status: "authenticated" },
    },
  });
});

export const makePendingPiProvider = (piSettings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* nowIso;
    const customOnlyModels = providerModelsFromSettings(
      [],
      PROVIDER,
      piSettings.customModels,
      DEFAULT_PI_MODEL_CAPABILITIES,
    );

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models: customOnlyModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi Agent is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customOnlyModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi Agent provider status has not been checked in this session yet.",
      },
    });
  });
