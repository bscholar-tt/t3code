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
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  collectStreamAsString,
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

/** Build the thinking select option descriptor (OFF / Low / Medium / High). */
function buildThinkingOptionDescriptor() {
  return buildSelectOptionDescriptor({
    id: "thinking",
    label: "Thinking",
    options: [
      { value: "off", label: "Off" },
      { value: "low", label: "Low" },
      { value: "medium", label: "Medium", isDefault: true },
      { value: "high", label: "High" },
    ],
  });
}

/** Build `ModelCapabilities` for a model that supports only thinking (no context variants). */
function buildThinkingCapabilities(): ModelCapabilities {
  return createModelCapabilities({ optionDescriptors: [buildThinkingOptionDescriptor()] });
}

/**
 * Parse a context-size variant string (e.g. `"1m"`, `"300k"`, `"200k"`) into
 * a comparable numeric value so variants can be sorted largest-first.
 */
function parseContextSizeValue(variant: string): number {
  const lower = variant.toLowerCase();
  const num = parseFloat(lower);
  if (lower.endsWith("m")) return num * 1_000_000;
  if (lower.endsWith("k")) return num * 1_000;
  return num || 0;
}

/**
 * Discover Pi models dynamically by running `pi --list-models`.
 *
 * Models whose slugs share a base name but differ only by a `@contextsize`
 * suffix (e.g. `claude-opus-4-7@1m` / `claude-opus-4-7@300k`) are grouped
 * under a single entry with a `contextWindow` select descriptor. Models with
 * only one context variant are also collapsed to their base slug but carry no
 * selector (the `@`-suffix is embedded as the display name is stripped).
 *
 * All other models use `provider/model` slugs directly.
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
  const models: ServerProviderModel[] = [];

  // Separate rows into plain models (no @) and variant groups (@contextsize).
  type VariantRow = { provider: string; model: string; thinking: boolean; variant: string };
  const variantGroups = new Map<string, VariantRow[]>();

  for (const row of rows) {
    const atIdx = row.model.indexOf("@");
    if (atIdx !== -1) {
      const base = row.model.slice(0, atIdx);
      const variant = row.model.slice(atIdx + 1);
      const groupKey = `${row.provider}/${base}`;
      let group = variantGroups.get(groupKey);
      if (!group) {
        group = [];
        variantGroups.set(groupKey, group);
      }
      group.push({ ...row, variant });
    } else {
      // Plain model — emit directly.
      models.push({
        slug: `${row.provider}/${row.model}`,
        name: `${row.provider}/${row.model}`,
        isCustom: false,
        capabilities: row.thinking ? buildThinkingCapabilities() : DEFAULT_PI_MODEL_CAPABILITIES,
      });
    }
  }

  // Emit one entry per variant group.
  for (const [groupSlug, variants] of variantGroups) {
    // Sort largest context first; that variant becomes the default.
    const sorted = variants.toSorted(
      (a, b) => parseContextSizeValue(b.variant) - parseContextSizeValue(a.variant),
    );
    const hasThinking = sorted.some((v) => v.thinking);

    if (sorted.length === 1) {
      // Single variant: collapse to base slug for display; bake @variant into
      // the slug itself so the Pi CLI receives the correct model identifier.
      const v = sorted[0];
      if (!v) continue;
      models.push({
        slug: `${v.provider}/${v.model}@${v.variant}`,
        name: groupSlug, // display without @-suffix
        isCustom: false,
        capabilities: hasThinking ? buildThinkingCapabilities() : DEFAULT_PI_MODEL_CAPABILITIES,
      });
    } else {
      // Multiple variants: expose as a single entry with a `contextWindow`
      // select option. PiAdapter reads the selection and appends `@<value>`
      // to the slug before passing it to the Pi CLI.
      const contextOptions = sorted.map((v, i) => ({
        value: v.variant,
        label: v.variant.toUpperCase(),
        isDefault: i === 0,
      }));
      const optionDescriptors = [
        ...(hasThinking ? [buildThinkingOptionDescriptor()] : []),
        buildSelectOptionDescriptor({
          id: "contextWindow",
          label: "Context",
          options: contextOptions,
        }),
      ];
      models.push({
        slug: groupSlug,
        name: groupSlug,
        isCustom: false,
        capabilities: createModelCapabilities({ optionDescriptors }),
      });
    }
  }

  return models;
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

interface RpcSlashCommand {
  readonly name: string;
  readonly description?: string;
  readonly source: "extension" | "prompt" | "skill";
  readonly sourceInfo?: { readonly path?: string; readonly scope?: string };
}

const PI_RPC_PROBE_TIMEOUT_MS = 8_000;

function parseRpcCommandsResponse(line: string): ReadonlyArray<RpcSlashCommand> | null {
  // eslint-disable-next-line no-restricted-syntax -- parsing external RPC JSON
  let msg: Record<string, unknown>;
  try {
    msg = JSON.parse(line) as Record<string, unknown>; // @effect-diagnostics-ignore preferSchemaOverJson
  } catch {
    return null;
  }
  if (msg["type"] === "response" && msg["command"] === "get_commands" && msg["success"] === true) {
    const data = msg["data"] as { commands?: unknown[] } | undefined;
    if (Array.isArray(data?.commands)) {
      return data.commands.filter(
        (c): c is RpcSlashCommand =>
          typeof c === "object" &&
          c !== null &&
          typeof (c as Record<string, unknown>)["name"] === "string",
      );
    }
  }
  return null;
}

const probePiRpcCommands = Effect.fn("probePiRpcCommands")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ReadonlyArray<RpcSlashCommand>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const piEnvironment = yield* makePiEnvironment(piSettings, environment);
  const binaryPath = piSettings.binaryPath || "pi";

  const command = ChildProcess.make(binaryPath, ["--mode", "rpc"], {
    env: piEnvironment,
    cwd,
    shell: false,
  });
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(command);

      const requestPayload = `{"type":"get_commands","id":"probe"}\n`;
      yield* Stream.make(new TextEncoder().encode(requestPayload)).pipe(Stream.run(child.stdin));

      const stdout = yield* collectStreamAsString(child.stdout);

      const commands: RpcSlashCommand[] = [];
      for (const line of stdout.split("\n")) {
        const parsed = parseRpcCommandsResponse(line.trim());
        if (parsed) {
          commands.push(...parsed);
          break;
        }
      }
      return commands as ReadonlyArray<RpcSlashCommand>;
    }),
  ).pipe(Effect.orElseSucceed(() => [] as ReadonlyArray<RpcSlashCommand>));
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
  readonly rpcCommands: ReadonlyArray<RpcSlashCommand>;
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
  const [versionResult, skills, rpcCommands] = yield* Effect.all(
    [
      runPiCommand(piSettings, ["--version"], environment).pipe(
        Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
        Effect.result,
      ),
      discoverPiSkills(cwd, piSettings).pipe(
        Effect.orElseSucceed(() => [] as ServerProviderSkill[]),
      ),
      probePiRpcCommands(piSettings, cwd, environment).pipe(
        Effect.timeoutOption(PI_RPC_PROBE_TIMEOUT_MS),
        Effect.map((opt) => (Option.isSome(opt) ? opt.value : [])),
        Effect.orElseSucceed(() => [] as ReadonlyArray<RpcSlashCommand>),
      ),
    ],
    { concurrency: "unbounded" },
  );
  return { versionResult, skills, rpcCommands };
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

  const { versionResult: versionProbe, skills, rpcCommands } = yield* resolveProbe();

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

  const seen = new Set<string>();
  const slashCommands: ServerProviderSlashCommand[] = [
    { name: "compact", description: "Manually compact the session context" },
  ];
  seen.add("compact");

  const skillNames = new Set(skills.map((s) => s.name));
  const allSkills = [...skills];

  for (const cmd of rpcCommands) {
    if (cmd.source === "skill") {
      const skillName = cmd.name.replace(/^skill:/, "");
      if (!skillNames.has(skillName)) {
        skillNames.add(skillName);
        allSkills.push({
          name: skillName,
          path: cmd.sourceInfo?.path ?? skillName,
          enabled: true,
          ...(cmd.sourceInfo?.scope ? { scope: cmd.sourceInfo.scope } : {}),
          ...(cmd.description ? { description: cmd.description } : {}),
        });
      }
      continue;
    }
    if (!seen.has(cmd.name)) {
      seen.add(cmd.name);
      slashCommands.push({
        name: cmd.name,
        ...(cmd.description ? { description: cmd.description } : {}),
      });
    }
  }

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models: customOnlyModels,
    slashCommands,
    skills: allSkills,
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
