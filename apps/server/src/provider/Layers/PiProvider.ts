import {
  type PiSettings,
  type ModelCapabilities,
  type ServerProviderModel,
  type ServerProviderSkill,
  ProviderDriverKind,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
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
import { makePiEnvironment, resolvePiHomePath } from "../Drivers/PiHome.ts";

const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const PROVIDER = ProviderDriverKind.make("piAgent");
const PI_PRESENTATION = {
  displayName: "Pi Agent",
  showInteractionModeToggle: true,
} as const;

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "claude-sonnet-4-6",
    name: "Claude Sonnet 4.6",
    isCustom: false,
    capabilities: createModelCapabilities({
      optionDescriptors: [
        buildSelectOptionDescriptor({
          id: "thinking",
          label: "Thinking",
          options: [
            { value: "off", label: "Off" },
            { value: "low", label: "Low" },
            { value: "medium", label: "Medium", isDefault: true },
            { value: "high", label: "High" },
            { value: "xhigh", label: "Extra High" },
          ],
        }),
      ],
    }),
  },
  {
    slug: "claude-opus-4-7",
    name: "Claude Opus 4.7",
    isCustom: false,
    capabilities: createModelCapabilities({
      optionDescriptors: [
        buildSelectOptionDescriptor({
          id: "thinking",
          label: "Thinking",
          options: [
            { value: "off", label: "Off" },
            { value: "low", label: "Low" },
            { value: "medium", label: "Medium" },
            { value: "high", label: "High", isDefault: true },
            { value: "xhigh", label: "Extra High" },
          ],
        }),
      ],
    }),
  },
  {
    slug: "claude-haiku-4-5",
    name: "Claude Haiku 4.5",
    isCustom: false,
    capabilities: createModelCapabilities({
      optionDescriptors: [
        buildSelectOptionDescriptor({
          id: "thinking",
          label: "Thinking",
          options: [
            { value: "off", label: "Off" },
            { value: "low", label: "Low", isDefault: true },
            { value: "medium", label: "Medium" },
            { value: "high", label: "High" },
          ],
        }),
      ],
    }),
  },
];

export function getPiModelCapabilities(model: string | null | undefined): ModelCapabilities {
  const slug = model?.trim();
  return (
    BUILT_IN_MODELS.find((candidate) => candidate.slug === slug)?.capabilities ??
    DEFAULT_PI_MODEL_CAPABILITIES
  );
}

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

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const allModels = providerModelsFromSettings(
    BUILT_IN_MODELS,
    PROVIDER,
    piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: allModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi Agent is disabled in T3 Code settings.",
      },
    });
  }

  const versionProbe = yield* runPiCommand(piSettings, ["--version"], environment).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: allModels,
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
      models: allModels,
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
      models: allModels,
      probe: {
        installed: true,
        version: parsedVersion,
        status: "error",
        auth: { status: "unknown" },
        message: detail ?? "Pi Agent CLI returned an error during health check.",
      },
    });
  }

  const skills = yield* discoverPiSkills(cwd, piSettings).pipe(
    Effect.orElseSucceed(() => [] as ServerProviderSkill[]),
  );

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models: allModels,
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
    const models = providerModelsFromSettings(
      BUILT_IN_MODELS,
      PROVIDER,
      piSettings.customModels,
      DEFAULT_PI_MODEL_CAPABILITIES,
    );

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
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
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi Agent provider status has not been checked in this session yet.",
      },
    });
  });
