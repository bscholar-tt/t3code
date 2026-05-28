import { type AgentSessionEvent, type RpcCommand } from "@earendil-works/pi-coding-agent";
import {
  ApprovalRequestId,
  type CanonicalItemType,
  EventId,
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { parseCliArgs } from "@t3tools/shared/cliArgs";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { makePiEnvironment } from "../Drivers/PiHome.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";

// RpcExtensionUIRequest / RpcExtensionUIResponse are defined in the pi package's
// internal rpc-types module but not re-exported from the public index, so we
// mirror the relevant subset here.
type RpcExtensionUIRequest =
  | {
      type: "extension_ui_request";
      id: string;
      method: "select";
      title: string;
      options: string[];
      timeout?: number;
    }
  | {
      type: "extension_ui_request";
      id: string;
      method: "confirm";
      title: string;
      message: string;
      timeout?: number;
    }
  | {
      type: "extension_ui_request";
      id: string;
      method: "input";
      title: string;
      placeholder?: string;
      timeout?: number;
    }
  | { type: "extension_ui_request"; id: string; method: "editor"; title: string; prefill?: string }
  | {
      type: "extension_ui_request";
      id: string;
      method: "notify";
      message: string;
      notifyType?: "info" | "warning" | "error";
    }
  | {
      type: "extension_ui_request";
      id: string;
      method: "setStatus";
      statusKey: string;
      statusText: string | undefined;
    }
  | {
      type: "extension_ui_request";
      id: string;
      method: "setWidget";
      widgetKey: string;
      widgetLines: string[] | undefined;
      widgetPlacement?: "aboveEditor" | "belowEditor";
    }
  | { type: "extension_ui_request"; id: string; method: "setTitle"; title: string }
  | { type: "extension_ui_request"; id: string; method: "set_editor_text"; text: string };

type RpcExtensionUIResponse =
  | { type: "extension_ui_response"; id: string; value: string }
  | { type: "extension_ui_response"; id: string; confirmed: boolean }
  | { type: "extension_ui_response"; id: string; cancelled: true };

const PROVIDER = ProviderDriverKind.make("piAgent");

interface PiToolItem {
  readonly id: RuntimeItemId;
  readonly type: CanonicalItemType;
  readonly toolName: string;
  readonly args: unknown;
}

interface PiTurnState {
  readonly turnId: TurnId;
  readonly startedAt: string;
  readonly items: Array<PiToolItem>;
  activeTextItemId: RuntimeItemId | undefined;
  activeReasoningItemId: RuntimeItemId | undefined;
}

interface PendingExtensionUI {
  readonly piId: string;
  readonly questionId: string;
  readonly deferred: Deferred.Deferred<ProviderUserInputAnswers, never>;
  readonly method: "select" | "confirm" | "input" | "editor";
  // Populated for `input` requests that were parsed as numbered lists.
  // Used to map selected labels back to 1-based indices for Pi's multi-select protocol.
  readonly numberedOptions?: ReadonlyArray<string>;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly sessionScope: Scope.Closeable;
  writeCommand: (cmd: RpcCommand) => Effect.Effect<void>;
  writeExtensionResponse: (resp: RpcExtensionUIResponse) => Effect.Effect<void>;
  stdoutFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingRequests: Map<string, Deferred.Deferred<unknown, never>>;
  readonly pendingExtensionUIs: Map<ApprovalRequestId, PendingExtensionUI>;
  readonly startedAt: string;
  turnState: PiTurnState | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<PiToolItem> }>;
  stopped: boolean;
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

function readPiResumeState(resumeCursor: unknown): { sessionFile: string } | undefined {
  if (!resumeCursor || typeof resumeCursor !== "object") return undefined;
  const cursor = resumeCursor as Record<string, unknown>;
  return typeof cursor.sessionFile === "string" && cursor.sessionFile.trim().length > 0
    ? { sessionFile: cursor.sessionFile }
    : undefined;
}

function classifyToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("agent") ||
    normalized.includes("subagent") ||
    normalized.includes("sub-agent") ||
    normalized === "task" ||
    normalized === "skill"
  ) {
    return "collab_agent_tool_call";
  }
  if (
    normalized.includes("bash") ||
    normalized.includes("shell") ||
    normalized.includes("command") ||
    normalized.includes("terminal") ||
    normalized.includes("exec")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("file") ||
    normalized.includes("patch") ||
    normalized.includes("apply") ||
    normalized.includes("replace") ||
    normalized.includes("create") ||
    normalized.includes("delete")
  ) {
    return "file_change";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("websearch") || normalized.includes("web search")) {
    return "web_search";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  return "dynamic_tool_call";
}

// Pi's RPC multi-select fallback encodes options as a numbered list in the title:
//   "Which colors?\n1. Red\n2. Blue\n3. Green"
// Returns the question title and extracted option strings, or null if no list found.
function parseNumberedList(text: string): { title: string; items: ReadonlyArray<string> } | null {
  const lines = text.split("\n");
  const items: string[] = [];
  for (const line of lines.slice(1)) {
    const m = /^\d+\.\s+(.+)$/.exec(line.trim());
    if (m?.[1]) items.push(m[1]);
  }
  return items.length >= 2 ? { title: lines[0] ?? text, items } : null;
}

function tryParseJsonObject(text: string): Record<string, unknown> | null {
  try {
    // eslint-disable-next-line no-restricted-syntax -- non-Effect context, no schema needed
    const v = JSON.parse(text) as unknown; // @effect-diagnostics-ignore preferSchemaOverJson
    return v !== null && typeof v === "object" ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function summarizePiToolArgs(args: unknown): string | undefined {
  if (!args || typeof args !== "object") return undefined;
  const input = args as Record<string, unknown>;

  const commandValue = input.command ?? input.cmd;
  if (typeof commandValue === "string" && commandValue.trim().length > 0) {
    return commandValue.trim().slice(0, 400);
  }

  const skillValue = input.skill ?? input.skillName;
  if (typeof skillValue === "string" && skillValue.trim().length > 0) {
    const skillArgs = typeof input.args === "string" ? input.args.trim() : undefined;
    return skillArgs ? `${skillValue.trim()} ${skillArgs}`.slice(0, 400) : skillValue.trim();
  }

  const descValue = input.description ?? input.prompt;
  if (typeof descValue === "string" && descValue.trim().length > 0) {
    return descValue.trim().slice(0, 400);
  }

  const pathValue = input.file_path ?? input.path ?? input.filePath;
  if (typeof pathValue === "string" && pathValue.trim().length > 0) {
    return pathValue.trim().slice(0, 400);
  }

  const patternValue = input.pattern ?? input.query;
  if (typeof patternValue === "string" && patternValue.trim().length > 0) {
    return patternValue.trim().slice(0, 400);
  }

  try {
    const serialized = JSON.stringify(input);
    if (serialized.length <= 400) return serialized;
    return `${serialized.slice(0, 397)}...`;
  } catch {
    return undefined;
  }
}

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
}

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
) {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("piAgent");
  const serverConfig = yield* ServerConfig;
  const piEnvironment = yield* makePiEnvironment(piSettings, options?.environment);
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;

  const sessions = new Map<ThreadId, PiSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.make(id));
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const offerRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  const completeTurn = Effect.fn("completeTurn")(function* (
    context: PiSessionContext,
    state: ProviderRuntimeTurnStatus,
    message?: string,
  ) {
    const turnState = context.turnState;
    if (!turnState) return;

    if (turnState.activeTextItemId) {
      const closingTextId = turnState.activeTextItemId;
      turnState.activeTextItemId = undefined;
      const closingStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        eventId: closingStamp.eventId,
        provider: PROVIDER,
        createdAt: closingStamp.createdAt,
        threadId: context.session.threadId,
        turnId: turnState.turnId,
        itemId: closingTextId,
        type: "item.completed",
        payload: { itemType: "assistant_message", status: "completed" },
        providerRefs: {},
      });
    }
    if (turnState.activeReasoningItemId) {
      const closingReasoningId = turnState.activeReasoningItemId;
      turnState.activeReasoningItemId = undefined;
      const closingStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        eventId: closingStamp.eventId,
        provider: PROVIDER,
        createdAt: closingStamp.createdAt,
        threadId: context.session.threadId,
        turnId: turnState.turnId,
        itemId: closingReasoningId,
        type: "item.completed",
        payload: { itemType: "reasoning", status: "completed" },
        providerRefs: {},
      });
    }

    context.turnState = undefined;
    context.turns.push({
      id: turnState.turnId,
      items: [...turnState.items],
    });

    const updatedAt = yield* nowIso;
    context.session = {
      ...context.session,
      status: "ready",
      activeTurnId: undefined,
      updatedAt,
    };

    const stamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.completed",
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      turnId: turnState.turnId,
      payload: {
        state,
        ...(message ? { message } : {}),
      },
      providerRefs: {},
    });
  });

  const handlePiEvent = Effect.fn("handlePiEvent")(function* (
    context: PiSessionContext,
    event: AgentSessionEvent,
  ) {
    const stamp = yield* makeEventStamp();
    const base = {
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
      providerRefs: {},
      raw: {
        source: "pi.sdk.event" as const,
        method: event.type,
        payload: event,
      },
    };

    switch (event.type) {
      case "agent_start":
        yield* offerRuntimeEvent({
          ...base,
          type: "session.state.changed",
          payload: { state: "running" },
        });
        return;

      case "turn_start": {
        if (!context.turnState) {
          const turnId = TurnId.make(yield* Random.nextUUIDv4);
          const startedAt = yield* nowIso;
          context.turnState = {
            turnId,
            startedAt,
            items: [],
            activeTextItemId: undefined,
            activeReasoningItemId: undefined,
          };
          const updatedAt = yield* nowIso;
          context.session = {
            ...context.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt,
          };
          yield* offerRuntimeEvent({
            ...base,
            turnId,
            type: "turn.started",
            payload: {},
          });
        }
        return;
      }

      case "message_update": {
        if (!context.turnState) return;
        const assistantEvent = event.assistantMessageEvent;
        if (!assistantEvent) return;
        if (assistantEvent.type === "text_delta") {
          if (!context.turnState.activeTextItemId) {
            const textItemId = RuntimeItemId.make(yield* Random.nextUUIDv4);
            context.turnState.activeTextItemId = textItemId;
            yield* offerRuntimeEvent({
              ...base,
              turnId: context.turnState.turnId,
              itemId: textItemId,
              type: "item.started",
              payload: { itemType: "assistant_message" },
            });
          }
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId: context.turnState.activeTextItemId,
            type: "content.delta",
            payload: {
              streamKind: "assistant_text",
              delta: assistantEvent.delta,
            },
          });
        } else if (assistantEvent.type === "thinking_delta") {
          if (!context.turnState.activeReasoningItemId) {
            const reasoningItemId = RuntimeItemId.make(yield* Random.nextUUIDv4);
            context.turnState.activeReasoningItemId = reasoningItemId;
            yield* offerRuntimeEvent({
              ...base,
              turnId: context.turnState.turnId,
              itemId: reasoningItemId,
              type: "item.started",
              payload: { itemType: "reasoning" },
            });
          }
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId: context.turnState.activeReasoningItemId,
            type: "content.delta",
            payload: {
              streamKind: "reasoning_text",
              delta: assistantEvent.delta,
            },
          });
        }
        return;
      }

      case "tool_execution_start": {
        if (!context.turnState) return;
        if (context.turnState.activeTextItemId) {
          const closingTextId = context.turnState.activeTextItemId;
          context.turnState.activeTextItemId = undefined;
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId: closingTextId,
            type: "item.completed",
            payload: { itemType: "assistant_message", status: "completed" },
          });
        }
        if (context.turnState.activeReasoningItemId) {
          const closingReasoningId = context.turnState.activeReasoningItemId;
          context.turnState.activeReasoningItemId = undefined;
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId: closingReasoningId,
            type: "item.completed",
            payload: { itemType: "reasoning", status: "completed" },
          });
        }
        const itemId = RuntimeItemId.make(event.toolCallId);
        const itemType = classifyToolItemType(event.toolName);
        const detail = summarizePiToolArgs(event.args);
        const argsObj =
          event.args && typeof event.args === "object"
            ? (event.args as Record<string, unknown>)
            : undefined;
        context.turnState.items.push({
          id: itemId,
          type: itemType,
          toolName: event.toolName,
          args: event.args,
        });
        yield* offerRuntimeEvent({
          ...base,
          turnId: context.turnState.turnId,
          itemId,
          type: "item.started",
          payload: {
            itemType,
            title: event.toolName,
            ...(detail ? { detail } : {}),
            ...(argsObj ? { data: { toolName: event.toolName, input: argsObj } } : {}),
          },
        });
        return;
      }

      case "tool_execution_update": {
        if (!context.turnState) return;
        const itemId = RuntimeItemId.make(event.toolCallId);
        const itemType = classifyToolItemType(event.toolName);
        if (event.partialResult !== undefined) {
          const partial =
            typeof event.partialResult === "string"
              ? event.partialResult
              : String(event.partialResult);
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId,
            type: "content.delta",
            payload: {
              streamKind:
                itemType === "command_execution" ? "command_output" : "file_change_output",
              delta: partial,
            },
          });
        }
        return;
      }

      case "tool_execution_end": {
        if (!context.turnState) return;
        const itemId = RuntimeItemId.make(event.toolCallId);
        const itemType = classifyToolItemType(event.toolName);
        const storedItem = context.turnState.items.find((item) => item.id === itemId);
        const args = storedItem?.args;
        const detail = summarizePiToolArgs(args);
        const argsObj =
          args && typeof args === "object" ? (args as Record<string, unknown>) : undefined;
        if (event.toolName === "set_plan" && !event.isError) {
          const planMarkdown = typeof argsObj?.plan === "string" ? argsObj.plan.trim() : undefined;
          if (planMarkdown) {
            const planStamp = yield* makeEventStamp();
            yield* offerRuntimeEvent({
              type: "turn.proposed.completed",
              eventId: planStamp.eventId,
              provider: PROVIDER,
              createdAt: planStamp.createdAt,
              threadId: context.session.threadId,
              turnId: context.turnState.turnId,
              payload: { planMarkdown },
              providerRefs: {},
            });
          }
        }
        yield* offerRuntimeEvent({
          ...base,
          turnId: context.turnState.turnId,
          itemId,
          type: "item.completed",
          payload: {
            itemType,
            title: event.toolName,
            status: event.isError ? "failed" : "completed",
            ...(detail ? { detail } : {}),
            ...(argsObj ? { data: { toolName: event.toolName, input: argsObj } } : {}),
          },
        });
        return;
      }

      case "turn_end": {
        // Pi fires turn_end after each internal LLM call, but agent_end fires
        // after the full agent run. Completing here would fragment the Pi run
        // into multiple t3code turns, causing tool activities to disappear and
        // the timer to reset on every sub-turn. Let agent_end drive completion.
        return;
      }

      case "agent_end": {
        if (context.turnState) {
          const willRetry = "willRetry" in event ? Boolean(event.willRetry) : false;
          yield* completeTurn(context, willRetry ? "interrupted" : "completed");
        }
        return;
      }

      case "compaction_start": {
        yield* offerRuntimeEvent({
          ...base,
          type: "session.state.changed",
          payload: {
            state: "waiting",
            reason: `compaction:${"reason" in event ? String(event.reason) : "unknown"}`,
          },
        });
        return;
      }

      case "compaction_end": {
        yield* offerRuntimeEvent({
          ...base,
          type: "thread.state.changed",
          payload: { state: "compacted" },
        });
        return;
      }

      default:
        return;
    }
  });

  const handleExtensionUIRequest = Effect.fn("handleExtensionUIRequest")(function* (
    context: PiSessionContext,
    event: RpcExtensionUIRequest,
  ) {
    // Fire-and-forget side-effects: Pi does not wait for a response.
    if (
      event.method === "notify" ||
      event.method === "setStatus" ||
      event.method === "setWidget" ||
      event.method === "setTitle" ||
      event.method === "set_editor_text"
    ) {
      return;
    }

    // editor: surface as freeform text input (same protocol shape as input).
    // Falls through to the input branch below.

    // select / confirm / input: surface as user-input.requested
    const ourRequestId = ApprovalRequestId.make(yield* Random.nextUUIDv4);
    const questionId = String(ourRequestId);
    const deferred = yield* Deferred.make<ProviderUserInputAnswers, never>();

    let question: UserInputQuestion;
    let numberedOptions: ReadonlyArray<string> | undefined;

    if (event.method === "select") {
      question = {
        id: questionId,
        header: event.title.slice(0, 12),
        question: event.title,
        options: event.options
          .filter((label: string) => label !== "✏️ Type custom response...")
          .map((label: string) => ({ label, description: label })),
        multiSelect: false,
      };
    } else if (event.method === "confirm") {
      const body = event.message.length > 0 ? `${event.title}\n${event.message}` : event.title;
      question = {
        id: questionId,
        header: "Confirm",
        question: body,
        options: [
          { label: "Yes", description: "Confirm" },
          { label: "No", description: "Cancel" },
        ],
        multiSelect: false,
      };
    } else {
      // input / editor — both expect { value: "..." } back.
      // input: Pi uses this for multi-select (numbered list in title) and freeform.
      //   If the title contains a numbered list, parse it into real options with multiSelect.
      //   Otherwise surface with empty options; T3's auto-added "Other" field handles free text.
      // editor: multi-line freeform, no options — same freeform path.
      const title = "title" in event ? event.title : "";
      const parsed = event.method === "input" ? parseNumberedList(title) : null;
      if (parsed) {
        numberedOptions = parsed.items;
        question = {
          id: questionId,
          header: parsed.title.slice(0, 12),
          question: parsed.title,
          options: parsed.items.map((item) => ({ label: item, description: item })),
          multiSelect: true,
        };
      } else {
        question = {
          id: questionId,
          header: title.slice(0, 12),
          question: title,
          options: [],
          multiSelect: false,
        };
      }
    }

    context.pendingExtensionUIs.set(ourRequestId, {
      piId: event.id,
      questionId,
      deferred,
      method: event.method,
      ...(numberedOptions ? { numberedOptions } : {}),
    });

    const stamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "user-input.requested",
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
      requestId: RuntimeRequestId.make(ourRequestId),
      payload: { questions: [question] },
      providerRefs: {},
      raw: {
        source: "pi.sdk.event" as const,
        method: "extension_ui_request",
        payload: event,
      },
    });
  });

  const handleStdoutLine = Effect.fn("handleStdoutLine")(function* (
    context: PiSessionContext,
    line: string,
  ) {
    const trimmed = line.trim();
    if (!trimmed) return;

    const msg = tryParseJsonObject(trimmed);
    if (!msg) return;

    if (msg["type"] === "response") {
      if (typeof msg["id"] === "string") {
        const deferred = context.pendingRequests.get(msg["id"]);
        if (deferred) {
          context.pendingRequests.delete(msg["id"]);
          yield* Deferred.succeed(deferred, msg);
        }
      }
      return;
    }

    if (msg["type"] === "extension_ui_request") {
      yield* handleExtensionUIRequest(context, msg as RpcExtensionUIRequest);
      return;
    }

    yield* handlePiEvent(context, msg as AgentSessionEvent);
  });

  const stopSessionInternal = Effect.fn("stopSessionInternal")(function* (
    context: PiSessionContext,
    options?: { readonly emitExitEvent?: boolean },
  ) {
    if (context.stopped) return;
    context.stopped = true;

    if (context.turnState) {
      yield* completeTurn(context, "interrupted", "Session stopped.");
    }

    // Cancel any pending extension UI requests so Pi doesn't hang
    for (const [, pending] of context.pendingExtensionUIs) {
      yield* Effect.ignore(
        context.writeExtensionResponse({
          type: "extension_ui_response",
          id: pending.piId,
          cancelled: true,
        }),
      );
      yield* Effect.ignore(Deferred.interrupt(pending.deferred));
    }
    context.pendingExtensionUIs.clear();

    if (context.stdoutFiber) {
      yield* Fiber.interrupt(context.stdoutFiber);
      context.stdoutFiber = undefined;
    }

    yield* Effect.ignore(Scope.close(context.sessionScope, Exit.void));

    const updatedAt = yield* nowIso;
    context.session = {
      ...context.session,
      status: "closed",
      activeTurnId: undefined,
      updatedAt,
    };

    if (options?.emitExitEvent !== false) {
      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "session.exited",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId: context.session.threadId,
        payload: {
          reason: "Session stopped",
          exitKind: "graceful",
        },
        providerRefs: {},
      });
    }

    sessions.delete(context.session.threadId);
  });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<PiSessionContext, ProviderAdapterError> => {
    const context = sessions.get(threadId);
    if (!context) {
      return Effect.fail(
        new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        }),
      );
    }
    if (context.stopped || context.session.status === "closed") {
      return Effect.fail(
        new ProviderAdapterSessionClosedError({
          provider: PROVIDER,
          threadId,
        }),
      );
    }
    return Effect.succeed(context);
  };

  const startSession: PiAdapterShape["startSession"] = Effect.fn("startSession")(function* (input) {
    if (input.provider !== undefined && input.provider !== PROVIDER) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
      });
    }

    const existingContext = sessions.get(input.threadId);
    if (existingContext) {
      yield* stopSessionInternal(existingContext, { emitExitEvent: false }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("pi.session.replace.stop-failed", {
            threadId: input.threadId,
            cause,
          }),
        ),
      );
    }

    const startedAt = yield* nowIso;
    const threadId = input.threadId;
    const modelSelection =
      input.modelSelection !== undefined && input.modelSelection.instanceId === boundInstanceId
        ? input.modelSelection
        : undefined;

    const piResumeState = readPiResumeState(input.resumeCursor);
    const baseCwd = input.cwd ?? serverConfig.cwd;

    const spawnArgs: string[] = ["--mode", "rpc"];
    if (piResumeState) {
      spawnArgs.push("--session", piResumeState.sessionFile);
    }
    if (modelSelection?.model) {
      // If the model has a `contextWindow` option selected (e.g. "1m" or "300k"),
      // append it as `@<value>` so Pi receives the fully-qualified model slug
      // (e.g. `cursor/claude-opus-4-7@1m`). Plain models pass through unchanged.
      const contextWindow = getModelSelectionStringOptionValue(modelSelection, "contextWindow");
      const effectiveModel = contextWindow
        ? `${modelSelection.model}@${contextWindow}`
        : modelSelection.model;
      spawnArgs.push("--model", effectiveModel);
    }
    // User-configured extra args (e.g. `--thinking high`) are appended last so
    // they can override defaults set above when Pi's CLI does last-wins parsing.
    // Mirrors the ClaudeAdapter → `parseCliArgs(...).flags` pattern so behavior
    // stays consistent across drivers. `parseCliArgs` strips the leading `--`
    // and uses `null` to mean "boolean flag, no value".
    const extraSpawnArgs = parseCliArgs(piSettings.launchArgs).flags;
    for (const [flag, value] of Object.entries(extraSpawnArgs)) {
      spawnArgs.push(`--${flag}`);
      if (value !== null) {
        spawnArgs.push(value);
      }
    }

    const sessionScope = yield* Scope.make();

    const child = yield* spawner
      .spawn(
        ChildProcess.make(piSettings.binaryPath || "pi", spawnArgs, {
          env: piEnvironment,
          cwd: baseCwd,
          shell: false,
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, sessionScope),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId,
              detail: toMessage(cause, "Failed to start Pi Agent RPC process."),
              cause,
            }),
        ),
        Effect.onError(() => Effect.ignore(Scope.close(sessionScope, Exit.void))),
      );

    const outgoingQueue = yield* Queue.unbounded<Uint8Array>();

    const writeLine = (obj: RpcCommand | RpcExtensionUIResponse): Effect.Effect<void> =>
      Queue.offer(outgoingQueue, Buffer.from(JSON.stringify(obj) + "\n")).pipe(Effect.asVoid);

    const writeCommand = (cmd: RpcCommand): Effect.Effect<void> => writeLine(cmd);
    const writeExtensionResponse = (resp: RpcExtensionUIResponse): Effect.Effect<void> =>
      writeLine(resp);

    const pendingRequests = new Map<string, Deferred.Deferred<unknown, never>>();
    const pendingExtensionUIs = new Map<ApprovalRequestId, PendingExtensionUI>();

    const session: ProviderSession = {
      threadId,
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      status: "ready",
      runtimeMode: input.runtimeMode,
      ...(input.cwd ? { cwd: input.cwd } : {}),
      ...(modelSelection?.model ? { model: modelSelection.model } : {}),
      createdAt: startedAt,
      updatedAt: startedAt,
    };

    const context: PiSessionContext = {
      session,
      sessionScope,
      writeCommand,
      writeExtensionResponse,
      stdoutFiber: undefined,
      pendingRequests,
      pendingExtensionUIs,
      startedAt,
      turnState: undefined,
      turns: [],
      stopped: false,
    };
    sessions.set(threadId, context);

    const runtimeContext = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(runtimeContext);

    // Fork stdin writer — continuously drains the outgoing queue to the process stdin
    runFork(Stream.fromQueue(outgoingQueue).pipe(Stream.run(child.stdin), Effect.ignore));

    // Fork stdout reader — parses JSONL events and routes them
    const stdoutFiber = runFork(
      child.stdout.pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.mapEffect((line) => handleStdoutLine(context, line)),
        Stream.runDrain,
        Effect.ignore,
        // Emit session exit if the process dies while the session is still active
        Effect.ensuring(
          Effect.suspend(() => {
            if (!context.stopped && context.session.status !== "closed") {
              return stopSessionInternal(context, { emitExitEvent: true });
            }
            return Effect.void;
          }),
        ),
      ),
    );
    context.stdoutFiber = stdoutFiber;

    // Drain stderr to avoid blocking the process
    runFork(child.stderr.pipe(Stream.runDrain, Effect.ignore));

    // Fetch the session file path via get_state for resume cursor
    const stateReqId = yield* Random.nextUUIDv4;
    const stateDeferred = yield* Deferred.make<unknown, never>();
    pendingRequests.set(stateReqId, stateDeferred);
    yield* writeCommand({ type: "get_state", id: stateReqId });

    const stateResponse = yield* Deferred.await(stateDeferred).pipe(
      Effect.timeoutOption(5000),
      // Without this, a 5s timeout leaves the deferred parked in
      // `pendingRequests` until the session is torn down — one orphan per
      // missed startup probe. The reader fiber only deletes entries when a
      // matching response arrives, so we must clean up on the timeout path.
      Effect.ensuring(Effect.sync(() => pendingRequests.delete(stateReqId))),
    );

    const sessionFile: string | undefined = (() => {
      if (stateResponse._tag === "None") return undefined;
      const resp = stateResponse.value as Record<string, unknown>;
      if (resp["success"] !== true) return undefined;
      const data = resp["data"];
      if (!data || typeof data !== "object") return undefined;
      const sf = (data as Record<string, unknown>)["sessionFile"];
      return typeof sf === "string" && sf.trim().length > 0 ? sf.trim() : undefined;
    })();

    if (sessionFile !== undefined) {
      context.session = {
        ...context.session,
        resumeCursor: { sessionFile },
      };
    }

    const sessionStartedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.started",
      eventId: sessionStartedStamp.eventId,
      provider: PROVIDER,
      createdAt: sessionStartedStamp.createdAt,
      threadId,
      payload: {},
      providerRefs: {},
    });

    const configuredStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.configured",
      eventId: configuredStamp.eventId,
      provider: PROVIDER,
      createdAt: configuredStamp.createdAt,
      threadId,
      payload: {
        config: {
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
          ...(input.cwd ? { cwd: input.cwd } : {}),
        },
      },
      providerRefs: {},
    });

    const readyStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.state.changed",
      eventId: readyStamp.eventId,
      provider: PROVIDER,
      createdAt: readyStamp.createdAt,
      threadId,
      payload: { state: "ready" },
      providerRefs: {},
    });

    return { ...context.session };
  });

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const context = yield* requireSession(input.threadId);

    // Resolve attachments up-front so failures surface before we mutate any
    // session state (turnState, status, etc.). Pi's `prompt` RPC takes inline
    // `ImageContent` (mimeType + base64), so we read each file once here
    // rather than trying to stream them through Pi.
    const piImages = yield* Effect.forEach(
      input.attachments ?? [],
      (attachment) =>
        Effect.gen(function* () {
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!attachmentPath) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          }
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "prompt",
                  detail: `Failed to read attachment file: ${cause.message}.`,
                  cause,
                }),
            ),
          );
          return {
            type: "image" as const,
            mimeType: attachment.mimeType,
            data: Buffer.from(bytes).toString("base64"),
          };
        }),
      { concurrency: 1 },
    );

    if (context.turnState) {
      // The previous turn may still be executing on the Pi side. Ask Pi to
      // abort it before opening a new turn, and surface the old turn as
      // `interrupted` rather than `completed` so the UI doesn't claim a
      // settled result that never arrived.
      yield* Effect.ignore(context.writeCommand({ type: "abort" }));
      yield* completeTurn(context, "interrupted", "Superseded by a new turn.");
    }

    const turnId = TurnId.make(yield* Random.nextUUIDv4);
    const turnStartedAt = yield* nowIso;
    context.turnState = {
      turnId,
      startedAt: turnStartedAt,
      items: [],
      activeTextItemId: undefined,
      activeReasoningItemId: undefined,
    };
    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      updatedAt: turnStartedAt,
    };

    const turnStartedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.started",
      eventId: turnStartedStamp.eventId,
      provider: PROVIDER,
      createdAt: turnStartedStamp.createdAt,
      threadId: context.session.threadId,
      turnId,
      payload: {},
      providerRefs: {},
    });

    const rawPromptText = typeof input.input === "string" ? input.input : "";
    const trimmed = rawPromptText.trim();

    if (trimmed === "/compact" || trimmed.startsWith("/compact ")) {
      const customInstructions = trimmed.slice("/compact".length).trim() || undefined;
      yield* context
        .writeCommand({ type: "compact", ...(customInstructions ? { customInstructions } : {}) })
        .pipe(Effect.catchDefect(() => completeTurn(context, "failed", "Compaction failed.")));
      yield* completeTurn(context, "completed");
    } else {
      const rawSkillText = rawPromptText.replace(/^\$([a-zA-Z][\w:.-]*)/, "/skill:$1");
      const promptText =
        input.interactionMode === "plan"
          ? `<t3code_plan_mode>
You are in Plan Mode. Explore, research, and reason about the task, but do NOT write or modify any files or run side-effectful commands.

When you have gathered enough context to propose a complete plan, call the set_plan tool with the full plan as markdown. Include a clear title and numbered steps. Call set_plan exactly once per turn — only when the plan is ready for user review. Do not implement the plan yourself.
</t3code_plan_mode>

${rawSkillText}`
          : rawSkillText;
      yield* context
        .writeCommand({
          type: "prompt",
          message: promptText,
          ...(piImages.length > 0 ? { images: piImages } : {}),
        })
        .pipe(Effect.catchDefect(() => completeTurn(context, "failed", "Failed to send prompt.")));
    }

    return {
      threadId: context.session.threadId,
      turnId,
    };
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId, _turnId) {
      const context = yield* requireSession(threadId);
      yield* Effect.ignore(context.writeCommand({ type: "abort" }));
    },
  );

  const readThread: PiAdapterShape["readThread"] = Effect.fn("readThread")(function* (threadId) {
    const context = yield* requireSession(threadId);
    return {
      threadId,
      turns: context.turns.map((turn) => ({
        id: turn.id,
        items: [...turn.items],
      })),
    };
  });

  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const context = yield* requireSession(threadId);
      const nextLength = Math.max(0, context.turns.length - numTurns);
      context.turns.splice(nextLength);
      return {
        threadId,
        turns: context.turns.map((turn) => ({
          id: turn.id,
          items: [...turn.items],
        })),
      };
    },
  );

  const respondToRequest: PiAdapterShape["respondToRequest"] = (_threadId, _requestId, _decision) =>
    Effect.void;

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = Effect.fn("respondToUserInput")(
    function* (threadId, requestId, answers) {
      const context = yield* requireSession(threadId);
      const pending = context.pendingExtensionUIs.get(requestId);
      if (!pending) return;
      context.pendingExtensionUIs.delete(requestId);

      // Build the response Pi expects based on the original method
      let piResponse: RpcExtensionUIResponse;
      if (pending.method === "confirm") {
        const answer = answers[pending.questionId];
        piResponse = {
          type: "extension_ui_response",
          id: pending.piId,
          confirmed: answer === "Yes",
        };
      } else if (pending.method === "input" && pending.numberedOptions) {
        // Multi-select numbered list: map selected labels back to 1-based indices
        const raw = answers[pending.questionId];
        const selected: string[] = Array.isArray(raw)
          ? raw.map(String)
          : typeof raw === "string" && raw.length > 0
            ? [raw]
            : [];
        const indices = selected
          .map((label) => {
            const idx = pending.numberedOptions!.indexOf(label);
            return idx >= 0 ? String(idx + 1) : null;
          })
          .filter((i): i is string => i !== null);
        piResponse = { type: "extension_ui_response", id: pending.piId, value: indices.join(",") };
      } else {
        // select or freeform input: answer is a string value
        const answer = answers[pending.questionId];
        const value = typeof answer === "string" ? answer : "";
        piResponse = { type: "extension_ui_response", id: pending.piId, value };
      }

      yield* context.writeExtensionResponse(piResponse);
      yield* Deferred.succeed(pending.deferred, answers);

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "user-input.resolved",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId: context.session.threadId,
        requestId: RuntimeRequestId.make(requestId),
        payload: { answers },
        providerRefs: {},
      });
    },
  );

  const stopSession: PiAdapterShape["stopSession"] = Effect.fn("stopSession")(function* (threadId) {
    const context = yield* requireSession(threadId);
    yield* stopSessionInternal(context, { emitExitEvent: true });
  });

  const listSessions: PiAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session })));

  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });

  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(
      sessions,
      ([, context]) => stopSessionInternal(context, { emitExitEvent: true }),
      { discard: true },
    );

  yield* Effect.addFinalizer(() =>
    Effect.forEach(
      sessions,
      ([, context]) => stopSessionInternal(context, { emitExitEvent: false }),
      { discard: true },
    ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue))),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported" as const,
    },
    startSession,
    sendTurn,
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  } satisfies PiAdapterShape;
});
