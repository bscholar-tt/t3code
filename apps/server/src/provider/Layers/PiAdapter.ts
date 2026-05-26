import {
  type AgentSessionEvent,
  type RpcCommand,
} from "@earendil-works/pi-coding-agent";
import {
  type CanonicalItemType,
  EventId,
  type PiSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { makePiEnvironment } from "../Drivers/PiHome.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";

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
}

interface PiSessionContext {
  session: ProviderSession;
  readonly sessionScope: Scope.Closeable;
  writeCommand: (cmd: RpcCommand) => Effect.Effect<void>;
  stdoutFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingRequests: Map<string, Deferred.Deferred<unknown, never>>;
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
          context.turnState = { turnId, startedAt, items: [] };
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
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            type: "content.delta",
            payload: {
              streamKind: "assistant_text",
              delta: assistantEvent.delta,
            },
          });
        } else if (assistantEvent.type === "thinking_delta") {
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
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
      spawnArgs.push("--model", modelSelection.model);
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

    const writeCommand = (cmd: RpcCommand): Effect.Effect<void> =>
      Queue.offer(outgoingQueue, Buffer.from(JSON.stringify(cmd) + "\n")).pipe(Effect.asVoid);

    const pendingRequests = new Map<string, Deferred.Deferred<unknown, never>>();

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
      stdoutFiber: undefined,
      pendingRequests,
      startedAt,
      turnState: undefined,
      turns: [],
      stopped: false,
    };
    sessions.set(threadId, context);

    const runtimeContext = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(runtimeContext);

    // Fork stdin writer — continuously drains the outgoing queue to the process stdin
    runFork(
      Stream.fromQueue(outgoingQueue).pipe(
        Stream.run(child.stdin),
        Effect.ignore,
      ),
    );

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
    runFork(
      child.stderr.pipe(
        Stream.runDrain,
        Effect.ignore,
      ),
    );

    // Fetch the session file path via get_state for resume cursor
    const stateReqId = yield* Random.nextUUIDv4;
    const stateDeferred = yield* Deferred.make<unknown, never>();
    pendingRequests.set(stateReqId, stateDeferred);
    yield* writeCommand({ type: "get_state", id: stateReqId });

    const stateResponse = yield* Deferred.await(stateDeferred).pipe(
      Effect.timeoutOption(5000),
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

    if (context.turnState) {
      yield* completeTurn(context, "completed");
    }

    const turnId = TurnId.make(yield* Random.nextUUIDv4);
    const turnStartedAt = yield* nowIso;
    context.turnState = { turnId, startedAt: turnStartedAt, items: [] };
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

    const promptText = typeof input.input === "string" ? input.input : "";

    yield* context.writeCommand({ type: "prompt", message: promptText }).pipe(
      Effect.catchDefect(() => completeTurn(context, "failed", "Failed to send prompt.")),
    );

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

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = (
    _threadId,
    _requestId,
    _answers,
  ) => Effect.void;

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
