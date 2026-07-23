import { useEffect, useMemo, useState } from "react";
import { useStream } from "@langchain/langgraph-sdk/react";
import type {
  Message,
  ToolCallWithResult,
  UseDeepAgentStream,
} from "@langchain/langgraph-sdk";
import { logger } from "./logger";
import type { ProtocolEvent } from "./types";

// Resolve the backend base URL at runtime so the same static bundle works in
// any environment. In Kubernetes the UI container writes `window.__API_URL__`
// (see ui/docker-entrypoint.sh, fed from the `API_URL` env var / Helm values);
// when unset we fall back to the local dev server.
declare global {
  interface Window {
    __API_URL__?: string;
  }
}

/**
 * Resolve the configured API base to an ABSOLUTE URL.
 *
 * The LangGraph SDK builds requests with `new URL(`${apiUrl}${path}`)` (and
 * `new URL(apiUrl)` for WebSockets), which throws on a relative value like
 * "/api" because there's no base. Plain `fetch("/api/...")` tolerates a relative
 * URL, so the non-SDK calls (threads/assistants) work while every SDK-driven
 * action (run start, streaming) fails — the classic "UI loads but isn't
 * connected" symptom behind a same-origin path proxy. Resolving "/api" against
 * the current origin keeps it same-origin (nginx still proxies it) while giving
 * the SDK a valid absolute URL.
 */
export function resolveApiUrl(raw: string | undefined | null, origin?: string): string {
  const value = (raw ?? "").trim();
  if (!value) {
    return "http://localhost:2024";
  }
  if (/^https?:\/\//i.test(value)) {
    return value.replace(/\/$/, "");
  }
  const base =
    origin ?? (typeof window !== "undefined" ? window.location?.origin : undefined);
  if (base) {
    const path = value.startsWith("/") ? value : `/${value}`;
    return `${base}${path}`.replace(/\/$/, "");
  }
  return value.replace(/\/$/, "");
}

export const DEFAULT_API_URL = resolveApiUrl(
  (typeof window !== "undefined" && window.__API_URL__) || "http://localhost:2024",
);
export const ASSISTANT_ID = "deep-agent";

export type DeepResearchState = {
  messages: Message[];
  [key: string]: unknown;
};

export type DebugEvent = {
  id: string;
  channel: string;
  namespace: string[];
  timestamp: number;
  data: unknown;
};

export type DeepResearchStream = UseDeepAgentStream<DeepResearchState> & {
  debugEvents: DebugEvent[];
  clearDebugEvents: () => void;
};

type EventOptions = {
  namespace?: string[];
  mutate?: (update: unknown) => void;
};

type RunCallbackMeta = {
  run_id: string;
  thread_id: string;
};

function eventId(channel: string, data: unknown): string {
  const runId =
    typeof data === "object" && data !== null && "run_id" in data
      ? String((data as { run_id?: unknown }).run_id)
      : "event";
  return `${channel}-${runId}-${crypto.randomUUID()}`;
}

function normalizeNamespace(options: EventOptions): string[] {
  return options.namespace ?? [];
}

function appendDebugEvent(
  previous: DebugEvent[],
  event: Omit<DebugEvent, "timestamp">,
): DebugEvent[] {
  return [...previous, { ...event, timestamp: Date.now() }].slice(-250);
}

function debugEventFromProtocol(event: ProtocolEvent): Omit<DebugEvent, "timestamp"> {
  return {
    id: event.event_id,
    channel: event.method,
    namespace: event.params.namespace,
    data: event.params.data,
  };
}

function parseSseFrames(buffer: string): { frames: string[]; rest: string } {
  const normalized = buffer.replace(/\r\n/g, "\n");
  const parts = normalized.split("\n\n");
  return { frames: parts.slice(0, -1), rest: parts.at(-1) ?? "" };
}

function protocolEventFromSse(frame: string): ProtocolEvent | null {
  const eventLine = frame
    .split("\n")
    .find((line) => line.startsWith("data:"));
  if (!eventLine) {
    return null;
  }
  try {
    const parsed = JSON.parse(eventLine.slice("data:".length).trim()) as unknown;
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "type" in parsed &&
      (parsed as { type?: unknown }).type === "event"
    ) {
      return parsed as ProtocolEvent;
    }
  } catch (caught) {
    logger.warn("stream.protocolEvent.parseFailed", {
      message: caught instanceof Error ? caught.message : String(caught),
    });
  }
  return null;
}

export function useDeepResearchStream(
  apiUrl: string,
  threadId: string | null,
  onThreadId: (threadId: string) => void,
  onRunCreated?: (run: RunCallbackMeta) => void,
  assistantId: string = ASSISTANT_ID,
): DeepResearchStream {
  const [debugEvents, setDebugEvents] = useState<DebugEvent[]>([]);
  // logger.debug("stream.hook.render", { apiUrl, threadId });

  useEffect(() => {
    if (!threadId) {
      return undefined;
    }
    const controller = new AbortController();
    const subscribe = async (): Promise<void> => {
      try {
        logger.info("stream.protocol.subscribe", { threadId });
        const response = await fetch(`${apiUrl}/threads/${threadId}/stream/events`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            channels: ["tools", "messages", "lifecycle"],
            depth: 99,
          }),
          signal: controller.signal,
        });
        if (!response.ok || !response.body) {
          throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        }
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (!controller.signal.aborted) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          buffer += decoder.decode(value, { stream: true });
          const { frames, rest } = parseSseFrames(buffer);
          buffer = rest;
          for (const frame of frames) {
            const event = protocolEventFromSse(frame);
            if (!event) {
              continue;
            }
            setDebugEvents((events) => appendDebugEvent(events, debugEventFromProtocol(event)));
          }
        }
      } catch (caught) {
        if (controller.signal.aborted || (caught instanceof DOMException && caught.name === "AbortError")) {
          logger.debug("stream.protocol.aborted", { threadId });
          return;
        }
        logger.warn("stream.protocol.failed", {
          threadId,
          message: caught instanceof Error ? caught.message : String(caught),
        });
      }
    };
    void subscribe();
    return () => {
      controller.abort();
      logger.info("stream.protocol.unsubscribe", { threadId });
    };
  }, [apiUrl, threadId]);

  const streamOptions = {
    apiUrl,
    assistantId,
    threadId,
    onThreadId,
    messagesKey: "messages",
    reconnectOnMount: false,
    // The persisted transcript comes from run snapshots (GET .../runs/{id}/checkpoints),
    // NOT from thread history: `stream.messages` is only rendered for the live run
    // and `stream.values` is never consumed. Fetching 20 states POSTed
    // /threads/{id}/history on every load, dragging the whole (tens-of-MB) thread
    // history off disk (~5.5 s on a large thread) for data we don't render.
    // Keep only the CURRENT checkpoint (limit 1) so the SDK can still continue a
    // run / surface a pending interrupt on reload. If this agent never does
    // human-input interrupts, this can be `false` to drop the /history call
    // entirely. (We don't use `stream.history` or branch trees.)
    fetchStateHistory: { limit: 1 },
    subagentToolNames: ["task"],
    filterSubagentMessages: true,
    onCreated: (run: RunCallbackMeta) => {
      logger.info("stream.run.created", {
        runId: run.run_id,
        threadId: run.thread_id,
      });
      onRunCreated?.(run);
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: `metadata-${run.run_id}`,
          channel: "metadata",
          namespace: [],
          data: run,
        }),
      );
    },
    onUpdateEvent: (data: unknown, options: EventOptions) => {
      const namespace = normalizeNamespace(options);
      if (
        namespace.length === 0 &&
        typeof data === "object" &&
        data !== null &&
        "todos" in data &&
        Array.isArray((data as { todos?: unknown }).todos)
      ) {
        options.mutate?.({ todos: (data as { todos: unknown }).todos });
      }
      logger.debug("stream.event.updates", {
        namespace,
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("updates", data),
          channel: "updates",
          namespace,
          data,
        }),
      );
    },
    onToolEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.tools", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("tools", data),
          channel: "tools",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
    onTaskEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.tasks", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("tasks", data),
          channel: "tasks",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
    onMessageEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.messages", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("messages", data),
          channel: "messages",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
    onCheckpointEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.checkpoints", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("checkpoints", data),
          channel: "checkpoints",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
    onDebugEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.debug", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("debug", data),
          channel: "debug",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
    onCustomEvent: (data: unknown, options: EventOptions) => {
      logger.debug("stream.event.custom", {
        namespace: normalizeNamespace(options),
        shape: typeof data,
      });
      setDebugEvents((events) =>
        appendDebugEvent(events, {
          id: eventId("custom", data),
          channel: "custom",
          namespace: normalizeNamespace(options),
          data,
        }),
      );
    },
  };

  const stream = useStream(streamOptions as never) as unknown as UseDeepAgentStream<DeepResearchState>;

  return useMemo(
    () => {
      const normalized = {
        ...stream,
        messages: stream.messages ?? [],
        toolCalls: stream.toolCalls ?? [],
        subagents: stream.subagents ?? new Map(),
        activeSubagents: stream.activeSubagents ?? [],
        interrupts: stream.interrupts ?? [],
        debugEvents,
        clearDebugEvents: () => {
          logger.info("stream.debug.clear");
          setDebugEvents([]);
        },
      } as DeepResearchStream;
      // logger.debug("stream.normalized", {
      //   messages: normalized.messages.length,
      //   toolCalls: normalized.toolCalls.length,
      //   subagents: normalized.subagents.size,
      //   activeSubagents: normalized.activeSubagents.length,
      //   interrupts: normalized.interrupts.length,
      //   debugEvents: normalized.debugEvents.length,
      //   isLoading: normalized.isLoading,
      // });
      return normalized;
    },
    [debugEvents, stream],
  );
}

export function messageText(message: Message): string {
  if (typeof message.content === "string") {
    return message.content;
  }
  if (!Array.isArray(message.content)) {
    return "";
  }
  return message.content
    .map((block) => {
      if (typeof block === "string") {
        return block;
      }
      if (
        typeof block === "object" &&
        block !== null &&
        "type" in block &&
        block.type === "text" &&
        "text" in block
      ) {
        return String(block.text ?? "");
      }
      return "";
    })
    .join("");
}

export function toolCallName(toolCall: ToolCallWithResult): string {
  return typeof toolCall.call === "object" && toolCall.call !== null && "name" in toolCall.call
    ? String(toolCall.call.name)
    : "tool";
}

export function toolCallArgs(toolCall: ToolCallWithResult): Record<string, unknown> {
  if (typeof toolCall.call !== "object" || toolCall.call === null || !("args" in toolCall.call)) {
    return {};
  }
  const args = toolCall.call.args;
  if (typeof args === "string") {
    try {
      const parsed = JSON.parse(args) as unknown;
      return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
        ? (parsed as Record<string, unknown>)
        : {};
    } catch {
      return { input: args };
    }
  }
  return typeof args === "object" && args !== null && !Array.isArray(args)
    ? (args as Record<string, unknown>)
    : {};
}
