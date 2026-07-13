import { useEffect, useMemo, useState } from "react";
import { useStream } from "@langchain/langgraph-sdk/react";
import type {
  Message,
  ToolCallWithResult,
  UseDeepAgentStream,
} from "@langchain/langgraph-sdk";
import { logger } from "./logger";
import type { ProtocolEvent } from "./types";

export const DEFAULT_API_URL = "http://localhost:2024";
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
): DeepResearchStream {
  const [debugEvents, setDebugEvents] = useState<DebugEvent[]>([]);
  logger.debug("stream.hook.render", { apiUrl, threadId });

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
    assistantId: ASSISTANT_ID,
    threadId,
    onThreadId,
    messagesKey: "messages",
    reconnectOnMount: false,
    fetchStateHistory: { limit: 20 },
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
      logger.debug("stream.normalized", {
        messages: normalized.messages.length,
        toolCalls: normalized.toolCalls.length,
        subagents: normalized.subagents.size,
        activeSubagents: normalized.activeSubagents.length,
        interrupts: normalized.interrupts.length,
        debugEvents: normalized.debugEvents.length,
        isLoading: normalized.isLoading,
      });
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
