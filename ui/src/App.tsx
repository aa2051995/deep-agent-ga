import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import {
  Check,
  ChevronRight,
  CircleStop,
  FlaskConical,
  KeyRound,
  Loader2,
  MessageSquare,
  MoreHorizontal,
  PanelRight,
  Plus,
  Send,
  ShieldQuestion,
} from "lucide-react";
import type { Message } from "@langchain/langgraph-sdk";
import type { SubagentStreamInterface } from "@langchain/langgraph-sdk/react";
import { logger } from "./logger";
import { deleteThread, getRunDebugSnapshot, listRuns, listThreads, renameThread } from "./api";
import { DEFAULT_API_URL, messageText, toolCallArgs, toolCallName, useDeepResearchStream } from "./stream";
import type { DebugEvent, DeepResearchStream } from "./stream";
import { selectInputRequests, selectTodos, selectTodosFromValues, subagentStreamToCard } from "./selectors";
import type { InputRequest, ProtocolEvent, RunDebugSnapshot, RunSummary, SubagentCard, ThreadSummary, TodoItem, ToolDebugRow } from "./types";

const CURRENT_THREAD_KEY = "deep-research-ui:current-thread";
const THREAD_QUERY_PARAM = "thread_id";
const STREAM_MODES = [
  "messages-tuple",
  "values",
  "updates",
  "tools",
  "tasks",
  "checkpoints",
  "debug",
  "custom",
] as const;
const DEBUG_SECTION_IDS = ["todos", "subagents", "tools", "timeline"] as const;

type ActiveRun = {
  threadId: string;
  runId: string;
};
type DebugSectionId = (typeof DEBUG_SECTION_IDS)[number];
type RunActivity = {
  reasoning: string;
  action: string;
};
type MessageExchange = {
  userIndex: number;
  messageIndices: number[];
  userText: string;
  aiText: string;
};
const ACTIVE_RUN_STATUSES = new Set(["pending", "running"]);
const TERMINAL_RUN_EVENTS = new Set(["completed", "failed", "interrupted", "timeout"]);

function threadIdFromUrl(): string | null {
  const params = new URLSearchParams(window.location.search);
  return params.get(THREAD_QUERY_PARAM) || params.get("thread");
}

function initialThreadId(): string | null {
  return threadIdFromUrl() ?? localStorage.getItem(CURRENT_THREAD_KEY);
}

function writeThreadUrl(threadId: string | null, replace = false): void {
  const url = new URL(window.location.href);
  if (threadId) {
    url.searchParams.set(THREAD_QUERY_PARAM, threadId);
  } else {
    url.searchParams.delete(THREAD_QUERY_PARAM);
  }
  url.searchParams.delete("thread");
  const method = replace ? "replaceState" : "pushState";
  window.history[method]({}, "", url);
}

function upsertThread(threads: ThreadSummary[], threadId: string, title: string): ThreadSummary[] {
  logger.debug("threads.upsert.start", { threadId, titleLength: title.length });
  const updatedAt = new Date().toISOString();
  const existing = threads.find((thread) => thread.threadId === threadId);
  if (existing) {
    return threads.map((thread) =>
      thread.threadId === threadId
        ? { ...thread, title: thread.title === "New research" ? title : thread.title }
        : thread,
    );
  }
  return [{ threadId, title, updatedAt }, ...threads].slice(0, 24);
}

function statusText(isLoading: boolean, subagentCount: number): string {
  if (isLoading) {
    return subagentCount > 0 ? `${subagentCount} subagents active` : "Researching";
  }
  return "Ready";
}

function titleCase(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function objectValue(value: unknown, keys: string[]): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  for (const key of keys) {
    if (record[key] != null) {
      return record[key];
    }
  }
  return undefined;
}

function stringValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value == null) {
    return "";
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function todoProgress(todos: TodoItem[]): { completed: number; percentage: number } {
  const completed = todos.filter((todo) => todo.status === "completed").length;
  return {
    completed,
    percentage: todos.length ? Math.round((completed / todos.length) * 100) : 0,
  };
}

function collectStrings(value: unknown): string[] {
  if (typeof value === "string") {
    return [value];
  }
  if (Array.isArray(value)) {
    return value.flatMap(collectStrings);
  }
  if (typeof value === "object" && value !== null) {
    return Object.values(value as Record<string, unknown>).flatMap(collectStrings);
  }
  return [];
}

function isInternalTodoUpdate(message: Message, text: string): boolean {
  return message.type === "ai" && /^updated todo list(?:\s+to)?\s+/i.test(text.trim());
}

function domainFromText(value: string): string | null {
  const match = value.match(/https?:\/\/[^\s,)"']+/i);
  if (!match) {
    return null;
  }
  try {
    return new URL(match[0]).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

function compactDetail(value: string, maxLength = 54): string {
  const compacted = value.replace(/\s+/g, " ").trim();
  return compacted.length > maxLength ? `${compacted.slice(0, maxLength - 3)}...` : compacted;
}

function messageKey(message: Message, index: number): string {
  return message.id ?? `${message.type}-${index}`;
}

function messageSnippet(value: string, maxLength = 180): string {
  const compacted = value.replace(/\s+/g, " ").trim();
  return compacted.length > maxLength ? `${compacted.slice(0, maxLength - 3)}...` : compacted;
}

function latestUserIndex(messages: Message[]): number {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (messages[index].type === "human") {
      return index;
    }
  }
  return -1;
}

function exchangeForMessage(messages: Message[], index: number): MessageExchange | null {
  if (messages.length === 0) {
    return null;
  }
  const boundedIndex = Math.max(0, Math.min(index, messages.length - 1));
  let userIndex = -1;
  for (let cursor = boundedIndex; cursor >= 0; cursor -= 1) {
    if (messages[cursor].type === "human") {
      userIndex = cursor;
      break;
    }
  }
  if (userIndex === -1) {
    return null;
  }

  let endIndex = messages.length;
  for (let cursor = userIndex + 1; cursor < messages.length; cursor += 1) {
    if (messages[cursor].type === "human") {
      endIndex = cursor;
      break;
    }
  }

  const messageIndices = Array.from({ length: endIndex - userIndex }, (_, offset) => userIndex + offset);
  const aiText = messages
    .slice(userIndex + 1, endIndex)
    .filter((message) => message.type === "ai")
    .map(messageText)
    .filter(Boolean)
    .join("\n\n");

  return {
    userIndex,
    messageIndices,
    userText: messageText(messages[userIndex]),
    aiText,
  };
}

function actionFromTool(name: string, input: unknown): string {
  const normalized = name.toLowerCase();
  if (normalized.includes("search") || normalized.includes("tavily") || normalized.includes("web")) {
    const explicitDomain = stringValue(objectValue(input, ["domain", "site", "source"]));
    const discoveredDomain = collectStrings(input).map(domainFromText).find((domain): domain is string => Boolean(domain));
    const query = stringValue(objectValue(input, ["query", "q", "input"]));
    const target = explicitDomain || discoveredDomain || compactDetail(query || "the web");
    return `Searching ${target}`;
  }
  if (normalized.includes("plan") || normalized.includes("todo") || normalized.includes("think")) {
    return "Creating a plan";
  }
  if (normalized.includes("read") || normalized.includes("file") || normalized.includes("open")) {
    const path = stringValue(objectValue(input, ["path", "file", "filename", "file_path", "url"]));
    return path ? `Reading file ${compactDetail(path)}` : "Reading file";
  }
  if (normalized === "task") {
    return "Delegating to subagent";
  }
  return titleCase(name || "Working");
}

function toolStartData(event: DeepResearchStream["debugEvents"][number]): { name: string; input: unknown } | null {
  if (event.channel !== "tools" || typeof event.data !== "object" || event.data === null) {
    return null;
  }
  const data = event.data as Record<string, unknown>;
  const kind = data.event;
  if (kind !== "on_tool_start" && kind !== "tool-started") {
    return null;
  }
  const name = stringValue(data.name ?? data.tool_name);
  return { name, input: data.input };
}

function selectRunActivity(stream: DeepResearchStream): RunActivity | null {
  if (!stream.isLoading) {
    return null;
  }
  const latestToolEvent = [...stream.debugEvents].reverse().map(toolStartData).find((event): event is { name: string; input: unknown } => event !== null);
  const latestToolCall = stream.toolCalls.at(-1);
  const toolName = latestToolEvent?.name || (latestToolCall ? toolCallName(latestToolCall) : "");
  const input = latestToolEvent?.input ?? (latestToolCall ? toolCallArgs(latestToolCall) : undefined);
  return {
    reasoning: "Thinking through the request",
    action: toolName ? actionFromTool(toolName, input) : "Preparing action",
  };
}

function protocolEventData(event: ProtocolEvent): Record<string, unknown> {
  return typeof event.params.data === "object" && event.params.data !== null && !Array.isArray(event.params.data)
    ? (event.params.data as Record<string, unknown>)
    : {};
}

function debugEventsFromProtocol(events: ProtocolEvent[]): DebugEvent[] {
  return events.map((event) => ({
    id: event.event_id,
    channel: event.method,
    namespace: event.params.namespace,
    timestamp: event.params.timestamp,
    data: event.params.data,
  }));
}

function protocolEventsFromDebugEvents(events: DebugEvent[]): ProtocolEvent[] {
  return events.map((event, index) => ({
    type: "event",
    event_id: event.id,
    seq: index + 1,
    method: event.channel,
    params: {
      namespace: event.namespace,
      timestamp: event.timestamp,
      data: event.data,
    },
  }));
}

function subagentKeyFromNamespace(namespace: string[]): string | null {
  return namespace.find((part) => part.startsWith("tools:")) ?? null;
}

function toolRowsFromStream(toolCalls: DeepResearchStream["toolCalls"]): ToolDebugRow[] {
  return toolCalls.map((toolCall) => ({
    id: toolCall.id,
    name: toolCallName(toolCall),
    state: toolCall.state,
  }));
}

function toolRowsFromEvents(events: ProtocolEvent[]): ToolDebugRow[] {
  const rows = new Map<string, ToolDebugRow>();
  for (const event of events) {
    if (event.method !== "tools") {
      continue;
    }
    const data = protocolEventData(event);
    const id = String(data.tool_call_id ?? data.toolCallId ?? data.id ?? `${event.event_id}-${event.seq}`);
    const existing = rows.get(id);
    const name = String(data.tool_name ?? data.name ?? existing?.name ?? "tool");
    const kind = String(data.event ?? "");
    rows.set(id, {
      id,
      name,
      state: kind.includes("finish") || kind.includes("end") ? "completed" : "pending",
    });
  }
  return [...rows.values()];
}

function subagentCardsFromEvents(events: ProtocolEvent[]): SubagentCard[] {
  const cards = new Map<string, SubagentCard>();
  const activeMessageIds = new Map<string, string>();

  function ensureCard(key: string, event: ProtocolEvent): SubagentCard {
    const existing = cards.get(key);
    if (existing) {
      return existing;
    }
    const card: SubagentCard = {
      key,
      name: key.replace(/^tools:/, "subagent "),
      namespace: event.params.namespace,
      status: "running",
      description: "Subagent activity",
      progress: 35,
      messages: [],
      tools: [],
    };
    cards.set(key, card);
    return card;
  }

  for (const event of events) {
    const data = protocolEventData(event);
    const eventToolId = String(data.tool_call_id ?? data.toolCallId ?? data.id ?? `${event.event_id}-${event.seq}`);
    const eventToolName = String(data.tool_name ?? data.name ?? "tool");
    const key = subagentKeyFromNamespace(event.params.namespace) ?? (
      event.method === "tools" && eventToolName === "task" ? `tools:${eventToolId}` : null
    );
    if (!key) {
      continue;
    }
    const card = ensureCard(key, event);

    if (event.method === "lifecycle" && data.event === "completed") {
      card.status = "done";
      card.progress = 100;
    }

    if (event.method === "tools") {
      const toolId = eventToolId;
      const toolName = eventToolName;
      if (toolName === "task") {
        const input = typeof data.input === "object" && data.input !== null ? (data.input as Record<string, unknown>) : {};
        card.description = String(input.description ?? card.description);
        card.name = String(input.subagent_type ?? card.name);
      } else {
        const existingTool = card.tools.find((tool) => tool.id === toolId);
        const tool = existingTool ?? {
          id: toolId,
          name: toolName,
          namespace: event.params.namespace,
          componentKey: key,
          input: data.input,
          output: undefined,
          status: "running" as const,
        };
        tool.output = data.output ?? tool.output;
        tool.status = String(data.event ?? "").includes("finish") ? "done" : tool.status;
        if (!existingTool) {
          card.tools.push(tool);
        }
      }
    }

    if (event.method === "messages") {
      const messageEvent = String(data.event ?? "");
      if (messageEvent === "message-start") {
        const messageId = String(data.id ?? `${key}-${event.seq}`);
        activeMessageIds.set(key, messageId);
        card.messages.push({
          id: messageId,
          role: "ai",
          content: "",
          componentKey: key,
          namespace: event.params.namespace,
          status: "streaming",
        });
      }
      if (messageEvent === "content-block-delta" || messageEvent === "content-block-finish") {
        const content = typeof data.content === "object" && data.content !== null ? data.content as Record<string, unknown> : {};
        const text = typeof content.text === "string" ? content.text : "";
        const messageId = activeMessageIds.get(key) ?? `${key}-${event.seq}`;
        let message = card.messages.find((item) => item.id === messageId);
        if (!message) {
          message = {
            id: messageId,
            role: "ai",
            content: "",
            componentKey: key,
            namespace: event.params.namespace,
            status: "streaming",
          };
          card.messages.push(message);
        }
        message.content = messageEvent === "content-block-finish" ? text || message.content : message.content + text;
      }
      if (messageEvent === "message-finish") {
        const messageId = activeMessageIds.get(key);
        const message = messageId ? card.messages.find((item) => item.id === messageId) : card.messages.at(-1);
        if (message) {
          message.status = "done";
        }
        activeMessageIds.delete(key);
      }
    }
  }

  return [...cards.values()].map((card) => ({
    ...card,
    status: card.status === "running" && card.messages.every((message) => message.status === "done") ? "done" : card.status,
    progress: card.status === "done" ? 100 : Math.min(90, card.progress + card.messages.length * 10 + card.tools.length * 10),
  }));
}

function subagentIdsFromDebugEvents(events: DebugEvent[], runId: string | null): Set<string> {
  const ids = new Set<string>();
  if (!runId) {
    return ids;
  }
  for (const event of events) {
    if (event.channel !== "tools") {
      continue;
    }
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    if (data.run_id != null && data.run_id !== runId) {
      continue;
    }
    const name = String(data.name ?? data.tool_name ?? "");
    const kind = String(data.event ?? "");
    if (name !== "task" || (kind && !kind.includes("start"))) {
      continue;
    }
    for (const value of [data.toolCallId, data.tool_call_id, data.id]) {
      if (typeof value === "string" && value) {
        ids.add(value);
      }
    }
    const namespaceId = event.namespace.find((part) => part.startsWith("tools:"))?.slice("tools:".length);
    if (namespaceId) {
      ids.add(namespaceId);
    }
  }
  return ids;
}

function subagentCardsForLiveRun(
  events: DebugEvent[],
  runId: string | null,
  subagents: DeepResearchStream["subagents"],
): SubagentCard[] {
  const runEvents = events.filter((event) => {
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    return runId === null || data.run_id == null || data.run_id === runId;
  });
  const eventCards = subagentCardsFromEvents(protocolEventsFromDebugEvents(runEvents));
  const cards = new Map(eventCards.map((card) => [card.key.replace(/^tools:/, ""), card]));

  for (const [id, subagent] of subagents) {
    const key = id.replace(/^tools:/, "");
    const existing = cards.get(key);
    if (!existing) {
      continue;
    }
    cards.set(key, {
      ...existing,
      ...subagentStreamToCard(subagent),
      key: existing.key,
      name: existing.name,
      description: existing.description,
    });
  }
  return [...cards.values()];
}

function todosFromDebugEvents(events: DebugEvent[], runId: string | null): TodoItem[] {
  let latest: TodoItem[] = [];
  for (const event of events) {
    if (event.channel !== "updates" || event.namespace.length > 0) {
      continue;
    }
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    if (runId !== null && data.run_id != null && data.run_id !== runId) {
      continue;
    }
    const eventTodos = selectTodosFromValues(data);
    if (eventTodos !== null) {
      latest = eventTodos;
    }
  }
  return latest;
}

async function fetchRunStatus(apiUrl: string, run: ActiveRun): Promise<string | null> {
  const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}`);
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }
  const data = (await response.json()) as { status?: unknown };
  return typeof data.status === "string" ? data.status : null;
}

export function App() {
  const [apiUrl, setApiUrl] = useState(DEFAULT_API_URL);
  const [threadId, setThreadId] = useState<string | null>(initialThreadId);
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [threadsLoading, setThreadsLoading] = useState(false);
  const [draft, setDraft] = useState("");
  const [debugOpen, setDebugOpen] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeRun, setActiveRun] = useState<ActiveRun | null>(null);
  const [visibleMessages, setVisibleMessages] = useState<Message[]>([]);
  const [optimisticMessages, setOptimisticMessages] = useState<Message[]>([]);
  const [openThreadMenu, setOpenThreadMenu] = useState<string | null>(null);
  const [focusedMessageIndex, setFocusedMessageIndex] = useState(-1);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [currentRunId, setCurrentRunId] = useState<string | null>(null);
  const [runDebugSnapshots, setRunDebugSnapshots] = useState<Record<string, RunDebugSnapshot>>({});
  const [debugSnapshotLoading, setDebugSnapshotLoading] = useState(false);
  const [debugSnapshotError, setDebugSnapshotError] = useState<string | null>(null);
  const [runTodoCache, setRunTodoCache] = useState<Record<string, TodoItem[]>>({});
  const joinedRunIds = useRef(new Set<string>());
  const activeRunRef = useRef<ActiveRun | null>(null);
  const isLoadingRef = useRef(false);
  const pendingThreadTitleRef = useRef("New research");
  const loggedMessageTextRef = useRef(new Map<string, string>());
  const messagesViewportRef = useRef<HTMLDivElement | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const messageNodesRef = useRef(new Map<number, HTMLElement>());
  const switchThreadRef = useRef<DeepResearchStream["switchThread"] | null>(null);
  logger.debug("app.render", { threadId, apiUrl, debugOpen });

  const stream = useDeepResearchStream(
    apiUrl,
    threadId,
    (nextThreadId) => {
      logger.info("stream.thread.received", { threadId: nextThreadId });
      setThreadId(nextThreadId);
      localStorage.setItem(CURRENT_THREAD_KEY, nextThreadId);
      writeThreadUrl(nextThreadId, true);
      setThreads((current) => {
        return upsertThread(current, nextThreadId, pendingThreadTitleRef.current);
      });
    },
    (run) => {
      const now = new Date().toISOString();
      logger.info("stream.run.current", { threadId: run.thread_id, runId: run.run_id });
      setCurrentRunId(run.run_id);
      setRuns((current) => [
        {
          runId: run.run_id,
          threadId: run.thread_id,
          status: "running",
          createdAt: now,
          updatedAt: now,
        },
        ...current.filter((item) => item.runId !== run.run_id),
      ]);
    },
  );
  switchThreadRef.current = stream.switchThread;

  const todos = useMemo(() => selectTodos(stream), [stream]);
  const liveRunSubagentCards = useMemo(
    () => subagentCardsForLiveRun(stream.debugEvents, currentRunId, stream.subagents),
    [currentRunId, stream],
  );
  const liveEventTodos = useMemo(
    () => todosFromDebugEvents(stream.debugEvents, currentRunId),
    [currentRunId, stream.debugEvents],
  );
  const inputRequests = useMemo(
    () => (currentRunId !== null && activeRun === null ? selectInputRequests(stream) : []),
    [activeRun, currentRunId, stream],
  );
  const runActivity = useMemo(() => selectRunActivity(stream), [stream]);
  const displayedMessages = useMemo(() => {
    const confirmed = visibleMessages;
    const pending = optimisticMessages.filter(
      (optimistic) =>
        !confirmed.some(
          (message) => message.type === optimistic.type && messageText(message) === messageText(optimistic),
        ),
    );
    return [...confirmed, ...pending];
  }, [optimisticMessages, visibleMessages]);
  const runMessageIndex = useMemo(() => {
    if (!runActivity && liveRunSubagentCards.length === 0) {
      return -1;
    }
    return latestUserIndex(displayedMessages);
  }, [displayedMessages, liveRunSubagentCards.length, runActivity]);
  const currentStateMessageIndex = runMessageIndex >= 0 ? runMessageIndex : latestUserIndex(displayedMessages);
  const selectedExchange = useMemo(() => {
    const fallbackIndex = currentStateMessageIndex >= 0 ? currentStateMessageIndex : displayedMessages.length - 1;
    return exchangeForMessage(displayedMessages, focusedMessageIndex >= 0 ? focusedMessageIndex : fallbackIndex);
  }, [currentStateMessageIndex, displayedMessages, focusedMessageIndex]);
  const runsInMessageOrder = useMemo(
    () => [...runs].sort((left, right) => left.createdAt.localeCompare(right.createdAt)),
    [runs],
  );
  const selectedRun = useMemo(() => {
    if (!selectedExchange) {
      return null;
    }
    const exchangeOrdinal = displayedMessages
      .slice(0, selectedExchange.userIndex + 1)
      .filter((message) => message.type === "human").length - 1;
    return runsInMessageOrder[exchangeOrdinal] ?? null;
  }, [displayedMessages, runsInMessageOrder, selectedExchange]);
  function runForUserMessage(index: number): RunSummary | null {
    if (displayedMessages[index]?.type !== "human") {
      return null;
    }
    const exchangeOrdinal = displayedMessages
      .slice(0, index + 1)
      .filter((message) => message.type === "human").length - 1;
    return runsInMessageOrder[exchangeOrdinal] ?? null;
  }

  function subagentCardsForMessage(index: number): SubagentCard[] {
    const run = runForUserMessage(index);
    if (!run) {
      return currentRunId !== null && index === latestUserIndex(displayedMessages) ? liveRunSubagentCards : [];
    }
    if (run.runId === currentRunId) {
      return liveRunSubagentCards;
    }
    const snapshot = runDebugSnapshots[run.runId];
    return snapshot ? subagentCardsFromEvents(snapshot.events) : [];
  }

  const currentRun = currentRunId
    ? runs.find((run) => run.runId === currentRunId) ?? {
        runId: currentRunId,
        threadId: threadId ?? "",
        status: stream.isLoading ? "running" : "running",
        createdAt: "",
        updatedAt: "",
      }
    : null;
  const debugRun = selectedRun ?? currentRun;
  const selectedRunId = debugRun?.runId ?? null;
  const selectedSnapshot = selectedRunId ? runDebugSnapshots[selectedRunId] : undefined;
  const selectedRunIsLive = selectedRunId !== null && selectedRunId === currentRunId;
  const snapshotTodos = selectedSnapshot ? selectTodosFromValues(selectedSnapshot.values) ?? [] : [];
  const cachedTodos = selectedRunId ? runTodoCache[selectedRunId] ?? [] : [];
  const snapshotEvents = selectedSnapshot ? debugEventsFromProtocol(selectedSnapshot.events) : [];
  const debugTodos = selectedRunIsLive
    ? (todos.length > 0 ? todos : liveEventTodos.length > 0 ? liveEventTodos : cachedTodos.length > 0 ? cachedTodos : snapshotTodos)
    : snapshotTodos.length > 0 ? snapshotTodos : cachedTodos;
  const debugSubagents = selectedRunIsLive ? liveRunSubagentCards : selectedSnapshot ? subagentCardsFromEvents(selectedSnapshot.events) : [];
  const debugToolRows = selectedRunIsLive ? toolRowsFromStream(stream.toolCalls) : selectedSnapshot ? toolRowsFromEvents(selectedSnapshot.events) : [];
  const debugEvents = selectedRunIsLive ? stream.debugEvents : snapshotEvents;

  const registerMessageNode = useCallback((index: number, node: HTMLElement | null): void => {
    if (node) {
      messageNodesRef.current.set(index, node);
    } else {
      messageNodesRef.current.delete(index);
    }
  }, []);

  const updateFocusedMessageIndex = useCallback((): void => {
    const viewport = messagesViewportRef.current;
    if (!viewport || messageNodesRef.current.size === 0) {
      return;
    }
    const viewportRect = viewport.getBoundingClientRect();
    const anchor = viewportRect.top + Math.min(viewportRect.height * 0.35, 220);
    let bestIndex = -1;
    let bestDistance = Number.POSITIVE_INFINITY;

    for (const [index, node] of messageNodesRef.current) {
      const rect = node.getBoundingClientRect();
      if (rect.bottom < viewportRect.top || rect.top > viewportRect.bottom) {
        continue;
      }
      const distance = Math.abs(rect.top - anchor);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestIndex = index;
      }
    }

    if (bestIndex >= 0) {
      setFocusedMessageIndex((current) => (current === bestIndex ? current : bestIndex));
    }
  }, []);

  function logStreamingTokens(messages: Message[]): void {
    for (const [index, message] of messages.entries()) {
      if (message.type !== "ai") {
        continue;
      }
      const text = messageText(message);
      if (!text) {
        continue;
      }
      const key = message.id ?? `ai-${index}`;
      const previous = loggedMessageTextRef.current.get(key) ?? "";
      if (text === previous || !text.startsWith(previous)) {
        loggedMessageTextRef.current.set(key, text);
        continue;
      }
      const token = text.slice(previous.length);
      loggedMessageTextRef.current.set(key, text);
      logger.info("stream.token.received", {
        messageId: key,
        token,
        tokenLength: token.length,
        totalLength: text.length,
      });
    }
  }

  async function refreshThreads(): Promise<void> {
    setThreadsLoading(true);
    try {
      const next = await listThreads(apiUrl);
      setThreads(next);
      logger.info("threads.refresh.complete", { count: next.length });
    } catch (caught) {
      logger.error("threads.refresh.failed", {
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to load threads.");
    } finally {
      setThreadsLoading(false);
    }
  }

  async function refreshRuns(nextThreadId = threadId): Promise<void> {
    if (!nextThreadId) {
      setRuns([]);
      return;
    }
    try {
      const next = await listRuns(apiUrl, nextThreadId);
      setRuns(next);
      logger.info("runs.refresh.complete", { threadId: nextThreadId, count: next.length });
    } catch (caught) {
      logger.error("runs.refresh.failed", {
        threadId: nextThreadId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setRuns([]);
    }
  }

  function resetVisibleThread(nextThreadId: string | null): void {
    setActiveRun(null);
    setVisibleMessages([]);
    setOptimisticMessages([]);
    setFocusedMessageIndex(-1);
    setCurrentRunId(null);
    setDebugSnapshotError(null);
    messageNodesRef.current.clear();
    loggedMessageTextRef.current.clear();
    stream.clearDebugEvents();
    joinedRunIds.current.clear();
    stream.switchThread(nextThreadId);
    setThreadId(nextThreadId);
    if (nextThreadId) {
      localStorage.setItem(CURRENT_THREAD_KEY, nextThreadId);
    } else {
      localStorage.removeItem(CURRENT_THREAD_KEY);
    }
  }

  async function submit(): Promise<void> {
    const content = draft.trim();
    if (!content || stream.isLoading) {
      logger.debug("submit.skipped", {
        hasContent: Boolean(content),
        isLoading: stream.isLoading,
      });
      return;
    }
    setError(null);
    pendingThreadTitleRef.current = content;
    setDraft("");
    stream.clearDebugEvents();
    const optimisticMessage = {
      id: `optimistic-${crypto.randomUUID()}`,
      type: "human",
      content,
    } as Message;
    setOptimisticMessages((messages) => [...messages, optimisticMessage]);
    logger.info("submit.start", {
      threadId,
      contentLength: content.length,
      streamModes: STREAM_MODES.length,
    });
    try {
      await stream.submit(
        { messages: [{ type: "human", content }] },
        {
          streamMode: [...STREAM_MODES],
          streamSubgraphs: true,
          streamResumable: true,
          multitaskStrategy: "reject",
          metadata: { surface: "deep-research-ui" },
          config: { configurable: threadId ? { thread_id: threadId } : {} },
        },
      );
      logger.info("submit.completed", { threadId });
      await refreshThreads();
      await refreshRuns();
    } catch (caught) {
      setOptimisticMessages((messages) => messages.filter((message) => message.id !== optimisticMessage.id));
      logger.error("submit.failed", {
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to start the run.");
    }
  }

  async function resume(value: unknown): Promise<void> {
    setError(null);
    stream.clearDebugEvents();
    logger.info("resume.start", { valueType: typeof value });
    try {
      await stream.submit(null, {
        command: { resume: value },
        streamMode: [...STREAM_MODES],
        streamSubgraphs: true,
        streamResumable: true,
        multitaskStrategy: "reject",
        metadata: { surface: "deep-research-ui", action: "resume" },
      });
      logger.info("resume.completed");
    } catch (caught) {
      logger.error("resume.failed", {
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to resume the run.");
    }
  }

  async function continueActiveRun(run: ActiveRun): Promise<void> {
    logger.info("activeRun.continue.start", run);
    setError(null);
    stream.clearDebugEvents();
    try {
      const status = await fetchRunStatus(apiUrl, run);
      if (!status || !ACTIVE_RUN_STATUSES.has(status)) {
        logger.info("activeRun.continue.skipped.terminal", { ...run, status });
        setActiveRun(null);
        return;
      }
      const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/resume`, {
        method: "POST",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      joinedRunIds.current.add(run.runId);
      setActiveRun(null);
      setCurrentRunId(run.runId);
      setRuns((current) => {
        const now = new Date().toISOString();
        const existing = current.find((item) => item.runId === run.runId);
        const nextRun = existing
          ? { ...existing, status: status ?? existing.status, updatedAt: now }
          : {
              runId: run.runId,
              threadId: run.threadId,
              status: status ?? "running",
              createdAt: now,
              updatedAt: now,
            };
        return [nextRun, ...current.filter((item) => item.runId !== run.runId)];
      });
      await stream.joinStream(run.runId, undefined, { streamMode: [...STREAM_MODES] });
      logger.info("activeRun.continue.completed", run);
    } catch (caught) {
      logger.error("activeRun.continue.failed", {
        threadId: run.threadId,
        runId: run.runId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to continue the active run.");
    }
  }

  async function stopActiveRun(run: ActiveRun): Promise<void> {
    logger.info("activeRun.stop.start", run);
    setError(null);
    try {
      const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/cancel`, {
        method: "POST",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      joinedRunIds.current.delete(run.runId);
      setActiveRun(null);
      setCurrentRunId((runId) => (runId === run.runId ? null : runId));
      stream.clearDebugEvents();
      logger.info("activeRun.stop.completed", run);
    } catch (caught) {
      logger.error("activeRun.stop.failed", {
        threadId: run.threadId,
        runId: run.runId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to stop the active run.");
    }
  }

  function newThread(): void {
    logger.info("thread.new");
    setOpenThreadMenu(null);
    pendingThreadTitleRef.current = "New research";
    resetVisibleThread(null);
    writeThreadUrl(null);
  }

  function openThread(nextThreadId: string): void {
    setOpenThreadMenu(null);
    if (nextThreadId === threadId) {
      logger.debug("thread.open.skipped.active", { threadId: nextThreadId });
      return;
    }
    logger.info("thread.open", { threadId: nextThreadId });
    resetVisibleThread(nextThreadId);
    writeThreadUrl(nextThreadId);
  }

  async function renameThreadTitle(nextThreadId: string): Promise<void> {
    const thread = threads.find((item) => item.threadId === nextThreadId);
    const currentTitle = thread?.title ?? "";
    setOpenThreadMenu(null);
    const nextTitle = window.prompt("Rename thread", currentTitle);
    if (nextTitle === null) {
      return;
    }
    const title = nextTitle.trim();
    if (!title) {
      setError("Thread title cannot be empty.");
      return;
    }
    logger.info("thread.rename.start", { threadId: nextThreadId, titleLength: title.length });
    setError(null);
    try {
      const updated = await renameThread(apiUrl, nextThreadId, title);
      setThreads((current) =>
        current.map((item) =>
          item.threadId === nextThreadId
            ? { ...item, title: updated.title, updatedAt: updated.updatedAt }
            : item,
        ),
      );
      logger.info("thread.rename.complete", { threadId: nextThreadId });
    } catch (caught) {
      logger.error("thread.rename.failed", {
        threadId: nextThreadId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to rename thread.");
    }
  }

  async function removeThread(nextThreadId: string): Promise<void> {
    const thread = threads.find((item) => item.threadId === nextThreadId);
    const label = thread?.title ?? nextThreadId.slice(0, 8);
    setOpenThreadMenu(null);
    if (!window.confirm(`Delete "${label}"?`)) {
      return;
    }
    logger.info("thread.delete.start", { threadId: nextThreadId });
    setError(null);
    try {
      await deleteThread(apiUrl, nextThreadId);
      setThreads((current) => current.filter((item) => item.threadId !== nextThreadId));
      if (nextThreadId === threadId) {
        resetVisibleThread(null);
        writeThreadUrl(null, true);
      }
      logger.info("thread.delete.complete", { threadId: nextThreadId });
    } catch (caught) {
      logger.error("thread.delete.failed", {
        threadId: nextThreadId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to delete thread.");
    }
  }

  useEffect(() => {
    const closeMenu = (): void => setOpenThreadMenu(null);
    const closeOnEscape = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setOpenThreadMenu(null);
      }
    };
    window.addEventListener("click", closeMenu);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.removeEventListener("click", closeMenu);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, []);

  useEffect(() => {
    if (stream.messages.length === 0) {
      return;
    }
    logStreamingTokens(stream.messages);
    setVisibleMessages(stream.messages);
    setOptimisticMessages((messages) =>
      messages.filter(
        (optimistic) =>
          !stream.messages.some(
            (message) => message.type === optimistic.type && messageText(message) === messageText(optimistic),
          ),
      ),
    );
  }, [stream.isLoading, stream.messages]);

  useEffect(() => {
    if (!currentRunId || todos.length === 0) {
      return;
    }
    setRunTodoCache((current) => ({ ...current, [currentRunId]: todos }));
  }, [currentRunId, todos]);

  useLayoutEffect(() => {
    messagesEndRef.current?.scrollIntoView({ block: "end" });
  }, [displayedMessages]);

  useLayoutEffect(() => {
    updateFocusedMessageIndex();
  }, [displayedMessages, updateFocusedMessageIndex]);

  useEffect(() => {
    const viewport = messagesViewportRef.current;
    if (!viewport) {
      return undefined;
    }
    viewport.addEventListener("scroll", updateFocusedMessageIndex, { passive: true });
    window.addEventListener("resize", updateFocusedMessageIndex);
    return () => {
      viewport.removeEventListener("scroll", updateFocusedMessageIndex);
      window.removeEventListener("resize", updateFocusedMessageIndex);
    };
  }, [updateFocusedMessageIndex]);

  useEffect(() => {
    activeRunRef.current = activeRun;
    isLoadingRef.current = stream.isLoading;
  }, [activeRun, stream.isLoading]);

  useEffect(() => {
    if (stream.error) {
      logger.error("stream.error", {
        message: stream.error instanceof Error ? stream.error.message : String(stream.error),
      });
      setError(stream.error instanceof Error ? stream.error.message : String(stream.error));
    }
  }, [stream.error]);

  useEffect(() => {
    if (threadId && threadIdFromUrl() !== threadId) {
      writeThreadUrl(threadId, true);
    }
    // This only normalizes the initial localStorage fallback into the address bar.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let cancelled = false;
    setThreadsLoading(true);
    void listThreads(apiUrl)
      .then((next) => {
        if (cancelled) {
          return;
        }
        setThreads(next);
        logger.info("threads.startup.loaded", { count: next.length });
      })
      .catch((caught) => {
        if (cancelled) {
          return;
        }
        logger.error("threads.startup.failed", {
          message: caught instanceof Error ? caught.message : String(caught),
        });
        setError(caught instanceof Error ? caught.message : "Unable to load threads.");
      })
      .finally(() => {
        if (!cancelled) {
          setThreadsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [apiUrl]);

  useEffect(() => {
    void refreshRuns();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiUrl, threadId]);

  useEffect(() => {
    if (!threadId || !selectedRunId || selectedRunIsLive || runDebugSnapshots[selectedRunId]) {
      return undefined;
    }
    let cancelled = false;
    setDebugSnapshotLoading(true);
    setDebugSnapshotError(null);
    void getRunDebugSnapshot(apiUrl, threadId, selectedRunId)
      .then((snapshot) => {
        if (cancelled) {
          return;
        }
        setRunDebugSnapshots((current) => ({ ...current, [selectedRunId]: snapshot }));
      })
      .catch((caught) => {
        if (cancelled) {
          return;
        }
        logger.error("runs.debug.load.failed", {
          threadId,
          runId: selectedRunId,
          message: caught instanceof Error ? caught.message : String(caught),
        });
        setDebugSnapshotError(caught instanceof Error ? caught.message : "Unable to load run debug.");
      })
      .finally(() => {
        if (!cancelled) {
          setDebugSnapshotLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [apiUrl, runDebugSnapshots, selectedRunId, selectedRunIsLive, threadId]);

  useEffect(() => {
    const handlePopState = (): void => {
      const nextThreadId = threadIdFromUrl();
      logger.info("thread.url.popstate", { threadId: nextThreadId });
      setActiveRun(null);
      setVisibleMessages([]);
      setOptimisticMessages([]);
      setFocusedMessageIndex(-1);
      setCurrentRunId(null);
      setDebugSnapshotError(null);
      messageNodesRef.current.clear();
      joinedRunIds.current.clear();
      switchThreadRef.current?.(nextThreadId);
      setThreadId(nextThreadId);
      if (nextThreadId) {
        localStorage.setItem(CURRENT_THREAD_KEY, nextThreadId);
      } else {
        localStorage.removeItem(CURRENT_THREAD_KEY);
      }
    };
    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  useEffect(() => {
    if (!activeRun) {
      return undefined;
    }
    let cancelled = false;
    const verify = async (): Promise<void> => {
      try {
        const status = await fetchRunStatus(apiUrl, activeRun);
        if (cancelled) {
          return;
        }
        if (!status || !ACTIVE_RUN_STATUSES.has(status)) {
          logger.info("activeRun.status.cleared", { ...activeRun, status });
          setActiveRun((current) =>
            current?.threadId === activeRun.threadId && current.runId === activeRun.runId ? null : current,
          );
        }
      } catch (caught) {
        logger.warn("activeRun.status.failed", {
          ...activeRun,
          message: caught instanceof Error ? caught.message : String(caught),
        });
      }
    };
    void verify();
    const interval = window.setInterval(() => void verify(), 3000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [activeRun, apiUrl]);

  useEffect(() => {
    if (!threadId) {
      logger.debug("activeRunMonitor.skipped.noThread");
      joinedRunIds.current.clear();
      return undefined;
    }

    let cancelled = false;
    const showActiveRun = (runId: string, source: string): void => {
      if (!runId || cancelled) {
        return;
      }
      if (isLoadingRef.current) {
        logger.debug("activeRunMonitor.skipped.loading", { threadId, runId, source });
        return;
      }
      if (joinedRunIds.current.has(runId)) {
        logger.debug("activeRunMonitor.skipped.joined", { threadId, runId, source });
        return;
      }
      if (activeRunRef.current?.threadId === threadId && activeRunRef.current.runId === runId) {
        logger.debug("activeRunMonitor.skipped.bannerVisible", { threadId, runId, source });
        return;
      }
      logger.info("activeRunMonitor.discovered", { threadId, runId, source });
      setActiveRun((current) => current ?? { threadId, runId });
    };

    const clearActiveRun = (runId: string, source: string): void => {
      logger.info("activeRunMonitor.clear.requested", { threadId, runId, source });
      setActiveRun((current) =>
        current?.threadId === threadId && current.runId === runId ? null : current,
      );
    };

    const checkActiveRunOnce = async (): Promise<void> => {
      try {
        logger.debug("activeRunMonitor.initialCheck.start", { threadId });
        const response = await fetch(`${apiUrl}/threads/${threadId}/runs?limit=1&status=running`);
        if (response.status === 404) {
          logger.warn("activeRunMonitor.threadMissing", { threadId });
          joinedRunIds.current.clear();
          localStorage.removeItem(CURRENT_THREAD_KEY);
          writeThreadUrl(null, true);
          switchThreadRef.current?.(null);
          setThreadId(null);
          return;
        }
        if (!response.ok) {
          logger.warn("activeRunMonitor.initialCheck.failed", {
            threadId,
            status: response.status,
          });
          return;
        }
        const runs = (await response.json()) as Array<{ run_id?: string }>;
        const runId = runs[0]?.run_id;
        if (runId) {
          showActiveRun(runId, "initial-check");
        } else {
          logger.debug("activeRunMonitor.initialCheck.none", { threadId });
        }
      } catch {
        logger.warn("activeRunMonitor.initialCheck.error", { threadId });
      }
    };

    void checkActiveRunOnce();

    const url = new URL(`${apiUrl}/threads/${threadId}/stream`);
    url.searchParams.append("stream_mode", "lifecycle");
    const source = new EventSource(url.toString());
    logger.info("activeRunMonitor.lifecycle.subscribe", { threadId });

    source.addEventListener("metadata", (event) => {
      try {
        const data = JSON.parse(event.data) as { event?: unknown; run_id?: unknown };
        const runId = typeof data.run_id === "string" ? data.run_id : "";
        if (data.event === "running") {
          showActiveRun(runId, "lifecycle");
        }
        if (typeof data.event === "string" && TERMINAL_RUN_EVENTS.has(data.event)) {
          clearActiveRun(runId, "lifecycle");
        }
      } catch {
        logger.warn("activeRunMonitor.lifecycle.parseFailed", { threadId });
      }
    });

    source.onerror = () => {
      logger.warn("activeRunMonitor.lifecycle.error", { threadId });
    };

    return () => {
      cancelled = true;
      logger.info("activeRunMonitor.lifecycle.unsubscribe", { threadId });
      source.close();
    };
  }, [apiUrl, threadId]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <FlaskConical size={20} />
          <span>Deep Research</span>
        </div>
        <button className="new-thread" onClick={newThread} type="button">
          <Plus size={16} />
          <span>New research</span>
        </button>
        <div className="thread-list">
          {threadsLoading && <div className="thread-list-status">Loading threads...</div>}
          {threads.map((thread) => (
            <div className={thread.threadId === threadId ? "thread-row active" : "thread-row"} key={thread.threadId}>
              <button
                className="thread"
                onClick={() => openThread(thread.threadId)}
                type="button"
                title={thread.threadId}
              >
                <MessageSquare size={15} />
                <span>{thread.title}</span>
              </button>
              <div className="thread-menu-wrap" onClick={(event) => event.stopPropagation()}>
                <button
                  aria-expanded={openThreadMenu === thread.threadId}
                  aria-haspopup="menu"
                  className="thread-menu-trigger"
                  onClick={() =>
                    setOpenThreadMenu((current) => (current === thread.threadId ? null : thread.threadId))
                  }
                  type="button"
                  title="Thread actions"
                >
                  <MoreHorizontal size={16} />
                </button>
                {openThreadMenu === thread.threadId && (
                  <div className="thread-menu" role="menu">
                    <button onClick={() => void renameThreadTitle(thread.threadId)} role="menuitem" type="button">
                      Rename
                    </button>
                    <button className="danger" onClick={() => void removeThread(thread.threadId)} role="menuitem" type="button">
                      Delete
                    </button>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
        <label className="api-url">
          <span>API</span>
          <input value={apiUrl} onChange={(event) => setApiUrl(event.target.value)} />
        </label>
      </aside>

      <main className={debugOpen ? "workspace with-debug" : "workspace"}>
        <section className="chat">
          <header className="topbar">
            <div>
              <strong>{statusText(stream.isLoading, stream.activeSubagents.length)}</strong>
              <span>{threadId ? threadId.slice(0, 8) : "No thread yet"}</span>
            </div>
            <div className="topbar-actions">
              {stream.isLoading && (
                <button className="icon-button danger" onClick={() => void stream.stop()} type="button" title="Stop run">
                  <CircleStop size={18} />
                </button>
              )}
              <button
                className="icon-button"
                onClick={() => setDebugOpen((open) => !open)}
                type="button"
                title="Toggle debug panel"
              >
                <PanelRight size={18} />
              </button>
            </div>
          </header>

          <div className="messages" ref={messagesViewportRef}>
            {displayedMessages.length === 0 ? (
              <div className="empty-state">
                <h1>What should we research?</h1>
                <p>Ask for a market scan, technical comparison, literature review, or sourced brief.</p>
              </div>
            ) : (
              displayedMessages.map((message, index) => (
                <MessageBubble
                  key={messageKey(message, index)}
                  activity={index === runMessageIndex ? runActivity : null}
                  message={message}
                  setNode={(node) => registerMessageNode(index, node)}
                  subagents={subagentCardsForMessage(index)}
                />
              ))
            )}
            <div aria-hidden="true" ref={messagesEndRef} />
          </div>

          {inputRequests.length > 0 && <InputRequests requests={inputRequests} onResume={resume} />}

          {activeRun && (
            <div className="active-run-banner">
              <div>
                <strong>Active run found</strong>
                <span>{activeRun.runId.slice(0, 8)} can continue from its saved checkpoint or be stopped.</span>
              </div>
              <button onClick={() => void continueActiveRun(activeRun)} type="button">
                Continue streaming
              </button>
              <button className="secondary" onClick={() => void stopActiveRun(activeRun)} type="button">
                Stop run
              </button>
            </div>
          )}

          {error && <div className="error-banner">{error}</div>}

          <form
            className="composer"
            onSubmit={(event) => {
              event.preventDefault();
              void submit();
            }}
          >
            <textarea
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              placeholder="Ask the deep researcher..."
              rows={1}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void submit();
                }
              }}
            />
            <button disabled={!draft.trim() || stream.isLoading || activeRun !== null} type="submit" title="Send">
              {stream.isLoading ? <Loader2 className="spin" size={18} /> : <Send size={18} />}
            </button>
          </form>
        </section>

        {debugOpen && (
          <DebugPanel
            debugEvents={debugEvents}
            error={debugSnapshotError}
            exchange={selectedExchange}
            loading={debugSnapshotLoading}
            run={debugRun}
            scopedTodos={debugTodos}
            subagents={debugSubagents}
            toolRows={debugToolRows}
          />
        )}
      </main>
    </div>
  );
}

function MessageBubble({
  activity,
  message,
  setNode,
  subagents,
}: {
  activity: RunActivity | null;
  message: Message;
  setNode: (node: HTMLElement | null) => void;
  subagents: SubagentCard[];
}) {
  const text = messageText(message);
  const hideText = isInternalTodoUpdate(message, text);
  if (message.type === "remove" || ((!text || hideText) && message.type === "ai" && subagents.length === 0)) {
    return null;
  }
  return (
    <article className={`message ${message.type}`} ref={setNode}>
      <div className="avatar">{message.type === "human" ? "You" : "AI"}</div>
      <div className="message-body">
        {!hideText && (text || subagents.length === 0) && (
          <div className="message-content">{text || <span className="muted">Working...</span>}</div>
        )}
        {activity && message.type === "human" && (
          <div className="run-activity" aria-label="Run activity">
            <div>
              <span>Reasoning</span>
              <strong>{activity.reasoning}</strong>
            </div>
            <div>
              <span>Action</span>
              <strong>{activity.action}</strong>
            </div>
          </div>
        )}
        {subagents.length > 0 && (
          <div className="message-subagents" aria-label="Subagent activity">
            {subagents.map((subagent) => (
              <SubagentCardView
                key={subagent.key}
                subagent={subagent}
                variant="inline"
              />
            ))}
          </div>
        )}
      </div>
    </article>
  );
}

function InputRequests({
  requests,
  onResume,
}: {
  requests: InputRequest[];
  onResume: (value: unknown) => Promise<void>;
}) {
  const [responses, setResponses] = useState<Record<string, string>>({});
  return (
    <div className="hitl-panel">
      {requests.map((request) => (
        <div className="hitl-card" key={request.id}>
          {request.kind === "permission" ? <KeyRound size={18} /> : <ShieldQuestion size={18} />}
          <div>
            <strong>{request.title}</strong>
            <p>{request.detail}</p>
            <div className="hitl-actions">
              <input
                value={responses[request.id] ?? ""}
                onChange={(event) => setResponses((current) => ({ ...current, [request.id]: event.target.value }))}
                placeholder="Optional response"
              />
              <button onClick={() => void onResume(responses[request.id] || true)} type="button">
                <Check size={15} />
                Continue
              </button>
              {request.kind === "permission" && (
                <button className="secondary" onClick={() => void onResume(false)} type="button">
                  Deny
                </button>
              )}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function DebugPanel({
  debugEvents,
  error,
  exchange,
  loading,
  run,
  scopedTodos,
  subagents,
  toolRows,
}: {
  debugEvents: DeepResearchStream["debugEvents"];
  error: string | null;
  exchange: MessageExchange | null;
  loading: boolean;
  run: RunSummary | null;
  scopedTodos: TodoItem[];
  subagents: SubagentCard[];
  toolRows: ToolDebugRow[];
}) {
  const [openSections, setOpenSections] = useState<Record<DebugSectionId, boolean>>({
    todos: true,
    subagents: true,
    tools: true,
    timeline: true,
  });

  function toggleSection(section: DebugSectionId): void {
    setOpenSections((current) => ({ ...current, [section]: !current[section] }));
  }

  const progress = todoProgress(scopedTodos);
  const stateMessage = loading
    ? "Loading debug data for this run."
    : error ?? (run ? "No debug data was saved for this run." : "No run is mapped to this exchange.");

  return (
    <aside className="debug-panel">
      <section className="exchange-summary">
        <span>{run ? `Selected run ${run.runId.slice(0, 8)} (${run.status})` : "Selected exchange"}</span>
        {exchange ? (
          <>
            <strong>{messageSnippet(exchange.userText) || "User message"}</strong>
            <p>{messageSnippet(exchange.aiText) || "No AI reply yet."}</p>
          </>
        ) : (
          <p>No message selected.</p>
        )}
      </section>

      <DebugSection
        open={openSections.todos}
        title="Todos"
        onToggle={() => toggleSection("todos")}
      >
        <div className="todo-list">
          {scopedTodos.length === 0 ? <p className="muted">{stateMessage}</p> : null}
          {scopedTodos.length > 0 && (
            <div className="todo-progress">
              <div>
                <span>Progress</span>
                <strong>{progress.percentage}%</strong>
              </div>
              <div className="todo-progress-track">
                <div style={{ width: `${progress.percentage}%` }} />
              </div>
              <small>
                {progress.completed}/{scopedTodos.length} tasks
              </small>
            </div>
          )}
          {scopedTodos.map((todo) => (
            <div className={`todo ${todo.status}`} key={todo.id}>
              <span />
              <div>
                <strong>{todo.content}</strong>
                {todo.agent ? <small>{todo.agent}</small> : null}
              </div>
            </div>
          ))}
        </div>
      </DebugSection>

      <DebugSection
        open={openSections.subagents}
        title="Subagents"
        onToggle={() => toggleSection("subagents")}
      >
        <div className="subagent-list">
          {subagents.length === 0 ? <p className="muted">{stateMessage}</p> : null}
          {subagents.map((subagent) => (
            <SubagentCardView key={subagent.key} subagent={subagent} />
          ))}
        </div>
      </DebugSection>

      <DebugSection
        open={openSections.tools}
        title="Tools"
        onToggle={() => toggleSection("tools")}
      >
        <div className="tool-list">
          {toolRows.length === 0 ? <p className="muted">{stateMessage}</p> : null}
          {toolRows.map((toolCall) => (
            <div className="tool-row" key={toolCall.id}>
              <ChevronRight size={14} />
              <span>{toolCall.name}</span>
              <small>{toolCall.state}</small>
            </div>
          ))}
        </div>
      </DebugSection>

      <DebugSection
        open={openSections.timeline}
        title="Timeline"
        onToggle={() => toggleSection("timeline")}
      >
        <div className="timeline">
          {debugEvents.length === 0 ? <p className="muted">{stateMessage}</p> : null}
          {[...debugEvents].reverse().map((event) => (
            <div className="timeline-row" key={event.id}>
              <span>{event.channel}</span>
              <small>{event.namespace.join("/") || "root"}</small>
            </div>
          ))}
        </div>
      </DebugSection>
    </aside>
  );
}

function DebugSection({
  children,
  open,
  onToggle,
  title,
}: {
  children: ReactNode;
  open: boolean;
  onToggle: () => void;
  title: string;
}) {
  return (
    <section className={open ? "panel-section open" : "panel-section"}>
      <button
        aria-expanded={open}
        className="panel-section-header"
        onClick={onToggle}
        type="button"
      >
        <ChevronRight className={open ? "disclosure open" : "disclosure"} size={15} />
        <h2>{title}</h2>
      </button>
      {open && <div className="panel-section-body">{children}</div>}
    </section>
  );
}

function SubagentCardView({
  subagent,
  variant = "panel",
}: {
  subagent: SubagentCard;
  variant?: "panel" | "inline";
}) {
  const visibleMessages = subagent.messages.filter((message) => message.content.trim()).slice(-3);
  return (
    <article className={`subagent-card ${subagent.status} ${variant}`}>
      <header>
        <div>
          <strong>{subagent.name}</strong>
          <small>{subagent.description}</small>
        </div>
        <span>{subagent.status}</span>
      </header>
      <div className="progress">
        <div style={{ width: `${subagent.progress}%` }} />
      </div>
      <div className="subagent-meta">
        <span>{subagent.messages.length} messages</span>
        <span>{subagent.tools.length} tools</span>
      </div>
      <div className="subagent-stream">
        {visibleMessages.map((message) => (
          <p className={message.status === "streaming" ? "streaming" : undefined} key={message.id}>
            {message.content}
          </p>
        ))}
      </div>
    </article>
  );
}
