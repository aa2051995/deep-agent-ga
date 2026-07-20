import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  Check,
  CircleStop,
  FlaskConical,
  KeyRound,
  Loader2,
  MessageSquare,
  MoreHorizontal,
  Plus,
  Send,
  ShieldQuestion,
} from "lucide-react";
import type { Message } from "@langchain/langgraph-sdk";
import type { SubagentStreamInterface } from "@langchain/langgraph-sdk/react";
import { getLogMode, logger, LOG_MODES, setLogMode } from "./logger";
import type { LogMode } from "./logger";
import { deleteThread, getRunCheckpointSnapshot, listRuns, listThreads, renameThread } from "./api";
import { DEFAULT_API_URL, messageText, useDeepResearchStream } from "./stream";
import type { DebugEvent, DeepResearchStream } from "./stream";
import { selectInputRequests, subagentStreamToCard } from "./selectors";
import { hasEarlierUnhydratedRuns, selectRunsToHydrate } from "./runHydration";
import { cancelCurrentRunRequest } from "./runControl";
import {
  buildRunMessageEntries,
  messageIdSet,
  persistedOrLive,
  sameMessageIdentity,
  selectLiveRunMessages,
} from "./messageMerge";
import type { InputRequest, ProtocolEvent, RunCheckpointSnapshot, RunSummary, SubagentCard, ThreadSummary } from "./types";

const CURRENT_THREAD_KEY = "deep-research-ui:current-thread";
const THREAD_QUERY_PARAM = "thread_id";
const STREAM_MODES = [
  "messages-tuple",
  "values",
  "updates",
  "tools",
  "tasks",
  "checkpoints",
  "custom",
] as const;
const LOG_MODE_LABELS: Record<LogMode, string> = {
  stream: "Stream only",
  tokens: "Tokens only",
  off: "Off",
  error: "Errors",
  warn: "Warnings",
  info: "Info",
  debug: "Debug",
};

type ActiveRun = {
  threadId: string;
  runId: string;
};
type RunAction = {
  id: string;
  label: string;
  status: "running" | "done";
};
const loggedLiveSubagentTaskIds = new Set<string>();
const ACTIVE_RUN_STATUSES = new Set(["pending", "running"]);
const TERMINAL_RUN_EVENTS = new Set(["completed", "failed", "interrupted", "timeout"]);
const TERMINAL_EVENT_TO_RUN_STATUS: Record<string, string> = {
  completed: "success",
  failed: "error",
  interrupted: "interrupted",
  timeout: "timeout",
};
const PERSISTED_RUN_STATUSES = new Set(["success", "error", "interrupted", "timeout"]);
// Lazy hydration: how many of the newest finished runs to load checkpoint
// snapshots for on open, and how many more to reveal per "Load earlier runs".
const INITIAL_HYDRATED_RUN_LIMIT = 7;
const EARLIER_RUNS_BATCH = 5;

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

const EMPTY_MESSAGE_IDS: ReadonlySet<string> = new Set();

/** Reference-equality list compare, so re-capturing an unchanged run is a no-op. */
function sameMessageList(left: Message[] | undefined, right: Message[]): boolean {
  if (!left || left.length !== right.length) {
    return false;
  }
  return left.every((message, index) => message === right[index]);
}

/**
 * Structural compare for subagent cards, so re-capturing an unchanged run is a
 * no-op. Unlike messages (stable objects from the SDK), subagentCardsForLiveRun
 * rebuilds every card on each call, so reference equality would never match —
 * a cheap JSON compare is the practical option for these small arrays.
 */
function sameSubagentCards(left: SubagentCard[] | undefined, right: SubagentCard[]): boolean {
  if (!left) {
    return false;
  }
  return left.length === right.length && JSON.stringify(left) === JSON.stringify(right);
}

function sameMessage(left: Message, right: Message): boolean {
  // Stable ids are authoritative; content is only a fallback for optimistic /
  // un-id'd messages. Two different-id messages with identical text (e.g. the
  // fixture streaming the same plan every run) are NOT the same message.
  return sameMessageIdentity(left, right, (message) => message.id, (message) => message.type, messageText);
}

function isNearBottom(element: HTMLElement, threshold = 96): boolean {
  return element.scrollHeight - element.scrollTop - element.clientHeight <= threshold;
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

function actionRowsFromEvents(events: DebugEvent[], runId: string | null): RunAction[] {
  const actions = new Map<string, RunAction>();
  for (const event of events) {
    if (event.channel !== "tools" || typeof event.data !== "object" || event.data === null) {
      continue;
    }
    if (subagentKeyFromNamespace(event.namespace) !== null) {
      continue;
    }
    const data = event.data as Record<string, unknown>;
    if (runId !== null && data.run_id !== runId) {
      continue;
    }
    const toolId = String(data.tool_call_id ?? data.toolCallId ?? data.id ?? event.id);
    const name = stringValue(data.name ?? data.tool_name);
    if (!name) {
      continue;
    }
    const toolEvent = String(data.event ?? "");
    const status =
      toolEvent.includes("finish") || toolEvent.includes("end")
        ? "done"
        : "running";
    const existing = actions.get(toolId);
    actions.set(toolId, {
      id: toolId,
      label: existing?.label ?? actionFromTool(name, data.input),
      status: status === "done" ? "done" : existing?.status ?? "running",
    });
  }
  return [...actions.values()];
}

function actionRowsFromSubagents(subagents: SubagentCard[]): RunAction[] {
  return subagents.map((subagent) => ({
    id: subagent.key,
    label: `Delegating to ${subagent.name}`,
    status: subagent.status === "done" ? "done" : "running",
  }));
}

function protocolEventData(event: ProtocolEvent): Record<string, unknown> {
  return typeof event.params.data === "object" && event.params.data !== null && !Array.isArray(event.params.data)
    ? (event.params.data as Record<string, unknown>)
    : {};
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
    const rawData = event.params.data;
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
        const logKey = `${String(data.run_id ?? "run")}:${toolId}:${String(data.event ?? "tool")}`;
        if (!loggedLiveSubagentTaskIds.has(logKey)) {
          loggedLiveSubagentTaskIds.add(logKey);
          logger.stream("liveSubagent.task.discovered", {
            runId: typeof data.run_id === "string" ? data.run_id : undefined,
            toolCallId: toolId,
            toolEvent: String(data.event ?? ""),
            subagentType: String(input.subagent_type ?? ""),
            description: compactDetail(String(input.description ?? "")),
            namespace: event.params.namespace,
            seq: event.seq,
          });
        }
        card.description = String(input.description ?? card.description);
        card.name = String(input.subagent_type ?? card.name);
        if (String(data.event ?? "").includes("finish") || String(data.event ?? "").includes("end")) {
          card.status = "done";
          card.progress = 100;
        }
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
      if (!data.event && Array.isArray(rawData)) {
        const streamedMessage = rawData[0] as unknown;
        const message =
          typeof streamedMessage === "object" && streamedMessage !== null
            ? streamedMessage as Record<string, unknown>
            : {};
        const text = stringValue(message.content);
        if (text) {
          const messageId = String(message.id ?? `${key}-${event.seq}`);
          let cardMessage = card.messages.find((item) => item.id === messageId);
          if (!cardMessage) {
            cardMessage = {
              id: messageId,
              role: message.type === "human" ? "human" : "ai",
              content: "",
              componentKey: key,
              namespace: event.params.namespace,
              status: "streaming",
            };
            card.messages.push(cardMessage);
          }
          cardMessage.content += text;
        }
        continue;
      }
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
        const messageId = activeMessageIds.get(key) ?? `${key}-live`;
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

function subagentCardsForLiveRun(
  events: DebugEvent[],
  runId: string | null,
  subagents: DeepResearchStream["subagents"],
): SubagentCard[] {
  const runEvents = events.filter((event) => {
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    return runId === null ? data.run_id == null : data.run_id === runId;
  });
  const eventCards = subagentCardsFromEvents(protocolEventsFromDebugEvents(runEvents));
  const cards = new Map(eventCards.map((card) => [card.key.replace(/^tools:/, ""), card]));
  if (runId !== null) {
    // logger.token("subagentCardsForLiveRun", {
    //   runId,
    //   eventCards: eventCards.length,
    //   subagents: subagents.size,
    // });
  }
  for (const [id, subagent] of subagents) {
    const key = id.replace(/^tools:/, "");
    const existing = cards.get(key);
    if (!existing) {
      continue;
    }
    const sdkCard = subagentStreamToCard(subagent);
    cards.set(key, {
      ...existing,
      ...sdkCard,
      key: existing.key,
      name: existing.name,
      description: existing.description,
      messages: mergeSubagentMessages(existing.messages, sdkCard.messages),
      tools: sdkCard.tools.length > 0 ? sdkCard.tools : existing.tools,
    });
  }
  return [...cards.values()];
}

function mergeSubagentMessages(
  eventMessages: SubagentCard["messages"],
  sdkMessages: SubagentCard["messages"],
): SubagentCard["messages"] {
  const merged = new Map<string, SubagentCard["messages"][number]>();
  for (const message of eventMessages) {
    merged.set(message.id, message);
  }
  for (const message of sdkMessages) {
    const existing = merged.get(message.id);
    if (!existing || message.content.length >= existing.content.length) {
      merged.set(message.id, message);
    }
  }
  return [...merged.values()];
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

async function fetchRunActive(
  apiUrl: string,
  run: ActiveRun,
): Promise<{ isStreaming: boolean; status: string } | null> {
  const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/active`);
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }
  const data = (await response.json()) as { is_streaming?: unknown; status?: unknown };
  return {
    isStreaming: data.is_streaming === true,
    status: typeof data.status === "string" ? data.status : "unknown",
  };
}

export function App() {
  const [apiUrl, setApiUrl] = useState(DEFAULT_API_URL);
  const [threadId, setThreadId] = useState<string | null>(initialThreadId);
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [threadsLoading, setThreadsLoading] = useState(false);
  const [draft, setDraft] = useState("");
  const [logMode, setSelectedLogMode] = useState<LogMode>(() => getLogMode());
  const [error, setError] = useState<string | null>(null);
  const [activeRun, setActiveRun] = useState<ActiveRun | null>(null);
  const [cancellingRunId, setCancellingRunId] = useState<string | null>(null);
  // Messages captured live, keyed by the run that produced them. A run keeps its
  // own bucket for the whole session, so it stays on screen until (and after) its
  // persisted snapshot takes over.
  const [runLiveMessages, setRunLiveMessages] = useState<Record<string, Message[]>>({});
  // Subagent cards captured live, keyed by run — the card equivalent of
  // runLiveMessages. Retained so a finished run's cards survive the window where
  // its snapshot has been dropped and is being refetched.
  const [runSubagentCards, setRunSubagentCards] = useState<Record<string, SubagentCard[]>>({});
  const [optimisticMessages, setOptimisticMessages] = useState<Message[]>([]);
  const [openThreadMenu, setOpenThreadMenu] = useState<string | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [currentRunId, setCurrentRunId] = useState<string | null>(null);
  const [runCheckpointSnapshots, setRunCheckpointSnapshots] = useState<Record<string, RunCheckpointSnapshot>>({});
  const [hydratedRunLimit, setHydratedRunLimit] = useState(INITIAL_HYDRATED_RUN_LIMIT);
  const joinedRunIds = useRef(new Set<string>());
  // Message ids that already existed when the current run started. Everything
  // outside this set (and not owned by a persisted run) is the current run's
  // live output — see selectLiveRunMessages. Captured at run start so live
  // attribution never depends on snapshot-hydration timing.
  const liveBaselineIdsRef = useRef<Set<string>>(new Set());
  const streamMessagesRef = useRef<Message[]>([]);
  const joinRunStreamRef = useRef<((run: ActiveRun) => Promise<void>) | null>(null);
  const activeRunRef = useRef<ActiveRun | null>(null);
  const currentRunIdRef = useRef<string | null>(null);
  const isLoadingRef = useRef(false);
  const threadIdRef = useRef<string | null>(threadId);
  const threadRequestSeqRef = useRef(0);
  const handledTerminalRunIdsRef = useRef(new Set<string>());
  const pendingThreadTitleRef = useRef("New research");
  const loggedMessageTextRef = useRef(new Map<string, string>());
  const messagesViewportRef = useRef<HTMLDivElement | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const shouldStickToBottomRef = useRef(true);
  const switchThreadRef = useRef<DeepResearchStream["switchThread"] | null>(null);
  // logger.debug("app.render", { threadId, apiUrl });

  const stream = useDeepResearchStream(
    apiUrl,
    threadId,
    (nextThreadId) => {
      logger.info("stream.thread.received", { threadId: nextThreadId });
      threadIdRef.current = nextThreadId;
      threadRequestSeqRef.current += 1;
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
      if (run.thread_id !== threadIdRef.current) {
        logger.info("stream.run.current.ignored.staleThread", {
          currentThreadId: threadIdRef.current,
          runThreadId: run.thread_id,
          runId: run.run_id,
        });
        return;
      }
      // Everything already streamed belongs to earlier runs, not this one.
      liveBaselineIdsRef.current = messageIdSet(streamMessagesRef.current, (message) => message.id);
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

  // Updated on every render so effects can call joinStream without stale-closure issues.
  joinRunStreamRef.current = async (run: ActiveRun): Promise<void> => {
    if (joinedRunIds.current.has(run.runId)) {
      logger.debug("joinRunStream.skipped.alreadyJoined", run);
      return;
    }
    joinedRunIds.current.add(run.runId);
    setActiveRun((current) =>
      current?.threadId === run.threadId && current.runId === run.runId ? null : current,
    );
    // Everything already streamed belongs to earlier runs, not the joined one.
    liveBaselineIdsRef.current = messageIdSet(streamMessagesRef.current, (message) => message.id);
    setCurrentRunId(run.runId);
    logger.info("joinRunStream.start", run);
    try {
      await stream.joinStream(run.runId, undefined, { streamMode: [...STREAM_MODES] });
      logger.info("joinRunStream.completed", run);
    } catch (caught) {
      logger.error("joinRunStream.failed", {
        ...run,
        message: caught instanceof Error ? caught.message : String(caught),
      });
    }
  };

  const visibleActiveRun = activeRun?.threadId === threadId ? activeRun : null;

  const liveRunSubagentCards = useMemo(
    () => subagentCardsForLiveRun(stream.debugEvents, currentRunId, stream.subagents),
    [currentRunId, stream],
  );
  const liveRunActions = useMemo(
    () => actionRowsFromEvents(stream.debugEvents, currentRunId),
    [currentRunId, stream.debugEvents],
  );
  const inputRequests = useMemo(
    () => (currentRunId !== null && visibleActiveRun === null ? selectInputRequests(stream) : []),
    [visibleActiveRun, currentRunId, stream],
  );
  const runsInMessageOrder = useMemo(
    () =>
      runs
        .filter((run) => !threadId || run.threadId === threadId)
        .sort((left, right) => left.createdAt.localeCompare(right.createdAt)),
    [runs, threadId],
  );
  const currentRun = currentRunId
    ? runs.find((run) => run.runId === currentRunId && (!threadId || run.threadId === threadId)) ?? null
    : null;
  const currentRunStatus = currentRun?.status ?? null;
  const currentRunSnapshotLoaded = currentRunId ? Boolean(runCheckpointSnapshots[currentRunId]) : false;
  // The transcript is assembled per run, in run order, from exactly one source
  // per run: the persisted snapshot once it exists, otherwise the messages
  // captured live for that run. Every message therefore belongs to exactly one
  // run, so the render key `${runId}:${messageId}` is unique by construction.
  //
  // Keeping the live capture as the fallback is what stops a just-finished run
  // from vanishing: E14 drops its snapshot on the terminal event and E10 refetches
  // it, and during that window the run still renders from its own captured
  // messages instead of falling into a gap (neither live nor persisted).
  const displayedMessageEntries = useMemo(() => {
    const entries = buildRunMessageEntries<Message>(
      runsInMessageOrder.map((run) => run.runId),
      (runId) => runCheckpointSnapshots[runId]?.messages as Message[] | undefined,
      (runId) => runLiveMessages[runId],
      (message) => message.id,
    );
    // Optimistic messages trail the assembled transcript until confirmed.
    const pending = optimisticMessages.filter(
      (optimistic) => !entries.some(({ message }) => sameMessage(message, optimistic)),
    );
    return [...entries, ...pending.map((message) => ({ message, runId: currentRunId }))];
  }, [
    currentRunId,
    optimisticMessages,
    runCheckpointSnapshots,
    runLiveMessages,
    runsInMessageOrder,
  ]);

  const displayedMessages = useMemo(
    () => displayedMessageEntries.map((entry) => entry.message),
    [displayedMessageEntries],
  );

  function runIdForUserMessage(index: number): string | null {
    const entry = displayedMessageEntries[index];
    return entry?.message.type === "human" ? entry.runId : null;
  }

  // Cards for a run that is not the actively-streaming one: the persisted
  // snapshot's cards once it has content, otherwise this run's own retained
  // live cards (runSubagentCards) — never a hard empty. Without this fallback,
  // a run's cards blinked out the moment E14 dropped its snapshot on completion
  // and came back only once E10 refetched it (or, before that, not at all).
  function retainedSubagentCards(runId: string): SubagentCard[] {
    return persistedOrLive(runCheckpointSnapshots[runId]?.subagents, runSubagentCards[runId]);
  }

  function subagentCardsForMessage(index: number): SubagentCard[] {
    const runId = runIdForUserMessage(index);
    if (!runId) {
      if(displayedMessageEntries[index]?.message.type === "human") {
        logger.token("subagentCardsForMessage.none", { runId, messageIndex: index });
      }
      return [];
    }
    if (runId === currentRunId && (currentRunStatus === null || !PERSISTED_RUN_STATUSES.has(currentRunStatus))) {
      logger.token("subagentCardsForMessage.live", { runId, messageIndex: index });
      return liveRunSubagentCards;
    }
    return retainedSubagentCards(runId);
  }

  function actionsForMessage(index: number): RunAction[] {
    const runId = runIdForUserMessage(index);
    if (!runId) {
      return [];
    }
    if (runId === currentRunId && (currentRunStatus === null || !PERSISTED_RUN_STATUSES.has(currentRunStatus))) {
      logger.token("actionsForMessage.live", { runId, messageIndex: index });
      return liveRunActions;
    }
    return actionRowsFromSubagents(retainedSubagentCards(runId));
  }

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
      logger.token("stream.token.received", {
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

  async function refreshRuns(
    nextThreadId = threadId,
    options: { requestSeq?: number; signal?: AbortSignal } = {},
  ): Promise<void> {
    const requestSeq = options.requestSeq ?? threadRequestSeqRef.current;
    if (!nextThreadId) {
      if (threadIdRef.current === null && requestSeq === threadRequestSeqRef.current) {
        setRuns([]);
      }
      return;
    }
    try {
      const next = await listRuns(apiUrl, nextThreadId, options.signal);
      if (
        options.signal?.aborted ||
        threadIdRef.current !== nextThreadId ||
        requestSeq !== threadRequestSeqRef.current
      ) {
        logger.info("runs.refresh.ignored.staleThread", {
          requestThreadId: nextThreadId,
          currentThreadId: threadIdRef.current,
          requestSeq,
          currentSeq: threadRequestSeqRef.current,
        });
        return;
      }
      setRuns(next);
      logger.info("runs.refresh.complete", { threadId: nextThreadId, count: next.length });
    } catch (caught) {
      if (options.signal?.aborted || (caught instanceof DOMException && caught.name === "AbortError")) {
        logger.debug("runs.refresh.aborted", { threadId: nextThreadId });
        return;
      }
      logger.error("runs.refresh.failed", {
        threadId: nextThreadId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      if (threadIdRef.current === nextThreadId && requestSeq === threadRequestSeqRef.current) {
        setRuns([]);
      }
    }
  }

  function resetVisibleThread(nextThreadId: string | null): void {
    threadIdRef.current = nextThreadId;
    threadRequestSeqRef.current += 1;
    setActiveRun(null);
    setRunLiveMessages({});
    setRunSubagentCards({});
    setOptimisticMessages([]);
    setRuns([]);
    setCurrentRunId(null);
    setRunCheckpointSnapshots({});
    setHydratedRunLimit(INITIAL_HYDRATED_RUN_LIMIT);
    loggedMessageTextRef.current.clear();
    streamMessagesRef.current = [];
    liveBaselineIdsRef.current = new Set();
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
    shouldStickToBottomRef.current = true;
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
      if (threadIdRef.current !== run.threadId) {
        logger.info("activeRun.continue.skipped.staleThread", {
          currentThreadId: threadIdRef.current,
          runThreadId: run.threadId,
          runId: run.runId,
        });
        return;
      }
      const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/resume`, {
        method: "POST",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      if (threadIdRef.current !== run.threadId) {
        logger.info("activeRun.continue.join.skipped.staleThread", {
          currentThreadId: threadIdRef.current,
          runThreadId: run.threadId,
          runId: run.runId,
        });
        return;
      }
      joinedRunIds.current.add(run.runId);
      setActiveRun(null);
      // Everything already streamed belongs to earlier runs, not the resumed one.
      liveBaselineIdsRef.current = messageIdSet(streamMessagesRef.current, (message) => message.id);
      setCurrentRunId(run.runId);
      setRunCheckpointSnapshots((current) => {
        const { [run.runId]: _staleSnapshot, ...rest } = current;
        return rest;
      });
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
    setCancellingRunId(run.runId);
    try {
      const response = await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/cancel`, {
        method: "POST",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      // Prevent the activeRunMonitor from re-showing the banner while we wait for
      // the run to reach a terminal state (the lifecycle SSE will call clearActiveRun).
      joinedRunIds.current.add(run.runId);
      setCurrentRunId((runId) => (runId === run.runId ? null : runId));
      stream.clearDebugEvents();
      logger.info("activeRun.stop.requested", run);
    } catch (caught) {
      setCancellingRunId(null);
      logger.error("activeRun.stop.failed", {
        threadId: run.threadId,
        runId: run.runId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to stop the active run.");
    }
  }

  async function stopCurrentRun(): Promise<void> {
    logger.info("currentRun.stop.start", { threadId, runId: currentRunId });
    // Always tear down the client-side stream immediately for instant UI
    // feedback. NB: the SDK's own stream.stop() cannot cancel the backend run —
    // it only calls the backend cancel route via its internal runMetadataStorage,
    // which is null here because we pass reconnectOnMount: false (deliberately,
    // to skip a heavy /history fetch on load — see stream.ts). Without the
    // explicit cancel POST below, the worker keeps executing the run after the
    // client disconnects: it stays `status: "running"` server-side, so a new
    // submit gets rejected (`run_in_progress`) and the activeRunMonitor
    // rediscovers it as an "inactive run" once isLoading flips false.
    void stream.stop();
    const request = cancelCurrentRunRequest(apiUrl, threadId, currentRunId);
    if (!request) {
      return;
    }
    const { url, runId } = request;
    setError(null);
    // Claim the run before the network round-trip so the activeRunMonitor
    // (E15/E16) doesn't rediscover it — still `running` server-side, no longer
    // streaming client-side — and flash the "inactive run" banner while the
    // cancel request is in flight.
    joinedRunIds.current.add(runId);
    try {
      const response = await fetch(url, { method: "POST" });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      setCurrentRunId((current) => (current === runId ? null : current));
      stream.clearDebugEvents();
      logger.info("currentRun.stop.requested", { threadId, runId });
    } catch (caught) {
      // The claim above only suppresses rediscovery while the request is in
      // flight; if it failed the backend run is genuinely still active, so
      // release the claim and let the banner offer Resume/Cancel instead.
      joinedRunIds.current.delete(runId);
      logger.error("currentRun.stop.failed", {
        threadId,
        runId,
        message: caught instanceof Error ? caught.message : String(caught),
      });
      setError(caught instanceof Error ? caught.message : "Unable to stop the run.");
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
//E1
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
//E2
  useEffect(() => {
    if (stream.messages.length === 0) {
      return;
    }
    logStreamingTokens(stream.messages);
    // Mirrored so a run starting later can snapshot the ids that already exist.
    streamMessagesRef.current = stream.messages;
    if (currentRunId !== null) {
      // Route the accumulated stream into the current run's own bucket: only the
      // messages that appeared after this run started belong to it.
      const owned = selectLiveRunMessages(
        stream.messages,
        liveBaselineIdsRef.current,
        EMPTY_MESSAGE_IDS,
        (message) => message.id,
      );
      setRunLiveMessages((current) =>
        sameMessageList(current[currentRunId], owned)
          ? current
          : { ...current, [currentRunId]: owned },
      );
    }
    setOptimisticMessages((messages) =>
      currentRunId === null
        ? messages
        : messages.filter(
            (optimistic) =>
              !stream.messages.some(
                (message) => message.type === optimistic.type && messageText(message) === messageText(optimistic),
              ),
          ),
    );
  }, [currentRunId, stream.isLoading, stream.messages]);
//E2b
  useEffect(() => {
    if (currentRunId === null || liveRunSubagentCards.length === 0) {
      return;
    }
    // Retain the current run's subagent cards the same way E2 retains its
    // messages: without this, a run's cards had no home once E14 dropped its
    // snapshot on completion, so they disappeared until the snapshot refetched
    // (or, for runs beyond the hydration window, indefinitely).
    setRunSubagentCards((current) =>
      sameSubagentCards(current[currentRunId], liveRunSubagentCards)
        ? current
        : { ...current, [currentRunId]: liveRunSubagentCards },
    );
  }, [currentRunId, liveRunSubagentCards]);
//E3
  useLayoutEffect(() => {
    if (!shouldStickToBottomRef.current) {
      return;
    }
    messagesEndRef.current?.scrollIntoView({ block: "end" });
  }, [displayedMessages]);
//E$
  useEffect(() => {
    const viewport = messagesViewportRef.current;
    if (!viewport) {
      return undefined;
    }
    const handleScroll = (): void => {
      shouldStickToBottomRef.current = isNearBottom(viewport);
    };
    handleScroll();
    viewport.addEventListener("scroll", handleScroll, { passive: true });
    return () => viewport.removeEventListener("scroll", handleScroll);
  }, []);
//E5
  useEffect(() => {
    activeRunRef.current = activeRun;
    isLoadingRef.current = stream.isLoading;
    currentRunIdRef.current = currentRunId;
    threadIdRef.current = threadId;
  }, [activeRun, currentRunId, stream.isLoading, threadId]);
//E6
  useEffect(() => {
    if (stream.error) {
      logger.error("stream.error", {
        message: stream.error instanceof Error ? stream.error.message : String(stream.error),
      });
      setError(stream.error instanceof Error ? stream.error.message : String(stream.error));
    }
  }, [stream.error]);
//E7
  useEffect(() => {
    if (threadId && threadIdFromUrl() !== threadId) {
      writeThreadUrl(threadId, true);
    }
    // This only normalizes the initial localStorage fallback into the address bar.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
//E8
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
//E9
  useEffect(() => {
    const requestSeq = threadRequestSeqRef.current;
    const controller = new AbortController();
    void refreshRuns(threadId, { requestSeq, signal: controller.signal });
    return () => {
      controller.abort();
    };
    // refreshRuns intentionally reads the latest refs for stale-response guards.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiUrl, threadId]);
//E10
  useEffect(() => {
    if (!threadId) {
      return undefined;
    }
    const requestThreadId = threadId;
    const requestSeq = threadRequestSeqRef.current;
    // Lazy hydration: only fetch snapshots for the newest window of finished
    // runs (plus the run being viewed), not every finished run on the thread.
    const missingRuns = selectRunsToHydrate(
      runsInMessageOrder,
      runCheckpointSnapshots,
      hydratedRunLimit,
      PERSISTED_RUN_STATUSES,
      currentRunId,
    );
    if (missingRuns.length === 0) {
      return undefined;
    }
    let cancelled = false;
    const controller = new AbortController();
    const isStale = (): boolean =>
      cancelled ||
      controller.signal.aborted ||
      threadIdRef.current !== requestThreadId ||
      requestSeq !== threadRequestSeqRef.current;

    logger.info("runs.checkpoints.load.start", {
      threadId: requestThreadId,
      count: missingRuns.length,
      hydratedRunLimit,
    });

    // Fetch each run independently and apply as it resolves, so one slow or
    // failing run never blocks hydration of the others (unlike Promise.all).
    void Promise.allSettled(
      missingRuns.map(async (run) => {
        try {
          const snapshot = await getRunCheckpointSnapshot(apiUrl, run.threadId, run.runId, controller.signal);
          if (isStale() || snapshot.run.threadId !== requestThreadId) {
            return;
          }
          if (snapshot.messages.length === 0) {
            // The run can flip to a terminal status before its snapshot row is
            // written. Caching the empty result would pin the run to an empty
            // transcript forever (it is never refetched). Skip it instead: the
            // run keeps rendering from its live bucket, and because this writes
            // no state there is no refetch loop — the next effect run (new run,
            // refreshRuns, thread change) retries it.
            logger.warn("runs.checkpoints.load.empty", { threadId: requestThreadId, runId: run.runId });
            return;
          }
          setRunCheckpointSnapshots((current) =>
            current[run.runId] ? current : { ...current, [run.runId]: snapshot },
          );
        } catch (caught) {
          if (
            cancelled ||
            controller.signal.aborted ||
            (caught instanceof DOMException && caught.name === "AbortError")
          ) {
            logger.debug("runs.checkpoints.load.aborted", { threadId: requestThreadId, runId: run.runId });
            return;
          }
          logger.error("runs.checkpoints.load.failed", {
            threadId: requestThreadId,
            runId: run.runId,
            message: caught instanceof Error ? caught.message : String(caught),
          });
          if (threadIdRef.current === requestThreadId && requestSeq === threadRequestSeqRef.current) {
            setError(caught instanceof Error ? caught.message : "Unable to load run checkpoints.");
          }
        }
      }),
    );
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [apiUrl, currentRunId, hydratedRunLimit, runCheckpointSnapshots, runs, runsInMessageOrder, threadId]);
//E11
  useEffect(() => {
    const handlePopState = (): void => {
      const nextThreadId = threadIdFromUrl();
      logger.info("thread.url.popstate", { threadId: nextThreadId });
      threadIdRef.current = nextThreadId;
      threadRequestSeqRef.current += 1;
      setActiveRun(null);
      setRunLiveMessages({});
      setRunSubagentCards({});
      setOptimisticMessages([]);
      setRuns([]);
      setCurrentRunId(null);
      setRunCheckpointSnapshots({});
      setHydratedRunLimit(INITIAL_HYDRATED_RUN_LIMIT);
      streamMessagesRef.current = [];
      liveBaselineIdsRef.current = new Set();
      stream.clearDebugEvents();
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
//E12
  useEffect(() => {
    if (!activeRun) {
      return undefined;
    }
    
    return undefined;
  }, [activeRun, apiUrl]);
//E13
  useEffect(() => {
    if (!currentRunId || stream.isLoading) {
      return;
    }
    if (currentRunStatus && !ACTIVE_RUN_STATUSES.has(currentRunStatus)) {
      if (PERSISTED_RUN_STATUSES.has(currentRunStatus) && !currentRunSnapshotLoaded) {
        return;
      }
      logger.token("activeRun.current.completed", { threadId, runId: currentRunId, status: currentRunStatus });
      setCurrentRunId(null);
    }
  }, [currentRunId, currentRunSnapshotLoaded, currentRunStatus, stream.isLoading, threadId]);
//E14
  useEffect(() => {
    if (!threadId || !currentRunId || handledTerminalRunIdsRef.current.has(currentRunId)) {
      return;
    }
    const terminalEvent = stream.debugEvents.find((event) => {
      if (event.channel !== "lifecycle" || typeof event.data !== "object" || event.data === null) {
        return false;
      }
      const data = event.data as Record<string, unknown>;
      return data.run_id === currentRunId && TERMINAL_RUN_EVENTS.has(String(data.event ?? ""));
    });
    if (!terminalEvent || typeof terminalEvent.data !== "object" || terminalEvent.data === null) {
      return;
    }
    const status = TERMINAL_EVENT_TO_RUN_STATUS[String((terminalEvent.data as Record<string, unknown>).event ?? "")];
    handledTerminalRunIdsRef.current.add(currentRunId);
    logger.info("activeRun.terminal.resetToPersisted", { threadId, runId: currentRunId, status });
    joinedRunIds.current.delete(currentRunId);
    setActiveRun((current) =>
      current?.threadId === threadId && current.runId === currentRunId ? null : current,
    );
    setRuns((current) =>
      current.map((run) =>
        run.runId === currentRunId
          ? { ...run, status: status ?? run.status, updatedAt: new Date().toISOString() }
          : run,
      ),
    );
    setOptimisticMessages([]);
    // NB: the run's captured live messages AND subagent cards are deliberately
    // kept (runLiveMessages / runSubagentCards). Dropping its snapshot below
    // forces a refetch, and until that lands the run still renders from its own
    // retained data — otherwise it disappears from the transcript (or loses its
    // subagent cards) until the next event happens to trigger hydration.
    setRunCheckpointSnapshots((current) => {
      const { [currentRunId]: _staleSnapshot, ...rest } = current;
      return rest;
    });
    loggedMessageTextRef.current.clear();
    // setCurrentRunId(null);
    // Clear debug events so that the next run can start fresh.
    stream.clearDebugEvents();
    void refreshRuns(threadId);
    // refreshRuns intentionally owns async persisted reload after terminal lifecycle events.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentRunId, stream.debugEvents, threadId]);
//E15
  useEffect(() => {
    if (!threadId || stream.isLoading) {
      return;
    }
    const active = runs.find((run) => ACTIVE_RUN_STATUSES.has(run.status));
    if (!active) {
      return;
    }

    if (joinedRunIds.current.has(active.runId) || active.runId === currentRunIdRef.current) {
      logger.debug("activeRunMonitor.skipped.alreadyJoined", { threadId, runId: active.runId });
      return;
    }

    if (activeRunRef.current?.threadId === threadId && activeRunRef.current.runId === active.runId) {
      logger.debug("activeRunMonitor.skipped.alreadyShowing", { threadId, runId: active.runId });
      return;
    }

    // Ask the backend whether there is a live execution task before deciding.
    void (async () => {
      try {
        const result = await fetchRunActive(apiUrl, { threadId, runId: active.runId });
        if (!result || !ACTIVE_RUN_STATUSES.has(result.status)) {
          return;
        }
        if (result.isStreaming) {
          logger.info("activeRunMonitor.autoJoin", { threadId, runId: active.runId });
          void joinRunStreamRef.current?.({ threadId, runId: active.runId });
        } else {
          logger.info("activeRun.discovered", { threadId, runId: active.runId, status: result.status });
          setActiveRun({ threadId, runId: active.runId });
        }
      } catch {
        // Swallow — the lifecycle SSE or next effect run will retry.
      }
    })();
  }, [activeRun, currentRunId, runs, stream.isLoading, threadId]);
//16
  useEffect(() => {
    if (!threadId) {
      logger.debug("activeRunMonitor.skipped.noThread");
      joinedRunIds.current.clear();
      return undefined;
    }

    let cancelled = false;
    const controller = new AbortController();
    const showActiveRun = async (runId: string, source: string): Promise<void> => {
      if (!runId || cancelled) {
        return;
      }
      if (currentRunIdRef.current === runId || joinedRunIds.current.has(runId)) {
        logger.debug("activeRunMonitor.skipped.joined", { threadId, runId, source });
        return;
      }
      try {
        const active = await fetchRunActive(apiUrl, { threadId, runId });
        if (cancelled) {
          return;
        }
        if (active === null || !ACTIVE_RUN_STATUSES.has(active.status)) {
          logger.debug("activeRunMonitor.skipped.notActive", { threadId, runId, source, status: active?.status });
          return;
        }
        if (active.isStreaming) {
          // Backend confirmed an active execution task — auto-join without a dialog.
          logger.info("activeRunMonitor.autoJoin", { threadId, runId, source });
          void joinRunStreamRef.current?.({ threadId, runId });
        } else {
          // No active task — show dialog so user can resume or cancel.
          if (activeRunRef.current?.threadId === threadId && activeRunRef.current.runId === runId) {
            logger.debug("activeRunMonitor.skipped.bannerVisible", { threadId, runId, source });
            return;
          }
          logger.info("activeRunMonitor.discovered", { threadId, runId, source, status: active.status });
          setActiveRun((current) =>
            current?.threadId === threadId && current.runId === runId ? current : { threadId, runId },
          );
        }
      } catch (caught) {
        logger.warn("activeRunMonitor.discovery.failed", {
          threadId,
          runId,
          source,
          message: caught instanceof Error ? caught.message : String(caught),
        });
      }
    };

    const clearActiveRun = (runId: string, source: string): void => {
      logger.info("activeRunMonitor.clear.requested", { threadId, runId, source });
      setCancellingRunId((current) => (current === runId ? null : current));
      setActiveRun((current) =>
        current?.threadId === threadId && current.runId === runId ? null : current,
      );
    };

    const checkActiveRunOnce = async (): Promise<void> => {
      try {
        logger.debug("activeRunMonitor.initialCheck.start", { threadId });
        const response = await fetch(`${apiUrl}/threads/${threadId}/runs?limit=20`, {
          signal: controller.signal,
        });
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
        const runs = (await response.json()) as Array<{ run_id?: string; status?: string }>;
        const active = runs.find((run) => ACTIVE_RUN_STATUSES.has(run.status ?? ""));
        const runId = active?.run_id;
        if (runId) {
          void showActiveRun(runId, "initial-check");
        } else {
          logger.debug("activeRunMonitor.initialCheck.none", { threadId });
        }
      } catch (caught) {
        if (controller.signal.aborted || (caught instanceof DOMException && caught.name === "AbortError")) {
          logger.debug("activeRunMonitor.initialCheck.aborted", { threadId });
          return;
        }
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
          void showActiveRun(runId, "lifecycle");
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
      controller.abort();
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
        <label className="api-url">
          <span>Logging</span>
          <select
            value={logMode}
            onChange={(event) => {
              const nextMode = event.target.value as LogMode;
              setSelectedLogMode(nextMode);
              setLogMode(nextMode);
            }}
          >
            {LOG_MODES.map((mode) => (
              <option key={mode} value={mode}>
                {LOG_MODE_LABELS[mode]}
              </option>
            ))}
          </select>
        </label>
      </aside>

      <main className="workspace">
        <section className="chat">
          <header className="topbar">
            <div>
              <strong>{statusText(stream.isLoading, stream.activeSubagents.length)}</strong>
              <span>{threadId ? threadId.slice(0, 8) : "No thread yet"}</span>
            </div>
            <div className="topbar-actions">
              {stream.isLoading && (
                <button className="icon-button danger" onClick={() => void stopCurrentRun()} type="button" title="Stop run">
                  <CircleStop size={18} />
                </button>
              )}
            </div>
          </header>

          <div
            className="messages"
            ref={messagesViewportRef}
            onScroll={(event) => {
              shouldStickToBottomRef.current = isNearBottom(event.currentTarget);
            }}
          >
            {displayedMessageEntries.length > 0 &&
              hasEarlierUnhydratedRuns(runsInMessageOrder, PERSISTED_RUN_STATUSES, hydratedRunLimit) && (
                <div className="load-earlier-runs">
                  <button
                    type="button"
                    className="load-earlier-runs-button"
                    onClick={() => setHydratedRunLimit((limit) => limit + EARLIER_RUNS_BATCH)}
                  >
                    Load earlier runs
                  </button>
                </div>
              )}
            {displayedMessageEntries.length === 0 ? (
              <div className="empty-state">
                <h1>What should we research?</h1>
                <p>Ask for a market scan, technical comparison, literature review, or sourced brief.</p>
              </div>
            ) : (
              displayedMessageEntries.map((entry, index) => (
                <MessageBubble
                  key={`${entry.runId ?? "none"}:${entry.message.id ?? `${entry.message.type}-${index}`}`}
                  actions={actionsForMessage(index)}
                  message={entry.message}
                  subagents={subagentCardsForMessage(index)}
                />
              ))
            )}
            <div aria-hidden="true" ref={messagesEndRef} />
          </div>

          {inputRequests.length > 0 && <InputRequests requests={inputRequests} onResume={resume} />}

          {visibleActiveRun && (
            <div className="active-run-banner">
              <div>
                <strong>Inactive run found</strong>
                <span>
                  {cancellingRunId === visibleActiveRun.runId
                    ? "Cancelling run — waiting for the agent to stop…"
                    : `Run ${visibleActiveRun.runId.slice(0, 8)} is not actively streaming. Resume it or cancel.`}
                </span>
              </div>
              {cancellingRunId !== visibleActiveRun.runId && (
                <button onClick={() => void continueActiveRun(visibleActiveRun)} type="button">
                  Resume run
                </button>
              )}
              <button
                className="secondary"
                disabled={cancellingRunId === visibleActiveRun.runId}
                onClick={() => void stopActiveRun(visibleActiveRun)}
                type="button"
              >
                {cancellingRunId === visibleActiveRun.runId ? "Cancelling…" : "Cancel run"}
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
            <button disabled={!draft.trim() || stream.isLoading || visibleActiveRun !== null} type="submit" title="Send">
              {stream.isLoading ? <Loader2 className="spin" size={18} /> : <Send size={18} />}
            </button>
          </form>
        </section>
      </main>
    </div>
  );
}

function MessageBubble({
  actions,
  message,
  subagents,
}: {
  actions: RunAction[];
  message: Message;
  subagents: SubagentCard[];
}) {
  const text = messageText(message);
  const hideText = isInternalTodoUpdate(message, text);
  if (message.type === "remove" || ((!text || hideText) && message.type === "ai" && subagents.length === 0 && actions.length === 0)) {
    return null;
  }
  return (
    <article className={`message ${message.type}`}>
      <div className="avatar">{message.type === "human" ? "You" : "AI"}</div>
      <div className="message-body">
        {!hideText && (text || (subagents.length === 0 && actions.length === 0)) && (
          <div className="message-content">{text || <span className="muted">Working...</span>}</div>
        )}
        {actions.length > 0 && message.type === "human" && (
          <div className="message-actions" aria-label="Run actions">
            {actions.map((action) => (
              <div className={`action-row ${action.status}`} key={action.id}>
                <span />
                <strong>{action.label}</strong>
              </div>
            ))}
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

function SubagentCardView({
  subagent,
  variant = "panel",
}: {
  subagent: SubagentCard;
  variant?: "panel" | "inline";
}) {
  const taskInput = subagent.description.trim() || "Subagent task";
  const toolActions = subagent.tools.map((tool) => ({
    id: tool.id,
    label: actionFromTool(tool.name, tool.input),
    status: tool.status,
  }));
  const visibleMessages = subagent.messages.filter((message) => message.content.trim());
  return (
    <article className={`subagent-card ${subagent.status} ${variant}`}>
      <header>
        <div>
          <strong>{subagent.name}</strong>
        </div>
        <span>{subagent.status}</span>
      </header>
      <div className="progress">
        <div style={{ width: `${subagent.progress}%` }} />
      </div>
      <div className="subagent-sections">
        <section className="subagent-section subagent-input">
          <h4>Input</h4>
          <p>{taskInput}</p>
        </section>
        <section className="subagent-section subagent-activity">
          <h4>Actions</h4>
          {toolActions.length > 0 ? (
            toolActions.map((action) => (
              <div className={`action-row ${action.status}`} key={action.id}>
                <span />
                <strong>{action.label}</strong>
              </div>
            ))
          ) : (
            <p className="muted">Waiting for tool activity...</p>
          )}
          <h4>Messages</h4>
          {visibleMessages.length > 0 ? (
            visibleMessages.map((message) => (
              <p className={message.status === "streaming" ? "streaming" : undefined} key={message.id}>
                {message.content}
              </p>
            ))
          ) : (
            <p className="muted">Waiting for streamed output...</p>
          )}
        </section>
      </div>
    </article>
  );
}
