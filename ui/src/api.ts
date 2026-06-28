import type {
  PersistedMessage,
  ProtocolResponse,
  RunCheckpointSnapshot,
  RunSummary,
  SubagentCard,
  ThreadSummary,
  TodoItem,
} from "./types";
import { logger } from "./logger";

export const DEFAULT_API_URL = "http://localhost:2024";
export const ASSISTANT_ID = "deep-agent";

let commandId = 1;

type ThreadSearchItem = {
  thread_id?: string;
  created_at?: string;
  updated_at?: string;
  metadata?: Record<string, unknown>;
  values?: {
    messages?: Array<{
      type?: unknown;
      content?: unknown;
    }>;
  };
};

type RunApiItem = {
  run_id?: string;
  thread_id?: string;
  status?: string;
  created_at?: string;
  updated_at?: string;
};

type RunCheckpointApiItem = {
  run?: RunApiItem;
  values?: unknown;
  messages?: unknown;
  todos?: unknown;
  subagents?: unknown;
  checkpoints?: unknown;
};

function textFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((block) => {
      if (typeof block === "string") {
        return block;
      }
      if (typeof block === "object" && block !== null && "text" in block) {
        return String((block as { text?: unknown }).text ?? "");
      }
      return "";
    })
    .join("");
}

function threadTitle(thread: ThreadSearchItem): string {
  const metadataTitle = thread.metadata?.title;
  if (typeof metadataTitle === "string" && metadataTitle.trim()) {
    return metadataTitle.trim();
  }
  const firstHuman = thread.values?.messages?.find((message) => message.type === "human");
  const title = textFromContent(firstHuman?.content).replace(/\s+/g, " ").trim();
  if (!title) {
    return "New research";
  }
  return title.length > 80 ? `${title.slice(0, 77)}...` : title;
}

export async function listThreads(apiUrl: string): Promise<ThreadSummary[]> {
  logger.info("api.threads.list.start", { apiUrl });
  const response = await fetch(`${apiUrl}/threads/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      limit: 50,
      offset: 0,
      sort_by: "updated_at",
      sort_order: "desc",
    }),
  });
  if (!response.ok) {
    logger.error("api.threads.list.failed", { status: response.status });
    throw new Error(`Failed to list threads: ${response.statusText}`);
  }
  const body = (await response.json()) as ThreadSearchItem[];
  const threads = body
    .filter((thread): thread is ThreadSearchItem & { thread_id: string } => typeof thread.thread_id === "string")
    .map((thread) => ({
      threadId: thread.thread_id,
      title: threadTitle(thread),
      updatedAt: thread.updated_at ?? thread.created_at ?? new Date(0).toISOString(),
    }))
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  logger.info("api.threads.list.complete", { count: threads.length });
  return threads;
}

export async function deleteThread(apiUrl: string, threadId: string): Promise<void> {
  logger.info("api.threads.delete.start", { threadId });
  const response = await fetch(`${apiUrl}/threads/${threadId}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    logger.error("api.threads.delete.failed", { threadId, status: response.status });
    throw new Error(`Failed to delete thread: ${response.statusText}`);
  }
  logger.info("api.threads.delete.complete", { threadId });
}

export async function renameThread(apiUrl: string, threadId: string, title: string): Promise<ThreadSummary> {
  logger.info("api.threads.rename.start", { threadId, titleLength: title.length });
  const response = await fetch(`${apiUrl}/threads/${threadId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ metadata: { title } }),
  });
  if (!response.ok) {
    logger.error("api.threads.rename.failed", { threadId, status: response.status });
    throw new Error(`Failed to rename thread: ${response.statusText}`);
  }
  const body = (await response.json()) as ThreadSearchItem;
  if (typeof body.thread_id !== "string") {
    throw new Error("Rename response did not include `thread_id`.");
  }
  logger.info("api.threads.rename.complete", { threadId });
  return {
    threadId: body.thread_id,
    title: threadTitle(body),
    updatedAt: body.updated_at ?? body.created_at ?? new Date(0).toISOString(),
  };
}

function runSummary(run: RunApiItem): RunSummary | null {
  if (typeof run.run_id !== "string" || typeof run.thread_id !== "string") {
    return null;
  }
  return {
    runId: run.run_id,
    threadId: run.thread_id,
    status: typeof run.status === "string" ? run.status : "unknown",
    createdAt: run.created_at ?? new Date(0).toISOString(),
    updatedAt: run.updated_at ?? run.created_at ?? new Date(0).toISOString(),
  };
}

function recordValue(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function persistedMessage(value: unknown): PersistedMessage | null {
  const raw = recordValue(value);
  if (typeof raw.type !== "string") {
    return null;
  }
  return {
    id: typeof raw.id === "string" ? raw.id : crypto.randomUUID(),
    type: raw.type,
    content: raw.content,
    name: typeof raw.name === "string" ? raw.name : null,
    additional_kwargs: recordValue(raw.additional_kwargs),
    response_metadata: recordValue(raw.response_metadata),
  };
}

function todoItem(value: unknown, index: number): TodoItem | null {
  const raw = recordValue(value);
  const content = raw.content ?? raw.title ?? raw.task ?? raw.description;
  if (typeof content !== "string" || !content.trim()) {
    return null;
  }
  const status = raw.status === "completed" || raw.status === "in_progress" ? raw.status : "pending";
  return {
    id: typeof raw.id === "string" ? raw.id : `todo-${index}`,
    content,
    agent: typeof raw.agent === "string" ? raw.agent : undefined,
    status,
  };
}

function subagentCard(value: unknown): SubagentCard | null {
  const raw = recordValue(value);
  if (typeof raw.key !== "string" || typeof raw.name !== "string") {
    return null;
  }
  return raw as unknown as SubagentCard;
}

export async function listRuns(apiUrl: string, threadId: string): Promise<RunSummary[]> {
  logger.info("api.runs.list.start", { threadId });
  const response = await fetch(`${apiUrl}/threads/${threadId}/runs?limit=100`);
  if (!response.ok) {
    logger.error("api.runs.list.failed", { threadId, status: response.status });
    throw new Error(`Failed to list runs: ${response.statusText}`);
  }
  const body = (await response.json()) as RunApiItem[];
  const runs = body
    .map(runSummary)
    .filter((run): run is RunSummary => run !== null);
  logger.info("api.runs.list.complete", { threadId, count: runs.length });
  return runs;
}

export async function getRunCheckpointSnapshot(
  apiUrl: string,
  threadId: string,
  runId: string,
): Promise<RunCheckpointSnapshot> {
  logger.info("api.runs.checkpoints.start", { threadId, runId });
  const response = await fetch(`${apiUrl}/threads/${threadId}/runs/${runId}/checkpoints`);
  if (!response.ok) {
    logger.error("api.runs.checkpoints.failed", { threadId, runId, status: response.status });
    throw new Error(`Failed to load run checkpoints: ${response.statusText}`);
  }
  const body = (await response.json()) as RunCheckpointApiItem;
  const run = body.run ? runSummary(body.run) : null;
  if (run === null) {
    throw new Error("Run checkpoints response did not include a valid run.");
  }
  const values = recordValue(body.values);
  const messages = Array.isArray(body.messages)
    ? body.messages.map(persistedMessage).filter((message): message is PersistedMessage => message !== null)
    : [];
  const todos = Array.isArray(body.todos)
    ? body.todos.map(todoItem).filter((todo): todo is TodoItem => todo !== null)
    : [];
  const subagents = Array.isArray(body.subagents)
    ? body.subagents.map(subagentCard).filter((subagent): subagent is SubagentCard => subagent !== null)
    : [];
  const checkpoints = Array.isArray(body.checkpoints)
    ? body.checkpoints.map((checkpoint) => {
        const raw = recordValue(checkpoint);
        return {
          checkpoint: recordValue(raw.checkpoint),
          parent_checkpoint: raw.parent_checkpoint == null ? null : recordValue(raw.parent_checkpoint),
          metadata: recordValue(raw.metadata),
          next: Array.isArray(raw.next) ? raw.next.map(String) : [],
          created_at: typeof raw.created_at === "string" ? raw.created_at : null,
        };
      })
    : [];
  logger.info("api.runs.checkpoints.complete", {
    threadId,
    runId,
    messages: messages.length,
    subagents: subagents.length,
    checkpoints: checkpoints.length,
  });
  return { run, values, messages, todos, subagents, checkpoints };
}

export async function createThread(apiUrl: string): Promise<string> {
  logger.info("api.createThread.start", { apiUrl });
  const response = await fetch(`${apiUrl}/threads`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ assistant_id: ASSISTANT_ID }),
  });
  if (!response.ok) {
    logger.error("api.createThread.failed", { status: response.status });
    throw new Error(`Failed to create thread: ${response.statusText}`);
  }
  const body = (await response.json()) as { thread_id?: string };
  if (!body.thread_id) {
    logger.error("api.createThread.missingThreadId");
    throw new Error("Thread response did not include `thread_id`.");
  }
  logger.info("api.createThread.complete", { threadId: body.thread_id });
  return body.thread_id;
}

export async function startRun(
  apiUrl: string,
  threadId: string,
  content: string,
): Promise<ProtocolResponse> {
  logger.info("api.startRun.start", {
    threadId,
    contentLength: content.length,
  });
  const response = await fetch(`${apiUrl}/threads/${threadId}/commands`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id: commandId++,
      method: "run.start",
      params: {
        assistant_id: ASSISTANT_ID,
        input: { messages: [{ type: "human", content }] },
        multitaskStrategy: "reject",
        config: { configurable: { thread_id: threadId } },
        metadata: { surface: "deep-research-ui" },
      },
    }),
  });
  if (!response.ok) {
    logger.error("api.startRun.failed", { threadId, status: response.status });
    throw new Error(`Failed to start run: ${response.statusText}`);
  }
  const body = (await response.json()) as ProtocolResponse;
  logger.info("api.startRun.complete", { threadId, type: body.type });
  return body;
}

export async function respondToInput(
  apiUrl: string,
  threadId: string,
  requestId: string,
  responseValue: unknown,
): Promise<ProtocolResponse> {
  logger.info("api.respondToInput.start", {
    threadId,
    requestId,
    responseType: typeof responseValue,
  });
  const response = await fetch(`${apiUrl}/threads/${threadId}/commands`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id: commandId++,
      method: "input.respond",
      params: {
        assistant_id: ASSISTANT_ID,
        responses: [{ id: requestId, value: responseValue }],
        config: { configurable: { thread_id: threadId } },
        metadata: { surface: "deep-research-ui" },
      },
    }),
  });
  if (!response.ok) {
    logger.error("api.respondToInput.failed", { threadId, requestId, status: response.status });
    throw new Error(`Failed to respond to input request: ${response.statusText}`);
  }
  const body = (await response.json()) as ProtocolResponse;
  logger.info("api.respondToInput.complete", { threadId, requestId, type: body.type });
  return body;
}

export async function cancelRun(
  apiUrl: string,
  threadId: string,
  runId: string,
): Promise<void> {
  logger.info("api.cancelRun.start", { threadId, runId });
  const response = await fetch(`${apiUrl}/threads/${threadId}/runs/${runId}/cancel`, {
    method: "POST",
  });
  if (!response.ok) {
    logger.error("api.cancelRun.failed", { threadId, runId, status: response.status });
    throw new Error(`Failed to cancel run: ${response.statusText}`);
  }
  logger.info("api.cancelRun.complete", { threadId, runId });
}
