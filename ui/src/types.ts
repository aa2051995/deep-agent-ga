export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export type Namespace = string[];

export type ProtocolEvent = {
  type: "event";
  event_id: string;
  seq: number;
  method: string;
  params: {
    namespace: Namespace;
    timestamp: number;
    data: unknown;
    node?: string | null;
  };
};

export type ProtocolResponse =
  | {
      type: "success";
      id: number;
      result: Record<string, unknown>;
      meta?: Record<string, unknown> | null;
    }
  | {
      type: "error";
      id: number;
      error: string;
      message: string;
      meta?: Record<string, unknown> | null;
    };

export type ChatMessage = {
  id: string;
  role: "human" | "ai" | "tool" | "system";
  content: string;
  componentKey: string;
  namespace: Namespace;
  status: "streaming" | "done";
};

export type ToolActivity = {
  id: string;
  name: string;
  namespace: Namespace;
  componentKey: string;
  input?: unknown;
  output?: unknown;
  status: "running" | "done";
};

export type TodoItem = {
  id: string;
  content: string;
  agent?: string;
  status: "pending" | "in_progress" | "completed";
};

export type InputRequest = {
  id: string;
  kind: "interrupt" | "permission" | "input";
  title: string;
  detail: string;
  namespace: Namespace;
  raw: unknown;
};

export type SubagentCard = {
  key: string;
  name: string;
  namespace: Namespace;
  status: "pending" | "running" | "done" | "error";
  description: string;
  progress: number;
  messages: ChatMessage[];
  tools: ToolActivity[];
};

export type RunStatus = "idle" | "running" | "success" | "error" | "interrupted";

export type RunSummary = {
  runId: string;
  threadId: string;
  status: string;
  createdAt: string;
  updatedAt: string;
};

export type PersistedMessage = {
  id: string;
  type: string;
  content: unknown;
  name?: string | null;
  additional_kwargs?: Record<string, unknown>;
  response_metadata?: Record<string, unknown>;
};

export type RunCheckpointSnapshot = {
  run: RunSummary;
  values: Record<string, unknown>;
  messages: PersistedMessage[];
  todos: TodoItem[];
  subagents: SubagentCard[];
  checkpoints: Array<{
    checkpoint: Record<string, unknown>;
    parent_checkpoint: Record<string, unknown> | null;
    metadata: Record<string, unknown>;
    next: string[];
    created_at: string | null;
  }>;
  // True when the backend served this from the pre-projected run-snapshot
  // table (fast path) rather than re-deriving it from checkpoint history.
  fromSnapshot: boolean;
};

export type ThreadSummary = {
  threadId: string;
  title: string;
  updatedAt: string;
};
