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

export type ToolDebugRow = {
  id: string;
  name: string;
  state: string;
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

export type RunDebugSnapshot = {
  run: RunSummary;
  values: Record<string, unknown>;
  events: ProtocolEvent[];
};

export type ThreadSummary = {
  threadId: string;
  title: string;
  updatedAt: string;
};
