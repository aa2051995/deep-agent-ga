import type { Interrupt } from "@langchain/langgraph-sdk";
import type { SubagentStreamInterface } from "@langchain/langgraph-sdk/react";
import { logger } from "./logger";
import { messageText, toolCallArgs, toolCallName } from "./stream";
import type { DeepAgentGaStream } from "./stream";
import type { InputRequest, SubagentCard, TodoItem } from "./types";

function formatValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value == null) {
    return "";
  }
  return JSON.stringify(value, null, 2);
}

function statusFromSubagent(status: string): SubagentCard["status"] {
  if (status === "complete") {
    return "done";
  }
  if (status === "error") {
    return "error";
  }
  if (status === "running") {
    return "running";
  }
  return "pending";
}

function progressFromSubagent(status: SubagentCard["status"], messageCount: number, toolCount: number): number {
  if (status === "done" || status === "error") {
    return 100;
  }
  if (status === "running") {
    return Math.max(35, Math.min(85, 25 + messageCount * 10 + toolCount * 15));
  }
  return 10;
}

function messageRole(type: string): "human" | "ai" | "tool" | "system" {
  return type === "human" || type === "ai" || type === "tool" ? type : "system";
}

function normalizeTodoStatus(status: unknown): TodoItem["status"] {
  if (status === "completed" || status === "done" || status === "success") {
    return "completed";
  }
  if (status === "in_progress" || status === "running" || status === "active") {
    return "in_progress";
  }
  return "pending";
}

function todoFromValue(value: unknown, index: number): TodoItem | null {
  if (typeof value === "string") {
    return {
      id: `todo-${index}`,
      content: value,
      status: "pending",
    };
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const raw = value as Record<string, unknown>;
  const content = formatValue(raw.content ?? raw.title ?? raw.task ?? raw.description).replace(/\s+/g, " ").trim();
  if (!content) {
    return null;
  }
  return {
    id: String(raw.id ?? `todo-${index}`),
    content,
    agent: typeof raw.agent === "string" ? raw.agent : undefined,
    status: normalizeTodoStatus(raw.status),
  };
}

export function selectTodosFromValues(values: unknown): TodoItem[] | null {
  if (typeof values !== "object" || values === null || !("todos" in values)) {
    return null;
  }
  const rawTodos = (values as { todos?: unknown }).todos;
  if (!Array.isArray(rawTodos)) {
    return null;
  }
  const todos = rawTodos
    .map((todo, index) => todoFromValue(todo, index))
    .filter((todo): todo is TodoItem => todo !== null);
  logger.debug("selector.todos.values", { todos: todos.length, rawTodos: rawTodos.length });
  return todos;
}

export function selectTodos(stream: DeepAgentGaStream): TodoItem[] {
  const valueTodos = selectTodosFromValues(stream.values);
  if (valueTodos !== null) {
    return valueTodos;
  }
  logger.debug("selector.todos.values.missing");
  return [];
}

export function selectSubagents(stream: DeepAgentGaStream): SubagentCard[] {
  const subagents = stream.subagents ?? new Map();
  const cards = [...subagents.values()].map(subagentStreamToCard);
  logger.debug("selector.subagents", {
    cards: cards.length,
    active: stream.activeSubagents?.length ?? 0,
  });
  return cards;
}

export function subagentStreamToCard(subagent: SubagentStreamInterface): SubagentCard {
  const status = statusFromSubagent(subagent.status);
  return {
    key: subagent.id,
    name: subagent.toolCall.args.subagent_type ?? subagent.toolCall.name,
    namespace: subagent.namespace,
    status,
    description: subagent.toolCall.args.description ?? "Subagent activity",
    progress: progressFromSubagent(status, subagent.messages.length, subagent.toolCalls.length),
    messages: subagent.messages.map((message) => ({
      id: message.id ?? `${subagent.id}-${messageText(message)}`,
      role: messageRole(message.type),
      content: messageText(message),
      componentKey: subagent.id,
      namespace: subagent.namespace,
      status: subagent.status === "running" ? "streaming" as const : "done" as const,
    })),
    tools: subagent.toolCalls.map((toolCall) => ({
      id: toolCall.id,
      name: toolCallName(toolCall),
      namespace: subagent.namespace,
      componentKey: subagent.id,
      input: toolCallArgs(toolCall),
      output: toolCall.result?.content,
      status: toolCall.state === "pending" ? "running" : "done" as const,
    })),
  } satisfies SubagentCard;
}

export function selectInputRequests(stream: DeepAgentGaStream): InputRequest[] {
  const requests = (stream.interrupts ?? []).map((interrupt, index) => inputRequestFromInterrupt(interrupt, index));
  // logger.debug("selector.inputRequests", { requests: requests.length });
  return requests;
}

function inputRequestFromInterrupt(interrupt: Interrupt, index: number): InputRequest {
  const value = interrupt.value;
  const raw = typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
  const kind = String(raw.kind ?? raw.type ?? "").includes("permission") ? "permission" : "interrupt";
  return {
    id: interrupt.id ?? `interrupt-${index}`,
    kind,
    title: kind === "permission" ? "Permission request" : "Human input needed",
    detail: String(raw.prompt ?? raw.message ?? raw.action ?? formatValue(value)),
    namespace: interrupt.ns ?? [],
    raw: value,
  };
}
