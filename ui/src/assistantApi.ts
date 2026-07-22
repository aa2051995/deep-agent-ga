// Typed client for the backend assistant-management REST API
// (see stream-backend/app/assistant_api.py).
import { DEFAULT_API_URL } from "./api";

export type Permission = "allow" | "ask" | "deny";

export interface ModelConfig {
  provider: "google" | "anthropic" | "bedrock" | "openai";
  name: string;
  temperature: number;
  max_tokens: number | null;
  api_key?: string | null;
}

export interface ToolConfig {
  name: string;
  permission: Permission;
}

export interface MCPServerConfig {
  name: string;
  transport: "stdio" | "streamable_http" | "sse";
  command: string | null;
  args: string[];
  url: string | null;
  env: Record<string, string>;
  permission: Permission;
  enabled: boolean;
}

export interface SkillConfig {
  name: string;
  description: string;
  path: string;
  enabled: boolean;
}

export interface MemoryConfig {
  name: string;
  path: string;
  enabled: boolean;
}

export interface SubAgentConfig {
  name: string;
  description: string;
  system_prompt: string;
  tools: string[];
  skills: string[];
  model: string | null;
}

export interface MiddlewareConfig {
  name: string;
  enabled: boolean;
  config: Record<string, unknown>;
}

export interface AssistantConfig {
  assistant_id: string;
  name: string;
  description: string;
  created_at: string;
  updated_at: string;
  model: ModelConfig;
  system_prompt: string;
  tools: ToolConfig[];
  mcp: MCPServerConfig[];
  skills: SkillConfig[];
  memory: MemoryConfig[];
  subagents: SubAgentConfig[];
  middleware: MiddlewareConfig[];
  recursion_limit: number;
  metadata: Record<string, unknown>;
}

export interface CatalogTool {
  name: string;
  label?: string;
  description: string;
  kind: string;
}

export interface CatalogMiddleware {
  name: string;
  label: string;
  description: string;
  config_schema: Record<string, unknown>;
}

export interface CatalogModel {
  name: string;
  label: string;
}

export interface CatalogProvider {
  name: string;
  label: string;
  example: string;
  models?: CatalogModel[];
}

export interface Catalog {
  tools: CatalogTool[];
  builtin_tools: CatalogTool[];
  middleware: CatalogMiddleware[];
  providers: CatalogProvider[];
  permissions: Permission[];
}

export type AssistantUpsert = Omit<AssistantConfig, "assistant_id" | "created_at" | "updated_at"> & {
  assistant_id?: string;
};

async function json<T>(response: Response, action: string): Promise<T> {
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = (await response.json()) as { detail?: string };
      if (body?.detail) detail = body.detail;
    } catch {
      /* ignore */
    }
    throw new Error(`${action} failed: ${detail}`);
  }
  return (await response.json()) as T;
}

export async function fetchCatalog(apiUrl = DEFAULT_API_URL): Promise<Catalog> {
  return json(await fetch(`${apiUrl}/assistants/catalog`), "Load catalog");
}

export async function listAssistants(apiUrl = DEFAULT_API_URL): Promise<AssistantConfig[]> {
  return json(await fetch(`${apiUrl}/assistants`), "List assistants");
}

export async function getAssistant(id: string, apiUrl = DEFAULT_API_URL): Promise<AssistantConfig> {
  return json(await fetch(`${apiUrl}/assistants/${id}`), "Get assistant");
}

export async function createAssistant(payload: AssistantUpsert, apiUrl = DEFAULT_API_URL): Promise<AssistantConfig> {
  return json(
    await fetch(`${apiUrl}/assistants`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
    "Create assistant",
  );
}

export async function updateAssistant(
  id: string,
  payload: AssistantUpsert,
  apiUrl = DEFAULT_API_URL,
): Promise<AssistantConfig> {
  return json(
    await fetch(`${apiUrl}/assistants/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
    "Update assistant",
  );
}

export async function deleteAssistant(id: string, apiUrl = DEFAULT_API_URL): Promise<void> {
  const response = await fetch(`${apiUrl}/assistants/${id}`, { method: "DELETE" });
  if (!response.ok && response.status !== 204) {
    throw new Error(`Delete assistant failed: ${response.statusText}`);
  }
}

export async function writeSkill(
  id: string,
  body: { name: string; content: string; description?: string },
  apiUrl = DEFAULT_API_URL,
): Promise<SkillConfig> {
  return json(
    await fetch(`${apiUrl}/assistants/${id}/skills`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Write skill",
  );
}

export async function writeMemory(
  id: string,
  body: { name: string; content: string },
  apiUrl = DEFAULT_API_URL,
): Promise<MemoryConfig> {
  return json(
    await fetch(`${apiUrl}/assistants/${id}/memory`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Write memory",
  );
}

export interface AssistResult {
  content: string;
  source: "model" | "template";
}

export async function draftSystemPrompt(
  body: { name: string; description?: string; tools?: string[]; subagents?: string[]; instructions?: string; model?: ModelConfig },
  apiUrl = DEFAULT_API_URL,
): Promise<AssistResult> {
  return json(
    await fetch(`${apiUrl}/assistants/assist/system-prompt`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Draft system prompt",
  );
}

export async function draftSkill(
  body: { name: string; description?: string; instructions?: string; model?: ModelConfig },
  apiUrl = DEFAULT_API_URL,
): Promise<AssistResult> {
  return json(
    await fetch(`${apiUrl}/assistants/assist/skill`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Draft skill",
  );
}

export interface TestModelResult {
  ok: boolean;
  message: string;
  sample?: string;
}

export async function testModel(model: ModelConfig, apiUrl = DEFAULT_API_URL): Promise<TestModelResult> {
  return json(
    await fetch(`${apiUrl}/assistants/assist/test-model`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model }),
    }),
    "Test model",
  );
}

export function emptyAssistant(): AssistantUpsert {
  return {
    name: "",
    description: "",
    model: { provider: "google", name: "gemini-2.5-pro", temperature: 0, max_tokens: null, api_key: null },
    system_prompt: "",
    tools: [],
    mcp: [],
    skills: [],
    memory: [],
    subagents: [],
    middleware: [],
    recursion_limit: 50,
    metadata: {},
  };
}
