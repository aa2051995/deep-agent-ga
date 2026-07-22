import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ArrowLeft,
  Bot,
  Plus,
  Save,
  Sparkles,
  Trash2,
} from "lucide-react";
import { DEFAULT_API_URL } from "./api";
import {
  type AssistantConfig,
  type AssistantUpsert,
  type Catalog,
  type MCPServerConfig,
  type MiddlewareConfig,
  type Permission,
  type SubAgentConfig,
  type ToolConfig,
  createAssistant,
  deleteAssistant,
  draftSkill,
  draftSystemPrompt,
  emptyAssistant,
  fetchCatalog,
  listAssistants,
  updateAssistant,
  writeMemory,
  writeSkill,
} from "./assistantApi";

const TABS = [
  "General",
  "Model",
  "Tools",
  "MCP",
  "Skills",
  "Memory",
  "Subagents",
  "Middleware",
  "Permissions",
] as const;
type Tab = (typeof TABS)[number];

const PERMISSIONS: Permission[] = ["allow", "ask", "deny"];

function backToChat() {
  const url = new URL(window.location.href);
  url.searchParams.delete("assistants");
  window.location.href = url.toString();
}

export function AssistantManager() {
  const [apiUrl, setApiUrl] = useState(DEFAULT_API_URL);
  const [assistants, setAssistants] = useState<AssistantConfig[]>([]);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [isNew, setIsNew] = useState(false);
  const [draft, setDraft] = useState<AssistantUpsert>(emptyAssistant());
  const [tab, setTab] = useState<Tab>("General");
  const [status, setStatus] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [busy, setBusy] = useState(false);

  const flash = useCallback((message: string) => {
    setStatus(message);
    setError("");
    window.setTimeout(() => setStatus(""), 4000);
  }, []);

  const fail = useCallback((err: unknown) => {
    setError(err instanceof Error ? err.message : String(err));
  }, []);

  const loadAll = useCallback(async () => {
    try {
      const [items, cat] = await Promise.all([listAssistants(apiUrl), fetchCatalog(apiUrl)]);
      setAssistants(items);
      setCatalog(cat);
    } catch (err) {
      fail(err);
    }
  }, [apiUrl, fail]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const selectAssistant = useCallback((config: AssistantConfig) => {
    setSelectedId(config.assistant_id);
    setIsNew(false);
    setError("");
    const { assistant_id, created_at, updated_at, ...rest } = config;
    void created_at;
    void updated_at;
    setDraft({ assistant_id, ...rest });
    setTab("General");
  }, []);

  const startNew = useCallback(() => {
    setSelectedId(null);
    setIsNew(true);
    setError("");
    setDraft(emptyAssistant());
    setTab("General");
  }, []);

  const patch = useCallback((update: Partial<AssistantUpsert>) => {
    setDraft((current) => ({ ...current, ...update }));
  }, []);

  const save = useCallback(async () => {
    if (!draft.name.trim()) {
      setError("Name is required.");
      return;
    }
    setBusy(true);
    try {
      const saved = isNew
        ? await createAssistant(draft, apiUrl)
        : await updateAssistant(selectedId as string, draft, apiUrl);
      await loadAll();
      selectAssistant(saved);
      flash(`Saved “${saved.name}”.`);
    } catch (err) {
      fail(err);
    } finally {
      setBusy(false);
    }
  }, [apiUrl, draft, fail, flash, isNew, loadAll, selectAssistant, selectedId]);

  const remove = useCallback(async () => {
    if (!selectedId) return;
    if (!window.confirm(`Delete assistant “${draft.name}”? This removes its folder.`)) return;
    setBusy(true);
    try {
      await deleteAssistant(selectedId, apiUrl);
      await loadAll();
      startNew();
      flash("Assistant deleted.");
    } catch (err) {
      fail(err);
    } finally {
      setBusy(false);
    }
  }, [apiUrl, draft.name, fail, flash, loadAll, selectedId, startNew]);

  return (
    <div className="am-shell">
      <aside className="am-sidebar">
        <button className="am-back" type="button" onClick={backToChat}>
          <ArrowLeft size={15} /> Back to chat
        </button>
        <div className="am-brand">
          <Bot size={18} /> <span>Assistants</span>
        </div>
        <button className="am-new" type="button" onClick={startNew}>
          <Plus size={15} /> New assistant
        </button>
        <div className="am-list">
          {assistants.map((assistant) => (
            <button
              key={assistant.assistant_id}
              type="button"
              className={assistant.assistant_id === selectedId ? "am-item active" : "am-item"}
              onClick={() => selectAssistant(assistant)}
            >
              <span className="am-item-name">{assistant.name}</span>
              <span className="am-item-id">{assistant.assistant_id}</span>
            </button>
          ))}
          {assistants.length === 0 && <div className="am-empty">No assistants yet.</div>}
        </div>
        <label className="am-api">
          <span>API</span>
          <input value={apiUrl} onChange={(event) => setApiUrl(event.target.value)} />
        </label>
      </aside>

      <main className="am-main">
        <header className="am-header">
          <div>
            <h1>{isNew ? "New assistant" : draft.name || "Assistant"}</h1>
            {!isNew && selectedId && <code className="am-code">{selectedId}</code>}
          </div>
          <div className="am-header-actions">
            {!isNew && (
              <button className="am-danger" type="button" onClick={() => void remove()} disabled={busy}>
                <Trash2 size={15} /> Delete
              </button>
            )}
            <button className="am-primary" type="button" onClick={() => void save()} disabled={busy}>
              <Save size={15} /> {isNew ? "Create" : "Save"}
            </button>
          </div>
        </header>

        {(status || error) && (
          <div className={error ? "am-banner am-banner-error" : "am-banner"}>{error || status}</div>
        )}

        <nav className="am-tabs">
          {TABS.map((name) => (
            <button
              key={name}
              type="button"
              className={name === tab ? "am-tab active" : "am-tab"}
              onClick={() => setTab(name)}
            >
              {name}
            </button>
          ))}
        </nav>

        <div className="am-panel">
          {tab === "General" && (
            <GeneralTab draft={draft} patch={patch} apiUrl={apiUrl} onError={fail} />
          )}
          {tab === "Model" && catalog && <ModelTab draft={draft} patch={patch} catalog={catalog} />}
          {tab === "Tools" && catalog && <ToolsTab draft={draft} patch={patch} catalog={catalog} />}
          {tab === "MCP" && <MCPTab draft={draft} patch={patch} />}
          {tab === "Skills" && (
            <SkillsTab
              draft={draft}
              patch={patch}
              apiUrl={apiUrl}
              assistantId={selectedId}
              isNew={isNew}
              onSaved={loadAll}
              onFlash={flash}
              onError={fail}
            />
          )}
          {tab === "Memory" && (
            <MemoryTab
              draft={draft}
              apiUrl={apiUrl}
              assistantId={selectedId}
              isNew={isNew}
              onSaved={loadAll}
              onFlash={flash}
              onError={fail}
            />
          )}
          {tab === "Subagents" && catalog && <SubagentsTab draft={draft} patch={patch} catalog={catalog} />}
          {tab === "Middleware" && catalog && <MiddlewareTab draft={draft} patch={patch} catalog={catalog} />}
          {tab === "Permissions" && <PermissionsTab draft={draft} patch={patch} />}
        </div>
      </main>
    </div>
  );
}

// --- Tabs -----------------------------------------------------------------

function GeneralTab({
  draft,
  patch,
  apiUrl,
  onError,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  apiUrl: string;
  onError: (e: unknown) => void;
}) {
  const [drafting, setDrafting] = useState(false);
  const [instructions, setInstructions] = useState("");

  const runDraft = async () => {
    setDrafting(true);
    try {
      const result = await draftSystemPrompt(
        {
          name: draft.name || "Assistant",
          description: draft.description,
          tools: draft.tools.filter((t) => t.permission !== "deny").map((t) => t.name),
          subagents: draft.subagents.map((s) => s.name),
          instructions,
          model: draft.model,
        },
        apiUrl,
      );
      patch({ system_prompt: result.content });
    } catch (err) {
      onError(err);
    } finally {
      setDrafting(false);
    }
  };

  return (
    <div className="am-form">
      <label className="am-field">
        <span>Name</span>
        <input value={draft.name} onChange={(e) => patch({ name: e.target.value })} placeholder="Research Assistant" />
      </label>
      <label className="am-field">
        <span>Description</span>
        <input
          value={draft.description}
          onChange={(e) => patch({ description: e.target.value })}
          placeholder="What this assistant is for"
        />
      </label>
      <div className="am-field">
        <div className="am-field-head">
          <span>System prompt</span>
          <div className="am-assist-row">
            <input
              className="am-assist-input"
              placeholder="Extra guidance for the drafter (optional)"
              value={instructions}
              onChange={(e) => setInstructions(e.target.value)}
            />
            <button className="am-ghost" type="button" onClick={() => void runDraft()} disabled={drafting}>
              <Sparkles size={14} /> {drafting ? "Drafting…" : "Help me write"}
            </button>
          </div>
        </div>
        <textarea
          className="am-textarea"
          rows={14}
          value={draft.system_prompt}
          onChange={(e) => patch({ system_prompt: e.target.value })}
          placeholder="Leave blank to use the built-in research prompt, or draft one with the assistant."
        />
      </div>
    </div>
  );
}

function ModelTab({
  draft,
  patch,
  catalog,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  catalog: Catalog;
}) {
  const model = draft.model;
  return (
    <div className="am-form">
      <label className="am-field">
        <span>Provider</span>
        <select
          value={model.provider}
          onChange={(e) =>
            patch({ model: { ...model, provider: e.target.value as typeof model.provider } })
          }
        >
          {catalog.providers.map((p) => (
            <option key={p.name} value={p.name}>
              {p.label}
            </option>
          ))}
        </select>
      </label>
      <label className="am-field">
        <span>Model name</span>
        <input
          value={model.name}
          onChange={(e) => patch({ model: { ...model, name: e.target.value } })}
          placeholder={catalog.providers.find((p) => p.name === model.provider)?.example}
        />
      </label>
      <label className="am-field">
        <span>Temperature</span>
        <input
          type="number"
          step="0.1"
          min="0"
          max="2"
          value={model.temperature}
          onChange={(e) => patch({ model: { ...model, temperature: Number(e.target.value) } })}
        />
      </label>
      <label className="am-field">
        <span>Max tokens (optional)</span>
        <input
          type="number"
          value={model.max_tokens ?? ""}
          onChange={(e) => patch({ model: { ...model, max_tokens: e.target.value ? Number(e.target.value) : null } })}
        />
      </label>
      <label className="am-field">
        <span>Recursion limit</span>
        <input
          type="number"
          value={draft.recursion_limit}
          onChange={(e) => patch({ recursion_limit: Number(e.target.value) })}
        />
      </label>
    </div>
  );
}

function ToolsTab({
  draft,
  patch,
  catalog,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  catalog: Catalog;
}) {
  const byName = useMemo(() => new Map(draft.tools.map((t) => [t.name, t])), [draft.tools]);

  const setPermission = (name: string, permission: Permission) => {
    const others = draft.tools.filter((t) => t.name !== name);
    patch({ tools: [...others, { name, permission }] });
  };
  const toggle = (name: string, on: boolean) => {
    if (on) setPermission(name, "allow");
    else patch({ tools: draft.tools.filter((t) => t.name !== name) });
  };

  return (
    <div className="am-form">
      <p className="am-hint">
        Grant custom tools and choose how each is gated. Built-in filesystem, planning and subagent
        tools are always available.
      </p>
      <div className="am-cards">
        {catalog.tools.map((tool) => {
          const current = byName.get(tool.name);
          return (
            <div className="am-card" key={tool.name}>
              <label className="am-card-head">
                <input
                  type="checkbox"
                  checked={Boolean(current)}
                  onChange={(e) => toggle(tool.name, e.target.checked)}
                />
                <strong>{tool.label ?? tool.name}</strong>
              </label>
              <p className="am-muted">{tool.description}</p>
              {current && (
                <PermissionPicker
                  value={current.permission}
                  onChange={(permission) => setPermission(tool.name, permission)}
                  permissions={catalog.permissions}
                />
              )}
            </div>
          );
        })}
      </div>
      <div className="am-builtin">
        <span className="am-muted">Always on:</span>
        {catalog.builtin_tools.map((tool) => (
          <code key={tool.name} className="am-chip">
            {tool.name}
          </code>
        ))}
      </div>
    </div>
  );
}

function MCPTab({ draft, patch }: { draft: AssistantUpsert; patch: (u: Partial<AssistantUpsert>) => void }) {
  const add = () => {
    const server: MCPServerConfig = {
      name: `server-${draft.mcp.length + 1}`,
      transport: "stdio",
      command: "",
      args: [],
      url: null,
      env: {},
      permission: "allow",
      enabled: true,
    };
    patch({ mcp: [...draft.mcp, server] });
  };
  const update = (index: number, changes: Partial<MCPServerConfig>) => {
    patch({ mcp: draft.mcp.map((s, i) => (i === index ? { ...s, ...changes } : s)) });
  };
  const remove = (index: number) => patch({ mcp: draft.mcp.filter((_, i) => i !== index) });

  return (
    <div className="am-form">
      <p className="am-hint">
        Connect MCP servers to expose their tools. Loaded at build time when <code>langchain-mcp-adapters</code>{" "}
        is installed on the backend.
      </p>
      {draft.mcp.map((server, index) => (
        <div className="am-card" key={index}>
          <div className="am-row">
            <input
              className="am-grow"
              placeholder="Server name"
              value={server.name}
              onChange={(e) => update(index, { name: e.target.value })}
            />
            <select value={server.transport} onChange={(e) => update(index, { transport: e.target.value as MCPServerConfig["transport"] })}>
              <option value="stdio">stdio</option>
              <option value="streamable_http">streamable_http</option>
              <option value="sse">sse</option>
            </select>
            <label className="am-check">
              <input type="checkbox" checked={server.enabled} onChange={(e) => update(index, { enabled: e.target.checked })} /> enabled
            </label>
            <button className="am-icon-danger" type="button" onClick={() => remove(index)}>
              <Trash2 size={14} />
            </button>
          </div>
          {server.transport === "stdio" ? (
            <div className="am-row">
              <input
                className="am-grow"
                placeholder="command (e.g. npx)"
                value={server.command ?? ""}
                onChange={(e) => update(index, { command: e.target.value })}
              />
              <input
                className="am-grow"
                placeholder="args (space separated)"
                value={server.args.join(" ")}
                onChange={(e) => update(index, { args: e.target.value.split(" ").filter(Boolean) })}
              />
            </div>
          ) : (
            <input
              placeholder="URL"
              value={server.url ?? ""}
              onChange={(e) => update(index, { url: e.target.value })}
            />
          )}
          <PermissionPicker
            value={server.permission}
            onChange={(permission) => update(index, { permission })}
            permissions={PERMISSIONS}
          />
        </div>
      ))}
      <button className="am-ghost" type="button" onClick={add}>
        <Plus size={14} /> Add MCP server
      </button>
    </div>
  );
}

function SkillsTab({
  draft,
  patch,
  apiUrl,
  assistantId,
  isNew,
  onSaved,
  onFlash,
  onError,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  apiUrl: string;
  assistantId: string | null;
  isNew: boolean;
  onSaved: () => Promise<void>;
  onFlash: (m: string) => void;
  onError: (e: unknown) => void;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [instructions, setInstructions] = useState("");
  const [content, setContent] = useState("");
  const [drafting, setDrafting] = useState(false);
  const [saving, setSaving] = useState(false);

  const runDraft = async () => {
    if (!name.trim()) {
      onError(new Error("Give the skill a name first."));
      return;
    }
    setDrafting(true);
    try {
      const result = await draftSkill({ name, description, instructions, model: draft.model }, apiUrl);
      setContent(result.content);
    } catch (err) {
      onError(err);
    } finally {
      setDrafting(false);
    }
  };

  const persist = async () => {
    if (!assistantId || isNew) {
      onError(new Error("Save the assistant first, then add skills."));
      return;
    }
    if (!name.trim() || !content.trim()) {
      onError(new Error("Skill name and content are required."));
      return;
    }
    setSaving(true);
    try {
      await writeSkill(assistantId, { name, content, description }, apiUrl);
      await onSaved();
      onFlash(`Skill “${name}” saved.`);
      setName("");
      setDescription("");
      setContent("");
      setInstructions("");
    } catch (err) {
      onError(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="am-form">
      <p className="am-hint">
        Skills are on-demand playbooks (SKILL.md) stored in the assistant folder. Draft one with help,
        then save it.
      </p>
      <ul className="am-list-inline">
        {draft.skills.map((skill) => (
          <li key={skill.path}>
            <strong>{skill.name}</strong> <span className="am-muted">{skill.path}</span>
          </li>
        ))}
        {draft.skills.length === 0 && <li className="am-muted">No skills yet.</li>}
      </ul>
      <div className="am-row">
        <input className="am-grow" placeholder="Skill name" value={name} onChange={(e) => setName(e.target.value)} />
        <input
          className="am-grow"
          placeholder="Short description"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
      </div>
      <div className="am-assist-row">
        <input
          className="am-assist-input"
          placeholder="What should this skill do? (guidance for the drafter)"
          value={instructions}
          onChange={(e) => setInstructions(e.target.value)}
        />
        <button className="am-ghost" type="button" onClick={() => void runDraft()} disabled={drafting}>
          <Sparkles size={14} /> {drafting ? "Drafting…" : "Help me write"}
        </button>
      </div>
      <textarea
        className="am-textarea"
        rows={12}
        placeholder="SKILL.md content"
        value={content}
        onChange={(e) => setContent(e.target.value)}
      />
      <button className="am-primary" type="button" onClick={() => void persist()} disabled={saving}>
        <Save size={14} /> {saving ? "Saving…" : "Save skill"}
      </button>
    </div>
  );
}

function MemoryTab({
  draft,
  apiUrl,
  assistantId,
  isNew,
  onSaved,
  onFlash,
  onError,
}: {
  draft: AssistantUpsert;
  apiUrl: string;
  assistantId: string | null;
  isNew: boolean;
  onSaved: () => Promise<void>;
  onFlash: (m: string) => void;
  onError: (e: unknown) => void;
}) {
  const [name, setName] = useState("AGENTS.md");
  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);

  const persist = async () => {
    if (!assistantId || isNew) {
      onError(new Error("Save the assistant first, then add memory."));
      return;
    }
    if (!content.trim()) {
      onError(new Error("Memory content is required."));
      return;
    }
    setSaving(true);
    try {
      await writeMemory(assistantId, { name, content }, apiUrl);
      await onSaved();
      onFlash(`Memory “${name}” saved.`);
      setContent("");
    } catch (err) {
      onError(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="am-form">
      <p className="am-hint">
        Memory (AGENTS.md) is always loaded into the system prompt — persistent project context.
      </p>
      <ul className="am-list-inline">
        {draft.memory.map((mem) => (
          <li key={mem.path}>
            <strong>{mem.name}</strong> <span className="am-muted">{mem.path}</span>
          </li>
        ))}
        {draft.memory.length === 0 && <li className="am-muted">No memory files yet.</li>}
      </ul>
      <input className="am-grow" placeholder="File name" value={name} onChange={(e) => setName(e.target.value)} />
      <textarea
        className="am-textarea"
        rows={12}
        placeholder="# Project notes the agent should always know"
        value={content}
        onChange={(e) => setContent(e.target.value)}
      />
      <button className="am-primary" type="button" onClick={() => void persist()} disabled={saving}>
        <Save size={14} /> {saving ? "Saving…" : "Save memory"}
      </button>
    </div>
  );
}

function SubagentsTab({
  draft,
  patch,
  catalog,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  catalog: Catalog;
}) {
  const add = () => {
    const sub: SubAgentConfig = {
      name: `subagent-${draft.subagents.length + 1}`,
      description: "",
      system_prompt: "",
      tools: [],
      skills: [],
      model: null,
    };
    patch({ subagents: [...draft.subagents, sub] });
  };
  const update = (index: number, changes: Partial<SubAgentConfig>) =>
    patch({ subagents: draft.subagents.map((s, i) => (i === index ? { ...s, ...changes } : s)) });
  const remove = (index: number) => patch({ subagents: draft.subagents.filter((_, i) => i !== index) });
  const toggleTool = (index: number, name: string, on: boolean) => {
    const sub = draft.subagents[index];
    const tools = on ? [...sub.tools, name] : sub.tools.filter((t) => t !== name);
    update(index, { tools });
  };

  return (
    <div className="am-form">
      <p className="am-hint">Subagents are delegated to via the built-in <code>task</code> tool.</p>
      {draft.subagents.map((sub, index) => (
        <div className="am-card" key={index}>
          <div className="am-row">
            <input
              className="am-grow"
              placeholder="Subagent name"
              value={sub.name}
              onChange={(e) => update(index, { name: e.target.value })}
            />
            <button className="am-icon-danger" type="button" onClick={() => remove(index)}>
              <Trash2 size={14} />
            </button>
          </div>
          <input
            placeholder="Description (how the main agent decides to call it)"
            value={sub.description}
            onChange={(e) => update(index, { description: e.target.value })}
          />
          <textarea
            className="am-textarea"
            rows={5}
            placeholder="System prompt (blank uses a sensible default)"
            value={sub.system_prompt}
            onChange={(e) => update(index, { system_prompt: e.target.value })}
          />
          <div className="am-chips">
            {catalog.tools.map((tool) => (
              <label key={tool.name} className="am-chip-check">
                <input
                  type="checkbox"
                  checked={sub.tools.includes(tool.name)}
                  onChange={(e) => toggleTool(index, tool.name, e.target.checked)}
                />
                {tool.name}
              </label>
            ))}
          </div>
        </div>
      ))}
      <button className="am-ghost" type="button" onClick={add}>
        <Plus size={14} /> Add subagent
      </button>
    </div>
  );
}

function MiddlewareTab({
  draft,
  patch,
  catalog,
}: {
  draft: AssistantUpsert;
  patch: (u: Partial<AssistantUpsert>) => void;
  catalog: Catalog;
}) {
  const byName = useMemo(() => new Map(draft.middleware.map((m) => [m.name, m])), [draft.middleware]);
  const toggle = (name: string, on: boolean) => {
    const others = draft.middleware.filter((m) => m.name !== name);
    if (on) {
      const entry: MiddlewareConfig = byName.get(name) ?? { name, enabled: true, config: {} };
      patch({ middleware: [...others, { ...entry, enabled: true }] });
    } else {
      patch({ middleware: others });
    }
  };

  return (
    <div className="am-form">
      <p className="am-hint">
        The core deepagents stack (planning, filesystem, subagents, prompt-caching) is always applied.
        Toggle optional layers here.
      </p>
      <div className="am-cards">
        {catalog.middleware.map((mw) => (
          <div className="am-card" key={mw.name}>
            <label className="am-card-head">
              <input
                type="checkbox"
                checked={byName.has(mw.name)}
                onChange={(e) => toggle(mw.name, e.target.checked)}
              />
              <strong>{mw.label}</strong>
            </label>
            <p className="am-muted">{mw.description}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function PermissionsTab({ draft, patch }: { draft: AssistantUpsert; patch: (u: Partial<AssistantUpsert>) => void }) {
  const setToolPermission = (name: string, permission: Permission) => {
    patch({ tools: draft.tools.map((t) => (t.name === name ? { ...t, permission } : t)) });
  };
  const setMcpPermission = (name: string, permission: Permission) => {
    patch({ mcp: draft.mcp.map((s) => (s.name === name ? { ...s, permission } : s)) });
  };

  return (
    <div className="am-form">
      <p className="am-hint">
        <strong>allow</strong> runs the tool freely, <strong>ask</strong> pauses for human approval
        (human-in-the-loop), <strong>deny</strong> removes the tool entirely.
      </p>
      <table className="am-table">
        <thead>
          <tr>
            <th>Capability</th>
            <th>Type</th>
            <th>Permission</th>
          </tr>
        </thead>
        <tbody>
          {draft.tools.map((tool: ToolConfig) => (
            <tr key={`tool-${tool.name}`}>
              <td>{tool.name}</td>
              <td className="am-muted">tool</td>
              <td>
                <PermissionPicker
                  value={tool.permission}
                  onChange={(permission) => setToolPermission(tool.name, permission)}
                  permissions={PERMISSIONS}
                />
              </td>
            </tr>
          ))}
          {draft.mcp.map((server) => (
            <tr key={`mcp-${server.name}`}>
              <td>{server.name}</td>
              <td className="am-muted">mcp</td>
              <td>
                <PermissionPicker
                  value={server.permission}
                  onChange={(permission) => setMcpPermission(server.name, permission)}
                  permissions={PERMISSIONS}
                />
              </td>
            </tr>
          ))}
          {draft.tools.length === 0 && draft.mcp.length === 0 && (
            <tr>
              <td colSpan={3} className="am-muted">
                Add tools or MCP servers to configure permissions.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function PermissionPicker({
  value,
  onChange,
  permissions,
}: {
  value: Permission;
  onChange: (permission: Permission) => void;
  permissions: Permission[];
}) {
  return (
    <div className="am-perm">
      {permissions.map((permission) => (
        <button
          key={permission}
          type="button"
          className={permission === value ? `am-perm-btn ${permission} active` : "am-perm-btn"}
          onClick={() => onChange(permission)}
        >
          {permission}
        </button>
      ))}
    </div>
  );
}
