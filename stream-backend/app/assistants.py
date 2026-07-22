"""Folder-backed assistant definitions for deepagents-powered agents.

Each assistant is stored in its own folder under a configurable root
(default ``stream-backend/assistants/<assistant_id>/``). A folder contains:

- ``assistant.json``      the :class:`AssistantConfig` (model, prompt, tools,
                          mcp servers, skills, memory, subagents, middleware,
                          and per-capability permissions).
- ``skills/<name>/SKILL.md`` skill directories (Anthropic Agent Skills format).
- ``memory/*.md``         AGENTS.md-style memory files always loaded into the
                          system prompt.

The store is intentionally free of any LangChain / deepagents imports so it can
be used (and unit-tested) without the heavy agent runtime installed. Turning a
config into a live agent lives in :mod:`app.assistant_builder`.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from .models import now_iso

logger = logging.getLogger("stream_backend.assistants")

# ``allow``  -> tool is available with no gate.
# ``ask``    -> tool call pauses for human approval (HumanInTheLoop / interrupt).
# ``deny``   -> tool is not attached to the agent at all.
Permission = Literal["allow", "ask", "deny"]

PERMISSIONS: tuple[str, ...] = ("allow", "ask", "deny")

_ID_RE = re.compile(r"[^a-z0-9_-]+")


def slugify(value: str) -> str:
    """Turn an arbitrary name into a filesystem/URL safe assistant id."""
    slug = _ID_RE.sub("-", (value or "").strip().lower()).strip("-")
    return slug or "assistant"


class ModelConfig(BaseModel):
    provider: Literal["google", "anthropic", "bedrock", "openai"] = "google"
    name: str = "gemini-2.5-pro"
    temperature: float = 0.0
    max_tokens: int | None = None
    # Optional API key for the provider. When empty, the builder falls back to
    # the provider's standard environment variable (GOOGLE_API_KEY, etc.).
    # Bedrock uses AWS credentials/profile, not this field.
    api_key: str | None = None


class ToolConfig(BaseModel):
    """A built-in / registered tool granted to the assistant."""

    name: str
    permission: Permission = "allow"


class MCPServerConfig(BaseModel):
    """An MCP server whose tools are exposed to the assistant.

    Loaded at build time via ``langchain_mcp_adapters`` when available. The
    config is always persisted so the UI can manage servers even if the
    adapter package is not installed on the runtime host.
    """

    name: str
    transport: Literal["stdio", "streamable_http", "sse"] = "stdio"
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    url: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    permission: Permission = "allow"
    enabled: bool = True


class SkillConfig(BaseModel):
    """A skill directory shipped inside the assistant folder.

    ``path`` is POSIX and relative to the assistant folder root (e.g.
    ``skills/web-research``). ``content`` is the SKILL.md body used when
    creating/updating the skill through the API.
    """

    name: str
    description: str = ""
    path: str = ""
    enabled: bool = True


class MemoryConfig(BaseModel):
    """An AGENTS.md memory file inside the assistant folder."""

    name: str
    path: str = ""
    enabled: bool = True


class SubAgentConfig(BaseModel):
    name: str
    description: str
    system_prompt: str = ""
    tools: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    model: str | None = None


class MiddlewareConfig(BaseModel):
    """An optional middleware layer toggled on the assistant."""

    name: str
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class AssistantConfig(BaseModel):
    assistant_id: str
    name: str
    description: str = ""
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)
    model: ModelConfig = Field(default_factory=ModelConfig)
    system_prompt: str = ""
    tools: list[ToolConfig] = Field(default_factory=list)
    mcp: list[MCPServerConfig] = Field(default_factory=list)
    skills: list[SkillConfig] = Field(default_factory=list)
    memory: list[MemoryConfig] = Field(default_factory=list)
    subagents: list[SubAgentConfig] = Field(default_factory=list)
    middleware: list[MiddlewareConfig] = Field(default_factory=list)
    recursion_limit: int = 50
    metadata: dict[str, Any] = Field(default_factory=dict)


class AssistantNotFound(KeyError):
    pass


class AssistantExists(ValueError):
    pass


def default_assistants_root() -> Path:
    """Root folder that holds one sub-folder per assistant.

    Overridable with ``STREAM_BACKEND_ASSISTANTS_DIR``. Defaults to
    ``stream-backend/assistants`` (parent of this ``app`` package).
    """
    override = os.getenv("STREAM_BACKEND_ASSISTANTS_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return (Path(__file__).resolve().parents[1] / "assistants").resolve()


class AssistantStore:
    """CRUD over folder-backed :class:`AssistantConfig` definitions."""

    CONFIG_FILENAME = "assistant.json"

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root).resolve() if root is not None else default_assistants_root()
        self.root.mkdir(parents=True, exist_ok=True)

    # ---- paths -----------------------------------------------------------
    def path_for(self, assistant_id: str) -> Path:
        return self.root / assistant_id

    def _config_path(self, assistant_id: str) -> Path:
        return self.path_for(assistant_id) / self.CONFIG_FILENAME

    # ---- read ------------------------------------------------------------
    def exists(self, assistant_id: str) -> bool:
        return self._config_path(assistant_id).is_file()

    def list_ids(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(
            child.name
            for child in self.root.iterdir()
            if child.is_dir() and (child / self.CONFIG_FILENAME).is_file()
        )

    def get(self, assistant_id: str) -> AssistantConfig:
        path = self._config_path(assistant_id)
        if not path.is_file():
            raise AssistantNotFound(assistant_id)
        data = json.loads(path.read_text(encoding="utf-8"))
        return AssistantConfig.model_validate(data)

    def list(self) -> list[AssistantConfig]:
        out: list[AssistantConfig] = []
        for assistant_id in self.list_ids():
            try:
                out.append(self.get(assistant_id))
            except Exception:
                logger.exception("assistants.list.load_failed id=%s", assistant_id)
        return out

    # ---- write -----------------------------------------------------------
    def _write(self, config: AssistantConfig) -> AssistantConfig:
        folder = self.path_for(config.assistant_id)
        (folder / "skills").mkdir(parents=True, exist_ok=True)
        (folder / "memory").mkdir(parents=True, exist_ok=True)
        self._config_path(config.assistant_id).write_text(
            json.dumps(config.model_dump(mode="json"), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("assistants.write id=%s folder=%s", config.assistant_id, folder)
        return config

    def create(self, config: AssistantConfig) -> AssistantConfig:
        if not config.assistant_id:
            config.assistant_id = slugify(config.name)
        if self.exists(config.assistant_id):
            raise AssistantExists(config.assistant_id)
        config.created_at = config.updated_at = now_iso()
        return self._write(config)

    def update(self, assistant_id: str, config: AssistantConfig) -> AssistantConfig:
        if not self.exists(assistant_id):
            raise AssistantNotFound(assistant_id)
        existing = self.get(assistant_id)
        config.assistant_id = assistant_id
        config.created_at = existing.created_at
        config.updated_at = now_iso()
        return self._write(config)

    def save(self, config: AssistantConfig) -> AssistantConfig:
        """Upsert: create if missing, otherwise update in place.

        Used both by the API and by the runtime when persisting the assistant
        snapshot alongside a thread checkpoint.
        """
        if self.exists(config.assistant_id):
            return self.update(config.assistant_id, config)
        return self.create(config)

    def delete(self, assistant_id: str) -> None:
        folder = self.path_for(assistant_id)
        if not self.exists(assistant_id):
            raise AssistantNotFound(assistant_id)
        shutil.rmtree(folder, ignore_errors=True)
        logger.info("assistants.delete id=%s", assistant_id)

    # ---- skills / memory files ------------------------------------------
    def write_skill(self, assistant_id: str, name: str, content: str, description: str = "") -> SkillConfig:
        """Persist a SKILL.md under ``skills/<name>/`` and register it."""
        config = self.get(assistant_id)
        skill_slug = slugify(name)
        skill_dir = self.path_for(assistant_id) / "skills" / skill_slug
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")
        rel = f"skills/{skill_slug}"
        entry = SkillConfig(name=skill_slug, description=description, path=rel, enabled=True)
        config.skills = [s for s in config.skills if s.path != rel] + [entry]
        self.update(assistant_id, config)
        return entry

    def write_memory(self, assistant_id: str, name: str, content: str) -> MemoryConfig:
        config = self.get(assistant_id)
        # Preserve the given case (AGENTS.md is the convention), sanitizing only
        # path-unsafe characters rather than lowercasing via slugify.
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", (name or "").strip()).strip("-") or "AGENTS"
        filename = safe if safe.endswith(".md") else f"{safe}.md"
        mem_path = self.path_for(assistant_id) / "memory" / filename
        mem_path.parent.mkdir(parents=True, exist_ok=True)
        mem_path.write_text(content, encoding="utf-8")
        rel = f"memory/{filename}"
        entry = MemoryConfig(name=filename, path=rel, enabled=True)
        config.memory = [m for m in config.memory if m.path != rel] + [entry]
        self.update(assistant_id, config)
        return entry

    # ---- seeding ---------------------------------------------------------
    def ensure_seeded(self) -> None:
        """Create the built-in default assistant if the root is empty."""
        if self.list_ids():
            return
        for config in default_seed_assistants():
            try:
                self.create(config)
            except AssistantExists:
                pass
            except Exception:
                logger.exception("assistants.seed_failed id=%s", config.assistant_id)


def default_seed_assistants() -> list[AssistantConfig]:
    """Ship a ready-to-run assistant mirroring the legacy research agent."""
    provider = os.getenv("RESEARCH_AGENT_PROVIDER", "google").strip().lower()
    model_name = os.getenv("RESEARCH_AGENT_MODEL", "gemini-2.5-pro")
    return [
        AssistantConfig(
            assistant_id="deep-agent",
            name="Deep Research Agent",
            description="Web research orchestrator that delegates to a researcher subagent.",
            model=ModelConfig(
                provider=provider if provider in {"google", "anthropic", "bedrock", "openai"} else "google",
                name=model_name,
                temperature=0.0,
            ),
            system_prompt="",  # empty -> builder falls back to the research prompt bundle
            tools=[
                ToolConfig(name="tavily_search", permission="allow"),
                ToolConfig(name="think_tool", permission="allow"),
            ],
            subagents=[
                SubAgentConfig(
                    name="research-agent",
                    description=(
                        "Delegate research to the sub-agent researcher. "
                        "Only give this researcher one topic at a time."
                    ),
                    system_prompt="",  # builder fills in RESEARCHER_INSTRUCTIONS
                    tools=["tavily_search", "think_tool"],
                )
            ],
            middleware=[
                MiddlewareConfig(name="summarization", enabled=True),
                MiddlewareConfig(name="anthropic_prompt_caching", enabled=True),
            ],
            metadata={"seed": True},
        )
    ]
