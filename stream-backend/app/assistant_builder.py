"""Turn an :class:`AssistantConfig` into a live deepagents agent.

All heavy imports (LangChain, deepagents) are deferred into the functions so
that :mod:`app.assistants` (pure config + folder IO) stays importable and
unit-testable without the agent runtime installed.

Mapping from config -> ``create_deep_agent`` arguments:

- ``model``       -> a chat model built from provider/name/temperature.
- ``tools``       -> resolved from the tool registry; ``deny`` drops the tool,
                     ``ask`` adds it to ``interrupt_on`` (human approval).
- ``mcp``         -> tools loaded via ``langchain_mcp_adapters`` when available.
- ``skills``      -> POSIX paths under the assistant folder for SkillsMiddleware.
- ``memory``      -> AGENTS.md paths under the assistant folder.
- ``subagents``   -> deepagents ``SubAgent`` dicts.
- ``middleware``  -> optional extra middleware (summarization tuning, caching).
- ``permissions`` -> tools marked ``ask`` become ``interrupt_on`` entries.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from .assistants import AssistantConfig, SubAgentConfig

logger = logging.getLogger("stream_backend.assistant_builder")


class AssistantBuildError(RuntimeError):
    pass


# --------------------------------------------------------------------------
# Tool registry
# --------------------------------------------------------------------------
def _tool_registry() -> dict[str, Any]:
    """Name -> tool object for every custom tool the builder can attach.

    Degrades gracefully: if the research tools module fails to import (e.g. an
    eager Tavily client with no API key), return an empty registry and warn so
    that assistants which do not use those tools can still be built. Missing
    tools are then skipped by :func:`_resolve_tools`.
    """
    try:
        from research_agent.tools import tavily_search, think_tool
    except Exception as exc:  # pragma: no cover - environment dependent
        logger.warning("assistant_builder.tool_registry_import_failed error=%s", exc)
        return {}
    return {"tavily_search": tavily_search, "think_tool": think_tool}


def _resolve_tools(names: list[str], registry: dict[str, Any]) -> list[Any]:
    resolved: list[Any] = []
    for name in names:
        tool = registry.get(name)
        if tool is None:
            logger.warning("assistant_builder.unknown_tool name=%s", name)
            continue
        resolved.append(tool)
    return resolved


# --------------------------------------------------------------------------
# Model factory
# --------------------------------------------------------------------------
def build_model(config: AssistantConfig) -> Any:
    provider = config.model.provider
    name = config.model.name
    temperature = config.model.temperature
    api_key = (config.model.api_key or "").strip() or None
    if provider == "anthropic":
        from langchain.chat_models import init_chat_model

        model_id = name if ":" in name else f"anthropic:{name}"
        kwargs: dict[str, Any] = {"model": model_id, "temperature": temperature}
        if api_key:
            kwargs["api_key"] = api_key
        return init_chat_model(**kwargs)
    if provider == "openai":
        from langchain.chat_models import init_chat_model

        model_id = name if ":" in name else f"openai:{name}"
        kwargs = {"model": model_id, "temperature": temperature}
        if api_key:
            kwargs["api_key"] = api_key
        return init_chat_model(**kwargs)
    if provider == "bedrock":
        try:
            from langchain_aws import ChatBedrockConverse
        except Exception as exc:  # pragma: no cover - environment dependent
            raise AssistantBuildError(
                "Install langchain-aws to use the bedrock provider."
            ) from exc
        # Use the assistant's OWN configured model id (not RESEARCH_AGENT_MODEL
        # from the env, which caused a Google model name to be sent to Bedrock).
        # Region/profile/endpoint still come from the AWS_* env.
        region = (
            os.getenv("RESEARCH_AGENT_AWS_REGION")
            or os.getenv("AWS_BEDROCK_REGION")
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
        )
        profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_BEDROCK_PROFILE")
        endpoint = os.getenv("AWS_BEDROCK_ENDPOINT_URL")
        bedrock_kwargs: dict[str, Any] = {"model": name, "temperature": temperature}
        if region:
            bedrock_kwargs["region_name"] = region
        if profile:
            bedrock_kwargs["credentials_profile_name"] = profile
        if endpoint:
            bedrock_kwargs["endpoint_url"] = endpoint
        if config.model.max_tokens:
            bedrock_kwargs["max_tokens"] = config.model.max_tokens
        return ChatBedrockConverse(**bedrock_kwargs)
    # default: google
    from langchain_google_genai import ChatGoogleGenerativeAI

    return ChatGoogleGenerativeAI(
        model=name,
        temperature=temperature,
        api_key=api_key or os.getenv("GOOGLE_API_KEY"),
    )


# --------------------------------------------------------------------------
# Prompt fallbacks (legacy research agent parity)
# --------------------------------------------------------------------------
def _research_instructions() -> str:
    import research_agent.prompts as prompts

    max_units = int(os.getenv("MAX_CONCURRENT_RESEARCH_UNITS", "3"))
    max_iters = int(os.getenv("MAX_RESEARCHER_ITERATIONS", "3"))
    return (
        prompts.RESEARCH_WORKFLOW_INSTRUCTIONS
        + "\n\n"
        + "=" * 80
        + "\n\n"
        + prompts.SUBAGENT_DELEGATION_INSTRUCTIONS.format(
            max_concurrent_research_units=max_units,
            max_researcher_iterations=max_iters,
        )
    )


def _researcher_prompt() -> str:
    import research_agent.prompts as prompts

    return prompts.RESEARCHER_INSTRUCTIONS.format(date=datetime.now().strftime("%Y-%m-%d"))


# --------------------------------------------------------------------------
# Subagents
# --------------------------------------------------------------------------
def _build_subagent(spec: SubAgentConfig, registry: dict[str, Any], assistant_dir: Path) -> dict[str, Any]:
    system_prompt = spec.system_prompt or (_researcher_prompt() if spec.name == "research-agent" else "")
    out: dict[str, Any] = {
        "name": spec.name,
        "description": spec.description,
        "system_prompt": system_prompt,
        "tools": _resolve_tools(spec.tools, registry),
    }
    if spec.model:
        out["model"] = spec.model
    if spec.skills:
        out["skills"] = [_posix_under(assistant_dir, s) for s in spec.skills]
    return out


# --------------------------------------------------------------------------
# Middleware
# --------------------------------------------------------------------------
def _build_extra_middleware(config: AssistantConfig, model: Any, backend: Any) -> list[Any]:
    """Only middleware NOT already in the deepagents core stack.

    Summarization / prompt-caching are part of the default stack, so toggling
    them 'off' means: append a no-op? deepagents always includes them, so we
    respect an explicit disable by leaving them out is not possible without
    forking the builder. Instead we treat these as tuning knobs when enabled
    and simply record the intent otherwise. Unknown middleware names are
    ignored with a warning.
    """
    extra: list[Any] = []
    for mw in config.middleware:
        if not mw.enabled:
            continue
        if mw.name in {"summarization", "anthropic_prompt_caching", "human_in_the_loop"}:
            # Handled by the deepagents core stack / interrupt_on.
            continue
        logger.warning("assistant_builder.unknown_middleware name=%s (ignored)", mw.name)
    return extra


# --------------------------------------------------------------------------
# Permissions -> interrupt_on
# --------------------------------------------------------------------------
def _interrupt_on(config: AssistantConfig) -> dict[str, Any]:
    interrupt: dict[str, Any] = {}
    for tool in config.tools:
        if tool.permission == "ask":
            interrupt[tool.name] = True
    for server in config.mcp:
        if server.enabled and server.permission == "ask":
            # MCP tools are namespaced; approximate by server name marker.
            interrupt[server.name] = True
    return interrupt


def _granted_tool_names(config: AssistantConfig) -> list[str]:
    return [t.name for t in config.tools if t.permission != "deny"]


def _posix_under(assistant_dir: Path, rel: str) -> str:
    """Absolute POSIX path for a skill/memory source inside the folder."""
    return (assistant_dir / rel).resolve().as_posix()


# --------------------------------------------------------------------------
# MCP
# --------------------------------------------------------------------------
async def load_mcp_tools(config: AssistantConfig) -> list[Any]:
    """Load MCP tools for enabled servers. Returns [] if adapter unavailable."""
    enabled = [s for s in config.mcp if s.enabled]
    if not enabled:
        return []
    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient
    except Exception:
        logger.warning(
            "assistant_builder.mcp_unavailable servers=%s (install langchain-mcp-adapters)",
            [s.name for s in enabled],
        )
        return []
    connections: dict[str, dict[str, Any]] = {}
    for server in enabled:
        if server.transport == "stdio":
            connections[server.name] = {
                "transport": "stdio",
                "command": server.command,
                "args": server.args,
                "env": server.env or None,
            }
        else:
            connections[server.name] = {"transport": server.transport, "url": server.url}
    try:
        client = MultiServerMCPClient(connections)
        return await client.get_tools()
    except Exception:
        logger.exception("assistant_builder.mcp_load_failed")
        return []


# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------
def build_agent(
    config: AssistantConfig,
    assistant_dir: Path,
    *,
    checkpointer: Any = None,
    extra_tools: list[Any] | None = None,
) -> Any:
    """Compile a deepagents agent from ``config``.

    ``assistant_dir`` is the folder that owns the assistant's skills/memory; a
    ``FilesystemBackend`` rooted there scopes file tools and loads skill/memory
    sources by relative path.
    """
    try:
        from deepagents import create_deep_agent
    except Exception as exc:  # pragma: no cover - environment dependent
        raise AssistantBuildError(f"deepagents not available: {exc}") from exc

    assistant_dir = Path(assistant_dir).resolve()
    assistant_dir.mkdir(parents=True, exist_ok=True)
    registry = _tool_registry()

    model = build_model(config)
    tools = _resolve_tools(_granted_tool_names(config), registry)
    if extra_tools:
        tools.extend(extra_tools)

    system_prompt = config.system_prompt or _research_instructions()

    subagents = [_build_subagent(s, registry, assistant_dir) for s in config.subagents]

    skills = [
        _posix_under(assistant_dir, s.path or f"skills/{s.name}")
        for s in config.skills
        if s.enabled
    ] or None
    memory = [
        _posix_under(assistant_dir, m.path or f"memory/{m.name}")
        for m in config.memory
        if m.enabled
    ] or None

    # Only mount a real on-disk backend when the assistant actually has skills or
    # memory to load from its folder. Otherwise use the default in-memory
    # StateBackend so the agent's file tools (ls/read_file/write_file/glob/grep)
    # operate on a virtual FS — never the host's real filesystem (which let the
    # agent list C:\ and read its own config).
    backend = None
    if skills is not None or memory is not None:
        from deepagents.backends.filesystem import FilesystemBackend

        backend = FilesystemBackend(root_dir=str(assistant_dir))
    interrupt_on = _interrupt_on(config) or None
    extra_middleware = _build_extra_middleware(config, model, backend)

    kwargs: dict[str, Any] = {
        "model": model,
        "tools": tools,
        "system_prompt": system_prompt,
        "subagents": subagents,
    }
    if backend is not None:
        kwargs["backend"] = backend
    if skills is not None:
        kwargs["skills"] = skills
    if memory is not None:
        kwargs["memory"] = memory
    if interrupt_on is not None:
        kwargs["interrupt_on"] = interrupt_on
    if extra_middleware:
        kwargs["middleware"] = extra_middleware
    if checkpointer is not None:
        kwargs["checkpointer"] = checkpointer

    logger.info(
        "assistant_builder.build id=%s provider=%s tools=%s subagents=%s skills=%s memory=%s interrupt=%s",
        config.assistant_id,
        config.model.provider,
        [t.name for t in config.tools if t.permission != "deny"],
        [s.name for s in config.subagents],
        len(skills or []),
        len(memory or []),
        list((interrupt_on or {}).keys()),
    )
    try:
        return create_deep_agent(**kwargs)
    except TypeError as exc:
        if checkpointer is None or "checkpointer" not in str(exc):
            raise
        kwargs.pop("checkpointer", None)
        return create_deep_agent(**kwargs)
