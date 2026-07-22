"""AI-assisted authoring for assistant system prompts and skills.

The UI's "help me write this" buttons call these helpers. When a chat model is
reachable (per the requested provider/model) the helper asks it to draft the
content; otherwise it falls back to a deterministic, well-structured template so
the feature works offline and in tests without burning API calls.
"""
from __future__ import annotations

import logging
from typing import Any

from .assistants import ModelConfig, slugify

logger = logging.getLogger("stream_backend.assistant_assist")


SYSTEM_PROMPT_META = (
    "You are an expert at writing system prompts for tool-using AI agents built "
    "on the deepagents library. Write a clear, structured system prompt for the "
    "described assistant. Cover: the agent's role and objective, how it should "
    "use its tools and subagents, its working style, and guardrails. Output only "
    "the prompt text — no preamble, no markdown code fences."
)

SKILL_META = (
    "You are an expert at writing Anthropic Agent Skills (SKILL.md files). Given "
    "a skill name and a description of what it should do, write a complete "
    "SKILL.md with YAML frontmatter (name, description) followed by markdown "
    "sections: '## When to Use', '## Steps', and '## Notes'. Output only the "
    "file contents."
)


def probe_model(model_config: ModelConfig) -> dict[str, Any]:
    """Construct the model and do a tiny round-trip to verify it works.

    Returns ``{"ok": bool, "message": str, "sample": str}``. Surfaces the real
    provider error (bad key, unknown model, missing package) instead of hiding
    it, so the UI's "Test" button can show why a config fails.
    """
    try:
        from .assistant_builder import build_model
        from .assistants import AssistantConfig

        stub = AssistantConfig(assistant_id="_probe", name="probe", model=model_config)
        model = build_model(stub)
    except Exception as exc:
        return {"ok": False, "message": f"Could not construct model: {exc}"}
    try:
        from langchain_core.messages import HumanMessage

        result = model.invoke([HumanMessage(content="Reply with the single word: ok")])
        content = getattr(result, "content", result)
        if isinstance(content, list):
            content = "".join(
                str(block.get("text", "")) if isinstance(block, dict) else str(block)
                for block in content
            )
        sample = str(content).strip()[:200]
        return {
            "ok": True,
            "message": f"{model_config.provider}:{model_config.name} responded successfully.",
            "sample": sample,
        }
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def _try_model(model_config: ModelConfig) -> Any | None:
    try:
        from .assistant_builder import build_model
        from .assistants import AssistantConfig

        stub = AssistantConfig(assistant_id="_assist", name="assist", model=model_config)
        return build_model(stub)
    except Exception:
        logger.info("assist.model_unavailable provider=%s", model_config.provider, exc_info=True)
        return None


def _invoke(model: Any, system: str, user: str) -> str | None:
    try:
        from langchain_core.messages import HumanMessage, SystemMessage

        result = model.invoke([SystemMessage(content=system), HumanMessage(content=user)])
        content = getattr(result, "content", result)
        if isinstance(content, list):
            return "".join(
                str(block.get("text", "")) if isinstance(block, dict) else str(block)
                for block in content
            ).strip()
        return str(content).strip()
    except Exception:
        logger.warning("assist.invoke_failed", exc_info=True)
        return None


def generate_system_prompt(
    *,
    name: str,
    description: str,
    tools: list[str],
    subagents: list[str],
    model_config: ModelConfig | None = None,
    instructions: str = "",
) -> dict[str, Any]:
    user = (
        f"Assistant name: {name}\n"
        f"Purpose: {description or 'a helpful task-oriented agent'}\n"
        f"Available tools: {', '.join(tools) or 'built-in filesystem + planning'}\n"
        f"Subagents it can delegate to: {', '.join(subagents) or 'none'}\n"
        f"Extra instructions: {instructions or 'none'}\n"
    )
    if model_config is not None:
        model = _try_model(model_config)
        if model is not None:
            text = _invoke(model, SYSTEM_PROMPT_META, user)
            if text:
                return {"content": text, "source": "model"}
    return {"content": _fallback_system_prompt(name, description, tools, subagents), "source": "template"}


def generate_skill(
    *,
    name: str,
    description: str,
    model_config: ModelConfig | None = None,
    instructions: str = "",
) -> dict[str, Any]:
    user = (
        f"Skill name: {name}\n"
        f"What it should do: {description}\n"
        f"Extra guidance: {instructions or 'none'}\n"
    )
    if model_config is not None:
        model = _try_model(model_config)
        if model is not None:
            text = _invoke(model, SKILL_META, user)
            if text:
                return {"content": text, "source": "model"}
    return {"content": _fallback_skill(name, description), "source": "template"}


def _fallback_system_prompt(name: str, description: str, tools: list[str], subagents: list[str]) -> str:
    tool_line = ", ".join(tools) if tools else "your built-in filesystem and planning tools"
    sub_line = (
        "You can delegate focused subtasks to: " + ", ".join(subagents) + "."
        if subagents
        else "You handle tasks yourself without subagents."
    )
    return (
        f"# {name}\n\n"
        f"You are {name}, {description or 'a capable, task-oriented AI agent'}.\n\n"
        "## Objective\n"
        "Complete the user's request accurately and efficiently, using tools when they help.\n\n"
        "## Tools\n"
        f"Use {tool_line} deliberately. Prefer gathering evidence before acting, and "
        "explain findings concisely.\n\n"
        "## Delegation\n"
        f"{sub_line}\n\n"
        "## Working style\n"
        "- Be concise and direct; avoid unnecessary preamble.\n"
        "- Plan multi-step work with the todo tool.\n"
        "- Verify your output against what was asked before finishing.\n\n"
        "## Guardrails\n"
        "- Do not fabricate results; say when you are unsure.\n"
        "- Ask for clarification only when genuinely blocked.\n"
    )


def _fallback_skill(name: str, description: str) -> str:
    slug = slugify(name)
    return (
        "---\n"
        f"name: {slug}\n"
        f"description: {description or f'{name} skill'}\n"
        "---\n\n"
        f"# {name}\n\n"
        "## When to Use\n"
        f"- When the user needs help with: {description or name}.\n\n"
        "## Steps\n"
        "1. Understand the request and gather the relevant context.\n"
        "2. Perform the core work using the appropriate tools.\n"
        "3. Verify the result and summarize it for the user.\n\n"
        "## Notes\n"
        "- Keep outputs concise and actionable.\n"
    )
