"""Static catalog describing what an assistant can be granted.

Served to the UI so the create/edit forms can offer real, buildable options
(tools, middleware, model providers, permission modes) instead of free text.
Keep this in sync with the registries in :mod:`app.assistant_builder`.
"""
from __future__ import annotations

from typing import Any

from .assistants import PERMISSIONS

# Custom tools the builder knows how to instantiate by name. deepagents also
# grants built-in file/planning/subagent tools automatically; those are listed
# as ``builtin`` so the UI can show them as always-on.
TOOL_CATALOG: list[dict[str, Any]] = [
    {
        "name": "tavily_search",
        "label": "Tavily Web Search",
        "description": "Search the web with Tavily (needs TAVILY_API_KEY).",
        "kind": "custom",
    },
    {
        "name": "think_tool",
        "label": "Think Tool",
        "description": "Record a private reasoning checkpoint before acting.",
        "kind": "custom",
    },
]

BUILTIN_TOOLS: list[dict[str, Any]] = [
    {"name": "write_todos", "description": "Plan and track a todo list.", "kind": "builtin"},
    {"name": "ls / read_file / write_file / edit_file / glob / grep", "description": "Filesystem operations (scoped to the assistant folder).", "kind": "builtin"},
    {"name": "task", "description": "Delegate to a configured subagent.", "kind": "builtin"},
]

# Optional middleware the builder can construct. The core deepagents stack
# (todo/filesystem/subagents/patch-tool-calls) is always applied and is not
# listed here as toggleable.
MIDDLEWARE_CATALOG: list[dict[str, Any]] = [
    {
        "name": "summarization",
        "label": "Conversation Summarization",
        "description": "Summarize older turns when the context window fills up.",
        "config_schema": {
            "trigger": {"type": "number", "label": "Trigger (fraction of context)", "optional": True},
            "keep": {"type": "number", "label": "Recent messages to keep", "optional": True},
        },
    },
    {
        "name": "anthropic_prompt_caching",
        "label": "Anthropic Prompt Caching",
        "description": "Cache the system prompt on Anthropic models (ignored elsewhere).",
        "config_schema": {},
    },
    {
        "name": "human_in_the_loop",
        "label": "Human-in-the-loop",
        "description": "Pause for approval on tools marked 'ask'. Wired automatically from tool permissions.",
        "config_schema": {},
    },
]

MODEL_PROVIDERS: list[dict[str, Any]] = [
    {"name": "google", "label": "Google Gemini", "example": "gemini-2.5-pro"},
    {"name": "anthropic", "label": "Anthropic Claude", "example": "claude-sonnet-4-5-20250929"},
    {"name": "bedrock", "label": "AWS Bedrock", "example": "anthropic.claude-3-5-sonnet-20240620-v1:0"},
    {"name": "openai", "label": "OpenAI", "example": "gpt-4o"},
]


def catalog() -> dict[str, Any]:
    return {
        "tools": TOOL_CATALOG,
        "builtin_tools": BUILTIN_TOOLS,
        "middleware": MIDDLEWARE_CATALOG,
        "providers": MODEL_PROVIDERS,
        "permissions": list(PERMISSIONS),
    }
