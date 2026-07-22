"""Unit tests for the config->agent mapping helpers.

These cover the pure mapping logic (permissions, tool grants, path handling,
MCP short-circuit) without compiling a real model-backed agent.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from app import assistant_builder as ab
from app.assistants import (
    AssistantConfig,
    MCPServerConfig,
    ToolConfig,
)


def test_granted_tool_names_excludes_denied():
    config = AssistantConfig(
        assistant_id="t",
        name="T",
        tools=[
            ToolConfig(name="tavily_search", permission="allow"),
            ToolConfig(name="think_tool", permission="ask"),
            ToolConfig(name="secret_tool", permission="deny"),
        ],
    )
    assert ab._granted_tool_names(config) == ["tavily_search", "think_tool"]


def test_interrupt_on_maps_ask_permissions():
    config = AssistantConfig(
        assistant_id="t",
        name="T",
        tools=[
            ToolConfig(name="tavily_search", permission="allow"),
            ToolConfig(name="write_file", permission="ask"),
        ],
        mcp=[
            MCPServerConfig(name="github", permission="ask", enabled=True),
            MCPServerConfig(name="disabled", permission="ask", enabled=False),
        ],
    )
    interrupt = ab._interrupt_on(config)
    assert interrupt == {"write_file": True, "github": True}


def test_posix_under_is_absolute_posix(tmp_path):
    result = ab._posix_under(Path(tmp_path), "skills/web-research")
    assert result.endswith("skills/web-research")
    assert "\\" not in result


def test_load_mcp_tools_empty_without_servers():
    config = AssistantConfig(assistant_id="t", name="T")
    assert asyncio.run(ab.load_mcp_tools(config)) == []


def test_build_model_passes_api_key_openai(monkeypatch):
    captured = {}

    def fake_init_chat_model(**kwargs):
        captured.update(kwargs)
        return object()

    import langchain.chat_models as cm

    monkeypatch.setattr(cm, "init_chat_model", fake_init_chat_model)
    config = AssistantConfig(
        assistant_id="t",
        name="T",
        model={"provider": "openai", "name": "gpt-4o", "temperature": 0.2, "api_key": "sk-test"},
    )
    ab.build_model(config)
    assert captured["api_key"] == "sk-test"
    assert captured["model"] == "openai:gpt-4o"


def test_build_model_omits_api_key_when_absent(monkeypatch):
    captured = {}

    def fake_init_chat_model(**kwargs):
        captured.update(kwargs)
        return object()

    import langchain.chat_models as cm

    monkeypatch.setattr(cm, "init_chat_model", fake_init_chat_model)
    config = AssistantConfig(
        assistant_id="t",
        name="T",
        model={"provider": "anthropic", "name": "claude-sonnet-4-5-20250929"},
    )
    ab.build_model(config)
    assert "api_key" not in captured


def test_resolve_tools_skips_unknown():
    # Use an explicit registry so the test exercises the pure resolution logic
    # without importing research_agent.tools (whose root copy eagerly needs
    # TAVILY_API_KEY and depends on sys.path ordering).
    registry = {"tavily_search": object(), "think_tool": object()}
    resolved = ab._resolve_tools(["tavily_search", "does_not_exist"], registry)
    assert len(resolved) == 1
