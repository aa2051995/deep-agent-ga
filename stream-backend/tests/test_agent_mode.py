"""Tests for the testing/live agent-mode switch and the dummy agent's todos."""
from __future__ import annotations

import pytest

from app.agent_mode import is_test_agent_enabled, resolve_agent_mode
from app.deep_agent import DUMMY_TODOS_DONE, DeepAgentDemoRunner
from app.models import RunRecord
from app.store import InMemoryRepository


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("STREAM_BACKEND_TEST_AGENT", raising=False)
    monkeypatch.delenv("STREAM_BACKEND_AGENT_MODE", raising=False)


def test_test_agent_flag(monkeypatch):
    assert is_test_agent_enabled() is False
    monkeypatch.setenv("STREAM_BACKEND_TEST_AGENT", "true")
    assert is_test_agent_enabled() is True
    monkeypatch.setenv("STREAM_BACKEND_TEST_AGENT", "0")
    assert is_test_agent_enabled() is False


def test_resolve_defaults_to_auto():
    assert resolve_agent_mode() == "auto"


def test_test_agent_flag_takes_precedence(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_TEST_AGENT", "1")
    monkeypatch.setenv("STREAM_BACKEND_AGENT_MODE", "live")
    assert resolve_agent_mode() == "fixture"


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("testing", "fixture"),
        ("fixture", "fixture"),
        ("dummy", "fixture"),
        ("live", "research"),
        ("research", "research"),
        ("auto", "auto"),
        ("garbage", "auto"),
    ],
)
def test_resolve_agent_mode_values(monkeypatch, mode, expected):
    monkeypatch.setenv("STREAM_BACKEND_AGENT_MODE", mode)
    assert resolve_agent_mode() == expected


@pytest.mark.asyncio
async def test_dummy_agent_streams_todos_and_two_subagents():
    repo = InMemoryRepository()
    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).run(run, "Investigate the protocol")

    events = await repo.list_events("t1")
    todo_updates = [
        e for e in events
        if e.method == "updates" and isinstance(e.params.data, dict) and "todos" in e.params.data
    ]
    assert todo_updates, "dummy agent should stream todo updates"

    snapshot = await repo.get_run_snapshot("t1", "r1")
    assert snapshot is not None
    assert snapshot.todos == DUMMY_TODOS_DONE
    assert len(snapshot.subagents) == 2
