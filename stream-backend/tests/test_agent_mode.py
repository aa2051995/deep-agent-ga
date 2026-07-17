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


@pytest.mark.asyncio
async def test_dummy_agent_multiple_runs_on_one_thread_each_have_content():
    # A demo run on a thread that already has runs must still project to its own
    # messages/subagents (accumulation + run-scoped ids), not an empty snapshot.
    repo = InMemoryRepository()
    await repo.ensure_thread("t1", "assistant")
    runner = DeepAgentDemoRunner(repo)
    seen_ids: set[str] = set()
    for index in range(3):
        run = RunRecord(run_id=f"aaaa{index}111-2222-3333", thread_id="t1", assistant_id="assistant")
        await repo.create_run(run)
        await runner.run(run, f"demo {index}")
        snap = await repo.get_run_snapshot("t1", run.run_id)
        assert snap is not None
        assert len(snap.messages) >= 2, f"run {index} projected to empty messages"
        assert len(snap.subagents) == 2
        ids = {m.get("id") for m in snap.messages}
        assert not (ids & seen_ids), "message ids collided across runs"
        seen_ids.update(ids)
