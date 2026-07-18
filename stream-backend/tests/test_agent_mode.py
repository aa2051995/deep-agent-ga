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
async def test_dummy_agent_subagents_stream_multiple_tools_and_reasoning():
    # The fixture must emit a rich, progressive stream (reasoning messages + several
    # tool calls per subagent) so the UI's subagent cards stream like a real agent
    # instead of a single burst.
    repo = InMemoryRepository()
    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).run(run, "Investigate the protocol")

    events = await repo.list_events("t1")
    tool_started = [
        e for e in events
        if e.method == "tools" and isinstance(e.params.data, dict)
        and e.params.data.get("event") == "tool-started"
    ]
    # 3 tools per subagent (x2) + the 2 orchestrator `task` tool calls.
    assert len(tool_started) >= 8, f"expected many tool calls, got {len(tool_started)}"
    tool_names = {e.params.data.get("tool_name") for e in tool_started}
    assert {"search_web", "query_database", "fetch_page", "profile_columns"} <= tool_names

    message_starts = [
        e for e in events
        if e.method == "messages" and isinstance(e.params.data, dict)
        and e.params.data.get("event") == "message-start"
    ]
    # Many streamed reasoning messages (intro + before/after per tool + final),
    # far more than the old single-message-per-subagent fixture.
    assert len(message_starts) >= 12, f"expected many streamed messages, got {len(message_starts)}"


@pytest.mark.asyncio
async def test_dummy_agent_persists_snapshot_before_marking_success():
    # The run must not be flipped to `success` before its snapshot row exists.
    # Otherwise refreshRuns polling sees success during the (~1s) get_history
    # window, the UI fetches an empty snapshot and caches it, and the completed
    # run never renders without a manual reload.
    calls: list[tuple[str, str | None]] = []

    class SpyRepo(InMemoryRepository):
        async def save_run(self, run):  # type: ignore[override]
            calls.append(("save_run", run.status))
            return await super().save_run(run)

        async def save_run_snapshot(self, snapshot):  # type: ignore[override]
            calls.append(("save_snapshot", None))
            return await super().save_run_snapshot(snapshot)

    repo = SpyRepo()
    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).run(run, "demo")

    snap_idx = next(i for i, c in enumerate(calls) if c[0] == "save_snapshot")
    success_idx = next(i for i, c in enumerate(calls) if c == ("save_run", "success"))
    assert snap_idx < success_idx, f"snapshot must be saved before success: {calls}"


@pytest.mark.asyncio
async def test_dummy_agent_never_leaves_a_breakpoint_next():
    # The LangGraph SDK's useStream synthesizes a spurious "human input" breakpoint
    # interrupt whenever the thread-head checkpoint has a non-empty `next`. The
    # fixture drives its own tools and never pauses for a human, so NO persisted
    # root checkpoint may advertise a pending next (not just the final one) — the
    # intermediate state is the head for the whole subagent window.
    repo = InMemoryRepository()
    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).run(run, "Investigate the protocol")

    history = await repo.get_history("t1", limit=50)
    assert history, "fixture run should persist checkpoints"
    assert all(state.next == [] for state in history), (
        f"no checkpoint may leave a non-empty next (would trigger a breakpoint "
        f"interrupt); got {[s.next for s in history]}"
    )


@pytest.mark.asyncio
async def test_dummy_agent_resume_completes_without_rerun():
    # Continuing/resuming a demo run must finalize it, not re-run the whole demo
    # (which re-emits events and loops the UI's continue/interrupt).
    repo = InMemoryRepository()
    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant", status="running")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).resume(run, {"answer": "continue"})

    final = await repo.get_run("t1", "r1")
    assert final is not None and final.status == "success"
    snap = await repo.get_run_snapshot("t1", "r1")
    assert snap is not None
    # resume acknowledges + finishes; it must NOT spawn the demo's 2 subagents.
    assert len(snap.subagents) == 0
    events = await repo.list_events("t1")
    completed = [
        e for e in events
        if e.method == "lifecycle" and isinstance(e.params.data, dict)
        and e.params.data.get("event") == "completed"
    ]
    assert completed, "resume should emit a completed lifecycle event"


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
