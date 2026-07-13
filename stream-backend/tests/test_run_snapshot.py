"""Unit tests for the run-snapshot fast-path persistence.

A completed run is projected once and stored in a dedicated snapshot table so
run data can be fetched with a single keyed lookup instead of scanning the
thread's checkpoint history.  These tests cover the projection, the repository
round-trip, the runner integration, and the endpoint fast path/fallback.
"""
from __future__ import annotations

import pytest

from app.models import Checkpoint, RunRecord, RunSnapshot, ThreadState
from app.projections import build_run_snapshot, project_run_checkpoints
from app.store import InMemoryRepository


def _root_state(run_id: str, messages: list[dict], *, next_nodes: list[str], parent: Checkpoint | None, step: int) -> ThreadState:
    return ThreadState(
        values={"messages": messages, "todos": [{"content": "task", "status": "completed"}]},
        next=next_nodes,
        checkpoint=Checkpoint(thread_id="t1", checkpoint_id=f"cp-{step}"),
        parent_checkpoint=parent,
        metadata={"step": step, "run_id": run_id},
    )


def test_build_run_snapshot_matches_live_projection() -> None:
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="success")
    root = Checkpoint(thread_id="t1", checkpoint_id="cp-0")
    initial = _root_state(
        "r1",
        [{"id": "m1", "type": "human", "content": "hello"}],
        next_nodes=["agent"],
        parent=root,
        step=1,
    )
    final = _root_state(
        "r1",
        [
            {"id": "m1", "type": "human", "content": "hello"},
            {"id": "m2", "type": "ai", "content": "answer"},
        ],
        next_nodes=[],
        parent=initial.checkpoint,
        step=2,
    )
    # get_history returns newest-first.
    history = [final, initial]

    snapshot = build_run_snapshot(run, history)
    live = project_run_checkpoints(run, history)

    assert snapshot.checkpoint_id == "cp-2"
    assert snapshot.to_projection()["messages"] == live["messages"]
    assert snapshot.to_projection()["values"] == live["values"]
    assert snapshot.to_projection()["checkpoints"] == live["checkpoints"]
    assert snapshot.to_projection()["from_snapshot"] is True


@pytest.mark.asyncio
async def test_repository_snapshot_roundtrip_and_delete() -> None:
    repo = InMemoryRepository()
    await repo.setup() if hasattr(repo, "setup") else None
    await repo.ensure_thread("t1")

    snapshot = RunSnapshot(
        thread_id="t1",
        run_id="r1",
        assistant_id="a1",
        status="success",
        checkpoint_id="cp-2",
        run={"run_id": "r1"},
        values={"messages": []},
        messages=[{"id": "m2", "type": "ai", "content": "answer"}],
    )
    await repo.save_run_snapshot(snapshot)

    fetched = await repo.get_run_snapshot("t1", "r1")
    assert fetched is not None
    assert fetched.run_id == "r1"
    assert fetched.checkpoint_id == "cp-2"
    assert len(fetched.messages) == 1

    # Unknown run returns None.
    assert await repo.get_run_snapshot("t1", "missing") is None

    # Deleting the thread cascades to its snapshots.
    assert await repo.delete_thread("t1") is True
    assert await repo.get_run_snapshot("t1", "r1") is None


@pytest.mark.asyncio
async def test_fixture_runner_persists_snapshot() -> None:
    from app.deep_agent import DeepAgentDemoRunner

    repo = InMemoryRepository()
    thread = await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant")
    await repo.create_run(run)

    await DeepAgentDemoRunner(repo).run(run, "Investigate the protocol")

    final_run = await repo.get_run("t1", "r1")
    assert final_run is not None
    assert final_run.status == "success"

    snapshot = await repo.get_run_snapshot("t1", "r1")
    assert snapshot is not None
    assert snapshot.status == "success"
    assert snapshot.run_id == "r1"
    # The fixture calls the task tool twice -> two subagents in the projection.
    assert len(snapshot.subagents) == 2

    # Stored snapshot equals a live projection of the same history.
    history = await repo.get_history("t1", limit=200)
    live = project_run_checkpoints(final_run, history)
    assert snapshot.messages == live["messages"]
    assert snapshot.subagents == live["subagents"]


@pytest.mark.asyncio
async def test_get_run_checkpoints_prefers_snapshot(monkeypatch) -> None:
    import app.main as main

    repo = InMemoryRepository()
    monkeypatch.setattr(main, "repo", repo)

    await repo.ensure_thread("t1", "assistant")
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="assistant", status="success")
    await repo.create_run(run)

    # Without a snapshot the endpoint projects live from history (fallback).
    fallback = await main.get_run_checkpoints("t1", "r1")
    assert "from_snapshot" not in fallback

    # With a snapshot the endpoint serves the pre-projected fast path.
    await repo.save_run_snapshot(
        RunSnapshot(
            thread_id="t1",
            run_id="r1",
            status="success",
            checkpoint_id="cp-2",
            run=run.model_dump(),
            messages=[{"id": "m2", "type": "ai", "content": "answer"}],
            todos=[{"content": "task", "status": "completed"}],
        )
    )
    fast = await main.get_run_checkpoints("t1", "r1")
    assert fast["from_snapshot"] is True
    assert len(fast["messages"]) == 1
    assert fast["todos"] == [{"content": "task", "status": "completed"}]


@pytest.mark.asyncio
async def test_get_run_checkpoints_missing_run(monkeypatch) -> None:
    import app.main as main
    from fastapi import HTTPException

    repo = InMemoryRepository()
    monkeypatch.setattr(main, "repo", repo)
    await repo.ensure_thread("t1")

    with pytest.raises(HTTPException) as excinfo:
        await main.get_run_checkpoints("t1", "does-not-exist")
    assert excinfo.value.status_code == 404
