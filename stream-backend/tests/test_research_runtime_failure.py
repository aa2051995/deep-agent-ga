import asyncio
import os
import unittest
from typing import Any
from unittest.mock import patch

from app.models import Checkpoint, RunRecord, ThreadRecord, ThreadState
from app.research_runtime import ResearchDeepAgentRunner


class _BoomError(RuntimeError):
    """Stand-in for a mid-stream failure such as GraphRecursionError."""


class _FakeAgent:
    """Agent whose event stream raises before completing."""

    def astream_events(self, *args: Any, **kwargs: Any):
        async def _gen():
            if False:  # pragma: no cover - keeps this an async generator
                yield {}
            raise _BoomError("Recursion limit of 25 reached")

        return _gen()


class _FakeRepo:
    """Minimal repository capturing the calls the runner makes."""

    def __init__(self) -> None:
        self.saved_runs: list[RunRecord] = []
        self.appended: list[tuple[str, dict[str, Any]]] = []
        self.saved_snapshots: list[Any] = []
        self.thread = ThreadRecord(
            thread_id="t1",
            assistant_id="a1",
            state=ThreadState(
                values={"messages": []},
                next=[],
                checkpoint=Checkpoint(thread_id="t1", checkpoint_id="cp0"),
                metadata={"step": 0},
            ),
        )

    async def save_run(self, run: RunRecord) -> None:
        # Store a copy of the status at call time so later mutations don't mask it.
        self.saved_runs.append(run.model_copy(deep=True))

    async def append_event(self, thread_id: str, channel: str, data: dict[str, Any]) -> None:
        self.appended.append((channel, data))

    async def ensure_thread(self, thread_id: str, assistant_id: str | None) -> ThreadRecord:
        return self.thread

    async def save_thread_state(self, thread_id: str, state: ThreadState) -> None:
        self.thread.state = state

    async def get_history(self, thread_id: str, limit: int = 200) -> list[ThreadState]:
        return []

    async def save_run_snapshot(self, snapshot: Any) -> None:
        self.saved_snapshots.append(snapshot)


def _make_runner(repo: _FakeRepo) -> ResearchDeepAgentRunner:
    runner = ResearchDeepAgentRunner(repo)  # type: ignore[arg-type]
    runner._agent = _FakeAgent()
    runner._prompt_mtime = None
    runner._current_prompt_mtime = lambda: None  # type: ignore[method-assign]
    return runner


def _run_record() -> RunRecord:
    return RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="running")


class ResearchRuntimeFailureTests(unittest.TestCase):
    def test_run_reraises_and_marks_error_on_stream_failure(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with self.assertRaises(_BoomError):
            asyncio.run(runner.run(run, "please research X"))

        # The failure must propagate so the worker never records a success.
        self.assertEqual(run.status, "error")
        # A terminal "failed" lifecycle event is emitted for stream consumers.
        failed = [d for ch, d in repo.appended if ch == "lifecycle" and d.get("event") == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertIn("Recursion limit", failed[0]["error"])

    def test_run_persists_snapshot_on_failure(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with self.assertRaises(_BoomError):
            asyncio.run(runner.run(run, "please research X"))

        # A run snapshot is persisted even on failure so the checkpoints
        # endpoint has something to serve instead of finding nothing.
        self.assertEqual(len(repo.saved_snapshots), 1)
        self.assertEqual(repo.saved_snapshots[0].status, "error")

    def test_run_config_reads_recursion_limit_from_env(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "75"}):
            config = runner._run_config(run)
        self.assertEqual(config["recursion_limit"], 75)

    def test_run_config_ignores_invalid_recursion_limit(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "not-a-number"}):
            config = runner._run_config(run)
        self.assertNotIn("recursion_limit", config)

    def test_run_config_respects_explicit_recursion_limit(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        run.kwargs = {"config": {"recursion_limit": 10}}

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "75"}):
            config = runner._run_config(run)
        # An explicit caller value wins over the env default.
        self.assertEqual(config["recursion_limit"], 10)


if __name__ == "__main__":
    unittest.main()
