import asyncio
import os
import unittest
from typing import Any
from unittest.mock import patch

from app.models import Checkpoint, RunRecord, ThreadRecord, ThreadState
from app.research_runtime import (
    ResearchDeepAgentRunner,
    RunCancelled,
    _cancel_poll_interval_seconds,
)


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


class _CancellableAgent:
    """Agent whose event stream yields several events, tracking how many were
    consumed and whether the generator was closed — lets tests confirm the
    streaming loop actually stopped early on cancellation rather than running
    every event to completion."""

    def __init__(self, count: int = 5) -> None:
        self.count = count
        self.yielded = 0
        self.closed = False

    def astream_events(self, *args: Any, **kwargs: Any):
        agent = self

        async def _gen():
            try:
                for index in range(agent.count):
                    agent.yielded += 1
                    yield {"event": "on_custom_test_event", "data": {"index": index}}
            finally:
                agent.closed = True

        return _gen()


class _FakeRepo:
    """Minimal repository capturing the calls the runner makes."""

    def __init__(self) -> None:
        self.saved_runs: list[RunRecord] = []
        self.appended: list[tuple[str, dict[str, Any]]] = []
        self.saved_snapshots: list[Any] = []
        # What get_run() returns — the "stored" row a cancel request updates.
        # None means "no row" (matches production's get_run returning None).
        self.run: RunRecord | None = None
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

    async def get_run(self, thread_id: str, run_id: str) -> RunRecord | None:
        return self.run

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


async def _dummy_checkpointer() -> object:
    """Stand-in for _ensure_checkpointer: resume() only checks truthiness."""
    return object()


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

    def test_run_config_defaults_recursion_limit_to_50(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        env = {k: v for k, v in os.environ.items() if k != "LANGGRAPH_RECURSION_LIMIT"}
        with patch.dict(os.environ, env, clear=True):
            config = runner._run_config(run)
        self.assertEqual(config["recursion_limit"], 50)

    def test_run_config_reads_recursion_limit_from_env(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "75"}):
            config = runner._run_config(run)
        self.assertEqual(config["recursion_limit"], 75)

    def test_run_config_falls_back_to_default_on_invalid_limit(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "not-a-number"}):
            config = runner._run_config(run)
        self.assertEqual(config["recursion_limit"], 50)

    def test_run_config_respects_explicit_recursion_limit(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        run.kwargs = {"config": {"recursion_limit": 10}}

        with patch.dict(os.environ, {"LANGGRAPH_RECURSION_LIMIT": "75"}):
            config = runner._run_config(run)
        # An explicit caller value wins over the env default.
        self.assertEqual(config["recursion_limit"], 10)


class ResearchRuntimeCancellationTests(unittest.TestCase):
    """A cancel request is the ONLY thing that stops an in-progress Celery run:
    task.cancel() (used for the in-process asyncio backend) has no equivalent
    there, and Celery's terminate=True cannot kill a running thread-pool worker
    (celery.concurrency.thread.TaskPool has no kill_job). These cover the
    cooperative poll that makes cancellation actually work in that case.
    """

    def test_run_stops_early_and_marks_interrupted_on_cancel_request(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        agent = _CancellableAgent(count=5)
        runner._agent = agent
        # The stored row a cancel request would have updated.
        repo.run = run.model_copy(deep=True)
        repo.run.cancel_requested = True

        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "0"}):
            with self.assertRaises(RunCancelled):
                asyncio.run(runner.run(run, "please research X"))

        self.assertEqual(run.status, "interrupted")
        # Stopped after the first event, not after consuming all 5.
        self.assertLess(agent.yielded, agent.count)
        self.assertTrue(agent.closed)

        interrupted = [d for ch, d in repo.appended if ch == "lifecycle" and d.get("event") == "interrupted"]
        self.assertEqual(len(interrupted), 1)
        self.assertEqual(interrupted[0].get("reason"), "cancelled")
        # A cancellation must never also be reported as a failure.
        failed = [d for ch, d in repo.appended if ch == "lifecycle" and d.get("event") == "failed"]
        self.assertEqual(failed, [])

    def test_run_persists_snapshot_on_cancel(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        runner._agent = _CancellableAgent(count=5)
        repo.run = run.model_copy(deep=True)
        repo.run.cancel_requested = True

        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "0"}):
            with self.assertRaises(RunCancelled):
                asyncio.run(runner.run(run, "please research X"))

        self.assertEqual(len(repo.saved_snapshots), 1)
        self.assertEqual(repo.saved_snapshots[0].status, "interrupted")

    def test_run_completes_normally_when_not_cancelled(self) -> None:
        # A zero poll interval alone must not cause spurious cancellation.
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        agent = _CancellableAgent(count=5)
        runner._agent = agent
        repo.run = run.model_copy(deep=True)
        repo.run.cancel_requested = False

        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "0"}):
            asyncio.run(runner.run(run, "please research X"))

        self.assertEqual(run.status, "success")
        self.assertEqual(agent.yielded, agent.count)

    def test_resume_stops_early_and_marks_interrupted_on_cancel_request(self) -> None:
        repo = _FakeRepo()
        runner = _make_runner(repo)
        run = _run_record()
        agent = _CancellableAgent(count=5)
        runner._agent = agent
        # resume() requires a checkpointer before it will proceed; a real one
        # needs Postgres, so stand in with a truthy dummy.
        runner._ensure_checkpointer = _dummy_checkpointer  # type: ignore[method-assign]
        repo.run = run.model_copy(deep=True)
        repo.run.cancel_requested = True

        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "0"}):
            with self.assertRaises(RunCancelled):
                asyncio.run(runner.resume(run, {"answer": "yes"}))

        self.assertEqual(run.status, "interrupted")
        self.assertLess(agent.yielded, agent.count)
        interrupted = [d for ch, d in repo.appended if ch == "lifecycle" and d.get("event") == "interrupted"]
        self.assertEqual(len(interrupted), 1)


class CancelPollIntervalTests(unittest.TestCase):
    def test_defaults_to_one_second(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "RESEARCH_AGENT_CANCEL_POLL_INTERVAL"}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(_cancel_poll_interval_seconds(), 1.0)

    def test_reads_env_override(self) -> None:
        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "2.5"}):
            self.assertEqual(_cancel_poll_interval_seconds(), 2.5)

    def test_falls_back_to_default_on_invalid_value(self) -> None:
        with patch.dict(os.environ, {"RESEARCH_AGENT_CANCEL_POLL_INTERVAL": "soon"}):
            self.assertEqual(_cancel_poll_interval_seconds(), 1.0)


if __name__ == "__main__":
    unittest.main()
