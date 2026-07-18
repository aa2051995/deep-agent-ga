import asyncio
import unittest
from typing import Any
from unittest.mock import patch

from app.models import RunRecord


class _BoomError(RuntimeError):
    pass


class _RaisingRunner:
    async def run(self, run: RunRecord, input_value: Any) -> None:
        # Simulate the real runner: record error + re-raise (as the fixed
        # ResearchDeepAgentRunner now does) instead of swallowing.
        run.status = "error"
        raise _BoomError("Recursion limit of 25 reached")


class _FakeRepo:
    def __init__(self, run: RunRecord) -> None:
        self._run = run
        self.status_history: list[str] = []

    async def setup(self) -> None:  # pragma: no cover - trivial
        pass

    async def close(self) -> None:  # pragma: no cover - trivial
        pass

    async def get_run(self, thread_id: str, run_id: str) -> RunRecord:
        return self._run

    async def save_run(self, run: RunRecord) -> None:
        self._run = run
        self.status_history.append(run.status)

    async def append_event(self, thread_id: str, channel: str, data: dict[str, Any]) -> None:
        pass


class WorkerFailureStatusTests(unittest.TestCase):
    def test_execute_run_direct_marks_error_never_success(self) -> None:
        from worker import tasks

        run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="pending")
        repo = _FakeRepo(run)

        with patch.object(tasks, "create_publishing_repository", return_value=repo), patch.object(
            tasks, "runner_for_mode", return_value=_RaisingRunner()
        ):
            with self.assertRaises(_BoomError):
                asyncio.run(
                    tasks.execute_run_direct(
                        run.model_dump(), action="run", input_value="research X"
                    )
                )

        # The regression: a failed run must never be recorded as success.
        self.assertNotIn("success", repo.status_history)
        self.assertEqual(repo.status_history[-1], "error")


class DeterministicErrorRetryTests(unittest.TestCase):
    def test_deterministic_error_set_contents(self) -> None:
        from worker import tasks
        from app.research_runtime import ResearchRuntimeUnavailable

        # Deterministic failures that must NOT be retried.
        for exc_type in (ValueError, TypeError, KeyError, ResearchRuntimeUnavailable):
            self.assertIn(exc_type, tasks.DETERMINISTIC_RUN_ERRORS)
        if tasks.GraphRecursionError is not None:
            self.assertIn(tasks.GraphRecursionError, tasks.DETERMINISTIC_RUN_ERRORS)
        # Transient/infra failures should still fall through to autoretry.
        self.assertNotIn(ConnectionError, tasks.DETERMINISTIC_RUN_ERRORS)
        self.assertNotIn(TimeoutError, tasks.DETERMINISTIC_RUN_ERRORS)

    def test_run_agent_swallows_deterministic_error_without_retry(self) -> None:
        from worker import tasks

        calls = {"n": 0}

        async def _boom(*args: Any, **kwargs: Any) -> None:
            calls["n"] += 1
            raise ValueError("malformed run payload")

        run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="pending")

        with patch.object(tasks, "execute_run_direct", _boom):
            result = tasks.run_agent.apply(args=[run.model_dump(), "research X"])

        # Deterministic error is swallowed inside the task, so Celery never
        # engages autoretry: the task completes and execute_run_direct runs once.
        self.assertTrue(result.successful())
        self.assertEqual(calls["n"], 1)


if __name__ == "__main__":
    unittest.main()
