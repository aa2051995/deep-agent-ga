import importlib.util
import sys
import unittest
from typing import Any
from unittest.mock import patch

from app.models import RunRecord
from app.service import ProtocolService
from app.store import InMemoryRepository


class FakeRunScheduler:
    def __init__(self) -> None:
        self.runs: list[tuple[dict[str, Any], Any, str | None]] = []
        self.resumes: list[tuple[dict[str, Any], Any, str | None]] = []
        self.revokes: list[tuple[str, bool]] = []

    def enqueue_run(self, run_record: dict[str, Any], input_value: Any = None, task_id: str | None = None) -> str:
        self.runs.append((run_record, input_value, task_id))
        return task_id or f"task-run-{len(self.runs)}"

    def enqueue_resume(self, run_record: dict[str, Any], resume_value: Any = None, task_id: str | None = None) -> str:
        self.resumes.append((run_record, resume_value, task_id))
        return task_id or f"task-resume-{len(self.resumes)}"

    def revoke(self, task_id: str, *, terminate: bool = False) -> None:
        self.revokes.append((task_id, terminate))

    def is_task_active(self, task_id: str) -> bool:
        return bool(task_id)


class CelerySchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_run_task_enqueues_and_persists_worker_metadata(self) -> None:
        repo = InMemoryRepository()
        scheduler = FakeRunScheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent")
        await repo.create_run(run)

        scheduled = await service.start_run_task(run, {"messages": []})
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(scheduled)
        self.assertEqual(len(scheduler.runs), 1)
        self.assertEqual(scheduler.runs[0][0]["thread_id"], "thread-1")
        self.assertEqual(scheduler.runs[0][0]["run_id"], "run-1")
        self.assertEqual(scheduler.runs[0][1], {"messages": []})
        self.assertIsNotNone(saved)
        self.assertEqual(saved.metadata["worker_backend"], "celery")
        self.assertEqual(saved.metadata["celery_action"], "run")
        # Task id is pre-generated, persisted, passed to enqueue, and already in
        # the enqueued run_record (so the worker's copy carries it).
        task_id = saved.metadata["celery_task_id"]
        self.assertTrue(task_id)
        self.assertEqual(scheduler.runs[0][2], task_id)
        self.assertEqual(scheduler.runs[0][0]["metadata"]["celery_task_id"], task_id)

    async def test_resume_run_does_not_duplicate_known_celery_task(self) -> None:
        repo = InMemoryRepository()
        scheduler = FakeRunScheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(
            run_id="run-1",
            thread_id="thread-1",
            assistant_id="deep-agent",
            metadata={"worker_backend": "celery", "celery_task_id": "task-run-1"},
        )
        await repo.create_run(run)

        resumed = await service.resume_run("thread-1", "run-1")

        self.assertTrue(resumed)
        self.assertEqual(scheduler.resumes, [])

    async def test_resume_run_enqueues_detached_run_with_resume_value(self) -> None:
        repo = InMemoryRepository()
        scheduler = FakeRunScheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent")
        await repo.create_run(run)

        resumed = await service.resume_run("thread-1", "run-1", {"answer": "continue"})
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(resumed)
        self.assertEqual(len(scheduler.resumes), 1)
        self.assertEqual(scheduler.resumes[0][0]["thread_id"], "thread-1")
        self.assertEqual(scheduler.resumes[0][0]["run_id"], "run-1")
        self.assertEqual(scheduler.resumes[0][1], {"answer": "continue"})
        self.assertIsNotNone(saved)
        self.assertEqual(saved.metadata["worker_backend"], "celery")
        self.assertEqual(saved.metadata["celery_action"], "resume")
        self.assertEqual(saved.kwargs["resume"], {"answer": "continue"})
        task_id = saved.metadata["celery_task_id"]
        self.assertTrue(task_id)
        self.assertEqual(scheduler.resumes[0][2], task_id)
        self.assertEqual(scheduler.resumes[0][0]["metadata"]["celery_task_id"], task_id)

    async def test_cancel_run_revokes_celery_task(self) -> None:
        repo = InMemoryRepository()
        scheduler = FakeRunScheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(
            run_id="run-1",
            thread_id="thread-1",
            assistant_id="deep-agent",
            metadata={"worker_backend": "celery", "celery_task_id": "task-run-1"},
        )
        await repo.create_run(run)

        with patch.dict("os.environ", {"STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL": "false"}):
            cancelled = await service.cancel_run("thread-1", "run-1")
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(cancelled)
        self.assertEqual(scheduler.revokes, [("task-run-1", False)])
        self.assertIsNotNone(saved)
        self.assertTrue(saved.cancel_requested)
        self.assertEqual(saved.status, "interrupted")


@unittest.skipIf(importlib.util.find_spec("celery") is None, "celery is not installed")
class CeleryTaskRegistrationTests(unittest.TestCase):
    def test_worker_tasks_are_registered_when_app_module_loads(self) -> None:
        from worker.celery_app import celery_app

        self.assertIn("deep_research.run_agent", celery_app.tasks)
        self.assertIn("deep_research.resume_agent", celery_app.tasks)


class WindowsEventLoopPolicyTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "win32", "Windows-only event loop policy")
    def test_worker_uses_selector_event_loop_policy_on_windows(self) -> None:
        import asyncio

        from worker.asyncio_policy import configure_windows_event_loop_policy

        configure_windows_event_loop_policy()

        self.assertIsInstance(
            asyncio.get_event_loop_policy(),
            asyncio.WindowsSelectorEventLoopPolicy,
        )


if __name__ == "__main__":
    unittest.main()
