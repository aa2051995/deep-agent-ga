import importlib.util
import sys
import unittest
from unittest.mock import MagicMock, patch

from app.models import RunRecord
from app.service import ProtocolService
from app.store import InMemoryRepository

CELERY_AVAILABLE = importlib.util.find_spec("celery") is not None


def make_scheduler(*, active: bool = True):
    """A real CeleryRunScheduler with only its external I/O mocked.

    Exercises the actual enqueue/revoke/is_task_active code paths (no fake), but
    never touches a live broker: send_task echoes the explicit task_id, control
    is captured, and task-activity is driven through the result-backend path.
    """
    from worker.client import CeleryRunScheduler

    scheduler = CeleryRunScheduler()

    def _send(name, kwargs=None, queue=None, task_id=None):
        result = MagicMock()
        result.id = task_id or "celery-generated-id"
        return result

    scheduler.app.send_task = MagicMock(side_effect=_send)
    scheduler.app.control = MagicMock()
    scheduler.app.conf.result_backend = "rpc://"
    status_result = MagicMock()
    status_result.status = "STARTED" if active else "SUCCESS"
    scheduler.app.AsyncResult = MagicMock(return_value=status_result)
    return scheduler


@unittest.skipIf(not CELERY_AVAILABLE, "celery is not installed")
class CelerySchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_run_task_enqueues_and_persists_worker_metadata(self) -> None:
        repo = InMemoryRepository()
        scheduler = make_scheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent")
        await repo.create_run(run)

        scheduled = await service.start_run_task(run, {"messages": []})
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(scheduled)
        scheduler.app.send_task.assert_called_once()
        call = scheduler.app.send_task.call_args
        self.assertEqual(call.args[0], "deep_research.run_agent")
        self.assertEqual(call.kwargs["kwargs"]["input_value"], {"messages": []})
        self.assertEqual(call.kwargs["kwargs"]["run_record"]["thread_id"], "thread-1")
        self.assertEqual(call.kwargs["kwargs"]["run_record"]["run_id"], "run-1")
        self.assertIsNotNone(saved)
        self.assertEqual(saved.metadata["worker_backend"], "celery")
        self.assertEqual(saved.metadata["celery_action"], "run")
        # Task id is pre-generated, persisted, passed to send_task, and already
        # inside the enqueued run_record (so the worker's copy carries it).
        task_id = saved.metadata["celery_task_id"]
        self.assertTrue(task_id)
        self.assertEqual(call.kwargs["task_id"], task_id)
        self.assertEqual(call.kwargs["kwargs"]["run_record"]["metadata"]["celery_task_id"], task_id)

    async def test_resume_run_does_not_duplicate_known_celery_task(self) -> None:
        repo = InMemoryRepository()
        scheduler = make_scheduler(active=True)  # is_task_active -> True
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
        scheduler.app.send_task.assert_not_called()

    async def test_resume_run_enqueues_detached_run_with_resume_value(self) -> None:
        repo = InMemoryRepository()
        scheduler = make_scheduler()
        service = ProtocolService(repo, run_scheduler=scheduler)
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent")
        await repo.create_run(run)

        resumed = await service.resume_run("thread-1", "run-1", {"answer": "continue"})
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(resumed)
        scheduler.app.send_task.assert_called_once()
        call = scheduler.app.send_task.call_args
        self.assertEqual(call.args[0], "deep_research.resume_agent")
        self.assertEqual(call.kwargs["kwargs"]["resume_value"], {"answer": "continue"})
        self.assertIsNotNone(saved)
        self.assertEqual(saved.metadata["worker_backend"], "celery")
        self.assertEqual(saved.metadata["celery_action"], "resume")
        self.assertEqual(saved.kwargs["resume"], {"answer": "continue"})
        task_id = saved.metadata["celery_task_id"]
        self.assertTrue(task_id)
        self.assertEqual(call.kwargs["task_id"], task_id)

    async def test_cancel_run_revokes_celery_task(self) -> None:
        repo = InMemoryRepository()
        scheduler = make_scheduler()
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
        scheduler.app.control.revoke.assert_called_once_with("task-run-1", terminate=False)
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
