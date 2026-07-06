import unittest
from typing import Any

from app.models import RunRecord
from app.service import ProtocolService
from app.store import InMemoryRepository


class FakeRunScheduler:
    def __init__(self) -> None:
        self.runs: list[tuple[dict[str, Any], Any]] = []
        self.resumes: list[tuple[dict[str, Any], Any]] = []
        self.revokes: list[tuple[str, bool]] = []

    def enqueue_run(self, run_record: dict[str, Any], input_value: Any = None) -> str:
        self.runs.append((run_record, input_value))
        return f"task-run-{len(self.runs)}"

    def enqueue_resume(self, run_record: dict[str, Any], resume_value: Any = None) -> str:
        self.resumes.append((run_record, resume_value))
        return f"task-resume-{len(self.resumes)}"

    def revoke(self, task_id: str, *, terminate: bool = False) -> None:
        self.revokes.append((task_id, terminate))


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
        self.assertEqual(saved.metadata["celery_task_id"], "task-run-1")
        self.assertEqual(saved.metadata["celery_action"], "run")

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
        self.assertEqual(saved.metadata["celery_task_id"], "task-resume-1")
        self.assertEqual(saved.metadata["celery_action"], "resume")
        self.assertEqual(saved.kwargs["resume"], {"answer": "continue"})

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

        cancelled = await service.cancel_run("thread-1", "run-1")
        saved = await repo.get_run("thread-1", "run-1")

        self.assertTrue(cancelled)
        self.assertEqual(scheduler.revokes, [("task-run-1", False)])
        self.assertIsNotNone(saved)
        self.assertTrue(saved.cancel_requested)
        self.assertEqual(saved.status, "interrupted")


if __name__ == "__main__":
    unittest.main()
