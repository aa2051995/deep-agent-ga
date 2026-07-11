from __future__ import annotations

import logging
import os
from typing import Any, Literal

logger = logging.getLogger("stream_backend.worker.client")


TaskStatus = Literal["PENDING", "STARTED", "SUCCESS", "FAILURE", "RETRY", "REVOKED"]


class CeleryRunScheduler:
    """Thin API-side client for enqueueing agent work onto Celery queues."""

    def __init__(self) -> None:
        try:
            from .celery_app import celery_app
        except Exception as exc:
            raise RuntimeError(
                "Install celery and configure STREAM_BACKEND_CELERY_BROKER_URL "
                "to use STREAM_BACKEND_RUNNER_BACKEND=celery."
            ) from exc
        self.app = celery_app
        self.queue = os.getenv("STREAM_BACKEND_CELERY_QUEUE", "deep-research-runs")

    def get_task_status(self, task_id: str) -> TaskStatus | None:
        """Get the status of a Celery task."""
        try:
            result = self.app.AsyncResult(task_id)
            status = result.status
            return status if isinstance(status, str) else None
        except Exception:
            logger.exception("celery.task_status.failed task_id=%s", task_id)
            return None

    def is_task_active(self, task_id: str) -> bool:
        """Check if a task is still running or pending."""
        status = self.get_task_status(task_id)
        active = status in {"PENDING", "STARTED", "RETRY"}
        logger.debug("celery.task.active task_id=%s status=%s active=%s", task_id, status, active)
        return active

    def enqueue_run(self, run_record: dict[str, Any], input_value: Any = None) -> str:
        result = self.app.send_task(
            "deep_research.run_agent",
            kwargs={"run_record": run_record, "input_value": input_value},
            queue=self.queue,
        )
        logger.info("celery.enqueue.run thread_id=%s run_id=%s task_id=%s", run_record.get("thread_id"), run_record.get("run_id"), result.id)
        return str(result.id)

    def enqueue_resume(self, run_record: dict[str, Any], resume_value: Any = None) -> str:
        result = self.app.send_task(
            "deep_research.resume_agent",
            kwargs={"run_record": run_record, "resume_value": resume_value},
            queue=self.queue,
        )
        logger.info("celery.enqueue.resume thread_id=%s run_id=%s task_id=%s", run_record.get("thread_id"), run_record.get("run_id"), result.id)
        return str(result.id)

    def revoke(self, task_id: str, *, terminate: bool = False) -> None:
        self.app.control.revoke(task_id, terminate=terminate)
        logger.info("celery.revoke task_id=%s terminate=%s", task_id, terminate)
