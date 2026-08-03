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
        self.queue = os.getenv("STREAM_BACKEND_CELERY_QUEUE", "deep-agent-ga-runs")

    def _result_backend_enabled(self) -> bool:
        try:
            return bool(self.app.conf.result_backend)
        except Exception:
            return False

    def get_task_status(self, task_id: str) -> TaskStatus | None:
        """Get the status of a Celery task via the result backend.

        Returns None when no result backend is configured (the default) — task
        state simply cannot be queried this way, so callers fall back to the
        worker inspect API. This avoids the noisy
        ``'DisabledBackend' object has no attribute '_get_task_meta_for'`` crash.
        """
        if not self._result_backend_enabled():
            return None
        try:
            status = self.app.AsyncResult(task_id).status
            return status if isinstance(status, str) else None
        except Exception:
            logger.warning("celery.task_status.unavailable task_id=%s", task_id)
            return None

    def is_task_active(self, task_id: str) -> bool:
        """Whether a task is still running/queued on a worker.

        Uses the result backend when one is configured; otherwise asks the
        workers directly over the broker (control/inspect), which does not need a
        result backend.
        """
        if self._result_backend_enabled():
            status = self.get_task_status(task_id)
            active = status in {"PENDING", "STARTED", "RETRY"}
            logger.debug("celery.task.active task_id=%s status=%s active=%s", task_id, status, active)
            return active
        return self._is_task_active_via_inspect(task_id)

    @staticmethod
    def _entry_task_id(entry: Any) -> str | None:
        if not isinstance(entry, dict):
            return None
        task_id = entry.get("id")
        if task_id:
            return str(task_id)
        request = entry.get("request")
        if isinstance(request, dict) and request.get("id"):
            return str(request["id"])
        return None

    def _is_task_active_via_inspect(self, task_id: str) -> bool:
        try:
            timeout = float(os.getenv("STREAM_BACKEND_CELERY_INSPECT_TIMEOUT", "3.0"))
            inspector = self.app.control.inspect(timeout=timeout)
            any_worker_responded = False
            for getter in (inspector.active, inspector.reserved, inspector.scheduled):
                report = getter()
                if report is None:
                    continue  # no worker answered this probe
                any_worker_responded = True
                for entries in report.values():
                    for entry in entries or []:
                        if self._entry_task_id(entry) == task_id:
                            logger.debug("celery.task.active.inspect task_id=%s found=True", task_id)
                            return True
            if not any_worker_responded:
                # Inspect timed out / no worker answered — we cannot tell. Assume
                # active so the UI joins the stream and resume does not spawn a
                # second execution of a run that may still be running.
                logger.warning("celery.task.active.inspect_no_response task_id=%s assume_active=True", task_id)
                return True
            logger.debug("celery.task.active.inspect task_id=%s found=False", task_id)
            return False
        except Exception:
            logger.warning("celery.task.active.inspect_failed task_id=%s assume_active=True", task_id)
            return True

    def enqueue_run(self, run_record: dict[str, Any], input_value: Any = None, task_id: str | None = None) -> str:
        result = self.app.send_task(
            "deep_agent_ga.run_agent",
            kwargs={"run_record": run_record, "input_value": input_value},
            queue=self.queue,
            task_id=task_id,
        )
        logger.info("celery.enqueue.run thread_id=%s run_id=%s task_id=%s", run_record.get("thread_id"), run_record.get("run_id"), result.id)
        return str(result.id)

    def enqueue_resume(self, run_record: dict[str, Any], resume_value: Any = None, task_id: str | None = None) -> str:
        result = self.app.send_task(
            "deep_agent_ga.resume_agent",
            kwargs={"run_record": run_record, "resume_value": resume_value},
            queue=self.queue,
            task_id=task_id,
        )
        logger.info("celery.enqueue.resume thread_id=%s run_id=%s task_id=%s", run_record.get("thread_id"), run_record.get("run_id"), result.id)
        return str(result.id)

    def revoke(self, task_id: str, *, terminate: bool = False) -> None:
        self.app.control.revoke(task_id, terminate=terminate)
        logger.info("celery.revoke task_id=%s terminate=%s", task_id, terminate)
