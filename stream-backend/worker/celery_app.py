from __future__ import annotations

import os

from worker.asyncio_policy import (
    configure_windows_celery_env,
    configure_windows_event_loop_policy,
)

# Must run BEFORE importing celery/billiard: on Windows the spawned prefork
# workers otherwise crash every task with "not enough values to unpack
# (expected 3, got 0)" in fast_trace_task.
configure_windows_celery_env()

from celery import Celery  # noqa: E402
from celery.signals import worker_process_init  # noqa: E402

configure_windows_event_loop_policy()


def celery_broker_url() -> str:
    return (
        os.getenv("STREAM_BACKEND_CELERY_BROKER_URL")
        or os.getenv("CELERY_BROKER_URL")
        or "amqp://guest:guest@localhost:5672//"
    )


def celery_result_backend() -> str | None:
    return os.getenv("STREAM_BACKEND_CELERY_RESULT_BACKEND") or os.getenv("CELERY_RESULT_BACKEND")


celery_app = Celery(
    "deep_research_worker",
    broker=celery_broker_url(),
    backend=celery_result_backend(),
    include=["worker.tasks"],
)
celery_app.conf.update(
    imports=("worker.tasks",),
    task_default_queue=os.getenv("STREAM_BACKEND_CELERY_QUEUE", "deep-research-runs"),
    task_default_queue_type="quorum",
    task_default_exchange="celery_topic",
    task_default_exchange_type="topic",
    task_default_routing_key="celery",

    task_default_delivery_mode="persistent",

    control_queue_exclusive=True,
    event_queue_exclusive=True,
      # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

celery_app.conf.update(
    task_track_started=True,
    worker_prefetch_multiplier=int(os.getenv("STREAM_BACKEND_CELERY_PREFETCH_MULTIPLIER", "1")),
    # Acknowledge EARLY (on delivery), not late (after completion). A run's own
    # lifecycle/recovery is tracked in our own store, so we do NOT want the broker
    # to redeliver a task if a worker dies mid-run — that would re-execute the run
    # and race the original. Default is early-ack; set ACKS_LATE=true to override.
    task_acks_late=os.getenv("STREAM_BACKEND_CELERY_ACKS_LATE", "false").lower() not in {"0", "false", "no"},
    # With early ack there is nothing to requeue on worker loss; make that explicit.
    task_reject_on_worker_lost=False,
)


# @worker_process_init.connect
# async def on_worker_init(**kwargs) -> None:
#     """Called when each worker process starts; recover stale runs from previous crashes."""
#     if sys.platform == "win32":
#         asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
#     from worker.tasks import recover_stale_runs
    
#     logger = kwargs.get("logger")
#     if logger:
#         logger.info("worker.init.recover_stale_runs")
#     try:
#         recovered = await recover_stale_runs()
#         if logger:
#             logger.info("worker.init.recovery_complete count=%s", len(recovered))
#     except Exception:
#         if logger:
#             logger.exception("worker.init.recovery_failed")


# Import tasks after the Celery app is created so decorators register when the
# worker is launched as `celery -A worker.celery_app.celery_app worker`.
import worker.tasks  # noqa: E402,F401
