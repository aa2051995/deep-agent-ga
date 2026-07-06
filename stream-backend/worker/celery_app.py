from __future__ import annotations

import asyncio
import os

from celery import Celery, app
from celery.signals import worker_process_init


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
    task_acks_late=os.getenv("STREAM_BACKEND_CELERY_ACKS_LATE", "true").lower() not in {"0", "false", "no"},
)


@worker_process_init.connect
async def on_worker_init(**kwargs) -> None:
    """Called when each worker process starts; recover stale runs from previous crashes."""
    from .tasks import recover_stale_runs
    
    logger = kwargs.get("logger")
    if logger:
        logger.info("worker.init.recover_stale_runs")
    try:
        recovered = await recover_stale_runs()
        if logger:
            logger.info("worker.init.recovery_complete count=%s", len(recovered))
    except Exception:
        if logger:
            logger.exception("worker.init.recovery_failed")
