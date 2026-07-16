"""Unit tests for smart run rescheduling logic."""
from __future__ import annotations

import importlib.util

import pytest
from unittest.mock import MagicMock

from app.models import RunRecord

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("celery") is None, reason="celery is not installed"
)


def _scheduler_with_result_backend():
    # A real CeleryRunScheduler with a result backend enabled and AsyncResult
    # mocked, so is_task_active/get_task_status read AsyncResult.status.
    from worker.client import CeleryRunScheduler

    scheduler = CeleryRunScheduler()
    scheduler.app.conf.result_backend = "rpc://"
    scheduler.app.AsyncResult = MagicMock()
    return scheduler


def test_celery_scheduler_task_active() -> None:
    scheduler = _scheduler_with_result_backend()
    mock_result = MagicMock()
    mock_result.status = "STARTED"
    scheduler.app.AsyncResult.return_value = mock_result

    assert scheduler.is_task_active("test-task-id")

    mock_result.status = "SUCCESS"
    assert not scheduler.is_task_active("test-task-id")


def test_celery_scheduler_get_task_status() -> None:
    scheduler = _scheduler_with_result_backend()
    mock_result = MagicMock()
    mock_result.status = "PENDING"
    scheduler.app.AsyncResult.return_value = mock_result

    assert scheduler.get_task_status("test-task-id") == "PENDING"


def test_celery_scheduler_get_task_status_none_without_result_backend() -> None:
    scheduler = _scheduler_with_result_backend()
    scheduler.app.conf.result_backend = None
    assert scheduler.get_task_status("test-task-id") is None


def test_celery_scheduler_task_active_states() -> None:
    scheduler = _scheduler_with_result_backend()
    for active_status in ["PENDING", "STARTED", "RETRY"]:
        mock_result = MagicMock()
        mock_result.status = active_status
        scheduler.app.AsyncResult.return_value = mock_result
        assert scheduler.is_task_active("test-task-id")


def test_celery_scheduler_task_inactive_states() -> None:
    scheduler = _scheduler_with_result_backend()
    for inactive_status in ["SUCCESS", "FAILURE", "REVOKED"]:
        mock_result = MagicMock()
        mock_result.status = inactive_status
        scheduler.app.AsyncResult.return_value = mock_result
        assert not scheduler.is_task_active("test-task-id")


def test_run_record_reschedule_counter() -> None:
    run = RunRecord(
        run_id="test-run",
        thread_id="test-thread",
        assistant_id="test",
        metadata={"reschedule_count": 1},
    )
    
    assert run.metadata.get("reschedule_count") == 1


def test_reschedule_count_increment() -> None:
    run = RunRecord(
        run_id="test-run",
        thread_id="test-thread",
        assistant_id="test",
        metadata={"reschedule_count": 0},
    )
    
    reschedule_count = int(run.metadata.get("reschedule_count", 0))
    run.metadata = {**run.metadata, "reschedule_count": reschedule_count + 1}
    
    assert run.metadata.get("reschedule_count") == 1


def test_max_reschedules_limit() -> None:
    max_reschedules = 2
    
    run = RunRecord(
        run_id="test-run",
        thread_id="test-thread",
        assistant_id="test",
        metadata={"reschedule_count": 2},
    )
    
    reschedule_count = int(run.metadata.get("reschedule_count", 0))
    assert reschedule_count >= max_reschedules
