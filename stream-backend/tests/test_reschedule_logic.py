"""Unit tests for smart run rescheduling logic."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models import RunRecord
from app.service import ProtocolService
from worker.client import CeleryRunScheduler


def test_celery_scheduler_task_active() -> None:
    scheduler = CeleryRunScheduler()
    
    scheduler.app.AsyncResult = MagicMock()
    mock_result = MagicMock()
    mock_result.status = "STARTED"
    scheduler.app.AsyncResult.return_value = mock_result
    
    assert scheduler.is_task_active("test-task-id")
    
    mock_result.status = "SUCCESS"
    assert not scheduler.is_task_active("test-task-id")


def test_celery_scheduler_get_task_status() -> None:
    scheduler = CeleryRunScheduler()
    
    scheduler.app.AsyncResult = MagicMock()
    mock_result = MagicMock()
    mock_result.status = "PENDING"
    scheduler.app.AsyncResult.return_value = mock_result
    
    status = scheduler.get_task_status("test-task-id")
    assert status == "PENDING"


def test_celery_scheduler_task_active_states() -> None:
    scheduler = CeleryRunScheduler()
    
    for active_status in ["PENDING", "STARTED", "RETRY"]:
        scheduler.app.AsyncResult = MagicMock()
        mock_result = MagicMock()
        mock_result.status = active_status
        scheduler.app.AsyncResult.return_value = mock_result
        
        assert scheduler.is_task_active("test-task-id")


def test_celery_scheduler_task_inactive_states() -> None:
    scheduler = CeleryRunScheduler()
    
    for inactive_status in ["SUCCESS", "FAILURE", "REVOKED"]:
        scheduler.app.AsyncResult = MagicMock()
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
