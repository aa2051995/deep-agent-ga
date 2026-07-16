"""Tests for runner-backend resolution and 'why not scheduled to worker' logging.

When STREAM_BACKEND_RUNNER_BACKEND is not 'celery' (e.g. left as an event-broker
value like 'rabbitmq'), runs execute in-process instead of on the Celery worker.
The service must make that decision observable.
"""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import RunRecord
from app.service import ProtocolService
from app.store import InMemoryRepository


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_AGENT_MODE", "fixture")  # avoid heavy research runner
    monkeypatch.delenv("STREAM_BACKEND_EXECUTION_BACKEND", raising=False)


def test_unrecognized_runner_backend_runs_in_process(monkeypatch, caplog):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "rabbitmq")
    with caplog.at_level(logging.WARNING, logger="stream_backend.service"):
        service = ProtocolService(InMemoryRepository())

    assert service.run_scheduler is None
    assert service.runner_backend == "rabbitmq"
    assert "not 'celery'" in (service._scheduler_unavailable_reason or "")
    assert any("runner_backend.unrecognized" in rec.message for rec in caplog.records)


def test_asyncio_backend_reason(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "asyncio")
    service = ProtocolService(InMemoryRepository())
    assert service.run_scheduler is None
    assert "asyncio" in (service._scheduler_unavailable_reason or "")


def test_injected_scheduler_is_used_without_worker_import(monkeypatch):
    # Even with celery selected, an injected scheduler is used as-is (no import).
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "celery")
    scheduler = MagicMock()
    service = ProtocolService(InMemoryRepository(), run_scheduler=scheduler)
    assert service.run_scheduler is scheduler
    assert service._scheduler_unavailable_reason is None


@pytest.mark.asyncio
async def test_start_run_task_logs_not_scheduled_reason(monkeypatch, caplog):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "rabbitmq")
    service = ProtocolService(InMemoryRepository())
    # Replace the runner so the in-process task is an instant no-op.
    service.runner = MagicMock()
    service.runner.run = AsyncMock(return_value=None)
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1")

    with caplog.at_level(logging.WARNING, logger="stream_backend.service"):
        scheduled = await service.start_run_task(run, None)

    assert scheduled is True
    reasons = [rec.message for rec in caplog.records if "not_scheduled_to_worker" in rec.message]
    assert reasons and "rabbitmq" in reasons[0]


@pytest.mark.asyncio
async def test_start_run_task_schedules_to_worker(monkeypatch, caplog):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "celery")
    scheduler = MagicMock()
    scheduler.enqueue_run.return_value = "task-123"
    repo = InMemoryRepository()
    service = ProtocolService(repo, run_scheduler=scheduler)
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1")

    with caplog.at_level(logging.INFO, logger="stream_backend.service"):
        scheduled = await service.start_run_task(run, {"input": "hi"})

    assert scheduled is True
    scheduler.enqueue_run.assert_called_once()
    assert run.metadata.get("celery_task_id") == "task-123"
    assert any("scheduled_to_worker" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_start_run_task_skips_terminal_run(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "celery")
    scheduler = MagicMock()
    repo = InMemoryRepository()
    service = ProtocolService(repo, run_scheduler=scheduler)
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="success")
    await repo.create_run(run)

    assert await service.start_run_task(run, None) is False
    scheduler.enqueue_run.assert_not_called()


@pytest.mark.asyncio
async def test_start_run_task_skips_when_worker_task_active(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "celery")
    scheduler = MagicMock()
    scheduler.is_task_active.return_value = True
    repo = InMemoryRepository()
    service = ProtocolService(repo, run_scheduler=scheduler)
    run = RunRecord(
        run_id="r1", thread_id="t1", assistant_id="a1", status="running",
        metadata={"celery_task_id": "task-xyz"},
    )
    await repo.create_run(run)

    assert await service.start_run_task(run, None) is False
    scheduler.is_task_active.assert_called_once_with("task-xyz")
    scheduler.enqueue_run.assert_not_called()


@pytest.mark.asyncio
async def test_start_run_task_skips_when_asyncio_task_active(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "asyncio")
    repo = InMemoryRepository()
    service = ProtocolService(repo)
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1", status="running")
    await repo.create_run(run)
    live_task = MagicMock()
    live_task.done.return_value = False
    service.run_tasks[("t1", "r1")] = live_task

    assert await service.start_run_task(run, None) is False


@pytest.mark.asyncio
async def test_start_run_task_is_idempotent(monkeypatch):
    monkeypatch.setenv("STREAM_BACKEND_RUNNER_BACKEND", "celery")
    scheduler = MagicMock()
    scheduler.enqueue_run.return_value = "task-1"
    scheduler.is_task_active.return_value = True  # after first enqueue it's active
    repo = InMemoryRepository()
    service = ProtocolService(repo, run_scheduler=scheduler)
    run = RunRecord(run_id="r1", thread_id="t1", assistant_id="a1")
    await repo.create_run(run)

    first = await service.start_run_task(run, None)
    second = await service.start_run_task(run, None)

    assert first is True
    assert second is False
    scheduler.enqueue_run.assert_called_once()
