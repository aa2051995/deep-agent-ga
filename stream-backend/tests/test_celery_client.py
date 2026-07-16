"""Tests for CeleryRunScheduler task-status handling.

Uses the real CeleryRunScheduler (celery is installed in the dev/dra env) with
only its external I/O mocked. With no result backend configured, Celery uses
DisabledBackend and querying AsyncResult.status raises
``'DisabledBackend' object has no attribute '_get_task_meta_for'``; the client
must not crash and must fall back to the worker inspect API for activity.
"""
from __future__ import annotations

import importlib.util

import pytest
from unittest.mock import MagicMock

from worker.client import CeleryRunScheduler

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("celery") is None, reason="celery is not installed"
)


def _scheduler(*, backend: bool) -> CeleryRunScheduler:
    """Real CeleryRunScheduler with only its external I/O mocked."""
    scheduler = CeleryRunScheduler()
    scheduler.app.conf.result_backend = "rpc://" if backend else None
    scheduler.app.AsyncResult = MagicMock()
    scheduler.app.control = MagicMock()
    scheduler.app.send_task = MagicMock()
    return scheduler


def test_get_task_status_none_without_backend_and_no_asyncresult_call():
    scheduler = _scheduler(backend=False)
    assert scheduler.get_task_status("T") is None
    scheduler.app.AsyncResult.assert_not_called()


def test_get_task_status_reads_backend_when_enabled():
    scheduler = _scheduler(backend=True)
    scheduler.app.AsyncResult.return_value.status = "STARTED"
    assert scheduler.get_task_status("T") == "STARTED"


def test_is_task_active_uses_inspect_without_backend():
    scheduler = _scheduler(backend=False)
    inspector = scheduler.app.control.inspect.return_value
    inspector.active.return_value = {"worker1": [{"id": "T"}]}
    inspector.reserved.return_value = {}
    inspector.scheduled.return_value = {}
    assert scheduler.is_task_active("T") is True
    assert scheduler.is_task_active("OTHER") is False


def test_is_task_active_finds_reserved_and_scheduled_shapes():
    scheduler = _scheduler(backend=False)
    inspector = scheduler.app.control.inspect.return_value
    inspector.active.return_value = {}
    inspector.reserved.return_value = {"w": [{"id": "R"}]}
    inspector.scheduled.return_value = {"w": [{"request": {"id": "S"}}]}
    assert scheduler.is_task_active("R") is True
    assert scheduler.is_task_active("S") is True


def test_is_task_active_inspect_failure_assumes_active():
    # Cannot determine -> assume active, so the UI joins and resume does not
    # double-execute a possibly-still-running run.
    scheduler = _scheduler(backend=False)
    scheduler.app.control.inspect.side_effect = RuntimeError("broker down")
    assert scheduler.is_task_active("T") is True


def test_is_task_active_no_worker_response_assumes_active():
    scheduler = _scheduler(backend=False)
    inspector = scheduler.app.control.inspect.return_value
    inspector.active.return_value = None
    inspector.reserved.return_value = None
    inspector.scheduled.return_value = None
    assert scheduler.is_task_active("T") is True


def test_is_task_active_false_when_workers_respond_without_task():
    scheduler = _scheduler(backend=False)
    inspector = scheduler.app.control.inspect.return_value
    inspector.active.return_value = {"worker1": []}
    inspector.reserved.return_value = {"worker1": []}
    inspector.scheduled.return_value = {"worker1": []}
    assert scheduler.is_task_active("T") is False


def test_is_task_active_uses_status_when_backend_enabled():
    scheduler = _scheduler(backend=True)
    scheduler.app.AsyncResult.return_value.status = "STARTED"
    assert scheduler.is_task_active("T") is True
    scheduler.app.control.inspect.assert_not_called()


def test_enqueue_run_forwards_task_id_to_send_task():
    scheduler = _scheduler(backend=False)
    scheduler.app.send_task.return_value.id = "my-id"
    scheduler.enqueue_run({"thread_id": "t", "run_id": "r"}, "in", task_id="my-id")
    assert scheduler.app.send_task.call_args.kwargs["task_id"] == "my-id"


def test_enqueue_resume_forwards_task_id_to_send_task():
    scheduler = _scheduler(backend=False)
    scheduler.app.send_task.return_value.id = "my-id"
    scheduler.enqueue_resume({"thread_id": "t", "run_id": "r"}, "resume", task_id="my-id")
    assert scheduler.app.send_task.call_args.kwargs["task_id"] == "my-id"


def test_entry_task_id_shapes():
    assert CeleryRunScheduler._entry_task_id({"id": "A"}) == "A"
    assert CeleryRunScheduler._entry_task_id({"request": {"id": "B"}}) == "B"
    assert CeleryRunScheduler._entry_task_id({"no": "id"}) is None
    assert CeleryRunScheduler._entry_task_id("junk") is None
