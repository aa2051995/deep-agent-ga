"""Tests for CeleryRunScheduler task-status handling without a result backend.

With no result backend configured, Celery uses DisabledBackend and querying
AsyncResult.status raises
``'DisabledBackend' object has no attribute '_get_task_meta_for'``. The client
must not crash and must fall back to the worker inspect API for activity.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from worker.client import CeleryRunScheduler


def _scheduler(app: MagicMock) -> CeleryRunScheduler:
    # Bypass __init__ (which imports celery) and inject a fake app.
    scheduler = CeleryRunScheduler.__new__(CeleryRunScheduler)
    scheduler.app = app
    scheduler.queue = "q"
    return scheduler


def _app_without_backend() -> MagicMock:
    app = MagicMock()
    app.conf.result_backend = None
    return app


def _app_with_backend() -> MagicMock:
    app = MagicMock()
    app.conf.result_backend = "rpc://"
    return app


def test_get_task_status_none_without_backend_and_no_asyncresult_call():
    app = _app_without_backend()
    scheduler = _scheduler(app)
    assert scheduler.get_task_status("T") is None
    app.AsyncResult.assert_not_called()


def test_get_task_status_reads_backend_when_enabled():
    app = _app_with_backend()
    app.AsyncResult.return_value.status = "STARTED"
    scheduler = _scheduler(app)
    assert scheduler.get_task_status("T") == "STARTED"


def test_is_task_active_uses_inspect_without_backend():
    app = _app_without_backend()
    inspector = app.control.inspect.return_value
    inspector.active.return_value = {"worker1": [{"id": "T"}]}
    inspector.reserved.return_value = {}
    inspector.scheduled.return_value = {}
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("T") is True
    assert scheduler.is_task_active("OTHER") is False


def test_is_task_active_finds_reserved_and_scheduled_shapes():
    app = _app_without_backend()
    inspector = app.control.inspect.return_value
    inspector.active.return_value = {}
    inspector.reserved.return_value = {"w": [{"id": "R"}]}
    inspector.scheduled.return_value = {"w": [{"request": {"id": "S"}}]}
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("R") is True
    assert scheduler.is_task_active("S") is True


def test_is_task_active_inspect_failure_assumes_active():
    # Cannot determine -> assume active, so the UI joins and resume does not
    # double-execute a possibly-still-running run.
    app = _app_without_backend()
    app.control.inspect.side_effect = RuntimeError("broker down")
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("T") is True


def test_is_task_active_no_worker_response_assumes_active():
    app = _app_without_backend()
    inspector = app.control.inspect.return_value
    inspector.active.return_value = None
    inspector.reserved.return_value = None
    inspector.scheduled.return_value = None
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("T") is True


def test_is_task_active_false_when_workers_respond_without_task():
    app = _app_without_backend()
    inspector = app.control.inspect.return_value
    inspector.active.return_value = {"worker1": []}
    inspector.reserved.return_value = {"worker1": []}
    inspector.scheduled.return_value = {"worker1": []}
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("T") is False


def test_is_task_active_uses_status_when_backend_enabled():
    app = _app_with_backend()
    app.AsyncResult.return_value.status = "STARTED"
    scheduler = _scheduler(app)
    assert scheduler.is_task_active("T") is True
    app.control.inspect.assert_not_called()


def test_entry_task_id_shapes():
    assert CeleryRunScheduler._entry_task_id({"id": "A"}) == "A"
    assert CeleryRunScheduler._entry_task_id({"request": {"id": "B"}}) == "B"
    assert CeleryRunScheduler._entry_task_id({"no": "id"}) is None
    assert CeleryRunScheduler._entry_task_id("junk") is None
