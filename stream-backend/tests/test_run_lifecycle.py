"""Unit tests for run lifecycle management."""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.streaming import (
    ManagedThreadSubscription,
    RunHandle,
    RunStreamFilter,
    StreamSubscriptionManager,
)
from app.models import EventParams, ProtocolEvent


def test_run_handle_creation() -> None:
    handle = RunHandle(
        thread_id="test-thread",
        run_id="test-run",
        subscription_id="test-sub",
    )
    
    assert handle.thread_id == "test-thread"
    assert handle.run_id == "test-run"
    assert handle.subscription_id == "test-sub"
    assert handle.status == "active"
    assert handle.retry_count == 0
    assert handle.can_retry()


def test_run_handle_retry_tracking() -> None:
    handle = RunHandle(
        thread_id="test-thread",
        run_id="test-run",
        subscription_id="test-sub",
        max_retries=3,
    )
    
    assert handle.can_retry()
    handle.record_retry()
    assert handle.retry_count == 1
    assert handle.last_retry_at is not None
    assert handle.can_retry()
    
    handle.record_retry()
    handle.record_retry()
    assert handle.retry_count == 3
    assert not handle.can_retry()


def test_run_handle_status_transitions() -> None:
    handle = RunHandle(
        thread_id="test-thread",
        run_id="test-run",
        subscription_id="test-sub",
    )
    
    assert handle.status == "active"
    
    handle.mark_completed()
    assert handle.status == "completed"
    assert not handle.can_retry()
    
    handle2 = RunHandle(
        thread_id="test-thread",
        run_id="test-run-2",
        subscription_id="test-sub-2",
    )
    handle2.mark_failed()
    assert handle2.status == "failed"
    
    handle3 = RunHandle(
        thread_id="test-thread",
        run_id="test-run-3",
        subscription_id="test-sub-3",
    )
    handle3.mark_cancelled()
    assert handle3.status == "cancelled"


def test_run_handle_age() -> None:
    handle = RunHandle(
        thread_id="test-thread",
        run_id="test-run",
        subscription_id="test-sub",
        created_at=time.time() - 10.0,
    )
    
    age = handle.age_seconds
    assert 9.0 < age < 11.0


@pytest.mark.asyncio
async def test_stream_manager_register_with_run_handle() -> None:
    repo_mock = AsyncMock()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.thread_id == "test-thread"
    assert handle.run_id == "test-run"
    assert handle.status == "active"


@pytest.mark.asyncio
async def test_stream_manager_retry_recording() -> None:
    repo_mock = AsyncMock()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    result = await manager.record_run_retry("test-run")
    assert result
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.retry_count == 1
    
    non_existent = await manager.record_run_retry("non-existent")
    assert not non_existent


@pytest.mark.asyncio
async def test_stream_manager_cancel_handle() -> None:
    repo_mock = AsyncMock()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    result = await manager.cancel_run_handle("test-run")
    assert result
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.status == "cancelled"


@pytest.mark.asyncio
async def test_stream_manager_cleanup_run_subscription() -> None:
    repo_mock = AsyncMock()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    await manager.cleanup_run_subscription("test-run")
    
    subscription_mock.close.assert_called_once()
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.status == "completed"


def test_run_stream_filter_terminal_detection() -> None:
    event_filter = RunStreamFilter(modes={"run_modes"}, run_id="test-run")
    
    completed_event = ProtocolEvent(
        type="event",
        event_id="1",
        seq=1,
        method="lifecycle",
        params=EventParams(
            namespace=[],
            data={"event": "completed", "run_id": "test-run"},
        ),
    )
    
    assert event_filter.is_terminal(completed_event)
    
    running_event = ProtocolEvent(
        type="event",
        event_id="2",
        seq=2,
        method="lifecycle",
        params=EventParams(
            namespace=[],
            data={"event": "running", "run_id": "test-run"},
        ),
    )
    
    assert not event_filter.is_terminal(running_event)
    
    wrong_run_event = ProtocolEvent(
        type="event",
        event_id="3",
        seq=3,
        method="lifecycle",
        params=EventParams(
            namespace=[],
            data={"event": "completed", "run_id": "other-run"},
        ),
    )
    
    assert not event_filter.is_terminal(wrong_run_event)


def test_run_stream_filter_terminal_events() -> None:
    event_filter = RunStreamFilter(modes={"run_modes"}, run_id="test-run")
    
    for terminal_event in ["completed", "failed", "interrupted"]:
        event = ProtocolEvent(
            type="event",
            event_id="1",
            seq=1,
            method="lifecycle",
            params=EventParams(
                namespace=[],
                data={"event": terminal_event, "run_id": "test-run"},
            ),
        )
        assert event_filter.is_terminal(event)


@pytest.mark.asyncio
async def test_stream_manager_unregister_clears_handle() -> None:
    repo_mock = AsyncMock()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    
    await manager.unregister_subscription(managed.subscription_id)
    
    handle_after = await manager.get_run_handle("test-run")
    assert handle_after is None


@pytest.mark.asyncio
async def test_get_active_run_for_thread() -> None:
    repo_mock = AsyncMock()
    repo_mock.list_runs = AsyncMock(return_value=[])
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo_mock, broker_mock)
    
    subscription_mock = AsyncMock()
    subscription_mock.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription_mock,
        cursor=None,
        run_id="test-run",
    )
    
    await manager.register_subscription(managed)
    
    active_run = await manager.get_active_run_for_thread("test-thread")
    assert active_run is not None
    assert active_run["thread_id"] == "test-thread"
    assert active_run["run_id"] == "test-run"
    
    inactive_run = await manager.get_active_run_for_thread("other-thread")
    assert inactive_run is None


def test_run_handle_max_retries_exhausted() -> None:
    handle = RunHandle(
        thread_id="test-thread",
        run_id="test-run",
        subscription_id="test-sub",
        max_retries=2,
    )
    
    assert handle.can_retry()
    handle.record_retry()
    assert handle.can_retry()
    handle.record_retry()
    assert not handle.can_retry()
    assert handle.retry_count == 2
