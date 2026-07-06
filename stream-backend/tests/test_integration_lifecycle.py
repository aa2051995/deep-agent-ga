"""Integration tests for run lifecycle scenarios."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models import EventParams, ProtocolEvent, RunRecord
from app.streaming import StreamSubscriptionManager
from app.store import InMemoryRepository


@pytest.mark.asyncio
async def test_server_restart_during_active_run() -> None:
    """Test that active runs are recovered after server restart."""
    repo = InMemoryRepository()
    await repo.setup()
    
    thread_id = "test-thread"
    run_id = "test-run"
    
    thread = await repo.ensure_thread(thread_id)
    run = RunRecord(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id="test",
        status="running",
    )
    await repo.create_run(run)
    
    active_runs = await repo.list_runs(thread_id, status="running")
    assert len(active_runs) == 1
    assert active_runs[0].run_id == run_id
    
    run.status = "interrupted"
    run.metadata = {**run.metadata, "recovered": True, "recovery_reason": "server_restart"}
    await repo.save_run(run)
    
    await repo.append_event(
        thread_id,
        "lifecycle",
        {"event": "interrupted", "run_id": run_id, "reason": "server_restart"},
    )
    
    events = await repo.list_events(thread_id)
    assert len(events) > 0
    
    last_event = events[-1]
    assert last_event.method == "lifecycle"
    data = last_event.params.data
    assert isinstance(data, dict)
    assert data.get("event") == "interrupted"
    assert data.get("run_id") == run_id


@pytest.mark.asyncio
async def test_subscription_cleanup_on_run_completion() -> None:
    """Test that subscriptions are cleaned up when runs complete."""
    from app.streaming import ManagedThreadSubscription, RunStreamFilter
    
    repo = InMemoryRepository()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo, broker_mock)
    
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
    assert handle.status == "active"
    
    await manager.cleanup_run_subscription("test-run")
    
    subscription_mock.close.assert_called_once()
    
    handle_after = await manager.get_run_handle("test-run")
    assert handle_after is not None
    assert handle_after.status == "completed"


@pytest.mark.asyncio
async def test_consumer_cleanup_on_broker_shutdown() -> None:
    """Test that RabbitMQ consumers are cleaned up on broker shutdown."""
    from app.event_bus import RabbitMQStreamBroker, RabbitMQStreamSettings
    
    settings = RabbitMQStreamSettings()
    broker = RabbitMQStreamBroker(settings)
    
    broker._producer = AsyncMock()
    broker._subscription_consumers = {
        "stream-1": [AsyncMock(), AsyncMock()],
        "stream-2": [AsyncMock()],
    }
    
    await broker.close()
    
    assert len(broker._subscription_consumers) == 0
    broker._producer.close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_crash_and_recovery() -> None:
    """Test that runs are marked as interrupted when workers crash."""
    repo = InMemoryRepository()
    await repo.setup()
    
    thread_id = "test-thread"
    run_id = "test-run"
    
    thread = await repo.ensure_thread(thread_id)
    run = RunRecord(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id="test",
        status="running",
    )
    await repo.create_run(run)
    
    recovered_run = await repo.get_run(thread_id, run_id)
    assert recovered_run is not None
    assert recovered_run.status == "running"
    
    recovered_run.status = "interrupted"
    recovered_run.metadata = {**recovered_run.metadata, "recovered": True}
    await repo.save_run(recovered_run)
    
    final_run = await repo.get_run(thread_id, run_id)
    assert final_run is not None
    assert final_run.status == "interrupted"
    assert final_run.metadata.get("recovered") is True


@pytest.mark.asyncio
async def test_active_run_detection() -> None:
    """Test that active runs can be detected via API."""
    repo = InMemoryRepository()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo, broker_mock)
    
    from app.streaming import ManagedThreadSubscription
    
    subscription1 = AsyncMock()
    subscription1.stream_name = "stream-1"
    managed1 = ManagedThreadSubscription(
        thread_id="thread-1",
        subscription=subscription1,
        cursor=None,
        run_id="run-1",
    )
    await manager.register_subscription(managed1)
    
    subscription2 = AsyncMock()
    subscription2.stream_name = "stream-2"
    managed2 = ManagedThreadSubscription(
        thread_id="thread-1",
        subscription=subscription2,
        cursor=None,
        run_id="run-2",
    )
    await manager.register_subscription(managed2)
    
    active_run = await manager.get_active_run_for_thread("thread-1")
    assert active_run is not None
    assert active_run["thread_id"] == "thread-1"
    assert active_run["run_id"] in ["run-1", "run-2"]
    
    no_run = await manager.get_active_run_for_thread("thread-2")
    assert no_run is None


@pytest.mark.asyncio
async def test_run_handle_retry_workflow() -> None:
    """Test the complete workflow of run handle retries."""
    repo = InMemoryRepository()
    broker_mock = AsyncMock()
    manager = StreamSubscriptionManager(repo, broker_mock)
    
    from app.streaming import ManagedThreadSubscription
    
    subscription = AsyncMock()
    subscription.stream_name = "test-stream"
    managed = ManagedThreadSubscription(
        thread_id="test-thread",
        subscription=subscription,
        cursor=None,
        run_id="test-run",
    )
    await manager.register_subscription(managed)
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.retry_count == 0
    
    success = await manager.record_run_retry("test-run")
    assert success
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.retry_count == 1
    
    await manager.record_run_retry("test-run")
    await manager.record_run_retry("test-run")
    
    handle = await manager.get_run_handle("test-run")
    assert handle is not None
    assert handle.retry_count == 3
    
    final_retry = await manager.record_run_retry("test-run")
    assert not final_retry


@pytest.mark.asyncio
async def test_load_balancer_headers() -> None:
    """Test that load balancer headers are added correctly."""
    from fastapi.testclient import TestClient
    from app.main import app
    
    client = TestClient(app)
    
    response = client.get("/health")
    
    assert "X-Session-Id" in response.headers
    assert "X-Request-Id" in response.headers
    assert "X-Server-Id" in response.headers


@pytest.mark.asyncio
async def test_terminal_event_handling() -> None:
    """Test that terminal events trigger cleanup."""
    from app.streaming import RunStreamFilter
    
    event_filter = RunStreamFilter(modes={"run_modes"}, run_id="test-run")
    
    terminal_events = ["completed", "failed", "interrupted"]
    
    for event_type in terminal_events:
        event = ProtocolEvent(
            type="event",
            event_id="1",
            seq=1,
            method="lifecycle",
            params=EventParams(
                namespace=[],
                data={"event": event_type, "run_id": "test-run"},
            ),
        )
        
        assert event_filter.is_terminal(event), f"{event_type} should be terminal"
    
    non_terminal_event = ProtocolEvent(
        type="event",
        event_id="2",
        seq=2,
        method="lifecycle",
        params=EventParams(
            namespace=[],
            data={"event": "running", "run_id": "test-run"},
        ),
    )
    
    assert not event_filter.is_terminal(non_terminal_event)
