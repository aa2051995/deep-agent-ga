"""Unit tests for worker lifecycle management."""
from __future__ import annotations

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.tasks import WorkerShutdownManager, get_shutdown_manager


def test_shutdown_manager_creation() -> None:
    manager = WorkerShutdownManager()
    
    assert not manager.is_shutting_down()
    assert manager._setup_complete is False


def test_shutdown_manager_singleton() -> None:
    manager1 = get_shutdown_manager()
    manager2 = get_shutdown_manager()
    
    assert manager1 is manager2


@pytest.mark.asyncio
async def test_shutdown_manager_shutdown_event() -> None:
    manager = WorkerShutdownManager()
    
    assert not manager.is_shutting_down()
    
    manager._shutdown_event.set()
    
    assert manager.is_shutting_down()


@pytest.mark.asyncio
async def test_shutdown_manager_task_registration() -> None:
    manager = WorkerShutdownManager()
    
    async def dummy_task() -> None:
        await asyncio.sleep(10)
    
    task = asyncio.create_task(dummy_task())
    manager.register_task(task)
    
    assert task in manager._active_tasks
    
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_shutdown_manager_graceful_shutdown() -> None:
    manager = WorkerShutdownManager()
    
    completed = []
    
    async def completing_task() -> None:
        await asyncio.sleep(0.1)
        completed.append(True)
    
    task = asyncio.create_task(compleating_task())
    manager.register_task(task)
    
    await manager._handle_shutdown()
    
    assert manager.is_shutting_down()
    assert len(completed) == 0 or task.done()


@pytest.mark.asyncio
async def test_shutdown_manager_cancel_ongoing_tasks() -> None:
    manager = WorkerShutdownManager()
    
    cancelled = False
    
    async def long_running_task() -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise
    
    task = asyncio.create_task(long_running_task())
    manager.register_task(task)
    
    await manager._handle_shutdown()
    
    assert cancelled or task.done()


@pytest.mark.asyncio
async def test_shutdown_manager_double_shutdown() -> None:
    manager = WorkerShutdownManager()
    
    await manager._handle_shutdown()
    assert manager.is_shutting_down()
    
    await manager._handle_shutdown()
    assert manager.is_shutting_down()


def test_shutdown_manager_signal_handler_setup() -> None:
    manager = WorkerShutdownManager()
    
    with patch('asyncio.get_event_loop') as mock_loop:
        loop_mock = MagicMock()
        mock_loop.return_value = loop_mock
        
        manager.setup_signal_handlers()
        
        assert manager._setup_complete
        
        calls = loop_mock.add_signal_handler.call_count
        assert calls == 0 or calls == 2


def test_shutdown_manager_signal_handler_from_worker_thread_does_not_raise() -> None:
    """Regression: under Celery's thread/prefork pools the task runs in a
    non-main thread, where loop.add_signal_handler() raises
    RuntimeError('set_wakeup_fd only works in main thread ...'). setup must skip
    gracefully instead of crashing the task."""
    import threading

    results: dict[str, object] = {}

    def worker() -> None:
        manager = WorkerShutdownManager()
        # A loop whose add_signal_handler raises exactly like CPython does off-thread.
        loop_mock = MagicMock()
        loop_mock.add_signal_handler.side_effect = RuntimeError(
            "set_wakeup_fd only works in main thread of the main interpreter"
        )
        with patch("asyncio.get_event_loop", return_value=loop_mock):
            try:
                manager.setup_signal_handlers()  # must NOT raise
                results["ok"] = True
                results["complete"] = manager._setup_complete
                # In a non-main thread we skip before ever touching the loop.
                results["add_calls"] = loop_mock.add_signal_handler.call_count
            except BaseException as exc:  # noqa: BLE001 - capture for the assertion
                results["error"] = exc

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert results.get("error") is None, results.get("error")
    assert results.get("ok") is True
    assert results.get("complete") is True
    assert results.get("add_calls") == 0  # skipped: never called off the main thread


def test_shutdown_manager_signal_handler_idempotent() -> None:
    manager = WorkerShutdownManager()
    
    with patch('asyncio.get_event_loop') as mock_loop:
        loop_mock = MagicMock()
        mock_loop.return_value = loop_mock
        
        manager.setup_signal_handlers()
        manager.setup_signal_handlers()
        
        assert manager._setup_complete
        assert loop_mock.add_signal_handler.call_count <= 4


@pytest.mark.asyncio
async def test_shutdown_manager_multiple_tasks() -> None:
    manager = WorkerShutdownManager()
    
    completed_tasks = []
    
    async def numbered_task(num: int) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            completed_tasks.append(num)
            raise
    
    tasks = []
    for i in range(3):
        task = asyncio.create_task(numbered_task(i))
        manager.register_task(task)
        tasks.append(task)
    
    await asyncio.sleep(0.05)
    
    await manager._handle_shutdown()
    
    assert len(completed_tasks) == 3


@pytest.mark.asyncio
async def test_shutdown_manager_empty_tasks() -> None:
    manager = WorkerShutdownManager()
    
    await manager._handle_shutdown()
    
    assert manager.is_shutting_down()


@pytest.mark.asyncio
async def test_shutdown_manager_unhandled_signal() -> None:
    manager = WorkerShutdownManager()
    
    async def failing_cleanup() -> None:
        await asyncio.sleep(0.1)
        raise RuntimeError("test error")
    
    task = asyncio.create_task(failing_cleanup())
    manager.register_task(task)
    
    await manager._handle_shutdown()
    
    assert manager.is_shutting_down()
