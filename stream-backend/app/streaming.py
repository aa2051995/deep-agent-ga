from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Literal

from fastapi import Request

from .event_bus import EventBroker, EventSubscription
from .models import ProtocolEvent
from .protocol import matches_subscription
from .store import Repository

logger = logging.getLogger("stream_backend.streaming")


@dataclass
class RunHandle:
    """Tracks a run's streaming handle for ack/retry operations."""

    thread_id: str
    run_id: str
    subscription_id: str
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    last_retry_at: float | None = None
    max_retries: int = 3
    status: Literal["active", "completed", "failed", "cancelled"] = "active"

    def can_retry(self) -> bool:
        """Check if this handle can be retried."""
        return self.status == "active" and self.retry_count < self.max_retries

    def record_retry(self) -> None:
        """Record a retry attempt."""
        self.retry_count += 1
        self.last_retry_at = time.time()

    def mark_completed(self) -> None:
        """Mark the handle as completed."""
        self.status = "completed"

    def mark_failed(self) -> None:
        """Mark the handle as failed."""
        self.status = "failed"

    def mark_cancelled(self) -> None:
        """Mark the handle as cancelled."""
        self.status = "cancelled"

    @property
    def age_seconds(self) -> float:
        """Return the age of this handle in seconds."""
        return time.time() - self.created_at


@dataclass(frozen=True)
class ProtocolStreamFilter:
    channels: list[str]
    namespaces: list[list[str]] | None = None
    depth: int | None = None

    def matches(self, event: ProtocolEvent) -> bool:
        return matches_subscription(event, self.channels, self.namespaces, self.depth)


@dataclass(frozen=True)
class RunStreamFilter:
    modes: set[str]
    run_id: str | None = None

    def matches(self, event: ProtocolEvent) -> bool:
        data = event.params.data if isinstance(event.params.data, dict) else {}
        if self.run_id is not None and data.get("run_id") not in {None, self.run_id}:
            return False
        if not self.modes or "run_modes" in self.modes:
            return True
        normalized_modes = {
            "messages" if mode in {"messages-tuple", "messages_tuple"} else mode
            for mode in self.modes
        }
        if event.method in normalized_modes:
            return True
        if event.method == "lifecycle" and "lifecycle" in self.modes:
            return True
        if event.method in {"values", "updates", "checkpoints"} and "state_update" in self.modes:
            return True
        return False

    def is_terminal(self, event: ProtocolEvent) -> bool:
        data = event.params.data if isinstance(event.params.data, dict) else {}
        return (
            self.run_id is not None
            and event.method == "lifecycle"
            and data.get("run_id") == self.run_id
            and data.get("event") in {"completed", "failed", "interrupted"}
        )


@dataclass
class ManagedThreadSubscription:
    thread_id: str
    subscription: EventSubscription
    cursor: int | None
    subscription_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str | None = None

    @property
    def stream_name(self) -> str:
        return self.subscription.stream_name

    async def close(self) -> None:
        await self.subscription.close()


class StreamSubscriptionManager:
    def __init__(self, repo: Repository, broker: EventBroker) -> None:
        self.repo = repo
        self.broker = broker
        self._active_subscriptions: dict[str, ManagedThreadSubscription] = {}
        self._run_handles: dict[str, RunHandle] = {}
        self._lock = asyncio.Lock()

    async def register_subscription(self, managed: ManagedThreadSubscription) -> None:
        async with self._lock:
            self._active_subscriptions[managed.subscription_id] = managed
            if managed.run_id:
                handle = RunHandle(
                    thread_id=managed.thread_id,
                    run_id=managed.run_id,
                    subscription_id=managed.subscription_id,
                )
                self._run_handles[managed.run_id] = handle
            logger.info(
                "stream.subscription.registered thread_id=%s subscription_id=%s run_id=%s",
                managed.thread_id,
                managed.subscription_id,
                managed.run_id,
            )

    async def unregister_subscription(self, subscription_id: str) -> None:
        async with self._lock:
            managed = self._active_subscriptions.pop(subscription_id, None)
            if managed is not None:
                if managed.run_id and managed.run_id in self._run_handles:
                    handle = self._run_handles.pop(managed.run_id)
                    handle.mark_completed()
                logger.info(
                    "stream.subscription.unregistered thread_id=%s subscription_id=%s",
                    managed.thread_id,
                    subscription_id,
                )

    async def get_run_handle(self, run_id: str) -> RunHandle | None:
        """Get a run handle by run_id."""
        async with self._lock:
            return self._run_handles.get(run_id)

    async def record_run_retry(self, run_id: str) -> bool:
        """Record a retry for a run handle. Returns True if retry is allowed."""
        async with self._lock:
            handle = self._run_handles.get(run_id)
            if handle and handle.can_retry():
                handle.record_retry()
                logger.info(
                    "stream.run.retry thread_id=%s run_id=%s retry_count=%s",
                    handle.thread_id,
                    run_id,
                    handle.retry_count,
                )
                return True
            return False

    async def cancel_run_handle(self, run_id: str) -> bool:
        """Cancel a run handle."""
        async with self._lock:
            handle = self._run_handles.get(run_id)
            if handle:
                handle.mark_cancelled()
                logger.info(
                    "stream.run.cancel thread_id=%s run_id=%s",
                    handle.thread_id,
                    run_id,
                )
                return True
            return False

    async def cleanup_run_subscription(self, run_id: str) -> None:
        """Clean up subscription for a specific run."""
        async with self._lock:
            handle = self._run_handles.get(run_id)
            if handle:
                managed = self._active_subscriptions.get(handle.subscription_id)
                if managed:
                    try:
                        await managed.close()
                        logger.info(
                            "stream.run.cleanup thread_id=%s run_id=%s subscription_id=%s",
                            managed.thread_id,
                            run_id,
                            handle.subscription_id,
                        )
                    except Exception:
                        logger.exception(
                            "stream.run.cleanup_failed run_id=%s subscription_id=%s",
                            run_id,
                            handle.subscription_id,
                        )
                    finally:
                        self._active_subscriptions.pop(handle.subscription_id, None)
                handle.mark_completed()

    async def get_active_run_for_thread(self, thread_id: str) -> dict[str, Any] | None:
        async with self._lock:
            for sub in self._active_subscriptions.values():
                if sub.thread_id == thread_id and sub.run_id is not None:
                    return {"thread_id": thread_id, "run_id": sub.run_id}
            
        pending_runs = await self.repo.list_runs(thread_id, limit=1, status="pending")
        if pending_runs:
            return {"thread_id": thread_id, "run_id": pending_runs[0].run_id}
        
        running_runs = await self.repo.list_runs(thread_id, limit=1, status="running")
        if running_runs:
            return {"thread_id": thread_id, "run_id": running_runs[0].run_id}
        
        return None

    async def subscribe_thread(
        self,
        thread_id: str,
        since: int | None = None,
        run_id: str | None = None,
    ) -> ManagedThreadSubscription:
        subscription = await self.broker.subscribe(thread_id, since)
        managed = ManagedThreadSubscription(thread_id, subscription, since, run_id=run_id)
        await self.register_subscription(managed)
        return managed

    async def iter_events(
        self,
        managed: ManagedThreadSubscription,
        request: Request | None,
        timeout: float = 10.0,
        run_id: str | None = None,
    ) -> AsyncIterator[ProtocolEvent | None]:
        event_filter = RunStreamFilter(modes={"run_modes"}, run_id=run_id) if run_id else None
        try:
            while True:
                if request is not None and await request.is_disconnected():
                    return

                try:
                    event = await managed.subscription.next_event(timeout)
                except asyncio.TimeoutError:
                    yield None
                    continue

                if managed.cursor is not None and event.seq <= managed.cursor:
                    continue
                managed.cursor = event.seq
                
                if event_filter and event_filter.is_terminal(event):
                    yield event
                    if managed.run_id:
                        await self.cleanup_run_subscription(managed.run_id)
                    return
                    
                yield event
        finally:
            try:
                await self.unregister_subscription(managed.subscription_id)
            except Exception:
                logger.exception("stream.subscription.unregister_failed subscription_id=%s", managed.subscription_id)
            try:
                await managed.close()
            except Exception:
                logger.exception("stream.subscription.close_failed subscription_id=%s", managed.subscription_id)

    async def wait_for_next_event(
        self,
        thread_id: str,
        cursor: int | None,
        timeout: float = 30.0,
    ) -> int | None:
        managed = await self.subscribe_thread(thread_id, cursor)
        try:
            event = await managed.subscription.next_event(timeout)
            if cursor is None or event.seq > cursor:
                return event.seq
            return cursor
        finally:
            try:
                await self.unregister_subscription(managed.subscription_id)
            except Exception:
                logger.exception("stream.subscription.unregister_failed subscription_id=%s", managed.subscription_id)
            try:
                await managed.close()
            except Exception:
                logger.exception("stream.subscription.close_failed subscription_id=%s", managed.subscription_id)

    async def close_all_subscriptions(self) -> None:
        logger.info("stream.subscription.close_all start count=%s", len(self._active_subscriptions))
        async with self._lock:
            for managed in list(self._active_subscriptions.values()):
                try:
                    await managed.close()
                    logger.info(
                        "stream.subscription.closed thread_id=%s subscription_id=%s",
                        managed.thread_id,
                        managed.subscription_id,
                    )
                except Exception:
                    logger.exception(
                        "stream.subscription.close_failed thread_id=%s subscription_id=%s",
                        managed.thread_id,
                        managed.subscription_id,
                    )
            self._active_subscriptions.clear()
        logger.info("stream.subscription.close_all complete")
