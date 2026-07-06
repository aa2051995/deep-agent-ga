from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from fastapi import Request

from .event_bus import EventBroker, EventSubscription
from .models import ProtocolEvent
from .protocol import matches_subscription
from .store import Repository

logger = logging.getLogger("stream_backend.streaming")


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
        self._lock = asyncio.Lock()

    async def register_subscription(self, managed: ManagedThreadSubscription) -> None:
        async with self._lock:
            self._active_subscriptions[managed.subscription_id] = managed
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
                logger.info(
                    "stream.subscription.unregistered thread_id=%s subscription_id=%s",
                    managed.thread_id,
                    subscription_id,
                )

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
    ) -> AsyncIterator[ProtocolEvent | None]:
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
