from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from fastapi import Request

from .event_bus import EventBroker, EventSubscription
from .models import ProtocolEvent
from .protocol import matches_subscription
from .store import Repository


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

    @property
    def stream_name(self) -> str:
        return self.subscription.stream_name

    async def close(self) -> None:
        await self.subscription.close()


class StreamSubscriptionManager:
    def __init__(self, repo: Repository, broker: EventBroker) -> None:
        self.repo = repo
        self.broker = broker

    async def subscribe_thread(
        self,
        thread_id: str,
        since: int | None = None,
    ) -> ManagedThreadSubscription:
        subscription = await self.broker.subscribe(thread_id, since)
        return ManagedThreadSubscription(thread_id, subscription, since)

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
            await managed.close()

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
            await managed.close()
