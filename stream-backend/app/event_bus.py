from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import unquote, urlparse

from .models import EventParams, ProtocolEvent, RunRecord, ThreadRecord, ThreadState, now_ms
from .store import Repository


class EventSubscription(Protocol):
    stream_name: str

    async def next_event(self, timeout: float) -> ProtocolEvent: ...
    async def close(self) -> None: ...


class EventBroker(Protocol):
    async def setup(self) -> None: ...
    async def close(self) -> None: ...
    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent: ...
    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]: ...
    async def subscribe(self, thread_id: str, since: int | None = None) -> EventSubscription: ...


@dataclass
class InMemoryEventSubscription:
    stream_name: str
    queue: asyncio.Queue[ProtocolEvent]
    broker: "InMemoryEventBroker"
    closed: bool = False

    async def next_event(self, timeout: float) -> ProtocolEvent:
        return await asyncio.wait_for(self.queue.get(), timeout=timeout)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.broker.unsubscribe(self.stream_name, self.queue)


class InMemoryEventBroker:
    def __init__(self, prefix: str = "langgraphjs.stream") -> None:
        self.prefix = prefix
        self._events: dict[str, list[ProtocolEvent]] = defaultdict(list)
        self._subscribers: dict[str, set[asyncio.Queue[ProtocolEvent]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def setup(self) -> None:
        return None

    async def close(self) -> None:
        async with self._lock:
            self._subscribers.clear()

    def stream_name(self, thread_id: str) -> str:
        return f"{self.prefix}.thread.{thread_id}.events"

    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent:
        stream_name = self.stream_name(thread_id)
        async with self._lock:
            events = self._events[thread_id]
            seq = events[-1].seq + 1 if events else 0
            event = ProtocolEvent(
                event_id=str(seq),
                seq=seq,
                method=method,
                params=EventParams(
                    namespace=namespace or [],
                    data=data,
                    node=node,
                ),
            )
            events.append(event)
            if len(events) > 1000:
                del events[:-1000]
            subscribers = list(self._subscribers.get(stream_name, ()))
        for queue in subscribers:
            queue.put_nowait(event)
        return event

    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]:
        async with self._lock:
            events = self._events.get(thread_id, [])
            if since is None:
                selected = events
            else:
                selected = [event for event in events if event.seq > since]
            return list(selected)

    async def subscribe(self, thread_id: str, since: int | None = None) -> InMemoryEventSubscription:
        stream_name = self.stream_name(thread_id)
        queue: asyncio.Queue[ProtocolEvent] = asyncio.Queue(maxsize=1000)
        async with self._lock:
            events = self._events.get(thread_id, [])
            for event in events:
                if since is None or event.seq > since:
                    queue.put_nowait(event)
            self._subscribers[stream_name].add(queue)
        return InMemoryEventSubscription(stream_name, queue, self)

    def unsubscribe(self, stream_name: str, queue: asyncio.Queue[ProtocolEvent]) -> None:
        subscribers = self._subscribers.get(stream_name)
        if subscribers is None:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscribers.pop(stream_name, None)


@dataclass(frozen=True)
class RabbitMQStreamSettings:
    host: str = "localhost"
    port: int = 5552
    username: str = "guest"
    password: str = "guest"
    vhost: str = "/"


def parse_rabbitmq_stream_url(raw_url: str | None) -> RabbitMQStreamSettings:
    if not raw_url:
        return RabbitMQStreamSettings()
    parsed = urlparse(raw_url)
    if not parsed.scheme and parsed.path:
        parsed = urlparse(f"rabbitmq-stream://{raw_url}")

    host = parsed.hostname or "localhost"
    port = parsed.port or 5552
    username = unquote(parsed.username or "guest")
    password = unquote(parsed.password or "guest")
    raw_vhost = parsed.path.lstrip("/")
    vhost = unquote(raw_vhost) if raw_vhost else "/"
    return RabbitMQStreamSettings(
        host=host,
        port=port,
        username=username,
        password=password,
        vhost=vhost,
    )


class RabbitMQStreamSubscription:
    def __init__(
        self,
        stream_name: str,
        consumer: Any,
        subscriber_id: int,
        events: asyncio.Queue[ProtocolEvent],
    ) -> None:
        self.stream_name = stream_name
        self._consumer = consumer
        self._subscriber_id = subscriber_id
        self._events = events
        self._closed = False

    async def next_event(self, timeout: float) -> ProtocolEvent:
        return await asyncio.wait_for(self._events.get(), timeout=timeout)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._consumer.unsubscribe(self._subscriber_id)


class RabbitMQStreamBroker:
    """RabbitMQ Stream protocol broker using durable streams per thread."""

    def __init__(
        self,
        settings: RabbitMQStreamSettings,
        prefix: str = "langgraphjs.stream",
        stream_max_length_bytes: int | None = None,
    ) -> None:
        self.settings = settings
        self.prefix = prefix
        self.stream_max_length_bytes = stream_max_length_bytes
        self._producer: Any = None
        self._consumer: Any = None
        self._declared: set[str] = set()
        self._lock = asyncio.Lock()

    async def setup(self) -> None:
        try:
            from rstream import Consumer, Producer
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise RuntimeError(
                "Install rstream to use STREAM_BACKEND_EVENT_BROKER=rabbitmq."
            ) from exc

        self._producer = Producer(
            host=self.settings.host,
            port=self.settings.port,
            username=self.settings.username,
            password=self.settings.password,
            vhost=self.settings.vhost,
            connection_name="langgraphjs-stream-backend-producer",
        )
        self._consumer = Consumer(
            host=self.settings.host,
            port=self.settings.port,
            username=self.settings.username,
            password=self.settings.password,
            vhost=self.settings.vhost,
            connection_name="langgraphjs-stream-backend-consumer",
        )
        await self._producer.start()
        await self._consumer.start()

    async def close(self) -> None:
        if self._consumer is not None:
            await self._consumer.close()
        if self._producer is not None:
            await self._producer.close()

    def stream_name(self, thread_id: str) -> str:
        safe_thread_id = "".join(
            char if char.isalnum() or char in {"-", "_"} else "_"
            for char in thread_id
        )
        return f"{self.prefix}.thread.{safe_thread_id}.events"

    async def _ensure_stream(self, thread_id: str) -> str:
        if self._producer is None:
            raise RuntimeError("RabbitMQStreamBroker.setup() was not called.")
        stream_name = self.stream_name(thread_id)
        async with self._lock:
            if stream_name in self._declared:
                return stream_name

            arguments: dict[str, Any] = {}
            if self.stream_max_length_bytes is not None:
                arguments["max-length-bytes"] = self.stream_max_length_bytes
            await self._producer.create_stream(
                stream_name,
                arguments=arguments,
                exists_ok=True,
            )
            self._declared.add(stream_name)
            return stream_name

    def _payload_bytes(
        self,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> bytes:
        payload = {
            "method": method,
            "params": {
                "namespace": namespace or [],
                "timestamp": now_ms(),
                "data": data,
                "node": node,
            },
        }
        return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")

    def _event_from_message_body(self, body: bytes, offset: int) -> ProtocolEvent:
        payload = json.loads(body)
        if payload.get("type") == "event":
            method = payload["method"]
            params = payload["params"]
            return ProtocolEvent(
                event_id=str(offset),
                seq=offset,
                method=method,
                params=EventParams.model_validate(params),
            )
        method = payload["method"]
        params = payload["params"]
        return ProtocolEvent(
            event_id=str(offset),
            seq=offset,
            method=method,
            params=EventParams.model_validate(params),
        )

    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent:
        if self._producer is None:
            raise RuntimeError("RabbitMQStreamBroker.setup() was not called.")
        try:
            from rstream import AMQPMessage
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise RuntimeError("rstream is required for RabbitMQ publishing.") from exc

        stream_name = await self._ensure_stream(thread_id)
        body = self._payload_bytes(method, data, namespace, node)
        message = AMQPMessage(
            body=body,
            application_properties={
                "thread_id": thread_id,
                "method": method,
            },
        )
        await self._producer.send_wait(stream_name, message)
        return ProtocolEvent(
            event_id="-1",
            seq=-1,
            method=method,
            params=EventParams(namespace=namespace or [], data=data, node=node),
        )

    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]:
        subscription = await self.subscribe(thread_id, since)
        events: list[ProtocolEvent] = []
        try:
            while True:
                try:
                    event = await subscription.next_event(0.1)
                except asyncio.TimeoutError:
                    return events
                events.append(event)
        finally:
            await subscription.close()

    async def subscribe(self, thread_id: str, since: int | None = None) -> RabbitMQStreamSubscription:
        if self._consumer is None:
            raise RuntimeError("RabbitMQStreamBroker.setup() was not called.")
        try:
            from rstream import ConsumerOffsetSpecification, OffsetType, amqp_decoder
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise RuntimeError("rstream is required for RabbitMQ subscriptions.") from exc

        stream_name = await self._ensure_stream(thread_id)
        events: asyncio.Queue[ProtocolEvent] = asyncio.Queue(maxsize=1000)

        async def on_message(message: Any, context: Any) -> None:
            body = message if isinstance(message, bytes) else message.body
            event = self._event_from_message_body(body, context.offset)
            await events.put(event)

        if since is None:
            offset_specification = ConsumerOffsetSpecification(OffsetType.FIRST)
        else:
            offset_specification = ConsumerOffsetSpecification(OffsetType.OFFSET, since + 1)

        subscriber_id = await self._consumer.subscribe(
            stream_name,
            on_message,
            decoder=amqp_decoder,
            offset_specification=offset_specification,
            initial_credit=100,
        )
        return RabbitMQStreamSubscription(stream_name, self._consumer, subscriber_id, events)


class PublishingRepository:
    """Repository decorator that publishes every persisted event to a broker."""

    def __init__(self, inner: Repository, broker: EventBroker) -> None:
        self.inner = inner
        self.broker = broker

    async def setup(self) -> None:
        setup = getattr(self.inner, "setup", None)
        if setup is not None:
            await setup()
        await self.broker.setup()

    async def close(self) -> None:
        await self.broker.close()
        close = getattr(self.inner, "close", None)
        if close is not None:
            await close()

    async def get_thread(self, thread_id: str) -> ThreadRecord | None:
        return await self.inner.get_thread(thread_id)

    async def list_threads(self, limit: int = 50, offset: int = 0) -> list[ThreadRecord]:
        return await self.inner.list_threads(limit=limit, offset=offset)

    async def ensure_thread(self, thread_id: str, assistant_id: str | None = None) -> ThreadRecord:
        return await self.inner.ensure_thread(thread_id, assistant_id)

    async def update_thread_metadata(self, thread_id: str, metadata: dict[str, object]) -> ThreadRecord | None:
        return await self.inner.update_thread_metadata(thread_id, metadata)

    async def delete_thread(self, thread_id: str) -> bool:
        return await self.inner.delete_thread(thread_id)

    async def save_thread_state(self, thread_id: str, state: ThreadState) -> None:
        await self.inner.save_thread_state(thread_id, state)

    async def get_history(self, thread_id: str, limit: int) -> list[ThreadState]:
        return await self.inner.get_history(thread_id, limit)

    async def create_run(self, run: RunRecord) -> RunRecord:
        return await self.inner.create_run(run)

    async def get_run(self, thread_id: str, run_id: str) -> RunRecord | None:
        return await self.inner.get_run(thread_id, run_id)

    async def list_runs(
        self,
        thread_id: str,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> list[RunRecord]:
        return await self.inner.list_runs(thread_id, limit=limit, offset=offset, status=status)

    async def save_run(self, run: RunRecord) -> None:
        await self.inner.save_run(run)

    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent:
        event = await self.inner.append_event(thread_id, method, data, namespace, node)
        await self.broker.append_event(thread_id, method, data, namespace, node)
        return event

    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]:
        return await self.broker.list_events(thread_id, since)

    async def wait_for_event(self, thread_id: str, after_seq: int | None, timeout: float) -> None:
        subscription = await self.broker.subscribe(thread_id, after_seq)
        try:
            await subscription.next_event(timeout)
        finally:
            await subscription.close()


def create_event_broker() -> EventBroker:
    mode = (
        os.getenv("STREAM_BACKEND_EVENT_BROKER")
        or os.getenv("STREAM_BACKEND_STREAM_BROKER")
        or "memory"
    ).lower()
    prefix = os.getenv("STREAM_BACKEND_RABBITMQ_PREFIX", "langgraphjs.stream")
    if mode == "rabbitmq":
        url = (
            os.getenv("RABBITMQ_STREAM_URL")
            or os.getenv("RABBITMQ_URL")
            or "http://localhost:5552/"
        )
        max_length = os.getenv("STREAM_BACKEND_RABBITMQ_STREAM_MAX_BYTES")
        return RabbitMQStreamBroker(
            settings=parse_rabbitmq_stream_url(url),
            prefix=prefix,
            stream_max_length_bytes=int(max_length) if max_length else None,
        )
    return InMemoryEventBroker(prefix=prefix)
