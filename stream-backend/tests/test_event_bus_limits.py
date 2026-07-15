"""Tests for event-bus resilience: oversized payloads and producer recovery.

A tool that returns a very large output (e.g. a downloaded document) used to
publish a single RabbitMQ Stream message bigger than the negotiated frame,
which closed the producer connection ("frame too large") and then failed every
subsequent publish — taking the whole run (and live streaming) down with it.
These tests cover the size cap, the reconnect-on-failure retry, and the
best-effort (non-fatal) publish in PublishingRepository.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from app.event_bus import (
    MAX_EVENT_BODY_BYTES,
    PublishingRepository,
    RabbitMQStreamBroker,
    RabbitMQStreamSettings,
    truncate_oversized_strings,
)


def test_truncate_oversized_strings_caps_and_recurses() -> None:
    out = truncate_oversized_strings({"a": "x" * 10, "b": ["y" * 10]}, max_chars=4)
    assert out["a"].startswith("xxxx") and "truncated 6 chars" in out["a"]
    assert out["b"][0].startswith("yyyy") and "truncated 6 chars" in out["b"][0]


def test_truncate_preserves_short_strings_and_scalars() -> None:
    assert truncate_oversized_strings({"a": "short", "n": 5, "ok": True}, max_chars=100) == {
        "a": "short",
        "n": 5,
        "ok": True,
    }


def _broker() -> RabbitMQStreamBroker:
    return RabbitMQStreamBroker(RabbitMQStreamSettings())


def test_payload_bytes_small_event_roundtrips_unchanged() -> None:
    broker = _broker()
    body = broker._payload_bytes("messages", {"text": "hello"}, namespace=["ns"], node="agent")
    payload = json.loads(body)
    assert payload["method"] == "messages"
    assert payload["params"]["data"] == {"text": "hello"}
    assert len(body) <= MAX_EVENT_BODY_BYTES


def test_payload_bytes_caps_oversized_output_below_frame_limit() -> None:
    broker = _broker()
    huge = {"event": "tool-finished", "output": "A" * (5 * 1024 * 1024)}
    body = broker._payload_bytes("tools", huge)
    assert len(body) <= MAX_EVENT_BODY_BYTES
    payload = json.loads(body)
    # String field was truncated, but the event shape/metadata survives.
    assert payload["method"] == "tools"
    assert "truncated" in payload["params"]["data"]["output"]


def test_payload_bytes_placeholder_when_still_too_large() -> None:
    broker = _broker()
    # Many oversized strings: even after per-string truncation the body would be
    # too big, so the payload is compacted (big fields dropped, scalars kept).
    huge = {f"k{i}": "B" * 50_000 for i in range(200)}
    body = broker._payload_bytes("updates", huge)
    assert len(body) <= MAX_EVENT_BODY_BYTES
    payload = json.loads(body)
    assert payload["params"]["data"]["_truncated"] is True
    assert payload["params"]["data"]["_truncated_reason"] == "event_too_large"


def test_payload_bytes_compaction_keeps_tool_event_discriminator() -> None:
    broker = _broker()
    # A tools event with a giant output must still keep `event`/ids so the SDK
    # doesn't throw "Unexpected tool event: undefined".
    huge_tool = {
        "event": "tool-finished",
        "tool_call_id": "call-1",
        "tool_name": "tavily_search",
        "run_id": "r1",
        "output": [{"content": "X" * 200_000} for _ in range(50)],
    }
    body = broker._payload_bytes("tools", huge_tool)
    assert len(body) <= MAX_EVENT_BODY_BYTES
    data = json.loads(body)["params"]["data"]
    assert data["event"] == "tool-finished"
    assert data["tool_call_id"] == "call-1"
    assert data["tool_name"] == "tavily_search"
    assert data["_truncated"] is True


@pytest.mark.asyncio
async def test_append_event_reconnects_after_send_failure() -> None:
    broker = _broker()

    # First producer's send fails (dead connection); a fresh one succeeds.
    failing = AsyncMock()
    failing.send_wait.side_effect = ConnectionError("frame too large")
    failing.create_stream = AsyncMock()
    healthy = AsyncMock()
    healthy.create_stream = AsyncMock()
    producers = iter([failing, healthy])
    broker._new_producer = lambda: next(producers)  # type: ignore[method-assign]
    broker._build_message = lambda body, thread_id, method: object()  # type: ignore[method-assign]

    event = await broker.append_event("t1", "messages", {"text": "hi"})

    assert event.method == "messages"
    failing.send_wait.assert_awaited_once()
    healthy.send_wait.assert_awaited_once()
    failing.close.assert_awaited()  # dead producer was torn down


@pytest.mark.asyncio
async def test_publishing_repository_publish_is_non_fatal() -> None:
    inner = AsyncMock()
    sentinel_event = object()
    inner.append_event = AsyncMock(return_value=sentinel_event)
    broker = AsyncMock()
    broker.append_event = AsyncMock(side_effect=RuntimeError("broker down"))

    repo = PublishingRepository(inner, broker)
    result = await repo.append_event("t1", "messages", {"text": "hi"})

    assert result is sentinel_event
    inner.append_event.assert_awaited_once()
    broker.append_event.assert_awaited_once()
