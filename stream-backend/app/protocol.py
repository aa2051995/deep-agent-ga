from __future__ import annotations

from .models import ProtocolEvent


CHANNEL_TO_METHODS = {
    "values": {"values"},
    "updates": {"updates"},
    "messages": {"messages"},
    "tools": {"tools"},
    "custom": {"custom"},
    "lifecycle": {"lifecycle"},
    "input": {"input.requested"},
    "debug": {"debug"},
    "checkpoints": {"checkpoints"},
    "tasks": {"tasks"},
}


def normalize_segment(segment: str) -> str:
    index = segment.find(":")
    return segment if index == -1 else segment[:index]


def is_prefix_match(event_namespace: list[str], prefix: list[str]) -> bool:
    if len(prefix) > len(event_namespace):
        return False
    for index, segment in enumerate(prefix):
        candidate = event_namespace[index]
        if candidate == segment:
            continue
        if ":" in segment:
            return False
        if normalize_segment(candidate) == segment:
            continue
        return False
    return True


def method_matches_channel(method: str, channel: str) -> bool:
    if channel.startswith("custom:"):
        return method == "custom"
    return method in CHANNEL_TO_METHODS.get(channel, {channel})


def namespace_matches(
    event_namespace: list[str],
    namespaces: list[list[str]] | None,
    depth: int | None,
) -> bool:
    if namespaces is None:
        if depth is None:
            return True
        return len(event_namespace) <= depth

    for prefix in namespaces:
        if not is_prefix_match(event_namespace, prefix):
            continue
        if depth is not None and len(event_namespace) - len(prefix) > depth:
            continue
        return True
    return False


def matches_subscription(
    event: ProtocolEvent,
    channels: list[str],
    namespaces: list[list[str]] | None,
    depth: int | None,
) -> bool:
    return any(method_matches_channel(event.method, channel) for channel in channels) and namespace_matches(
        event.params.namespace,
        namespaces,
        depth,
    )


def sse_frame(event: ProtocolEvent) -> str:
    payload = event.model_dump_json()
    return f"id: {event.seq}\nevent: {event.method}\ndata: {payload}\n\n"
