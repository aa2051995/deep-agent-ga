"""Projection helpers that derive run-scoped views from checkpoint history.

These functions turn a ``RunRecord`` plus the thread's ``ThreadState`` history
into the payload served by ``GET /threads/{id}/runs/{run_id}/checkpoints``.

The projection is expensive: it scans the full checkpoint history and rebuilds
messages, todos, and subagent cards on every request.  ``build_run_snapshot``
runs the projection once when a run finishes so the result can be stored in the
``stream_run_snapshots`` table and later returned with a single keyed lookup.
"""
from __future__ import annotations

import json
from typing import Any

from .models import RunRecord, RunSnapshot, ThreadState, new_id


def state_run_id(state: ThreadState) -> str | None:
    run_id = state.metadata.get("run_id")
    return run_id if isinstance(run_id, str) else None


def is_root_checkpoint(state: ThreadState) -> bool:
    return state.checkpoint.checkpoint_ns in {"", None}


def state_messages(state: ThreadState) -> list[dict[str, Any]]:
    values = state.values if isinstance(state.values, dict) else {}
    messages = values.get("messages")
    return messages if isinstance(messages, list) else []


def message_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    text = ""
    for block in content:
        if isinstance(block, str):
            text += block
        elif isinstance(block, dict) and block.get("type") == "text":
            text += str(block.get("text") or "")
    return text


def normalized_message(message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    msg_type = message.get("type")
    if msg_type not in {"human", "ai", "system"}:
        return None
    text = message_content_text(message.get("content")).strip()
    tool_calls = message.get("tool_calls")
    if not text and msg_type == "ai" and isinstance(tool_calls, list):
        return None
    if not text and msg_type != "human":
        return None
    return {
        "id": str(message.get("id") or new_id()),
        "type": msg_type,
        "content": message.get("content"),
        "name": message.get("name"),
        "additional_kwargs": message.get("additional_kwargs") if isinstance(message.get("additional_kwargs"), dict) else {},
        "response_metadata": message.get("response_metadata") if isinstance(message.get("response_metadata"), dict) else {},
    }


def parse_tool_args(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"input": value}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def tool_call_id(message: dict[str, Any]) -> str | None:
    for key in ("tool_call_id", "toolCallId", "id"):
        value = message.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def project_subagents(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    outputs: dict[str, dict[str, Any]] = {}
    for message in messages:
        if not isinstance(message, dict) or message.get("type") != "tool":
            continue
        call_id = tool_call_id(message)
        if call_id:
            outputs[call_id] = message

    subagents: list[dict[str, Any]] = []
    seen_call_ids: set[str] = set()
    for message in messages:
        if not isinstance(message, dict) or message.get("type") != "ai":
            continue
        tool_calls = message.get("tool_calls")
        if not isinstance(tool_calls, list):
            continue
        for call in tool_calls:
            if not isinstance(call, dict) or call.get("name") != "task":
                continue
            call_id = str(call.get("id") or new_id())
            if call_id in seen_call_ids:
                continue
            seen_call_ids.add(call_id)
            args = parse_tool_args(call.get("args"))
            output = outputs.get(call_id)
            output_text = message_content_text(output.get("content")) if output else ""
            subagents.append(
                {
                    "key": f"tools:{call_id}",
                    "name": str(args.get("subagent_type") or "subagent"),
                    "namespace": [f"tools:{call_id}"],
                    "status": "done" if output else "running",
                    "description": str(args.get("description") or args.get("input") or "Subagent task"),
                    "progress": 100 if output else 35,
                    "messages": [
                        {
                            "id": f"{call_id}-input",
                            "role": "human",
                            "content": str(args.get("description") or args.get("input") or ""),
                            "componentKey": f"tools:{call_id}",
                            "namespace": [f"tools:{call_id}"],
                            "status": "done",
                        },
                        {
                            "id": f"{call_id}-output",
                            "role": "ai",
                            "content": output_text,
                            "componentKey": f"tools:{call_id}",
                            "namespace": [f"tools:{call_id}"],
                            "status": "done" if output else "streaming",
                        },
                    ],
                    "tools": [],
                }
            )
    return subagents


def previous_message_count_for_run(root_history: list[ThreadState], run: RunRecord) -> int:
    states_by_checkpoint_id = {
        state.checkpoint.checkpoint_id: state
        for state in root_history
        if state.checkpoint.checkpoint_id
    }
    run_states = [state for state in root_history if state_run_id(state) == run.run_id]
    first = run_states[0] if run_states else None
    if first is None:
        return 0

    parent = first.parent_checkpoint
    while parent is not None:
        parent_state = states_by_checkpoint_id.get(parent.checkpoint_id)
        if parent_state is None:
            return 0
        if state_run_id(parent_state) != run.run_id:
            return len(state_messages(parent_state))
        parent = parent_state.parent_checkpoint
    return max(len(state_messages(first)) - 1, 0)


def project_run_checkpoints(run: RunRecord, history: list[ThreadState]) -> dict[str, Any]:
    root_history = [state for state in reversed(history) if is_root_checkpoint(state)]
    run_states = [state for state in root_history if state_run_id(state) == run.run_id]
    latest = run_states[-1] if run_states else None
    previous_count = previous_message_count_for_run(root_history, run)
    all_messages = state_messages(latest) if latest else []
    run_messages = all_messages[previous_count:]
    visible_messages = [
        message
        for message in (normalized_message(message) for message in run_messages)
        if message is not None
    ]
    values = latest.values if latest and isinstance(latest.values, dict) else {}
    return {
        "run": run.model_dump(),
        "values": values,
        "messages": visible_messages,
        "todos": values.get("todos") if isinstance(values.get("todos"), list) else [],
        "subagents": project_subagents(run_messages),
        "checkpoints": [
            {
                "checkpoint": state.checkpoint.model_dump(),
                "parent_checkpoint": state.parent_checkpoint.model_dump() if state.parent_checkpoint else None,
                "metadata": state.metadata,
                "next": state.next,
                "created_at": state.created_at,
            }
            for state in run_states
        ],
    }


def build_run_snapshot(run: RunRecord, history: list[ThreadState]) -> RunSnapshot:
    """Project a completed run once so it can be stored for fast retrieval."""
    projection = project_run_checkpoints(run, history)
    checkpoints = projection.get("checkpoints") or []
    latest_checkpoint_id: str | None = None
    if checkpoints:
        latest = checkpoints[-1].get("checkpoint")
        if isinstance(latest, dict):
            value = latest.get("checkpoint_id")
            latest_checkpoint_id = value if isinstance(value, str) else None
    return RunSnapshot(
        thread_id=run.thread_id,
        run_id=run.run_id,
        assistant_id=run.assistant_id,
        status=run.status,
        created_at=run.created_at,
        updated_at=run.updated_at,
        checkpoint_id=latest_checkpoint_id,
        run=projection["run"],
        values=projection["values"],
        messages=projection["messages"],
        todos=projection["todos"],
        subagents=projection["subagents"],
        checkpoints=projection["checkpoints"],
    )
