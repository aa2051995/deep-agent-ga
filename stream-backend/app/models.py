from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def new_id() -> str:
    return str(uuid4())


JsonDict = dict[str, Any]
Namespace = list[str]


class Checkpoint(BaseModel):
    thread_id: str
    checkpoint_ns: str = ""
    checkpoint_id: str


class Interrupt(BaseModel):
    id: str
    value: Any = None


class ThreadTask(BaseModel):
    id: str
    name: str
    result: Any = None
    error: str | None = None
    interrupts: list[Interrupt] = Field(default_factory=list)
    checkpoint: Checkpoint | None = None
    state: "ThreadState | None" = None


class ThreadState(BaseModel):
    values: Any = Field(default_factory=dict)
    next: list[str] = Field(default_factory=list)
    checkpoint: Checkpoint
    metadata: JsonDict = Field(default_factory=dict)
    created_at: str | None = None
    parent_checkpoint: Checkpoint | None = None
    tasks: list[ThreadTask] = Field(default_factory=list)


class ThreadRecord(BaseModel):
    thread_id: str
    assistant_id: str | None = None
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)
    metadata: JsonDict = Field(default_factory=dict)
    state: ThreadState
    history: list[ThreadState] = Field(default_factory=list)


class RunRecord(BaseModel):
    run_id: str
    thread_id: str
    assistant_id: str
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)
    status: Literal["pending", "running", "error", "success", "timeout", "interrupted"] = "pending"
    metadata: JsonDict = Field(default_factory=dict)
    kwargs: JsonDict = Field(default_factory=dict)
    multitask_strategy: Literal["reject", "rollback", "interrupt", "enqueue"] = "rollback"
    cancel_requested: bool = False


class ProtocolCommand(BaseModel):
    id: int
    method: str
    params: JsonDict = Field(default_factory=dict)


class ProtocolSuccess(BaseModel):
    type: Literal["success"] = "success"
    id: int
    result: JsonDict = Field(default_factory=dict)
    meta: JsonDict | None = None


class ProtocolError(BaseModel):
    type: Literal["error"] = "error"
    id: int
    error: str
    message: str
    meta: JsonDict | None = None


class EventParams(BaseModel):
    namespace: Namespace = Field(default_factory=list)
    timestamp: int = Field(default_factory=now_ms)
    data: Any = Field(default_factory=dict)
    node: str | None = None


class ProtocolEvent(BaseModel):
    type: Literal["event"] = "event"
    event_id: str
    seq: int
    method: str
    params: EventParams


class EventStreamRequest(BaseModel):
    channels: list[str]
    namespaces: list[Namespace] | None = None
    depth: int | None = None
    since: int | None = None


class ThreadStateUpdate(BaseModel):
    values: Any = None
    checkpoint: Checkpoint | None = None
    checkpoint_id: str | None = None
    as_node: str | None = None


class ThreadHistoryRequest(BaseModel):
    limit: int = 10
    before: JsonDict | None = None
    metadata: JsonDict | None = None
    checkpoint: JsonDict | None = None


class Message(BaseModel):
    id: str = Field(default_factory=new_id)
    type: str
    content: Any
    name: str | None = None
    additional_kwargs: JsonDict = Field(default_factory=dict)
    response_metadata: JsonDict = Field(default_factory=dict)


ThreadTask.model_rebuild()
