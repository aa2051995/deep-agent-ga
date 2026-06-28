from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .models import (
    Checkpoint,
    ProtocolCommand,
    ProtocolError,
    ProtocolEvent,
    ProtocolSuccess,
    ThreadHistoryRequest,
    ThreadRecord,
    ThreadState,
    ThreadStateUpdate,
    new_id,
    now_iso,
)
from .event_bus import PublishingRepository, create_event_broker
from .protocol import matches_subscription, sse_frame
from .service import ProtocolService, merge_values
from .streaming import ProtocolStreamFilter, RunStreamFilter, StreamSubscriptionManager
from .store import InMemoryRepository

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
WHITE = "\033[37m"


class BackendFormatter(logging.Formatter):
    """Color backend log records and add spacing for console readability."""

    LEVEL_COLORS = {
        logging.DEBUG: DIM,
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: RED,
    }

    def __init__(self, *, use_color: bool) -> None:
        super().__init__()
        self.use_color = use_color

    def color(self, value: str, color: str) -> str:
        if not self.use_color:
            return value
        return f"{color}{value}{RESET}"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level_color = self.LEVEL_COLORS.get(record.levelno, WHITE)
        date = self.color(timestamp, CYAN)
        level = self.color(record.levelname, level_color)
        name = self.color(f"[{record.name}]", MAGENTA)
        data = self.color(record.getMessage(), WHITE)
        message = f"{date} {level} {name} {data}"
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        return f"{message}\n\n"


def configure_logging() -> None:
    level = os.getenv("STREAM_BACKEND_LOG_LEVEL", "DEBUG").upper()
    use_color = os.getenv("STREAM_BACKEND_LOG_COLOR", "true").lower() not in {"0", "false", "no"}
    formatter = BackendFormatter(use_color=use_color)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    handler.terminator = ""
    default_log_file = Path(tempfile.gettempdir()) / "deep-research-stream-backend.log"
    log_file = Path(os.getenv("STREAM_BACKEND_LOG_FILE", str(default_log_file)))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(BackendFormatter(use_color=False))
    file_handler.terminator = ""
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.addHandler(file_handler)
    root.setLevel(level)


configure_logging()
logger = logging.getLogger("stream_backend.main")

app = FastAPI(title="LangGraphJS Stream Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_repository():
    mode = os.getenv("STREAM_BACKEND_STORE", "memory").lower()
    logger.info("repository.create.start mode=%s", mode)
    if mode == "postgres":
        dsn = (
            os.getenv("STREAM_BACKEND_POSTGRES_URI")
            or os.getenv("POSTGRES_URI")
            or os.getenv("DATABASE_URL")
        )
        if not dsn:
            raise RuntimeError(
                "STREAM_BACKEND_STORE=postgres requires STREAM_BACKEND_POSTGRES_URI, "
                "POSTGRES_URI, or DATABASE_URL."
            )
        from .store_postgres import PostgresRepository

        logger.info("repository.create.postgres")
        return PostgresRepository(dsn)
    logger.info("repository.create.memory")
    return InMemoryRepository()

os.environ["STREAM_BACKEND_STORE"] = "postgres"  # --- IGNORE ---
os.environ["STREAM_BACKEND_POSTGRES_URI"] = "postgresql://postgres:am12345Eee@localhost:5432/myapp"  # --- IGNORE ---
os.environ["RESEARCH_AGENT_MODEL"] = "gemini-2.5-pro"  # --- IGNORE ---
os.environ["TAVILY_API_KEY"] = "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"
os.environ["GOOGLE_API_KEY"] = "AIzaSyBx0JdmhyXdoufg23j2Ec69ej968-LSymU"  # --- IGNORE ---
base_repo = create_repository()
event_broker = create_event_broker()
repo = PublishingRepository(base_repo, event_broker)
service = ProtocolService(repo)
stream_manager = StreamSubscriptionManager(repo, event_broker)


@app.on_event("startup")
async def startup() -> None:
    logger.info("app.startup.begin")
    setup = getattr(repo, "setup", None)
    if setup is not None:
        await setup()
    logger.info("app.startup.complete")


@app.on_event("shutdown")
async def shutdown() -> None:
    logger.info("app.shutdown.begin")
    close = getattr(repo, "close", None)
    if close is not None:
        await close()
    logger.info("app.shutdown.complete")


# @app.middleware("http")
# async def log_http_request(request: Request, call_next):
#     start = perf_counter()
#     logger.info("http.request.start method=%s path=%s", request.method, request.url.path)
#     try:
#         response = await call_next(request)
#     except Exception:
#         logger.exception("http.request.failed method=%s path=%s", request.method, request.url.path)
#         raise
#     elapsed_ms = (perf_counter() - start) * 1000
#     logger.info(
#         "http.request.complete method=%s path=%s status=%s elapsed_ms=%.2f",
#         request.method,
#         request.url.path,
#         response.status_code,
#         elapsed_ms,
#     )
#     return response


def sdk_sse_frame(seq: int, event: str, data: Any) -> str:
    return (
        f"id: {seq}\n"
        f"event: {event}\n"
        f"data: {json.dumps(data, ensure_ascii=False, separators=(',', ':'), default=str)}\n\n"
    )


def sdk_event_name(event: ProtocolEvent, channel: str | None = None) -> str:
    name = channel or event.method
    if not event.params.namespace:
        return name
    return "|".join([name, *event.params.namespace])


def stream_metadata(thread_id: str, event: ProtocolEvent) -> dict[str, Any]:
    namespace = event.params.namespace
    return {
        "thread_id": thread_id,
        "langgraph_node": event.params.node,
        "langgraph_checkpoint_ns": "|".join(namespace),
        "namespace": namespace,
        "timestamp": event.params.timestamp,
    }


def message_role(role: str | None) -> str:
    if role in {"human", "ai", "tool", "system"}:
        return role
    if role == "assistant":
        return "ai"
    return "ai"


def legacy_sse_frame(
    thread_id: str,
    event: ProtocolEvent,
    stream_state: dict[str, dict[str, dict[str, str]]],
) -> str | None:
    data = event.params.data if isinstance(event.params.data, dict) else event.params.data
    if event.method == "values":
        return sdk_sse_frame(event.seq, sdk_event_name(event), data)
    if event.method in {"updates", "tasks", "checkpoints", "debug", "custom"}:
        return sdk_sse_frame(event.seq, sdk_event_name(event), data)
    if event.method == "lifecycle":
        return sdk_sse_frame(event.seq, sdk_event_name(event, "metadata"), data)
    if event.method == "tools" and isinstance(data, dict):
        tool_id = str(data.get("tool_call_id") or data.get("id") or new_id())
        tools = stream_state.setdefault("tools", {})
        if data.get("event") == "tool-started":
            name = str(data.get("tool_name") or data.get("name") or "tool")
            tools[tool_id] = {"name": name}
            return sdk_sse_frame(
                event.seq,
                sdk_event_name(event),
                {
                    "event": "on_tool_start",
                    "id": tool_id,
                    "toolCallId": tool_id,
                    "name": name,
                    "input": data.get("input"),
                    "run_id": data.get("run_id"),
                },
            )
        if data.get("event") == "tool-finished":
            name = str(
                data.get("tool_name")
                or data.get("name")
                or tools.get(tool_id, {}).get("name")
                or "tool"
            )
            return sdk_sse_frame(
                event.seq,
                sdk_event_name(event),
                {
                    "event": "on_tool_end",
                    "id": tool_id,
                    "toolCallId": tool_id,
                    "name": name,
                    "output": data.get("output"),
                    "run_id": data.get("run_id"),
                },
            )
        return sdk_sse_frame(event.seq, sdk_event_name(event), data)
    if event.method != "messages" or not isinstance(data, dict):
        return sdk_sse_frame(event.seq, sdk_event_name(event), data)

    key = "|".join([*event.params.namespace, event.params.node or "agent"])
    messages = stream_state.setdefault("messages", {})
    kind = data.get("event")
    if kind == "message-start":
        messages[key] = {
            "id": str(data.get("id") or new_id()),
            "role": message_role(str(data.get("role") or "ai")),
        }
        return None
    if kind == "message-finish":
        messages.pop(key, None)
        return None
    if kind != "content-block-delta":
        return None

    content = data.get("content")
    if not isinstance(content, dict):
        return None
    text = content.get("text")
    if not isinstance(text, str) or not text:
        return None
    message = messages.setdefault(key, {"id": new_id(), "role": "ai"})
    return sdk_sse_frame(
        event.seq,
        sdk_event_name(event),
        [
            {
                "id": message["id"],
                "type": message_role(message.get("role")),
                "content": text,
            },
            stream_metadata(thread_id, event),
        ],
    )


def parse_last_event_id(value: str | None) -> int | None:
    if value in {None, "-", "-1"}:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return None if parsed < 0 else parsed


def run_payload_to_command(thread_id: str, payload: dict, command_id: int = 0) -> ProtocolCommand:
    config = payload.get("config")
    if isinstance(config, dict):
        configurable = config.get("configurable")
        if isinstance(configurable, dict):
            config = {**config, "configurable": {**configurable, "thread_id": thread_id}}
        else:
            config = {**config, "configurable": {"thread_id": thread_id}}
    command = payload.get("command")
    if isinstance(command, dict) and "resume" in command:
        return ProtocolCommand(
            id=command_id,
            method="input.respond",
            params={
                "assistant_id": payload.get("assistant_id") or "deep-agent",
                "response": command.get("resume"),
                "config": config,
                "metadata": payload.get("metadata") or {},
            },
        )
    return ProtocolCommand(
        id=command_id,
        method="run.start",
        params={
            "assistant_id": payload.get("assistant_id") or "deep-agent",
            "input": payload.get("input"),
            "config": config,
            "metadata": payload.get("metadata") or {},
            "multitaskStrategy": payload.get("multitask_strategy")
            or payload.get("multitaskStrategy")
            or "rollback",
        },
    )


def select_run_fields(run: dict, select: list[str] | None) -> dict:
    if not select:
        return run
    allowed = set(select)
    return {key: value for key, value in run.items() if key in allowed}


def event_data(event: ProtocolEvent) -> dict[str, Any]:
    return event.params.data if isinstance(event.params.data, dict) else {}


def is_root_run_lifecycle(event: ProtocolEvent, run_id: str, names: set[str]) -> bool:
    data = event_data(event)
    return (
        event.method == "lifecycle"
        and event.params.namespace == []
        and data.get("run_id") == run_id
        and data.get("event") in names
    )


def select_run_debug_events(events: list[ProtocolEvent], run_id: str) -> list[ProtocolEvent]:
    start_seq: int | None = None
    end_seq: int | None = None
    for event in events:
        if is_root_run_lifecycle(event, run_id, {"running"}):
            start_seq = event.seq
            break
    if start_seq is not None:
        for event in events:
            if event.seq < start_seq:
                continue
            if is_root_run_lifecycle(event, run_id, {"completed", "failed", "interrupted", "timeout"}):
                end_seq = event.seq
                break

    selected: list[ProtocolEvent] = []
    seen: set[tuple[int, str]] = set()
    for event in events:
        data = event_data(event)
        in_range = start_seq is not None and event.seq >= start_seq and (end_seq is None or event.seq <= end_seq)
        explicit = data.get("run_id") == run_id
        if not in_range and not explicit:
            continue
        key = (event.seq, event.event_id)
        if key in seen:
            continue
        seen.add(key)
        selected.append(event)
    return selected


def select_run_values(events: list[ProtocolEvent], run_id: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for event in events:
        data = event_data(event)
        if event.method == "values" and isinstance(data, dict):
            if data.get("run_id") == run_id or event.params.namespace == []:
                values.update({key: value for key, value in data.items() if key != "run_id"})
        if (
            event.method == "updates"
            and event.params.namespace == []
            and isinstance(data, dict)
            and "todos" in data
        ):
            values["todos"] = data["todos"]
    return values


def namespace_label(namespace: list[str]) -> str:
    return "/".join(namespace) if namespace else "root"


def text_from_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, dict):
        return ""
    text = content.get("text")
    return text if isinstance(text, str) else ""


def todo_progress_summary(todos: Any) -> str:
    if not isinstance(todos, list):
        return "Todo list changed"
    total = len(todos)
    completed = 0
    in_progress = 0
    for todo in todos:
        if not isinstance(todo, dict):
            continue
        status = todo.get("status")
        if status in {"completed", "done", "success"}:
            completed += 1
        elif status in {"in_progress", "running", "active"}:
            in_progress += 1
    pending = max(total - completed - in_progress, 0)
    return f"{completed}/{total} completed, {in_progress} in progress, {pending} pending"


def workflow_step(
    event: ProtocolEvent,
    kind: str,
    title: str,
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "timestamp": event.params.timestamp,
        "kind": kind,
        "title": title,
        "detail": detail,
        "namespace": event.params.namespace,
        "scope": namespace_label(event.params.namespace),
        "node": event.params.node,
    }


def workflow_step_from_event(event: ProtocolEvent) -> dict[str, Any] | None:
    data = event_data(event)
    if event.method == "lifecycle":
        state = str(data.get("event") or "updated")
        return workflow_step(event, "lifecycle", f"Run {state}", str(data.get("error") or "") or None)

    if event.method == "checkpoints":
        return workflow_step(event, "checkpoint", "Checkpoint saved", str(data.get("id") or "") or None)

    if event.method == "updates" and event.params.namespace == [] and "todos" in data:
        return workflow_step(event, "todo", "Todo progress updated", todo_progress_summary(data.get("todos")))

    if event.method == "tools":
        tool_name = str(data.get("tool_name") or data.get("name") or "tool")
        state = str(data.get("event") or "")
        if tool_name == "task" and state in {"tool-started", "on_tool_start"}:
            raw_input = data.get("input")
            task = raw_input if isinstance(raw_input, dict) else {}
            subagent_type = str(task.get("subagent_type") or "subagent")
            description = str(task.get("description") or "Subagent task")
            return workflow_step(event, "subagent", f"Started {subagent_type}", description)
        if state in {"tool-started", "on_tool_start"}:
            return workflow_step(event, "tool", f"Started {tool_name}")
        if state in {"tool-finished", "on_tool_end"}:
            return workflow_step(event, "tool", f"Completed {tool_name}")

    if event.method == "messages" and data.get("event") == "content-block-finish":
        text = text_from_message_content(data.get("content")).strip()
        if not text:
            return None
        title = "Subagent response completed" if event.params.namespace else "AI response completed"
        return workflow_step(event, "message", title, text[:240])

    return None


def select_run_workflow(events: list[ProtocolEvent]) -> list[dict[str, Any]]:
    workflow: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for event in events:
        step = workflow_step_from_event(event)
        if step is None:
            continue
        key = (
            str(step["kind"]),
            str(step["title"]),
            str(step["detail"] or ""),
            namespace_label(event.params.namespace),
        )
        if key in seen:
            continue
        seen.add(key)
        workflow.append(step)
    return workflow


async def list_debug_source_events(thread_id: str) -> list[ProtocolEvent]:
    events: list[ProtocolEvent] = []
    seen: set[str] = set()

    def add_event(event: ProtocolEvent) -> None:
        key = json.dumps(
            {
                "method": event.method,
                "namespace": event.params.namespace,
                "node": event.params.node,
                "timestamp": event.params.timestamp,
                "data": event.params.data,
            },
            sort_keys=True,
            default=str,
        )
        if key in seen:
            return
        seen.add(key)
        events.append(event)

    source = getattr(repo, "inner", None)
    if source is not None:
        for event in await source.list_events(thread_id):
            add_event(event)
    for event in await repo.list_events(thread_id):
        add_event(event)
    return sorted(events, key=lambda event: (event.params.timestamp, event.seq))


async def stream_thread_events(
    thread_id: str,
    request: Request,
    since: int | None = None,
    modes: set[str] | None = None,
    run_id: str | None = None,
    stop_on_terminal: bool = False,
) -> AsyncIterator[str]:
    logger.info(
        "thread.stream.subscribe thread_id=%s since=%s modes=%s run_id=%s stop_on_terminal=%s",
        thread_id,
        since,
        sorted(modes or {"run_modes"}),
        run_id,
        stop_on_terminal,
    )
    event_filter = RunStreamFilter(modes=modes or {"run_modes"}, run_id=run_id)
    stream_state: dict[str, dict[str, dict[str, str]]] = {}
    managed = await stream_manager.subscribe_thread(thread_id, since)
    async for event in stream_manager.iter_events(managed, request):
        if event is None:
            # logger.debug("thread.stream.heartbeat thread_id=%s run_id=%s", thread_id, run_id)
            yield ": heartbeat\n\n"
            continue
        if event_filter.matches(event):
            logger.debug(
                "thread.stream.event thread_id=%s run_id=%s seq=%s method=%s namespace=%s",
                thread_id,
                run_id,
                event.seq,
                event.method,
                event.params.namespace,
            )
            frame = legacy_sse_frame(thread_id, event, stream_state)
            if frame is not None:
                yield frame
        if stop_on_terminal and event_filter.is_terminal(event):
            logger.info("thread.stream.terminal thread_id=%s run_id=%s seq=%s", thread_id, run_id, event.seq)
            return


@app.get("/health")
async def health() -> dict[str, str]:
    logger.debug("health.check")
    return {"status": "ok"}


def thread_payload(thread: ThreadRecord) -> dict[str, Any]:
    return {
        "thread_id": thread.thread_id,
        "created_at": thread.created_at,
        "updated_at": thread.updated_at,
        "state_updated_at": thread.updated_at,
        "metadata": thread.metadata,
        "status": "idle",
        "values": thread.state.values,
    }


@app.post("/threads")
async def create_thread(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    thread_id = new_id()
    assistant_id = (payload or {}).get("assistant_id")
    logger.info("thread.create thread_id=%s assistant_id=%s", thread_id, assistant_id)
    thread = await repo.ensure_thread(thread_id, assistant_id)
    return thread_payload(thread)


@app.post("/threads/search")
async def search_threads(payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    limit = int((payload or {}).get("limit", 50))
    offset = int((payload or {}).get("offset", 0))
    logger.info("threads.search limit=%s offset=%s", limit, offset)
    threads = await repo.list_threads(limit=limit, offset=offset)
    return [thread_payload(thread) for thread in threads]


@app.delete("/threads/{thread_id}")
async def delete_thread(thread_id: str) -> Response:
    logger.info("thread.delete thread_id=%s", thread_id)
    deleted = await repo.delete_thread(thread_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Thread not found")
    return Response(status_code=204)


@app.patch("/threads/{thread_id}")
async def update_thread(thread_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    raw_metadata = payload.get("metadata")
    metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    title = metadata.get("title")
    if "title" in metadata and (not isinstance(title, str) or not title.strip()):
        raise HTTPException(status_code=400, detail="metadata.title must be a non-empty string")
    if isinstance(title, str):
        metadata = {**metadata, "title": title.strip()}
    logger.info("thread.update thread_id=%s metadata_keys=%s", thread_id, sorted(metadata))
    thread = await repo.update_thread_metadata(thread_id, metadata)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return thread_payload(thread)


@app.get("/threads/{thread_id}/state")
async def get_thread_state(thread_id: str) -> dict:
    logger.info("thread.state.get thread_id=%s", thread_id)
    thread = await repo.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return thread.state.model_dump()


@app.post("/threads/{thread_id}/state")
async def update_thread_state(thread_id: str, update: ThreadStateUpdate) -> dict:
    logger.info("thread.state.update.start thread_id=%s as_node=%s", thread_id, update.as_node)
    thread = await repo.ensure_thread(thread_id)
    previous = thread.state
    checkpoint = update.checkpoint or Checkpoint(thread_id=thread_id, checkpoint_id=update.checkpoint_id or new_id())
    values = merge_values(previous.values, update.values)
    state = ThreadState(
        values=values,
        next=[],
        checkpoint=checkpoint,
        parent_checkpoint=previous.checkpoint,
        metadata={"step": int(previous.metadata.get("step", 0)) + 1, "as_node": update.as_node},
        created_at=now_iso(),
        tasks=[],
    )
    await repo.save_thread_state(thread_id, state)
    await repo.append_event(thread_id, "values", values)
    logger.info("thread.state.update.complete thread_id=%s checkpoint_id=%s", thread_id, checkpoint.checkpoint_id)
    return {"configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint.checkpoint_id}}


@app.post("/threads/{thread_id}/history")
async def get_thread_history(thread_id: str, request: ThreadHistoryRequest) -> list[dict]:
    logger.info("thread.history.get thread_id=%s limit=%s", thread_id, request.limit)
    history = await repo.get_history(thread_id, limit=request.limit)
    return [state.model_dump() for state in history]


@app.get("/threads/{thread_id}/stream")
async def join_thread_stream(thread_id: str, request: Request) -> StreamingResponse:
    logger.info("thread.stream.join thread_id=%s", thread_id)
    thread = await repo.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    since = parse_last_event_id(request.headers.get("last-event-id"))
    raw_modes = request.query_params.getlist("stream_modes") or request.query_params.getlist("stream_mode")
    modes = set(raw_modes or ["run_modes"])
    return StreamingResponse(
        stream_thread_events(thread_id, request, since=since, modes=modes),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/threads/{thread_id}/commands")
async def protocol_command(thread_id: str, command: ProtocolCommand) -> dict:
    logger.info("protocol.command thread_id=%s command_id=%s method=%s", thread_id, command.id, command.method)
    result = await service.handle_command(thread_id, command)
    logger.info("protocol.command.result thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
    return result.model_dump(exclude_none=True)


@app.post("/threads/{thread_id}/stream/events")
async def protocol_events(thread_id: str, body: dict, request: Request) -> StreamingResponse:
    logger.info("protocol.events.subscribe.start thread_id=%s", thread_id)
    thread = await repo.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")

    channels = list(body.get("channels") or [])
    if not channels:
        raise HTTPException(status_code=400, detail="channels is required")
    namespaces = body.get("namespaces")
    depth = body.get("depth")
    since = body.get("since")
    logger.info(
        "protocol.events.subscribe thread_id=%s channels=%s namespaces=%s depth=%s since=%s",
        thread_id,
        channels,
        namespaces,
        depth,
        since,
    )

    async def event_iter() -> AsyncIterator[str]:
        event_filter = ProtocolStreamFilter(
            channels=channels,
            namespaces=namespaces,
            depth=depth,
        )
        managed = await stream_manager.subscribe_thread(
            thread_id,
            since if isinstance(since, int) else None,
        )
        async for event in stream_manager.iter_events(managed, request):
            if event is None:
                logger.debug("protocol.events.heartbeat thread_id=%s", thread_id)
                yield ": heartbeat\n\n"
                continue
            if event_filter.matches(event):
                logger.debug(
                    "protocol.events.emit thread_id=%s seq=%s method=%s namespace=%s",
                    thread_id,
                    event.seq,
                    event.method,
                    event.params.namespace,
                )
                yield sse_frame(event)

    return StreamingResponse(
        event_iter(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/threads/{thread_id}/stream/events")
async def protocol_events_websocket(websocket: WebSocket, thread_id: str) -> None:
    await websocket.accept()
    logger.info("protocol.websocket.accept thread_id=%s", thread_id)
    subscriptions: dict[str, dict] = {}
    cursor: int | None = None
    closed = asyncio.Event()

    async def send_matching_events(events: list) -> None:
        if not subscriptions:
            return
        for event in events:
            if any(
                matches_subscription(
                    event,
                    list(params.get("channels") or []),
                    params.get("namespaces"),
                    params.get("depth"),
                )
                for params in subscriptions.values()
            ):
                await websocket.send_json(event.model_dump())

    async def receive_loop() -> None:
        nonlocal cursor
        try:
            while True:
                payload = await websocket.receive_json()
                command = ProtocolCommand.model_validate(payload)
                logger.info("protocol.websocket.command thread_id=%s command_id=%s method=%s", thread_id, command.id, command.method)

                if command.method == "subscription.subscribe":
                    thread = await repo.get_thread(thread_id)
                    if thread is None:
                        error = ProtocolError(
                            id=command.id,
                            error="thread_not_found",
                            message="Thread not found",
                        )
                        await websocket.send_json(error.model_dump(exclude_none=True))
                        continue

                    subscription_id = f"ws-{command.id}-{uuid4()}"
                    params = dict(command.params)
                    subscriptions[subscription_id] = params
                    logger.info("protocol.websocket.subscribe thread_id=%s subscription_id=%s", thread_id, subscription_id)

                    replay = await repo.list_events(thread_id)
                    for event in replay:
                        if matches_subscription(
                            event,
                            list(params.get("channels") or []),
                            params.get("namespaces"),
                            params.get("depth"),
                        ):
                            await websocket.send_json(event.model_dump())
                            cursor = event.seq if cursor is None else max(cursor, event.seq)

                    response = ProtocolSuccess(
                        id=command.id,
                        result={"subscription_id": subscription_id},
                        meta={"applied_through_seq": cursor if cursor is not None else -1},
                    )
                    await websocket.send_json(response.model_dump(exclude_none=True))
                    continue

                if command.method == "subscription.unsubscribe":
                    subscription_id = command.params.get("subscription_id")
                    if isinstance(subscription_id, str):
                        subscriptions.pop(subscription_id, None)
                        logger.info("protocol.websocket.unsubscribe thread_id=%s subscription_id=%s", thread_id, subscription_id)
                    response = ProtocolSuccess(
                        id=command.id,
                        result={},
                        meta={"applied_through_seq": cursor if cursor is not None else -1},
                    )
                    await websocket.send_json(response.model_dump(exclude_none=True))
                    continue

                response = await service.handle_command(thread_id, command)
                await websocket.send_json(response.model_dump(exclude_none=True))
        except WebSocketDisconnect:
            logger.info("protocol.websocket.disconnect thread_id=%s", thread_id)
            closed.set()
        except Exception as exc:
            logger.exception("protocol.websocket.failed thread_id=%s error=%s", thread_id, exc)
            closed.set()
            try:
                await websocket.close(code=1011, reason=str(exc))
            except Exception:
                pass

    async def send_loop() -> None:
        nonlocal cursor
        managed = await stream_manager.subscribe_thread(thread_id, cursor)
        async for event in stream_manager.iter_events(managed, None):
            if closed.is_set():
                return
            if event is None:
                try:
                    await websocket.send_text("")
                except Exception:
                    closed.set()
                continue
            cursor = event.seq if cursor is None else max(cursor, event.seq)
            logger.debug("protocol.websocket.emit thread_id=%s seq=%s method=%s", thread_id, event.seq, event.method)
            await send_matching_events([event])

    receiver = asyncio.create_task(receive_loop())
    sender = asyncio.create_task(send_loop())
    done, pending = await asyncio.wait(
        {receiver, sender},
        return_when=asyncio.FIRST_COMPLETED,
    )
    closed.set()
    for task in pending:
        task.cancel()
    for task in done:
        task.exception()


@app.get("/threads/{thread_id}/runs")
async def list_runs(
    thread_id: str,
    limit: int = 10,
    offset: int = 0,
    status: str | None = None,
    select: list[str] | None = None,
) -> list[dict]:
    logger.info("runs.list thread_id=%s limit=%s offset=%s status=%s", thread_id, limit, offset, status)
    thread = await repo.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    runs = await repo.list_runs(thread_id, limit=limit, offset=offset, status=status)
    return [select_run_fields(run.model_dump(), select) for run in runs]


@app.post("/threads/{thread_id}/runs")
async def create_background_run(thread_id: str, payload: dict, response: Response) -> dict:
    logger.info("runs.create_background.start thread_id=%s", thread_id)
    command = run_payload_to_command(thread_id, payload)
    command_response = await service.handle_command(thread_id, command)
    if isinstance(command_response, ProtocolError):
        raise HTTPException(status_code=400, detail=command_response.message)
    run_id = command_response.result["run_id"]
    response_headers = {"Content-Location": f"/threads/{thread_id}/runs/{run_id}"}
    for key, value in response_headers.items():
        response.headers[key] = value
    run = await repo.get_run(thread_id, run_id)
    logger.info("runs.create_background.complete thread_id=%s run_id=%s", thread_id, run_id)
    return run.model_dump() if run else command_response.result


@app.post("/threads/{thread_id}/runs/stream")
async def stream_stateful_run(thread_id: str, payload: dict, request: Request) -> StreamingResponse:
    logger.info("runs.stream_stateful.start thread_id=%s", thread_id)
    before = await repo.list_events(thread_id)
    since = before[-1].seq if before else None
    response = await service.handle_command(thread_id, run_payload_to_command(thread_id, payload))
    if isinstance(response, ProtocolError):
        raise HTTPException(status_code=400, detail=response.message)
    run_id = response.result["run_id"]
    logger.info("runs.stream_stateful.created thread_id=%s run_id=%s since=%s", thread_id, run_id, since)
    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
    }
    return StreamingResponse(
        stream_thread_events(
            thread_id,
            request,
            since=since,
            run_id=run_id,
            stop_on_terminal=True,
        ),
        media_type="text/event-stream",
        headers=headers,
    )


@app.post("/runs/stream")
async def stream_stateless_run(payload: dict, request: Request) -> StreamingResponse:
    thread_id = new_id()
    logger.info("runs.stream_stateless.thread_created thread_id=%s", thread_id)
    return await stream_stateful_run(thread_id, payload, request)


@app.post("/threads/{thread_id}/runs/wait")
async def wait_run(thread_id: str, payload: dict, request: Request, response: Response) -> dict:
    logger.info("runs.wait.start thread_id=%s", thread_id)
    response_payload = await service.handle_command(thread_id, run_payload_to_command(thread_id, payload))
    if isinstance(response_payload, ProtocolError):
        raise HTTPException(status_code=400, detail=response_payload.message)
    run_id = response_payload.result["run_id"]
    response.headers["Content-Location"] = f"/threads/{thread_id}/runs/{run_id}"
    return await wait_for_run_output(thread_id, run_id, request)


@app.post("/runs/wait")
async def wait_stateless_run(payload: dict, request: Request, response: Response) -> dict:
    thread_id = new_id()
    logger.info("runs.wait_stateless.thread_created thread_id=%s", thread_id)
    result = await wait_run(thread_id, payload, request, response)
    response.headers["Content-Location"] = response.headers.get(
        "Content-Location",
        f"/threads/{thread_id}/runs",
    )
    return result


@app.post("/runs")
async def create_stateless_background_run(payload: dict, response: Response) -> dict:
    thread_id = new_id()
    logger.info("runs.create_stateless.thread_created thread_id=%s", thread_id)
    result = await create_background_run(thread_id, payload, response)
    if "Content-Location" not in response.headers and isinstance(result, dict):
        run_id = result.get("run_id")
        if isinstance(run_id, str):
            response.headers["Content-Location"] = f"/threads/{thread_id}/runs/{run_id}"
    return result


async def wait_for_run_output(thread_id: str, run_id: str, request: Request) -> dict:
    logger.info("runs.wait_output.start thread_id=%s run_id=%s", thread_id, run_id)
    run = await repo.get_run(thread_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    cursor = None
    while not await request.is_disconnected():
        events = await repo.list_events(thread_id, cursor)
        for event in events:
            cursor = max(cursor or 0, event.seq)
            data = event.params.data if isinstance(event.params.data, dict) else {}
            if (
                event.method == "lifecycle"
                and data.get("run_id") == run_id
                and data.get("event") in {"completed", "failed", "interrupted"}
            ):
                thread = await repo.get_thread(thread_id)
                logger.info("runs.wait_output.terminal thread_id=%s run_id=%s event=%s", thread_id, run_id, data.get("event"))
                return thread.state.values if thread else {}
        try:
            cursor = await stream_manager.wait_for_next_event(
                thread_id,
                cursor,
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            logger.debug("runs.wait_output.timeout thread_id=%s run_id=%s cursor=%s", thread_id, run_id, cursor)
            continue
    raise HTTPException(status_code=499, detail="Client disconnected")


@app.get("/threads/{thread_id}/runs/{run_id}")
async def get_run(thread_id: str, run_id: str) -> dict:
    logger.info("runs.get thread_id=%s run_id=%s", thread_id, run_id)
    run = await repo.get_run(thread_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run.model_dump()


@app.get("/threads/{thread_id}/runs/{run_id}/debug")
async def get_run_debug(thread_id: str, run_id: str, mode: str = "verbose") -> dict:
    normalized_mode = mode.lower()
    if normalized_mode not in {"verbose", "brief"}:
        raise HTTPException(status_code=400, detail="mode must be 'verbose' or 'brief'")
    logger.info("runs.debug.get thread_id=%s run_id=%s mode=%s", thread_id, run_id, normalized_mode)
    run = await repo.get_run(thread_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    events = await list_debug_source_events(thread_id)
    run_events = select_run_debug_events(events, run_id)
    values = select_run_values(run_events, run_id)
    workflow = select_run_workflow(run_events)
    response_events = run_events if normalized_mode == "verbose" else []
    logger.info(
        "runs.debug.get.complete thread_id=%s run_id=%s mode=%s events=%s workflow=%s values_keys=%s",
        thread_id,
        run_id,
        normalized_mode,
        len(run_events),
        len(workflow),
        sorted(values),
    )
    return {
        "mode": normalized_mode,
        "run": run.model_dump(),
        "values": values,
        "events": [event.model_dump() for event in response_events],
        "event_count": len(run_events),
        "workflow": workflow,
    }


@app.post("/threads/{thread_id}/runs/{run_id}/resume")
async def resume_run(thread_id: str, run_id: str) -> dict:
    logger.info("runs.resume.start thread_id=%s run_id=%s", thread_id, run_id)
    ok = await service.resume_run(thread_id, run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Run not found")
    run = await repo.get_run(thread_id, run_id)
    logger.info("runs.resume.complete thread_id=%s run_id=%s", thread_id, run_id)
    return run.model_dump() if run else {"ok": True}


@app.get("/threads/{thread_id}/runs/{run_id}/join")
async def join_run(
    thread_id: str,
    run_id: str,
    request: Request,
    cancel_on_disconnect: bool = False,
) -> dict:
    logger.info("runs.join.start thread_id=%s run_id=%s cancel_on_disconnect=%s", thread_id, run_id, cancel_on_disconnect)
    cursor = None
    while not await request.is_disconnected():
        run = await repo.get_run(thread_id, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status in {"success", "error", "interrupted", "timeout"}:
            thread = await repo.get_thread(thread_id)
            logger.info("runs.join.terminal thread_id=%s run_id=%s status=%s", thread_id, run_id, run.status)
            return thread.state.values if thread else {}
        try:
            cursor = await stream_manager.wait_for_next_event(
                thread_id,
                cursor,
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            logger.debug("runs.join.timeout thread_id=%s run_id=%s cursor=%s", thread_id, run_id, cursor)
            continue
    if cancel_on_disconnect:
        await service.cancel_run(thread_id, run_id)
    raise HTTPException(status_code=499, detail="Client disconnected")


@app.get("/threads/{thread_id}/runs/{run_id}/stream")
async def stream_existing_run(
    thread_id: str,
    run_id: str,
    request: Request,
    stream_mode: str | None = None,
    cancel_on_disconnect: bool = False,
) -> StreamingResponse:
    logger.info("runs.stream_existing.start thread_id=%s run_id=%s", thread_id, run_id)
    run = await repo.get_run(thread_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status in {"pending", "running"}:
        logger.info("runs.stream_existing.resume_active thread_id=%s run_id=%s status=%s", thread_id, run_id, run.status)
        await service.resume_run(thread_id, run_id)
    since = parse_last_event_id(request.headers.get("last-event-id"))
    requested_modes = (
        request.query_params.getlist("stream_mode")
        or request.query_params.getlist("stream_modes")
        or ([stream_mode] if stream_mode else [])
    )
    modes = set(requested_modes or ["run_modes"])

    async def event_iter() -> AsyncIterator[str]:
        try:
            async for frame in stream_thread_events(
                thread_id,
                request,
                since=since,
                modes=modes,
                run_id=run_id,
                stop_on_terminal=True,
            ):
                yield frame
        finally:
            if cancel_on_disconnect and await request.is_disconnected():
                logger.info("runs.stream_existing.cancel_on_disconnect thread_id=%s run_id=%s", thread_id, run_id)
                await service.cancel_run(thread_id, run_id)

    return StreamingResponse(
        event_iter(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/threads/{thread_id}/runs/{run_id}/cancel")
async def cancel_run(thread_id: str, run_id: str) -> dict:
    logger.info("runs.cancel.start thread_id=%s run_id=%s", thread_id, run_id)
    ok = await service.cancel_run(thread_id, run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Run not found")
    logger.info("runs.cancel.complete thread_id=%s run_id=%s", thread_id, run_id)
    return {"ok": True}


@app.post("/runs/cancel")
async def cancel_many(payload: dict) -> dict:
    logger.info("runs.cancel_many.start")
    thread_id = payload.get("thread_id")
    run_ids = payload.get("run_ids")
    cancelled = 0
    if isinstance(thread_id, str) and isinstance(run_ids, list):
        for run_id in run_ids:
            if isinstance(run_id, str) and await service.cancel_run(thread_id, run_id):
                cancelled += 1
    return {"cancelled": cancelled}
