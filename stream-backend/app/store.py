from __future__ import annotations

import asyncio
from collections import defaultdict
from copy import deepcopy
from typing import Protocol

from .models import (
    Checkpoint,
    EventParams,
    ProtocolEvent,
    RunRecord,
    ThreadRecord,
    ThreadState,
    new_id,
    now_iso,
)


class Repository(Protocol):
    async def get_thread(self, thread_id: str) -> ThreadRecord | None: ...
    async def list_threads(self, limit: int = 50, offset: int = 0) -> list[ThreadRecord]: ...
    async def ensure_thread(self, thread_id: str, assistant_id: str | None = None) -> ThreadRecord: ...
    async def update_thread_metadata(self, thread_id: str, metadata: dict[str, object]) -> ThreadRecord | None: ...
    async def delete_thread(self, thread_id: str) -> bool: ...
    async def save_thread_state(self, thread_id: str, state: ThreadState) -> None: ...
    async def get_history(self, thread_id: str, limit: int) -> list[ThreadState]: ...
    async def create_run(self, run: RunRecord) -> RunRecord: ...
    async def get_run(self, thread_id: str, run_id: str) -> RunRecord | None: ...
    async def list_runs(self, thread_id: str, limit: int = 10, offset: int = 0, status: str | None = None) -> list[RunRecord]: ...
    async def save_run(self, run: RunRecord) -> None: ...
    async def append_event(self, thread_id: str, method: str, data: object, namespace: list[str] | None = None, node: str | None = None) -> ProtocolEvent: ...
    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]: ...
    async def wait_for_event(self, thread_id: str, after_seq: int | None, timeout: float) -> None: ...


def empty_state(thread_id: str) -> ThreadState:
    checkpoint = Checkpoint(thread_id=thread_id, checkpoint_id=new_id())
    return ThreadState(
        values={"messages": []},
        next=[],
        checkpoint=checkpoint,
        metadata={"step": 0},
        created_at=now_iso(),
        parent_checkpoint=None,
        tasks=[],
    )


class InMemoryRepository:
    def __init__(self) -> None:
        self._threads: dict[str, ThreadRecord] = {}
        self._runs: dict[tuple[str, str], RunRecord] = {}
        self._events: dict[str, list[ProtocolEvent]] = defaultdict(list)
        self._conditions: dict[str, asyncio.Condition] = defaultdict(asyncio.Condition)
        self._lock = asyncio.Lock()

    async def get_thread(self, thread_id: str) -> ThreadRecord | None:
        async with self._lock:
            thread = self._threads.get(thread_id)
            return deepcopy(thread) if thread else None

    async def list_threads(self, limit: int = 50, offset: int = 0) -> list[ThreadRecord]:
        async with self._lock:
            threads = sorted(self._threads.values(), key=lambda thread: thread.updated_at, reverse=True)
            return deepcopy(threads[offset : offset + limit])

    async def ensure_thread(self, thread_id: str, assistant_id: str | None = None) -> ThreadRecord:
        async with self._lock:
            thread = self._threads.get(thread_id)
            if thread is None:
                state = empty_state(thread_id)
                thread = ThreadRecord(
                    thread_id=thread_id,
                    assistant_id=assistant_id,
                    state=state,
                    history=[state],
                )
                self._threads[thread_id] = thread
            elif assistant_id and thread.assistant_id is None:
                thread.assistant_id = assistant_id
                thread.updated_at = now_iso()
            return deepcopy(thread)

    async def delete_thread(self, thread_id: str) -> bool:
        async with self._lock:
            existed = self._threads.pop(thread_id, None) is not None
            self._events.pop(thread_id, None)
            self._conditions.pop(thread_id, None)
            for key in [key for key in self._runs if key[0] == thread_id]:
                self._runs.pop(key, None)
            return existed

    async def update_thread_metadata(self, thread_id: str, metadata: dict[str, object]) -> ThreadRecord | None:
        async with self._lock:
            thread = self._threads.get(thread_id)
            if thread is None:
                return None
            thread.metadata = {**thread.metadata, **metadata}
            return deepcopy(thread)

    async def save_thread_state(self, thread_id: str, state: ThreadState) -> None:
        async with self._lock:
            thread = self._threads.get(thread_id)
            if thread is None:
                thread = ThreadRecord(
                    thread_id=thread_id,
                    state=state,
                    history=[],
                )
                self._threads[thread_id] = thread
            state = deepcopy(state)
            thread.state = state
            thread.history.insert(0, state)
            thread.updated_at = now_iso()

    async def get_history(self, thread_id: str, limit: int) -> list[ThreadState]:
        async with self._lock:
            thread = self._threads.get(thread_id)
            if thread is None:
                return []
            return deepcopy(thread.history[:limit])

    async def create_run(self, run: RunRecord) -> RunRecord:
        async with self._lock:
            self._runs[(run.thread_id, run.run_id)] = deepcopy(run)
            return deepcopy(run)

    async def get_run(self, thread_id: str, run_id: str) -> RunRecord | None:
        async with self._lock:
            run = self._runs.get((thread_id, run_id))
            return deepcopy(run) if run else None

    async def list_runs(
        self,
        thread_id: str,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> list[RunRecord]:
        async with self._lock:
            runs = [
                run
                for (stored_thread_id, _), run in self._runs.items()
                if stored_thread_id == thread_id and (status is None or run.status == status)
            ]
            runs.sort(key=lambda run: run.created_at, reverse=True)
            return deepcopy(runs[offset : offset + limit])

    async def save_run(self, run: RunRecord) -> None:
        async with self._lock:
            run.updated_at = now_iso()
            self._runs[(run.thread_id, run.run_id)] = deepcopy(run)

    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent:
        async with self._lock:
            events = self._events[thread_id]
            seq = events[-1].seq + 1 if events else 1
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

        condition = self._conditions[thread_id]
        async with condition:
            condition.notify_all()
        return event

    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]:
        async with self._lock:
            events = self._events.get(thread_id, [])
            if since is None:
                selected = events
            else:
                selected = [event for event in events if event.seq > since]
            return deepcopy(selected)

    async def wait_for_event(self, thread_id: str, after_seq: int | None, timeout: float) -> None:
        condition = self._conditions[thread_id]
        async with condition:
            await asyncio.wait_for(condition.wait(), timeout=timeout)
