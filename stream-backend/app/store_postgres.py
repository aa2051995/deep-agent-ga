from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from copy import deepcopy
from typing import Any

from .models import (
    Checkpoint,
    EventParams,
    ProtocolEvent,
    RunRecord,
    RunSnapshot,
    ThreadRecord,
    ThreadState,
    now_iso,
)
from .store import empty_state

logger = logging.getLogger("stream_backend.store_postgres")


def sanitize_for_jsonb(value: Any) -> Any:
    """Recursively strip characters PostgreSQL's JSONB/text type cannot store.

    PostgreSQL rejects the NUL code point (``\\u0000``) inside ``jsonb``/``text``
    with ``UntranslatableCharacter``. Agent tool outputs and streamed message
    content occasionally carry raw/binary bytes that decode to NUL, which would
    otherwise crash every event/state write for that thread. We drop NULs from
    all strings (including dict keys) while preserving the rest of the payload.
    """
    if isinstance(value, str):
        return value.replace("\x00", "") if "\x00" in value else value
    if isinstance(value, dict):
        return {
            (sanitize_for_jsonb(key) if isinstance(key, str) else key): sanitize_for_jsonb(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_for_jsonb(item) for item in value]
    return value


class PostgresRepository:
    """Durable repository for thread state, run records, history, and events."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._pool: Any = None
        self._jsonb: Any = None
        self._conditions: dict[str, asyncio.Condition] = defaultdict(asyncio.Condition)
        self._lock = asyncio.Lock()

    async def setup(self) -> None:
        try:
            from psycopg.types.json import Jsonb
            from psycopg_pool import AsyncConnectionPool
        except Exception as exc:  # pragma: no cover - environment dependent
            raise RuntimeError(
                "Install psycopg[binary,pool] to use STREAM_BACKEND_STORE=postgres."
            ) from exc

        self._jsonb = Jsonb
        self._pool = AsyncConnectionPool(
            conninfo=self.dsn,
            open=False,
            kwargs={"autocommit": True, "prepare_threshold": 0},
        )
        await self._pool.open()
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_threads (
                    thread_id TEXT PRIMARY KEY,
                    assistant_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    state JSONB NOT NULL,
                    history JSONB NOT NULL
                )
                """
            )
            await conn.execute(
                """
                ALTER TABLE stream_threads
                ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_runs (
                    thread_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    assistant_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata JSONB NOT NULL,
                    kwargs JSONB NOT NULL,
                    multitask_strategy TEXT NOT NULL,
                    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (thread_id, run_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS stream_runs_thread_created_idx
                ON stream_runs (thread_id, created_at DESC)
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_run_snapshots (
                    thread_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    assistant_id TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    checkpoint_id TEXT,
                    data JSONB NOT NULL,
                    PRIMARY KEY (thread_id, run_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS stream_run_snapshots_thread_updated_idx
                ON stream_run_snapshots (thread_id, updated_at DESC)
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_events (
                    thread_id TEXT NOT NULL,
                    seq BIGINT NOT NULL,
                    event JSONB NOT NULL,
                    PRIMARY KEY (thread_id, seq)
                )
                """
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    def _require_pool(self) -> Any:
        if self._pool is None:
            raise RuntimeError("PostgresRepository.setup() was not called.")
        return self._pool

    def _json(self, value: Any) -> Any:
        if self._jsonb is None:
            raise RuntimeError("PostgresRepository.setup() was not called.")
        return self._jsonb(sanitize_for_jsonb(value))

    async def get_thread(self, thread_id: str) -> ThreadRecord | None:
        pool = self._require_pool()
        async with pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    SELECT thread_id, assistant_id, created_at, updated_at, metadata, state, history
                    FROM stream_threads
                    WHERE thread_id = %s
                    """,
                    (thread_id,),
                )
            ).fetchone()
        if row is None:
            return None
        return ThreadRecord(
            thread_id=row[0],
            assistant_id=row[1],
            created_at=row[2],
            updated_at=row[3],
            metadata=row[4],
            state=ThreadState.model_validate(row[5]),
            history=[ThreadState.model_validate(item) for item in row[6]],
        )

    async def list_threads(self, limit: int = 50, offset: int = 0) -> list[ThreadRecord]:
        pool = self._require_pool()
        async with pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT thread_id, assistant_id, created_at, updated_at, metadata, state, history
                    FROM stream_threads
                    ORDER BY updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            ).fetchall()
        return [
            ThreadRecord(
                thread_id=row[0],
                assistant_id=row[1],
                created_at=row[2],
                updated_at=row[3],
                metadata=row[4],
                state=ThreadState.model_validate(row[5]),
                history=[ThreadState.model_validate(item) for item in row[6]],
            )
            for row in rows
        ]

    async def ensure_thread(self, thread_id: str, assistant_id: str | None = None) -> ThreadRecord:
        async with self._lock:
            thread = await self.get_thread(thread_id)
            if thread is not None:
                if assistant_id and thread.assistant_id is None:
                    thread.assistant_id = assistant_id
                    thread.updated_at = now_iso()
                    pool = self._require_pool()
                    async with pool.connection() as conn:
                        await conn.execute(
                            """
                            UPDATE stream_threads
                            SET assistant_id = %s, updated_at = %s
                            WHERE thread_id = %s
                            """,
                            (assistant_id, thread.updated_at, thread_id),
                        )
                return deepcopy(thread)

            state = empty_state(thread_id)
            thread = ThreadRecord(
                thread_id=thread_id,
                assistant_id=assistant_id,
                state=state,
                history=[state],
            )
            pool = self._require_pool()
            async with pool.connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO stream_threads (
                        thread_id, assistant_id, created_at, updated_at, metadata, state, history
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (thread_id) DO NOTHING
                    """,
                    (
                        thread.thread_id,
                        thread.assistant_id,
                        thread.created_at,
                        thread.updated_at,
                        self._json(thread.metadata),
                        self._json(thread.state.model_dump(mode="json")),
                        self._json([item.model_dump(mode="json") for item in thread.history]),
                    ),
                )
            return deepcopy(thread)

    async def delete_thread(self, thread_id: str) -> bool:
        pool = self._require_pool()
        async with self._lock:
            async with pool.connection() as conn:
                async with conn.transaction():
                    await conn.execute("DELETE FROM stream_events WHERE thread_id = %s", (thread_id,))
                    await conn.execute("DELETE FROM stream_run_snapshots WHERE thread_id = %s", (thread_id,))
                    await conn.execute("DELETE FROM stream_runs WHERE thread_id = %s", (thread_id,))
                    result = await conn.execute("DELETE FROM stream_threads WHERE thread_id = %s", (thread_id,))
        return result.rowcount > 0

    async def update_thread_metadata(self, thread_id: str, metadata: dict[str, object]) -> ThreadRecord | None:
        async with self._lock:
            thread = await self.get_thread(thread_id)
            if thread is None:
                return None
            thread.metadata = {**thread.metadata, **metadata}
            pool = self._require_pool()
            async with pool.connection() as conn:
                await conn.execute(
                    """
                    UPDATE stream_threads
                    SET metadata = %s
                    WHERE thread_id = %s
                    """,
                    (self._json(thread.metadata), thread_id),
                )
            return deepcopy(thread)

    async def save_thread_state(self, thread_id: str, state: ThreadState) -> None:
        async with self._lock:
            pool = self._require_pool()
            state_json = state.model_dump(mode="json")
            async with pool.connection() as conn:
                async with conn.transaction():
                    row = await (
                        await conn.execute(
                            """
                            SELECT history
                            FROM stream_threads
                            WHERE thread_id = %s
                            FOR UPDATE
                            """,
                            (thread_id,),
                        )
                    ).fetchone()
                    history = [state_json]
                    if row is not None and isinstance(row[0], list):
                        history.extend(row[0])
                    if row is None:
                        await conn.execute(
                            """
                            INSERT INTO stream_threads (
                                thread_id, assistant_id, created_at, updated_at, metadata, state, history
                            )
                            VALUES (%s, NULL, %s, %s, %s, %s, %s)
                            """,
                            (
                                thread_id,
                                now_iso(),
                                now_iso(),
                                self._json({}),
                                self._json(state_json),
                                self._json(history),
                            ),
                        )
                    else:
                        await conn.execute(
                            """
                            UPDATE stream_threads
                            SET state = %s, history = %s, updated_at = %s
                            WHERE thread_id = %s
                            """,
                            (self._json(state_json), self._json(history), now_iso(), thread_id),
                        )

    async def get_history(self, thread_id: str, limit: int) -> list[ThreadState]:
        pool = self._require_pool()
        async with pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT history FROM stream_threads WHERE thread_id = %s",
                    (thread_id,),
                )
            ).fetchone()
        if row is None:
            return []
        return [ThreadState.model_validate(item) for item in row[0][:limit]]

    async def create_run(self, run: RunRecord) -> RunRecord:
        await self.save_run(run)
        return deepcopy(run)

    async def get_run(self, thread_id: str, run_id: str) -> RunRecord | None:
        pool = self._require_pool()
        async with pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    SELECT run_id, thread_id, assistant_id, created_at, updated_at, status,
                           metadata, kwargs, multitask_strategy, cancel_requested
                    FROM stream_runs
                    WHERE thread_id = %s AND run_id = %s
                    """,
                    (thread_id, run_id),
                )
            ).fetchone()
        if row is None:
            return None
        return RunRecord(
            run_id=row[0],
            thread_id=row[1],
            assistant_id=row[2],
            created_at=row[3],
            updated_at=row[4],
            status=row[5],
            metadata=row[6],
            kwargs=row[7],
            multitask_strategy=row[8],
            cancel_requested=row[9],
        )

    async def list_runs(
        self,
        thread_id: str,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> list[RunRecord]:
        pool = self._require_pool()
        query = """
            SELECT run_id, thread_id, assistant_id, created_at, updated_at, status,
                   metadata, kwargs, multitask_strategy, cancel_requested
            FROM stream_runs
            WHERE thread_id = %s
        """
        params: list[Any] = [thread_id]
        if status is not None:
            query += " AND status = %s"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        async with pool.connection() as conn:
            rows = await (await conn.execute(query, params)).fetchall()
        return [
            RunRecord(
                run_id=row[0],
                thread_id=row[1],
                assistant_id=row[2],
                created_at=row[3],
                updated_at=row[4],
                status=row[5],
                metadata=row[6],
                kwargs=row[7],
                multitask_strategy=row[8],
                cancel_requested=row[9],
            )
            for row in rows
        ]

    async def save_run(self, run: RunRecord) -> None:
        pool = self._require_pool()
        run.updated_at = now_iso()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO stream_runs (
                    thread_id, run_id, assistant_id, created_at, updated_at, status,
                    metadata, kwargs, multitask_strategy, cancel_requested
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (thread_id, run_id) DO UPDATE SET
                    assistant_id = EXCLUDED.assistant_id,
                    updated_at = EXCLUDED.updated_at,
                    status = EXCLUDED.status,
                    metadata = EXCLUDED.metadata,
                    kwargs = EXCLUDED.kwargs,
                    multitask_strategy = EXCLUDED.multitask_strategy,
                    cancel_requested = EXCLUDED.cancel_requested
                """,
                (
                    run.thread_id,
                    run.run_id,
                    run.assistant_id,
                    run.created_at,
                    run.updated_at,
                    run.status,
                    self._json(run.metadata),
                    self._json(run.kwargs),
                    run.multitask_strategy,
                    run.cancel_requested,
                ),
            )

    async def save_run_snapshot(self, snapshot: RunSnapshot) -> None:
        pool = self._require_pool()
        snapshot.updated_at = now_iso()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO stream_run_snapshots (
                    thread_id, run_id, assistant_id, status, created_at, updated_at,
                    checkpoint_id, data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (thread_id, run_id) DO UPDATE SET
                    assistant_id = EXCLUDED.assistant_id,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at,
                    checkpoint_id = EXCLUDED.checkpoint_id,
                    data = EXCLUDED.data
                """,
                (
                    snapshot.thread_id,
                    snapshot.run_id,
                    snapshot.assistant_id,
                    snapshot.status,
                    snapshot.created_at,
                    snapshot.updated_at,
                    snapshot.checkpoint_id,
                    self._json(snapshot.model_dump(mode="json")),
                ),
            )

    async def get_run_snapshot(self, thread_id: str, run_id: str) -> RunSnapshot | None:
        pool = self._require_pool()
        async with pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    SELECT data
                    FROM stream_run_snapshots
                    WHERE thread_id = %s AND run_id = %s
                    """,
                    (thread_id, run_id),
                )
            ).fetchone()
        if row is None:
            return None
        return RunSnapshot.model_validate(row[0])

    async def append_event(
        self,
        thread_id: str,
        method: str,
        data: object,
        namespace: list[str] | None = None,
        node: str | None = None,
    ) -> ProtocolEvent:
        pool = self._require_pool()
        # seq = MAX(seq)+1 is not atomic across processes: when a run's original
        # worker task and a resume/second task both append to the same thread,
        # they can compute the same seq and collide on the (thread_id, seq)
        # primary key. INSERT ... ON CONFLICT DO NOTHING + retry lets the unique
        # constraint serialize concurrent appenders instead of crashing.
        max_attempts = 25
        event: ProtocolEvent | None = None
        async with self._lock:  # cheap: removes intra-process contention
            for attempt in range(max_attempts):
                async with pool.connection() as conn:
                    row = await (
                        await conn.execute(
                            "SELECT COALESCE(MAX(seq), 0) + 1 FROM stream_events WHERE thread_id = %s",
                            (thread_id,),
                        )
                    ).fetchone()
                    seq = int(row[0])
                    candidate = ProtocolEvent(
                        event_id=str(seq),
                        seq=seq,
                        method=method,
                        params=EventParams(namespace=namespace or [], data=data, node=node),
                    )
                    result = await conn.execute(
                        """
                        INSERT INTO stream_events (thread_id, seq, event)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (thread_id, seq) DO NOTHING
                        """,
                        (thread_id, seq, self._json(candidate.model_dump(mode="json"))),
                    )
                if result.rowcount and result.rowcount > 0:
                    event = candidate
                    break
                logger.debug(
                    "store.append_event.seq_conflict thread_id=%s seq=%s attempt=%s",
                    thread_id,
                    seq,
                    attempt,
                )
            if event is None:
                raise RuntimeError(
                    f"append_event: could not allocate a sequence for thread {thread_id} "
                    f"after {max_attempts} attempts (high concurrent write contention)."
                )

        condition = self._conditions[thread_id]
        async with condition:
            condition.notify_all()
        return event

    async def list_events(self, thread_id: str, since: int | None = None) -> list[ProtocolEvent]:
        pool = self._require_pool()
        if since is None:
            query = """
                SELECT event
                FROM stream_events
                WHERE thread_id = %s
                ORDER BY seq ASC
                LIMIT 10000
            """
            params: tuple[Any, ...] = (thread_id,)
        else:
            query = """
                SELECT event
                FROM stream_events
                WHERE thread_id = %s AND seq > %s
                ORDER BY seq ASC
                LIMIT 10000
            """
            params = (thread_id, since)
        async with pool.connection() as conn:
            rows = await (await conn.execute(query, params)).fetchall()
        return [ProtocolEvent.model_validate(row[0]) for row in rows]

    async def wait_for_event(self, thread_id: str, after_seq: int | None, timeout: float) -> None:
        condition = self._conditions[thread_id]
        async with condition:
            await asyncio.wait_for(condition.wait(), timeout=timeout)
