"""Test that PostgresRepository.append_event survives cross-process seq races.

seq = MAX(seq)+1 is not atomic across processes; a resume/second worker task can
compute the same (thread_id, seq) and hit the primary-key unique constraint. The
append retries on an ON CONFLICT no-op instead of crashing.
"""
from __future__ import annotations

import pytest

from app.store_postgres import PostgresRepository


class _Cursor:
    def __init__(self, fetch=None, rowcount=None):
        self._fetch = fetch
        self.rowcount = rowcount

    async def fetchone(self):
        return self._fetch


class _Conn:
    def __init__(self, seqs, rowcounts):
        self.seqs = list(seqs)
        self.rowcounts = list(rowcounts)

    async def execute(self, sql, params=None):
        if sql.strip().upper().startswith("SELECT"):
            return _Cursor(fetch=(self.seqs.pop(0),))
        return _Cursor(rowcount=self.rowcounts.pop(0))


class _CM:
    def __init__(self, obj):
        self.obj = obj

    async def __aenter__(self):
        return self.obj

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        return _CM(self._conn)


def _repo(conn: _Conn) -> PostgresRepository:
    repo = PostgresRepository("postgresql://unused")
    repo._pool = _Pool(conn)
    repo._jsonb = lambda value: value  # identity adapter
    return repo


@pytest.mark.asyncio
async def test_append_event_retries_on_seq_conflict():
    # attempt 1: seq 5 -> INSERT conflicts (rowcount 0); attempt 2: seq 6 -> ok.
    conn = _Conn(seqs=[5, 6], rowcounts=[0, 1])
    repo = _repo(conn)
    event = await repo.append_event("t1", "messages", {"text": "hi"})
    assert event.seq == 6
    assert event.method == "messages"


@pytest.mark.asyncio
async def test_append_event_succeeds_first_try():
    conn = _Conn(seqs=[1], rowcounts=[1])
    repo = _repo(conn)
    event = await repo.append_event("t1", "lifecycle", {"event": "running"})
    assert event.seq == 1


@pytest.mark.asyncio
async def test_append_event_raises_after_exhausting_retries():
    # Always conflicts: 25 attempts, all rowcount 0.
    conn = _Conn(seqs=list(range(1, 60)), rowcounts=[0] * 60)
    repo = _repo(conn)
    with pytest.raises(RuntimeError, match="could not allocate a sequence"):
        await repo.append_event("t1", "messages", {"text": "hi"})
