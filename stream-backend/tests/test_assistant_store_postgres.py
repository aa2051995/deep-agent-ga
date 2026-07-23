"""Tests for the Postgres-backed assistant store.

The factory test runs everywhere. The round-trip integration test needs a real
Postgres and is skipped unless STREAM_BACKEND_TEST_POSTGRES_URI is set, e.g.:

    STREAM_BACKEND_TEST_POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/postgres \
        python -m pytest tests/test_assistant_store_postgres.py -q
"""
from __future__ import annotations

import importlib.util
import os
import uuid

import pytest

from app.assistants import AssistantConfig, AssistantStore, ModelConfig, create_assistant_store
from app.assistant_store_postgres import PostgresAssistantStore


def test_factory_selects_backend(monkeypatch):
    monkeypatch.delenv("STREAM_BACKEND_ASSISTANT_STORE", raising=False)
    assert isinstance(create_assistant_store(), AssistantStore)

    monkeypatch.setenv("STREAM_BACKEND_ASSISTANT_STORE", "postgres")
    store = create_assistant_store()
    assert isinstance(store, PostgresAssistantStore)
    # Constructing the Postgres store must NOT open a connection (import-safe).
    assert store._pool is None


def test_postgres_store_constructs_without_dsn(monkeypatch):
    """No DSN in the env + lazy pool => construction still succeeds; the error only
    surfaces when the DB is actually used."""
    for var in ("STREAM_BACKEND_POSTGRES_URI", "POSTGRES_URI", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    store = PostgresAssistantStore()  # no raise
    assert store._pool is None


_PG_URI = os.getenv("STREAM_BACKEND_TEST_POSTGRES_URI")
_HAS_PSYCOPG = importlib.util.find_spec("psycopg_pool") is not None

pg = pytest.mark.skipif(
    not (_PG_URI and _HAS_PSYCOPG),
    reason="set STREAM_BACKEND_TEST_POSTGRES_URI and install psycopg[pool] to run",
)


@pg
def test_postgres_store_roundtrip(tmp_path):
    store = PostgresAssistantStore(dsn=_PG_URI, scratch_dir=tmp_path)
    aid = f"pytest-{uuid.uuid4().hex[:8]}"
    try:
        cfg = AssistantConfig(
            assistant_id=aid,
            name="PG Test",
            model=ModelConfig(provider="anthropic", name="claude-sonnet-4-5-20250929"),
        )
        # create + get
        store.create(cfg)
        assert store.exists(aid)
        got = store.get(aid)
        assert got.model.provider == "anthropic"
        assert aid in store.list_ids()

        # skill + memory file bodies persist and re-materialize to disk
        store.write_skill(aid, "web research", "# SKILL\ndo research", description="d")
        store.write_memory(aid, "AGENTS", "always cite sources")
        folder = store.path_for(aid)
        assert (folder / "assistant.json").is_file()
        assert (folder / "skills" / "web-research" / "SKILL.md").read_text(encoding="utf-8").startswith("# SKILL")
        assert (folder / "memory" / "AGENTS.md").read_text(encoding="utf-8") == "always cite sources"

        # the config now references both files
        reloaded = store.get(aid)
        assert any(s.path == "skills/web-research" for s in reloaded.skills)
        assert any(m.path == "memory/AGENTS.md" for m in reloaded.memory)

        # update bumps updated_at -> re-materialization refreshes the config file
        reloaded.description = "changed"
        store.update(aid, reloaded)
        assert store.get(aid).description == "changed"
    finally:
        store.delete(aid)
        assert not store.exists(aid)
