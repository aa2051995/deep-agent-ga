"""Postgres-backed assistant store — a drop-in for :class:`app.assistants.AssistantStore`.

An assistant is fully represented as data:

- the :class:`~app.assistants.AssistantConfig` (model, tools, mcp, skills/memory
  metadata, subagents, middleware, ...) → one ``jsonb`` row in ``stream_assistants``;
- the skill/memory **file bodies** (``skills/<name>/SKILL.md``, ``memory/*.md``)
  → rows in ``stream_assistant_files`` (path + text content).

deepagents builds an agent from a *directory* (``FilesystemBackend`` reads
skills/memory off disk), so :meth:`path_for` **materializes** the config + files
into a per-pod scratch dir and returns it. Nothing is shared on a filesystem —
the source of truth is Postgres, which the apiserver and worker already share, so
this removes the need for an RWX/EFS volume.

Sync psycopg is used on purpose: the store API is synchronous and called from both
the async apiserver and the sync Celery worker. Reads are small and cached, mirroring
the blocking file I/O the filesystem store already does.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

from .assistants import (
    AssistantConfig,
    AssistantExists,
    AssistantNotFound,
    AssistantStore,
    MemoryConfig,
    SkillConfig,
    default_seed_assistants,
    slugify,
)
from .models import now_iso

logger = logging.getLogger("stream_backend.assistants_postgres")


def _resolve_dsn(explicit: str | None) -> str:
    dsn = (
        explicit
        or os.getenv("STREAM_BACKEND_POSTGRES_URI")
        or os.getenv("POSTGRES_URI")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "STREAM_BACKEND_ASSISTANT_STORE=postgres requires STREAM_BACKEND_POSTGRES_URI, "
            "POSTGRES_URI, or DATABASE_URL."
        )
    return dsn


class PostgresAssistantStore:
    """CRUD over Postgres-persisted assistant configs and skill/memory files."""

    CONFIG_FILENAME = "assistant.json"

    def __init__(self, dsn: str | None = None, scratch_dir: Path | str | None = None) -> None:
        # DSN is resolved lazily (on first DB use) so importing this module — and
        # constructing the store at import time — never touches the network or
        # requires the env to be present (keeps the image import guard happy).
        self._dsn_explicit = dsn
        self._pool: Any = None
        self._schema_ready = False
        self._lock = threading.RLock()
        self._scratch = (
            Path(scratch_dir)
            if scratch_dir is not None
            else Path(tempfile.gettempdir()) / "deep-agent-ga-assistants"
        )
        self._scratch.mkdir(parents=True, exist_ok=True)
        # assistant_id -> updated_at last written to the scratch dir.
        self._materialized: dict[str, str] = {}

    # ---- connection / schema --------------------------------------------
    def _get_pool(self) -> Any:
        # Fast path once both the pool exists and the schema has been created.
        if self._pool is not None and self._schema_ready:
            return self._pool
        with self._lock:
            # Create the pool ONCE and keep it. psycopg_pool reconnects in the
            # background, so if Postgres is slow/delayed at boot (or restarts
            # later) the same pool recovers — we must not drop it on failure or
            # we leak its background workers and never reconnect.
            if self._pool is None:
                try:
                    from psycopg_pool import ConnectionPool
                except Exception as exc:  # pragma: no cover - optional dependency
                    raise RuntimeError(
                        "Install psycopg[binary,pool] to use STREAM_BACKEND_ASSISTANT_STORE=postgres."
                    ) from exc
                pool = ConnectionPool(
                    conninfo=_resolve_dsn(self._dsn_explicit),
                    min_size=1,
                    max_size=int(os.getenv("STREAM_BACKEND_ASSISTANT_POOL_MAX", "4")),
                    kwargs={"autocommit": True},
                    open=False,
                )
                pool.open()
                self._pool = pool
                logger.info("assistants.pg.pool_ready")
            # Create the schema once. If Postgres is not reachable yet this
            # raises (caught by the best-effort callers), the pool is retained,
            # and the NEXT call retries the schema on the recovered pool.
            if not self._schema_ready:
                self._ensure_schema(self._pool)
                self._schema_ready = True
            return self._pool

    def _ensure_schema(self, pool: Any) -> None:
        with pool.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_assistants (
                    assistant_id text PRIMARY KEY,
                    config       jsonb NOT NULL,
                    created_at   text  NOT NULL,
                    updated_at   text  NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_assistant_files (
                    assistant_id text NOT NULL,
                    path         text NOT NULL,
                    content      text NOT NULL,
                    PRIMARY KEY (assistant_id, path)
                )
                """
            )

    # ---- read ------------------------------------------------------------
    def exists(self, assistant_id: str) -> bool:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM stream_assistants WHERE assistant_id = %s", (assistant_id,)
            ).fetchone()
        return row is not None

    def list_ids(self) -> list[str]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                "SELECT assistant_id FROM stream_assistants ORDER BY assistant_id"
            ).fetchall()
        return [r[0] for r in rows]

    def get(self, assistant_id: str) -> AssistantConfig:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT config FROM stream_assistants WHERE assistant_id = %s", (assistant_id,)
            ).fetchone()
        if row is None:
            raise AssistantNotFound(assistant_id)
        return AssistantConfig.model_validate(row[0])

    def list(self) -> list[AssistantConfig]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                "SELECT config FROM stream_assistants ORDER BY assistant_id"
            ).fetchall()
        out: list[AssistantConfig] = []
        for (cfg,) in rows:
            try:
                out.append(AssistantConfig.model_validate(cfg))
            except Exception:
                logger.exception("assistants.pg.list.load_failed")
        return out

    # ---- write -----------------------------------------------------------
    def _upsert_config(self, config: AssistantConfig) -> None:
        from psycopg.types.json import Jsonb

        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO stream_assistants (assistant_id, config, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (assistant_id)
                DO UPDATE SET config = EXCLUDED.config, updated_at = EXCLUDED.updated_at
                """,
                (config.assistant_id, Jsonb(config.model_dump(mode="json")), config.created_at, config.updated_at),
            )

    def create(self, config: AssistantConfig) -> AssistantConfig:
        if not config.assistant_id:
            config.assistant_id = slugify(config.name)
        if self.exists(config.assistant_id):
            raise AssistantExists(config.assistant_id)
        config.created_at = config.updated_at = now_iso()
        self._upsert_config(config)
        logger.info("assistants.pg.create id=%s", config.assistant_id)
        return config

    def update(self, assistant_id: str, config: AssistantConfig) -> AssistantConfig:
        existing = self.get(assistant_id)  # raises AssistantNotFound
        config.assistant_id = assistant_id
        config.created_at = existing.created_at
        config.updated_at = now_iso()
        self._upsert_config(config)
        logger.info("assistants.pg.update id=%s", assistant_id)
        return config

    def save(self, config: AssistantConfig) -> AssistantConfig:
        if self.exists(config.assistant_id):
            return self.update(config.assistant_id, config)
        return self.create(config)

    def delete(self, assistant_id: str) -> None:
        if not self.exists(assistant_id):
            raise AssistantNotFound(assistant_id)
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute("DELETE FROM stream_assistant_files WHERE assistant_id = %s", (assistant_id,))
            conn.execute("DELETE FROM stream_assistants WHERE assistant_id = %s", (assistant_id,))
        with self._lock:
            self._materialized.pop(assistant_id, None)
        shutil.rmtree(self._scratch / assistant_id, ignore_errors=True)
        logger.info("assistants.pg.delete id=%s", assistant_id)

    # ---- skills / memory files ------------------------------------------
    def _put_file(self, assistant_id: str, path: str, content: str) -> None:
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO stream_assistant_files (assistant_id, path, content)
                VALUES (%s, %s, %s)
                ON CONFLICT (assistant_id, path) DO UPDATE SET content = EXCLUDED.content
                """,
                (assistant_id, path, content),
            )

    def _list_files(self, assistant_id: str) -> list[tuple[str, str]]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                "SELECT path, content FROM stream_assistant_files WHERE assistant_id = %s",
                (assistant_id,),
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def write_skill(self, assistant_id: str, name: str, content: str, description: str = "") -> SkillConfig:
        config = self.get(assistant_id)
        skill_slug = slugify(name)
        rel_dir = f"skills/{skill_slug}"
        self._put_file(assistant_id, f"{rel_dir}/SKILL.md", content)
        entry = SkillConfig(name=skill_slug, description=description, path=rel_dir, enabled=True)
        config.skills = [s for s in config.skills if s.path != rel_dir] + [entry]
        self.update(assistant_id, config)
        return entry

    def write_memory(self, assistant_id: str, name: str, content: str) -> MemoryConfig:
        config = self.get(assistant_id)
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", (name or "").strip()).strip("-") or "AGENTS"
        filename = safe if safe.endswith(".md") else f"{safe}.md"
        rel = f"memory/{filename}"
        self._put_file(assistant_id, rel, content)
        entry = MemoryConfig(name=filename, path=rel, enabled=True)
        config.memory = [m for m in config.memory if m.path != rel] + [entry]
        self.update(assistant_id, config)
        return entry

    # ---- materialization (for FilesystemBackend at build time) ----------
    def path_for(self, assistant_id: str) -> Path:
        """Return a real directory holding the assistant's config + skill/memory
        files, written from Postgres. Re-materializes only when the stored
        ``updated_at`` changes, so the assistant.json mtime stays stable and the
        runtime's per-assistant agent cache is reused between runs.
        """
        self._materialize(assistant_id)
        return self._scratch / assistant_id

    def _materialize(self, assistant_id: str) -> None:
        try:
            config = self.get(assistant_id)
        except AssistantNotFound:
            return
        dest = self._scratch / assistant_id
        with self._lock:
            fresh = self._materialized.get(assistant_id) == config.updated_at
            if fresh and (dest / self.CONFIG_FILENAME).is_file():
                return
            (dest / "skills").mkdir(parents=True, exist_ok=True)
            (dest / "memory").mkdir(parents=True, exist_ok=True)
            (dest / self.CONFIG_FILENAME).write_text(
                json.dumps(config.model_dump(mode="json"), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            for rel, content in self._list_files(assistant_id):
                target = dest / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            self._materialized[assistant_id] = config.updated_at

    # ---- seeding / migration --------------------------------------------
    def ensure_seeded(self) -> None:
        """Populate an empty store. Migrates the image's baked-in filesystem
        assistants (config + skill/memory files) when present, otherwise inserts
        the built-in defaults."""
        if self.list_ids():
            return
        fs = AssistantStore()
        try:
            configs = fs.list()
        except Exception:
            logger.warning("assistants.pg.seed.fs_read_failed", exc_info=True)
            configs = []
        if not configs:
            configs = default_seed_assistants()
        for config in configs:
            try:
                if self.exists(config.assistant_id):
                    continue
                self._upsert_config(config)
                self._import_fs_files(fs, config.assistant_id)
                logger.info("assistants.pg.seed id=%s", config.assistant_id)
            except Exception:
                logger.exception("assistants.pg.seed_failed id=%s", config.assistant_id)

    def _import_fs_files(self, fs: AssistantStore, assistant_id: str) -> None:
        folder = fs.path_for(assistant_id)
        if not folder.is_dir():
            return
        for sub in ("skills", "memory"):
            base = folder / sub
            if not base.is_dir():
                continue
            for f in base.rglob("*"):
                if not f.is_file():
                    continue
                rel = f.relative_to(folder).as_posix()
                try:
                    self._put_file(assistant_id, rel, f.read_text(encoding="utf-8"))
                except Exception:
                    logger.warning("assistants.pg.seed.file_failed path=%s", rel, exc_info=True)
