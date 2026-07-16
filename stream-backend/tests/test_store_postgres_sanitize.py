"""Unit tests for JSONB null-byte sanitization in PostgresRepository.

PostgreSQL's jsonb/text type rejects the NUL code point (\\u0000). Agent tool
outputs / streamed content occasionally carry raw bytes that decode to NUL,
which previously crashed every event write for the thread with
``UntranslatableCharacter``. These tests cover the sanitizer and its wiring.
"""
from __future__ import annotations

import logging
import time

from app.store_postgres import PostgresRepository, sanitize_for_jsonb


def test_strips_null_from_plain_string() -> None:
    assert sanitize_for_jsonb("a\x00b\x00c") == "abc"


def test_log_timing_escalates_level_with_elapsed(caplog) -> None:
    repo = PostgresRepository("postgresql://unused")  # no setup / no pool needed
    with caplog.at_level(logging.DEBUG, logger="stream_backend.store_postgres"):
        repo._log_timing("op_fast", time.perf_counter(), thread_id="t1")
        repo._log_timing("op_slow", time.perf_counter() - 1.0, thread_id="t1")  # ~1000ms

    records = {rec.getMessage(): rec.levelname for rec in caplog.records}
    fast = [msg for msg in records if "op_fast" in msg]
    slow = [msg for msg in records if "op_slow" in msg]
    assert fast and "elapsed_ms" in fast[0] and records[fast[0]] == "DEBUG"
    assert slow and records[slow[0]] == "WARNING"  # >500ms -> WARNING


def test_preserves_strings_without_null() -> None:
    # Replacement chars and other control chars are valid in jsonb and kept.
    value = "clean � text  ok"
    assert sanitize_for_jsonb(value) is value


def test_recurses_into_dicts_lists_and_keys() -> None:
    payload = {
        "content": "raw\x00bytes",
        "nested": [{"text": "x\x00y"}, "z\x00"],
        "ke\x00y": "v",
    }
    cleaned = sanitize_for_jsonb(payload)
    assert cleaned == {
        "content": "rawbytes",
        "nested": [{"text": "xy"}, "z"],
        "key": "v",
    }


def test_tuples_become_lists_and_scalars_pass_through() -> None:
    assert sanitize_for_jsonb(("a\x00", 1, True, None)) == ["a", 1, True, None]
    assert sanitize_for_jsonb(42) == 42


def test_json_wrapper_sanitizes_before_serialization() -> None:
    repo = PostgresRepository("postgresql://unused")
    # Bypass setup(): _json only needs the Jsonb adapter; use identity so we can
    # inspect the sanitized Python object it would hand to psycopg.
    repo._jsonb = lambda value: value
    result = repo._json({"messages": [{"content": "bad\x00data"}]})
    assert result == {"messages": [{"content": "baddata"}]}
