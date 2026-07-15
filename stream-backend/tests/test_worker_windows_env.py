"""Tests for the Windows Celery prefork fix (FORKED_BY_MULTIPROCESSING).

Without this flag, Celery's spawned prefork workers on Windows crash every task
with "ValueError: not enough values to unpack (expected 3, got 0)" in
fast_trace_task. `configure_windows_celery_env` sets it before celery imports.
"""
from __future__ import annotations

import os
import sys

from worker.asyncio_policy import configure_windows_celery_env


def test_sets_flag_on_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delenv("FORKED_BY_MULTIPROCESSING", raising=False)
    assert configure_windows_celery_env() is True
    assert os.environ["FORKED_BY_MULTIPROCESSING"] == "1"


def test_does_not_override_existing_value(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("FORKED_BY_MULTIPROCESSING", "0")
    assert configure_windows_celery_env() is True
    # setdefault must not clobber an operator-provided value.
    assert os.environ["FORKED_BY_MULTIPROCESSING"] == "0"


def test_noop_off_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.delenv("FORKED_BY_MULTIPROCESSING", raising=False)
    assert configure_windows_celery_env() is False
    assert "FORKED_BY_MULTIPROCESSING" not in os.environ
