from __future__ import annotations

import asyncio
import os
import sys


def configure_windows_celery_env() -> bool:
    """Make Celery's prefork pool work on Windows.

    Windows has no ``fork``, so the prefork pool spawns child processes. Without
    ``FORKED_BY_MULTIPROCESSING=1`` those children skip the worker-optimization
    setup, leaving Celery's ``_loc`` global empty — so every task dies in
    ``fast_trace_task`` with ``ValueError: not enough values to unpack
    (expected 3, got 0)``. This must be set *before* celery/billiard are
    imported. Returns True if the flag was applied (Windows only).
    """
    if sys.platform != "win32":
        return False
    os.environ.setdefault("FORKED_BY_MULTIPROCESSING", "1")
    return True


def configure_windows_event_loop_policy() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
