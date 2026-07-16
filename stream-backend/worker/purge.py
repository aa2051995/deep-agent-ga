"""Drop all pending (not-yet-started) tasks from the Celery queue(s).

Purges ready messages on the broker for the configured task queue. Tasks already
delivered to / executing on a worker are unaffected (queue = pending only).

Usage:
    python -m worker.purge                 # purge the configured queue(s)
    python -m worker.purge my-queue other  # purge specific queue name(s)
"""
from __future__ import annotations

import os
import sys


def purge_queues(queue_names: list[str] | None = None) -> int:
    from worker.celery_app import celery_app

    if not queue_names:
        default_queue = os.getenv("STREAM_BACKEND_CELERY_QUEUE", "deep-research-runs")
        # Dedupe while preserving order.
        queue_names = list(dict.fromkeys([default_queue, "celery"]))

    total = 0
    with celery_app.connection_for_write() as conn:
        channel = conn.default_channel
        for name in queue_names:
            try:
                purged = channel.queue_purge(name)
                total += int(purged or 0)
                print(f"purged queue '{name}': {purged} message(s)")
            except Exception as exc:  # queue may not exist — that's fine
                print(f"skip queue '{name}': {type(exc).__name__}: {exc}")
    print(f"TOTAL purged: {total}")
    return total


def main() -> None:
    purge_queues(sys.argv[1:] or None)


if __name__ == "__main__":
    main()
