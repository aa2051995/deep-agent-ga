"""Tests for early-ack config and the queue-purge helper (real celery app)."""
from __future__ import annotations

import importlib.util

import pytest
from unittest.mock import MagicMock

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("celery") is None, reason="celery is not installed"
)


def test_tasks_acknowledge_early_and_do_not_requeue():
    from worker.celery_app import celery_app

    # Early ack (not late) so a worker crash mid-run does not redeliver/re-execute.
    assert celery_app.conf.task_acks_late is False
    assert celery_app.conf.task_reject_on_worker_lost is False


def test_purge_queues_sums_counts_and_purges_each(monkeypatch):
    from worker import celery_app as ca_mod
    from worker.purge import purge_queues

    channel = MagicMock()
    channel.queue_purge.side_effect = [3, 2]
    conn = MagicMock()
    conn.default_channel = channel
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    monkeypatch.setattr(ca_mod.celery_app, "connection_for_write", lambda: cm)

    total = purge_queues(["q1", "q2"])

    assert total == 5
    assert channel.queue_purge.call_count == 2
    channel.queue_purge.assert_any_call("q1")
    channel.queue_purge.assert_any_call("q2")


def test_purge_queues_tolerates_missing_queue(monkeypatch):
    from worker import celery_app as ca_mod
    from worker.purge import purge_queues

    channel = MagicMock()
    channel.queue_purge.side_effect = [RuntimeError("no such queue"), 4]
    conn = MagicMock()
    conn.default_channel = channel
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    monkeypatch.setattr(ca_mod.celery_app, "connection_for_write", lambda: cm)

    total = purge_queues(["missing", "present"])

    assert total == 4  # the failing queue is skipped, the other still purges
