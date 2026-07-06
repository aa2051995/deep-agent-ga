import unittest
from unittest.mock import AsyncMock, MagicMock

from app.streaming import StreamSubscriptionManager
from app.models import RunRecord


class TestStreamSubscriptionManager(unittest.IsolatedAsyncioTestCase):
    async def test_register_and_unregister_subscription(self) -> None:
        repo = MagicMock()
        repo.list_runs = AsyncMock(return_value=[])
        broker = MagicMock()
        broker.subscribe = AsyncMock()
        
        manager = StreamSubscriptionManager(repo, broker)
        
        self.assertEqual(len(manager._active_subscriptions), 0)

    async def test_get_active_run_no_runs(self) -> None:
        repo = MagicMock()
        repo.list_runs = AsyncMock(return_value=[])
        broker = MagicMock()
        
        manager = StreamSubscriptionManager(repo, broker)
        
        active = await manager.get_active_run_for_thread("thread-1")
        
        self.assertIsNone(active)
        repo.list_runs.assert_called()

    async def test_get_active_run_with_pending_run(self) -> None:
        repo = MagicMock()
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="demo", status="pending")
        repo.list_runs = AsyncMock(return_value=[run])
        broker = MagicMock()
        
        manager = StreamSubscriptionManager(repo, broker)
        
        active = await manager.get_active_run_for_thread("thread-1")
        
        self.assertIsNotNone(active)
        self.assertEqual(active["run_id"], "run-1")
        self.assertEqual(active["thread_id"], "thread-1")

    async def test_close_all_subscriptions(self) -> None:
        repo = MagicMock()
        broker = MagicMock()
        
        manager = StreamSubscriptionManager(repo, broker)
        
        await manager.close_all_subscriptions()
        
        self.assertEqual(len(manager._active_subscriptions), 0)


if __name__ == "__main__":
    unittest.main()
