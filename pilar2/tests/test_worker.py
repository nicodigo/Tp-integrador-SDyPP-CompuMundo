"""Unit tests for worker service and NCT dynamic worker tracking."""

import time
import unittest
from unittest.mock import MagicMock

from nct.state import NCTConfig, NCTState


# ---------------------------------------------------------------------------
# NCTState worker registry
# ---------------------------------------------------------------------------


class TestNCTStateWorkerRegistry(unittest.TestCase):
    def test_single_worker_active(self):
        state = NCTState(worker_timeout=60)
        state.update_worker("worker-1")
        self.assertEqual(state.get_active_worker_count(), 1)

    def test_multiple_workers(self):
        state = NCTState(worker_timeout=60)
        state.update_worker("worker-1")
        state.update_worker("worker-2")
        state.update_worker("worker-3")
        self.assertEqual(state.get_active_worker_count(), 3)

    def test_worker_expiry(self):
        state = NCTState(worker_timeout=0.1)  # 100ms timeout
        state.update_worker("worker-1")
        self.assertEqual(state.get_active_worker_count(), 1)

        time.sleep(0.15)
        self.assertEqual(state.get_active_worker_count(), 0)

    def test_active_workers_snapshot(self):
        state = NCTState(worker_timeout=60)
        state.update_worker("worker-b")
        state.update_worker("worker-a")
        state.update_worker("worker-c")
        self.assertEqual(state.active_workers_snapshot(), ["worker-a", "worker-b", "worker-c"])

    def test_update_resets_expiry(self):
        state = NCTState(worker_timeout=0.3)
        state.update_worker("worker-1")
        time.sleep(0.15)
        state.update_worker("worker-1")  # reset timer
        time.sleep(0.15)  # only 0.15s since reset, not 0.3
        self.assertEqual(state.get_active_worker_count(), 1)


class TestNCTConfigDefaults(unittest.TestCase):
    def test_defaults(self):
        config = NCTConfig()
        self.assertEqual(config.worker_count, 2)
        self.assertEqual(config.heartbeat_timeout, 15.0)
        self.assertEqual(config.heartbeat_interval, 5.0)


if __name__ == "__main__":
    unittest.main()
