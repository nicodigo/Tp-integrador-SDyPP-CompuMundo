"""Thread-safe shared state for the NCT orchestrator."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from shared.block import Block, Transaction


@dataclass
class NCTConfig:
    """NCT configuration read from environment variables."""

    redis_url: str = "redis://localhost:6379"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    worker_count: int = 2
    block_size: int = 5
    block_timeout: float = 30.0
    difficulty: int = 4
    nonce_space: int = 1_000_000_000
    port: int = 8080
    heartbeat_timeout: float = 15.0
    heartbeat_interval: float = 5.0


class NCTState:
    """Shared state between the three NCT threads.

    All mutable fields are protected by locks or threading primitives.
    """

    def __init__(self, worker_timeout: float = 15.0) -> None:
        # -- synchronisation primitives --
        self.lock = threading.Lock()
        self.tx_lock = threading.Lock()
        self.block_mined = threading.Event()
        self.shutdown = threading.Event()

        # -- current mining job (protected by self.lock) --
        self._current_block: Optional[Block] = None
        self._current_fingerprint: str = ""
        self._current_difficulty: int = 4
        self._current_nonce_space: int = 1_000_000_000

        # -- transaction pool (protected by self.tx_lock) --
        self._tx_pool: list[Transaction] = []

        # -- worker registry (protected by self._worker_lock) --
        self._worker_lock = threading.Lock()
        self._workers: dict[str, float] = {}          # worker_id → last_seen
        self._worker_timeout: float = worker_timeout   # seconds before expiry

        # -- chain height for /status (may be read without lock—best-effort) --
        self.chain_height: int = 0

    # ------------------------------------------------------------------
    # Current mining job
    # ------------------------------------------------------------------

    def set_current_block(self, block: Block, nonce_space: int) -> None:
        with self.lock:
            self._current_block = block
            self._current_fingerprint = block.fingerprint
            self._current_difficulty = block.difficulty
            self._current_nonce_space = nonce_space
            self.block_mined.clear()

    def get_current_for_verification(self) -> tuple[Optional[Block], str, int]:
        """Snapshot for result-loop verification (avoids holding lock)."""
        with self.lock:
            return (
                self._current_block,
                self._current_fingerprint,
                self._current_difficulty,
            )

    def get_current_nonce_space(self) -> int:
        with self.lock:
            return self._current_nonce_space

    # ------------------------------------------------------------------
    # Transaction pool
    # ------------------------------------------------------------------

    def add_transaction(self, tx: Transaction) -> None:
        with self.tx_lock:
            self._tx_pool.append(tx)

    def drain_pool(self, max_count: int) -> list[Transaction]:
        with self.tx_lock:
            taken = self._tx_pool[:max_count]
            self._tx_pool = self._tx_pool[max_count:]
            return taken

    def pool_size(self) -> int:
        with self.tx_lock:
            return len(self._tx_pool)

    def pool_snapshot(self) -> list[Transaction]:
        with self.tx_lock:
            return list(self._tx_pool)

    # ------------------------------------------------------------------
    # Worker registry (keep-alive)
    # ------------------------------------------------------------------

    def update_worker(self, worker_id: str) -> None:
        """Record a heartbeat from *worker_id*."""
        with self._worker_lock:
            self._workers[worker_id] = time.time()

    def get_active_worker_count(self) -> int:
        """Return the number of workers that have sent a heartbeat recently."""
        cutoff = time.time() - self._worker_timeout
        with self._worker_lock:
            stale = [wid for wid, ts in self._workers.items() if ts < cutoff]
            for wid in stale:
                del self._workers[wid]
            return len(self._workers)

    def active_workers_snapshot(self) -> list[str]:
        """Return a list of active worker IDs (for debugging)."""
        cutoff = time.time() - self._worker_timeout
        with self._worker_lock:
            return sorted(
                wid for wid, ts in self._workers.items() if ts >= cutoff
            )
