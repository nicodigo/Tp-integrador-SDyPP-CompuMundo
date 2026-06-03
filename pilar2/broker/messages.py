"""Message types for the blockchain RabbitMQ broker."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# TaskMessage  —  NCT  →  Workers
# ---------------------------------------------------------------------------


@dataclass
class TaskMessage:
    """A mining task published by the coordinator to the work queue.

    Workers receive one task per nonce range.  The ``difficulty`` field
    is an integer; the worker converts it to a zero-prefix string
    (``"0" * difficulty``) before invoking the CUDA miner.
    """

    task_id: str
    block_index: int
    fingerprint: str       # SHA-256 hex of the block (without nonce)
    difficulty: int        # number of leading zero nibbles for PoW
    range_min: int         # inclusive
    range_max: int         # inclusive

    @classmethod
    def create(
        cls,
        block_index: int,
        fingerprint: str,
        difficulty: int,
        range_min: int,
        range_max: int,
    ) -> TaskMessage:
        return cls(
            task_id=str(uuid.uuid4()),
            block_index=block_index,
            fingerprint=fingerprint,
            difficulty=difficulty,
            range_min=range_min,
            range_max=range_max,
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "block_index": self.block_index,
                "fingerprint": self.fingerprint,
                "difficulty": self.difficulty,
                "range_min": self.range_min,
                "range_max": self.range_max,
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, raw: str) -> TaskMessage:
        d: dict[str, Any] = json.loads(raw)
        return cls(
            task_id=d["task_id"],
            block_index=d["block_index"],
            fingerprint=d["fingerprint"],
            difficulty=d["difficulty"],
            range_min=d["range_min"],
            range_max=d["range_max"],
        )


# ---------------------------------------------------------------------------
# ResultMessage  —  Worker  →  NCT
# ---------------------------------------------------------------------------


@dataclass
class ResultMessage:
    """A PoW solution published by a worker after finding a valid nonce."""

    task_id: str
    block_index: int
    worker_id: str
    nonce: int
    hash: str              # MD5 hex digest (32 chars)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "block_index": self.block_index,
                "worker_id": self.worker_id,
                "nonce": self.nonce,
                "hash": self.hash,
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, raw: str) -> ResultMessage:
        d: dict[str, Any] = json.loads(raw)
        return cls(
            task_id=d["task_id"],
            block_index=d["block_index"],
            worker_id=d["worker_id"],
            nonce=d["nonce"],
            hash=d["hash"],
        )


# ---------------------------------------------------------------------------
# ControlMessage  —  NCT  →  Workers  (pub/sub via anonymous queues)
# ---------------------------------------------------------------------------


@dataclass
class ControlMessage:
    """A control signal broadcast to all workers."""

    action: str            # "abort"
    task_id: str

    def to_json(self) -> str:
        return json.dumps(
            {"action": self.action, "task_id": self.task_id},
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, raw: str) -> ControlMessage:
        d: dict[str, Any] = json.loads(raw)
        return cls(action=d["action"], task_id=d["task_id"])
