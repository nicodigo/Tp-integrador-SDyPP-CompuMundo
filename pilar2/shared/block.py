"""Transaction and Block schemas for the distributed blockchain."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

@dataclass
class Transaction:
    """A transfer of value from one user to another.

    Fields:
        sender:     User sending funds.
        receiver:   User receiving funds.
        amount:     Amount being transferred. Must be positive.
        timestamp:  Unix timestamp (UTC) of when this transaction was created.
    """

    sender: str
    receiver: str
    amount: float
    tx_type: str = ""
    concept: str = ""
    timestamp: float = field(default_factory=time.time)

    # ------------------------------------------------------------------
    # Derived
    # ------------------------------------------------------------------

    @property
    def tx_id(self) -> str:
        """SHA-256 content identifier, deterministic across instances."""
        raw = json.dumps(self.to_dict(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
            "tx_type": self.tx_type,
            "concept": self.concept,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Transaction:
        return cls(
            sender=data["sender"],
            receiver=data["receiver"],
            amount=data["amount"],
            tx_type=data.get("tx_type", ""),
            concept=data.get("concept", ""),
            timestamp=data["timestamp"],
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> list[str]:
        """Return a list of validation errors (empty if valid).

        Structural rules only — stateless. Balance validation happens at
        block-assembly time in the NCT's ``drain_pool_validated``.
        """
        errors: list[str] = []
        if not self.sender:
            errors.append("sender must not be empty")
        if not self.receiver:
            errors.append("receiver must not be empty")
        if self.amount <= 0:
            errors.append("amount must be positive")
        if self.sender == self.receiver:
            errors.append("sender and receiver must be different")
        if not self.concept:
            errors.append("concept must not be empty")
        if self.tx_type not in ("EARN", "SPEND"):
            errors.append("tx_type must be EARN or SPEND")
        if self.tx_type == "EARN":
            if self.sender != "ACADEMIC_SYSTEM":
                errors.append("EARN sender must be ACADEMIC_SYSTEM")
            if not self.receiver.startswith("student:"):
                errors.append("EARN receiver must start with 'student:'")
        if self.tx_type == "SPEND":
            if not self.sender.startswith("student:"):
                errors.append("SPEND sender must start with 'student:'")
            if not self.receiver.startswith("vendor:"):
                errors.append("SPEND receiver must start with 'vendor:'")
        return errors


# ---------------------------------------------------------------------------
# Block
# ---------------------------------------------------------------------------

@dataclass
class Block:
    """A block in the blockchain.

    Each block contains an ordered list of transactions, a reference to the
    previous block (via its SHA-256 hash), and a nonce that satisfies the
    Proof-of-Work difficulty target.

    Fields:
        index:          Position in the chain. 0 = genesis block.
        timestamp:      Unix timestamp (UTC) when this block was created.
        transactions:   List of transactions included in this block.
        previous_hash:  SHA-256 of the previous block (64 hex chars).
                        Genesis blocks use ``"0" * 64``.
        difficulty:     Number of leading zero nibbles required by PoW.
        nonce:          Integer found by miners that satisfies PoW.
        hash:           SHA-256 of *this* block's complete contents (computed
                        after mining and stored for chain linking).
    """

    index: int
    timestamp: float
    transactions: list[Transaction]
    previous_hash: str
    difficulty: int
    nonce: int = 0
    hash: str = ""

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def create_genesis(cls) -> Block:
        """Build the genesis block.  PoW is not enforced for block 0."""
        genesis = cls(
            index=0,
            timestamp=time.time(),
            transactions=[],
            previous_hash="0" * 64,
            difficulty=0,
        )
        genesis.hash = genesis.compute_hash()
        return genesis

    # ------------------------------------------------------------------
    # Hashing helpers
    # ------------------------------------------------------------------

    def _core_dict(self, include_nonce: bool = False) -> dict[str, Any]:
        d: dict[str, Any] = {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": [t.to_dict() for t in self.transactions],
            "previous_hash": self.previous_hash,
            "difficulty": self.difficulty,
        }
        if include_nonce:
            d["nonce"] = self.nonce
        return d

    @property
    def fingerprint(self) -> str:
        """SHA-256 block identifier **without** the nonce.

        This is the value that workers receive and use as the *base string*
        for Proof-of-Work mining::

            PoW_hash = MD5(fingerprint + str(nonce))
        """
        raw = json.dumps(self._core_dict(include_nonce=False),
                         sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()

    def compute_hash(self) -> str:
        """SHA-256 over **all** block data including the nonce.

        This is the final block identifier; it is stored in ``self.hash``
        after mining and used by the next block as ``previous_hash``.
        """
        raw = json.dumps(self._core_dict(include_nonce=True),
                         sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": [t.to_dict() for t in self.transactions],
            "previous_hash": self.previous_hash,
            "difficulty": self.difficulty,
            "nonce": self.nonce,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Block:
        return cls(
            index=data["index"],
            timestamp=data["timestamp"],
            transactions=[Transaction.from_dict(tx) for tx in data["transactions"]],
            previous_hash=data["previous_hash"],
            difficulty=data["difficulty"],
            nonce=data.get("nonce", 0),
            hash=data.get("hash", ""),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, previous_block: Optional[Block] = None) -> list[str]:
        """Validate structural integrity.

        *Note*: Proof-of-Work is **not** checked here — use
        :meth:`verify_pow` separately.  This keeps structural validation
        independent of the mining infrastructure.
        """
        errors: list[str] = []

        if self.index < 0:
            errors.append("index must be non-negative")

        # --- Chaining consistency (when a previous block is provided) ---
        if previous_block is not None:
            if self.index != previous_block.index + 1:
                errors.append(
                    f"expected index {previous_block.index + 1}, got {self.index}"
                )
            if self.previous_hash != previous_block.hash:
                errors.append("previous_hash does not match previous block's hash")

        # --- Genesis block ---
        if self.index == 0:
            if self.previous_hash != "0" * 64:
                errors.append("genesis block must have previous_hash = '0' * 64")
        else:
            if not self.transactions:
                errors.append("non-genesis block must contain at least one transaction")

        # --- Transaction validation ---
        for i, tx in enumerate(self.transactions):
            for e in tx.validate():
                errors.append(f"transaction[{i}]: {e}")

        # --- Hash integrity ---
        if self.hash and self.hash != self.compute_hash():
            errors.append(
                f"hash mismatch: computed {self.compute_hash()}, "
                f"stored {self.hash}"
            )

        return errors

    @staticmethod
    def verify_pow(block: Block) -> bool:
        """Check that MD5(fingerprint + nonce) satisfies the difficulty target.

        This is the canonical verification of the Proof-of-Work solution.
        """
        if block.index == 0:
            return True  # genesis block is not mined

        raw = (block.fingerprint + str(block.nonce)).encode()
        digest = hashlib.md5(raw).hexdigest()
        return digest.startswith("0" * block.difficulty)
