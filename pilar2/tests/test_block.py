"""Unit tests for Transaction and Block schemas (built-in unittest)."""

import json
import unittest

from shared.block import Block, Transaction


class TestTransaction(unittest.TestCase):
    def test_creation(self):
        tx = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        self.assertEqual(tx.sender, "Alice")
        self.assertEqual(tx.receiver, "Bob")
        self.assertEqual(tx.amount, 10.0)
        self.assertEqual(tx.timestamp, 1000.0)

    def test_tx_id_is_deterministic(self):
        tx_a = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        tx_b = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        self.assertEqual(tx_a.tx_id, tx_b.tx_id)
        self.assertEqual(len(tx_a.tx_id), 64)  # SHA-256 hex

    def test_serde_roundtrip(self):
        tx = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        restored = Transaction.from_dict(tx.to_dict())
        self.assertEqual(restored.sender, tx.sender)
        self.assertEqual(restored.receiver, tx.receiver)
        self.assertEqual(restored.amount, tx.amount)
        self.assertEqual(restored.timestamp, tx.timestamp)

    def test_validation_rejects_invalid(self):
        # Empty sender
        self.assertTrue(Transaction(sender="", receiver="Bob", amount=10.0, timestamp=1.0).validate())
        # Empty receiver
        self.assertTrue(Transaction(sender="Alice", receiver="", amount=10.0, timestamp=1.0).validate())
        # Non-positive amount
        self.assertTrue(Transaction(sender="Alice", receiver="Bob", amount=0, timestamp=1.0).validate())
        self.assertTrue(Transaction(sender="Alice", receiver="Bob", amount=-5, timestamp=1.0).validate())
        # Same sender and receiver
        self.assertTrue(Transaction(sender="Alice", receiver="Alice", amount=10.0, timestamp=1.0).validate())

    def test_validation_passes_valid(self):
        tx = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        self.assertEqual(tx.validate(), [])


class TestBlock(unittest.TestCase):
    def test_genesis_block(self):
        genesis = Block.create_genesis()
        self.assertEqual(genesis.index, 0)
        self.assertEqual(genesis.previous_hash, "0" * 64)
        self.assertEqual(genesis.transactions, [])
        self.assertEqual(genesis.hash, genesis.compute_hash())
        self.assertEqual(genesis.validate(), [])

    def test_genesis_serialisation_roundtrip(self):
        genesis = Block.create_genesis()
        restored = Block.from_dict(genesis.to_dict())
        self.assertEqual(restored.index, genesis.index)
        self.assertEqual(restored.hash, genesis.hash)
        self.assertEqual(restored.previous_hash, "0" * 64)

    def test_block_with_transactions(self):
        genesis = Block.create_genesis()

        tx1 = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        tx2 = Transaction(sender="Bob", receiver="Charlie", amount=5.0, timestamp=1001.0)

        block1 = Block(
            index=1,
            timestamp=2000.0,
            transactions=[tx1, tx2],
            previous_hash=genesis.hash,
            difficulty=4,
            nonce=0,
        )
        block1.hash = block1.compute_hash()

        # Structural assertions
        self.assertEqual(block1.index, 1)
        self.assertEqual(block1.previous_hash, genesis.hash)
        self.assertEqual(len(block1.transactions), 2)
        self.assertEqual(len(block1.hash), 64)  # SHA-256 hex

        # Fingerprint is deterministic and differs from hash (no nonce)
        self.assertTrue(block1.fingerprint)
        self.assertEqual(len(block1.fingerprint), 64)

        # Chaining validation
        errors = block1.validate(previous_block=genesis)
        self.assertEqual(errors, [], f"validation failed: {errors}")

    def test_validate_detects_chain_break(self):
        genesis = Block.create_genesis()
        tx = Transaction(sender="Alice", receiver="Bob", amount=5.0, timestamp=1000.0)

        block1 = Block(
            index=1,
            timestamp=2000.0,
            transactions=[tx],
            previous_hash="0" * 64,  # wrong — should be genesis.hash
            difficulty=4,
        )
        block1.hash = block1.compute_hash()

        errors = block1.validate(previous_block=genesis)
        self.assertTrue(any("previous_hash" in e for e in errors), f"expected hash error, got {errors}")

    def test_verify_pow_rejects_unsatisfied_nonce(self):
        genesis = Block.create_genesis()
        tx = Transaction(sender="Alice", receiver="Bob", amount=5.0, timestamp=1000.0)

        block1 = Block(
            index=1,
            timestamp=2000.0,
            transactions=[tx],
            previous_hash=genesis.hash,
            difficulty=4,        # require 4 leading zeros in MD5
            nonce=0,             # won't satisfy difficulty 4
        )
        block1.hash = block1.compute_hash()

        self.assertFalse(Block.verify_pow(block1))

    def test_json_output(self):
        """Produce indent JSON output for manual inspection."""
        genesis = Block.create_genesis()

        tx1 = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
        tx2 = Transaction(sender="Bob", receiver="Charlie", amount=5.0, timestamp=1001.0)

        block1 = Block(
            index=1,
            timestamp=2000.0,
            transactions=[tx1, tx2],
            previous_hash=genesis.hash,
            difficulty=4,
        )
        block1.hash = block1.compute_hash()

        # Pretty-print for documentation / visual check
        print("\n=== Genesis Block ===")
        print(json.dumps(genesis.to_dict(), indent=2))

        print("\n=== Block 1 ===")
        print(json.dumps(block1.to_dict(), indent=2))

        # Basic structural assertions
        self.assertEqual(genesis.index, 0)
        self.assertEqual(block1.index, 1)
        self.assertEqual(block1.previous_hash, genesis.hash)


if __name__ == "__main__":
    unittest.main()
