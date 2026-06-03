"""Unit tests for chain_store (mock Redis)."""

import json
import unittest
from unittest.mock import MagicMock

from shared.block import Block, Transaction
from storage.chain_store import (
    BLOCKS_KEY,
    get_block,
    get_chain_height,
    get_latest_block,
    save_block,
    validate_chain,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _genesis() -> Block:
    return Block.create_genesis()


def _block1(genesis_hash: str) -> Block:
    tx = Transaction(sender="Alice", receiver="Bob", amount=10.0, timestamp=1000.0)
    b = Block(
        index=1,
        timestamp=2000.0,
        transactions=[tx],
        previous_hash=genesis_hash,
        difficulty=4,
        nonce=42,
    )
    b.hash = b.compute_hash()
    return b


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSaveAndGetBlock(unittest.TestCase):
    def test_save_appends_to_list(self):
        client = MagicMock()
        genesis = _genesis()
        save_block(client, genesis)
        client.rpush.assert_called_once_with(
            BLOCKS_KEY, json.dumps(genesis.to_dict(), sort_keys=True)
        )

    def test_get_block_returns_deserialised(self):
        genesis = _genesis()
        payload = json.dumps(genesis.to_dict(), sort_keys=True)

        client = MagicMock()
        client.lindex.return_value = payload

        block = get_block(client, 0)
        assert block is not None
        self.assertEqual(block.index, 0)
        self.assertEqual(block.previous_hash, "0" * 64)
        self.assertEqual(block.hash, genesis.hash)

    def test_get_block_missing_returns_none(self):
        client = MagicMock()
        client.lindex.return_value = None
        self.assertIsNone(get_block(client, 99))

    def test_save_and_retrieve_full_roundtrip(self):
        """End-to-end through a fake in-memory list (no mocking)."""
        storage: list[str] = []

        class FakeClient(MagicMock):
            def rpush(self, key, value):  # type: ignore[override]
                storage.append(value)
                return len(storage)

            def lindex(self, key, index):  # type: ignore[override]
                if 0 <= index < len(storage):
                    return storage[index]
                return None

            def llen(self, key):  # type: ignore[override]
                return len(storage)

        client = FakeClient()
        genesis = _genesis()

        save_block(client, genesis)
        self.assertEqual(get_chain_height(client), 1)

        b1 = _block1(genesis.hash)
        save_block(client, b1)
        self.assertEqual(get_chain_height(client), 2)

        # Retrieve and verify
        g = get_block(client, 0)
        self.assertIsNotNone(g)
        assert g is not None
        self.assertEqual(g.index, 0)
        self.assertEqual(g.hash, genesis.hash)

        b = get_block(client, 1)
        self.assertIsNotNone(b)
        assert b is not None
        self.assertEqual(b.index, 1)
        self.assertEqual(b.previous_hash, genesis.hash)

        latest = get_latest_block(client)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.index, 1)


class TestChainValidation(unittest.TestCase):
    def test_validate_empty_chain(self):
        client = MagicMock()
        client.llen.return_value = 0
        self.assertEqual(validate_chain(client), [])

    def test_validate_valid_two_block_chain(self):
        genesis = _genesis()
        b1 = _block1(genesis.hash)

        storage: list[str] = [json.dumps(genesis.to_dict(), sort_keys=True),
                              json.dumps(b1.to_dict(), sort_keys=True)]

        class FakeClient(MagicMock):
            def lindex(self, key, index):  # type: ignore[override]
                if 0 <= index < len(storage):
                    return storage[index]
                return None

            def llen(self, key):  # type: ignore[override]
                return len(storage)

        client = FakeClient()
        errors = validate_chain(client)
        self.assertEqual(errors, [], f"unexpected errors: {errors}")

    def test_validate_detects_broken_chain(self):
        genesis = _genesis()
        # Deliberately wrong previous_hash
        bad_block = Block(
            index=1,
            timestamp=2000.0,
            transactions=[Transaction(sender="X", receiver="Y", amount=1.0, timestamp=1.0)],
            previous_hash="0" * 64,  # should be genesis.hash
            difficulty=4,
            nonce=0,
        )
        bad_block.hash = bad_block.compute_hash()

        storage: list[str] = [json.dumps(genesis.to_dict(), sort_keys=True),
                              json.dumps(bad_block.to_dict(), sort_keys=True)]

        class FakeClient(MagicMock):
            def lindex(self, key, index):  # type: ignore[override]
                if 0 <= index < len(storage):
                    return storage[index]
                return None

            def llen(self, key):  # type: ignore[override]
                return len(storage)

        client = FakeClient()
        errors = validate_chain(client)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["index"], 1)
        self.assertTrue(any("previous_hash" in e for e in errors[0]["errors"]))


if __name__ == "__main__":
    unittest.main()
