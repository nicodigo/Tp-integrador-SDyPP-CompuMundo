"""Redis-backed blockchain persistence layer.

Stores blocks as JSON strings in a Redis List.  Each block is appended
with ``RPUSH``, giving an append-only structure that mirrors the
conceptual blockchain.

Usage::

    from storage.chain_store import (
        connect, save_block, get_block, get_latest_block,
        get_chain_height, validate_chain,
    )

    redis_client = connect()
    save_block(redis_client, genesis)
    print(get_chain_height(redis_client))   # → 1
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from redis import Redis

from shared.block import Block

# ---------------------------------------------------------------------------
# Redis key layout
# ---------------------------------------------------------------------------

BLOCKS_KEY = "blockchain:blocks"

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def connect() -> "Redis":
    """Return a Redis client configured from the ``REDIS_URL`` environment variable.

    Defaults to ``redis://localhost:6379`` when the variable is not set.

    ``redis-py`` is imported lazily so the rest of the module is usable
    without a Redis installation (e.g. during testing).
    """
    # fmt: off
    from redis import Redis          # type: ignore[import-untyped]
    from redis.exceptions import RedisError
    # fmt: on

    url = os.getenv("REDIS_URL", "redis://localhost:6379")
    client: Redis = Redis.from_url(url, decode_responses=True)  # type: ignore[no-untyped-call]
    try:
        client.ping()
    except RedisError as exc:
        raise ConnectionError(f"Could not connect to Redis at {url}") from exc
    return client


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def save_block(client: Any, block: Block) -> None:
    """Append *block* to the end of the chain.

    The caller is responsible for ensuring the block is valid and
    correctly chained to the previous block.
    """
    payload = json.dumps(block.to_dict(), sort_keys=True)
    client.rpush(BLOCKS_KEY, payload)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def get_block(client: Any, index: int) -> Optional[Block]:
    """Return the block at *index*, or ``None`` if it does not exist.

    Indices are 0-based (``0`` is the genesis block).
    """
    raw = client.lindex(BLOCKS_KEY, index)
    if raw is None:
        return None
    return Block.from_dict(json.loads(raw))


def get_latest_block(client: Any) -> Optional[Block]:
    """Return the most recently appended block, or ``None`` if the chain is empty."""
    height = get_chain_height(client)
    if height == 0:
        return None
    return get_block(client, height - 1)


def get_chain_height(client: Any) -> int:
    """Return the number of blocks currently stored in the chain."""
    return client.llen(BLOCKS_KEY)


# ---------------------------------------------------------------------------
# Integrity
# ---------------------------------------------------------------------------


def validate_chain(client: Any) -> list[dict]:
    """Walk the entire chain and return a list of validation errors.

    Each entry is ``{"index": i, "errors": [...]}``.  An empty list
    means the chain is structurally valid.
    """
    errors: list[dict] = []
    height = get_chain_height(client)

    for i in range(height):
        block = get_block(client, i)
        prev = get_block(client, i - 1) if i > 0 else None
        block_errors = block.validate(prev) if block else ["block is unreadable"]
        if block_errors:
            errors.append({"index": i, "errors": block_errors})

    return errors
