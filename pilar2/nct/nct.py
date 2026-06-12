"""NCT — Node Coordinator for the distributed blockchain mining pool.

Orchestrates the full lifecycle of a block: transaction accumulation,
block creation, distributed mining via RabbitMQ, PoW verification,
and chain persistence into Redis.

Usage::

    python -m nct.nct
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import threading
import time
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI

from broker.broker import (
    RESULTS_QUEUE,
    WORKER_REGISTRY_QUEUE,
    broadcast_abort,
    declare_topology,
    get_connection,
    publish_mining_task,
)
from broker.messages import ResultMessage
from nct.state import NCTConfig, NCTState
from shared.block import Block, Transaction
from shared.schemas import (
    BalanceResponse,
    ErrorResponse,
    HealthResponse,
    NCTStatusResponse,
    TransactionRequest,
    TransactionResponse,
)
from storage.chain_store import (
    BALANCE_PREFIX,
    connect as redis_connect,
    get_balance,
    get_block,
    get_chain_height,
    get_latest_block,
    rebuild_balances_from_chain,
    save_block,
    update_balances_from_block,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def load_config() -> NCTConfig:
    return NCTConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        rabbitmq_url=os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        worker_count=_env_int("WORKER_COUNT", 2),
        block_size=_env_int("BLOCK_SIZE", 5),
        block_timeout=_env_float("BLOCK_TIMEOUT", 30.0),
        difficulty=_env_int("DIFFICULTY", 4),
        nonce_space=_env_int("NONCE_SPACE", 1_000_000_000),
        port=_env_int("PORT", 8080),
    )


def verify_pow_result(
    fingerprint: str,
    difficulty: int,
    nonce: int,
    claimed_hash: str,
) -> tuple[bool, str]:
    """Check that *claimed_hash* is ``MD5(fingerprint + nonce)`` and meets
    the difficulty target.

    Returns ``(is_valid, actual_md5_hash)``.
    """
    pow_hash = hashlib.md5((fingerprint + str(nonce)).encode()).hexdigest()
    valid = (pow_hash == claimed_hash) and pow_hash.startswith("0" * difficulty)
    return valid, pow_hash


def drain_pool_validated(
    state: NCTState,
    redis_client: Any,
    max_count: int,
) -> list[Transaction]:
    """Drain the transaction pool applying balance validation for SPEND txns.

    Maintains an in-memory *overlay* that tracks per-student deltas
    accumulated during this block's assembly.  This prevents double-spend
    within a single block without requiring synchronous balance checks at
    POST time.

    EARN transactions are always accepted (structural validation already
    passed).  SPEND transactions are accepted only when::

        confirmed_balance + overlay_delta >= amount

    Discarded transactions are **not** returned to the pool — the client
    must re-send the POST if it wants to retry.
    """
    candidates = state.drain_pool(max_count)
    overlay: dict[str, float] = {}   # student_id → accumulated delta in this block
    valid: list[Transaction] = []
    discarded: list[Transaction] = []

    for tx in candidates:
        if tx.tx_type == "EARN":
            valid.append(tx)
            overlay[tx.receiver] = overlay.get(tx.receiver, 0.0) + tx.amount

        elif tx.tx_type == "SPEND":
            confirmed = get_balance(redis_client, tx.sender)
            in_flight = overlay.get(tx.sender, 0.0)
            effective = confirmed + in_flight

            if effective >= tx.amount:
                valid.append(tx)
                overlay[tx.sender] = in_flight - tx.amount
            else:
                discarded.append(tx)
                logger.warning(
                    "SPEND descartado — saldo insuficiente: student=%s "
                    "confirmado=%.2f en_vuelo=%.2f requerido=%.2f concept=%s",
                    tx.sender, confirmed, in_flight, tx.amount, tx.concept,
                )

    if discarded:
        logger.info("%d transacción(es) descartada(s) por saldo insuficiente", len(discarded))

    return valid


def handle_result(
    state: NCTState,
    redis_client: Any,
    channel: Any,
    result: ResultMessage,
) -> bool:
    """Process a mining result: verify PoW, complete and persist the block,
    broadcast abort, and signal the block loop.

    Returns ``True`` if the block was successfully mined and persisted.
    """
    # ---- Duplicate guard: block already mined by another worker ----
    if state.block_mined.is_set():
        logger.debug("Resultado duplicado para bloque ya minado — descartado")
        return False

    current_block, fingerprint, difficulty = state.get_current_for_verification()
    if current_block is None:
        logger.debug("No current mining job — ignoring result for block %d", result.block_index)
        return False

    # ---- Stale check ----
    if result.block_index != current_block.index:
        logger.debug("Stale result for block %d (current is %d), ignoring",
                      result.block_index, current_block.index)
        return False

    # ---- PoW verification ----
    valid, actual_hash = verify_pow_result(fingerprint, difficulty, result.nonce, result.hash)
    if not valid:
        logger.warning(
            "Invalid PoW from %s: claimed %s, actual %s (nonce=%d)",
            result.worker_id, result.hash, actual_hash, result.nonce,
        )
        return False

    # ---- Complete the block ----
    current_block.nonce = result.nonce
    current_block.hash = current_block.compute_hash()

    # ---- Persist ----
    save_block(redis_client, current_block)
    update_balances_from_block(redis_client, current_block)
    state.chain_height = current_block.index + 1

    # ---- Broadcast abort / signal block loop ----
    broadcast_abort(channel, result.task_id)
    state.block_mined.set()

    logger.info(
        "Block %d mined by %s (nonce=%d, hash=%s)",
        current_block.index, result.worker_id, result.nonce, current_block.hash,
    )
    return True


def accumulate_transactions(
    state: NCTState,
    redis_client: Any,
    config: NCTConfig,
) -> list[Transaction]:
    """Block until the transaction pool meets the threshold or a timeout is reached.

    At least one transaction is required; returns an empty list only on shutdown.
    Transactions are validated for sufficient balance via ``drain_pool_validated``.
    """
    # Wait for at least one transaction
    while state.pool_size() == 0 and not state.shutdown.is_set():
        time.sleep(0.5)

    if state.shutdown.is_set():
        return []

    # Wait until BLOCK_SIZE is reached or BLOCK_TIMEOUT expires
    deadline = time.time() + config.block_timeout
    while state.pool_size() < config.block_size and time.time() < deadline:
        time.sleep(0.5)

    return drain_pool_validated(state, redis_client, config.block_size)


# ---------------------------------------------------------------------------
# Loops (one per thread)
# ---------------------------------------------------------------------------


def block_loop(
    state: NCTState,
    redis_client: Any,
    channel: Any,
    config: NCTConfig,
) -> None:
    """Thread 1 — accumulate transactions, create blocks, publish mining tasks."""
    logger.info("Block loop started")

    while not state.shutdown.is_set():
        # 1. Accumulate transactions
        txs = accumulate_transactions(state, redis_client, config)
        if not txs:
            continue  # shutdown or empty — retry

        # 2. Get latest block for chaining
        latest = get_latest_block(redis_client)
        if latest is None:
            logger.error("Chain is empty (no genesis block). Run init first.")
            time.sleep(2)
            continue

        # 3. Create new block
        block = Block(
            index=latest.index + 1,
            timestamp=time.time(),
            transactions=txs,
            previous_hash=latest.hash,
            difficulty=config.difficulty,
        )
        logger.info("Created block %d with %d transactions", block.index, len(txs))

        # 4. Mining loop with range expansion on timeout
        nonce_space = config.nonce_space
        mined = False

        while not mined and not state.shutdown.is_set():
            state.set_current_block(block, nonce_space)

            publish_mining_task(
                channel,
                block_index=block.index,
                fingerprint=block.fingerprint,
                difficulty=config.difficulty,
                range_size=nonce_space,
            )

            logger.info("Waiting for PoW solution for block %d (nonce_space=%d)...",
                         block.index, nonce_space)

            # Wait for the result loop to signal completion
            mined = state.block_mined.wait(timeout=config.block_timeout)

            if mined:
                break

            # Timeout — expand range and retry
            nonce_space *= 2
            logger.warning("Mining timeout for block %d, expanding to %d", block.index, nonce_space)

    logger.info("Block loop stopped")


def result_loop(
    state: NCTState,
    redis_client: Any,
    channel: Any,
) -> None:
    """Thread 2 — poll mining results and worker registry, verify PoW, persist blocks."""
    logger.info("Result loop started")

    while not state.shutdown.is_set():
        had_work = False

        # ---- Poll mining results ----
        method, _properties, body = channel.basic_get(
            queue=RESULTS_QUEUE, auto_ack=True,
        )
        if method and body:
            result = ResultMessage.from_json(body.decode())
            handle_result(state, redis_client, channel, result)
            had_work = True

        # ---- Poll worker registry (heartbeats) ----
        method, _properties, body = channel.basic_get(
            queue=WORKER_REGISTRY_QUEUE, auto_ack=True,
        )
        if method and body:
            data = json.loads(body.decode())
            state.update_worker(data["worker_id"])
            had_work = True

        if not had_work:
            time.sleep(0.1)

    logger.info("Result loop stopped")


# ---------------------------------------------------------------------------
# FastAPI health application
# ---------------------------------------------------------------------------


def create_health_app(state: NCTState, redis_client: Any) -> FastAPI:
    """Build a FastAPI app wired to the shared NCT state and Redis.

    The app is created once in ``main()`` and served by uvicorn in a
    background thread — same threading model as before, cleaner contracts.
    """
    app = FastAPI(title="NCT", version="1.0.0")

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/status", response_model=NCTStatusResponse)
    def status() -> NCTStatusResponse:
        cb, _, _ = state.get_current_for_verification()
        return NCTStatusResponse(
            chain_height=state.chain_height,
            pending_transactions=state.pool_size(),
            current_block=cb.index if cb else None,
        )

    @app.post(
        "/transaction",
        response_model=TransactionResponse,
        status_code=201,
        responses={400: {"model": ErrorResponse}},
    )
    def create_transaction(tx: TransactionRequest) -> TransactionResponse:
        t = Transaction(
            sender=tx.sender,
            receiver=tx.receiver,
            amount=tx.amount,
            tx_type=tx.tx_type,
            concept=tx.concept,
        )
        errors = t.validate()
        if errors:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=400, content=ErrorResponse(error="; ".join(errors)).model_dump(),
            )
        state.add_transaction(t)
        return TransactionResponse(tx_id=t.tx_id)

    @app.get("/balance/{student_id}", response_model=BalanceResponse)
    def get_student_balance(student_id: str) -> BalanceResponse:
        balance = get_balance(redis_client, f"student:{student_id}")
        return BalanceResponse(student_id=student_id, balance=balance)

    @app.get("/chain", response_model=list[dict])
    def get_chain() -> list[dict]:
        """Return the full serialised chain (audit trail)."""
        height = get_chain_height(redis_client)
        result: list[dict] = []
        for i in range(height):
            blk = get_block(redis_client, i)
            if blk is not None:
                result.append(blk.to_dict())
        return result

    return app


def health_loop(app: FastAPI, port: int) -> None:
    """Thread 3 — serve the FastAPI app via uvicorn."""
    logger.info("Health server listening on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def ensure_genesis(redis_client: Any) -> None:
    """Create and persist the genesis block if the chain is empty.

    If the chain already exists but the balance index is empty
    (e.g. after a crash between ``save_block`` and
    ``update_balances_from_block``), rebuild it from the chain.
    """
    existing = get_latest_block(redis_client)
    if existing is None:
        genesis = Block.create_genesis()
        save_block(redis_client, genesis)
        logger.info("Genesis block created (hash=%s)", genesis.hash)
        return

    # Recovery: chain exists but balance index is empty
    if get_chain_height(redis_client) > 0:
        balance_keys = redis_client.keys(f"{BALANCE_PREFIX}*")
        if not balance_keys:
            logger.warning("Índice de saldos vacío con cadena existente — reconstruyendo")
            rebuild_balances_from_chain(redis_client)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    log_file = os.getenv("LOG_FILE")
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=handlers,
    )

    config = load_config()
    logger.info("NCT starting with config: %s", config)

    # ---- Redis ----
    redis_client = redis_connect()
    ensure_genesis(redis_client)

    # ---- RabbitMQ ----
    rmq_conn = get_connection(url=config.rabbitmq_url)
    channel = rmq_conn.channel()
    declare_topology(channel)

    # ---- Shared state ----
    state = NCTState()
    state.chain_height = 1  # genesis is block 0 → height = 1

    # ---- FastAPI app (wired to shared state) ----
    health_app = create_health_app(state, redis_client)

    # ---- Threads ----
    threads = [
        threading.Thread(target=block_loop, args=(state, redis_client, channel, config),
                         name="block-loop", daemon=True),
        threading.Thread(target=result_loop, args=(state, redis_client, channel),
                         name="result-loop", daemon=True),
        threading.Thread(target=health_loop, args=(health_app, config.port),
                         name="health-loop", daemon=True),
    ]

    for t in threads:
        t.start()

    # ---- Graceful shutdown ----
    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        state.shutdown.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Keep main thread alive until shutdown
    try:
        while not state.shutdown.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        state.shutdown.set()

    logger.info("NCT stopped")


if __name__ == "__main__":
    main()
