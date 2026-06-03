"""RabbitMQ topology and operations for the blockchain mining pool.

All ``pika`` imports are lazy — the module is importable without a
RabbitMQ installation.  Only functions that actually connect to a broker
will trigger the import.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Optional

from .messages import ControlMessage, ResultMessage, TaskMessage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCHANGE = "blockchain"
TASKS_QUEUE = "mining_tasks"
RESULTS_QUEUE = "mining_results"
WORKER_REGISTRY_QUEUE = "worker_registry"
CONTROL_ROUTING_KEY = "control"

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def get_connection(
    url: Optional[str] = None,
    max_retries: int = 10,
    retry_delay: float = 2.0,
) -> Any:
    """Return a connected ``pika.BlockingConnection``, retrying on failure.

    Parameters
    ----------
    url:
        RabbitMQ connection URL.  Defaults to ``RABBITMQ_URL`` env var
        or ``amqp://localhost:5672``.
    """
    import pika  # type: ignore[import-untyped]
    import pika.exceptions

    if url is None:
        url = os.getenv("RABBITMQ_URL", "amqp://localhost:5672")

    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(url))
            logger.info("Connected to RabbitMQ at %s", url)
            return connection
        except pika.exceptions.AMQPConnectionError:
            if attempt == max_retries:
                raise
            logger.warning(
                "RabbitMQ not ready (attempt %d/%d), retrying in %ds…",
                attempt, max_retries, retry_delay,
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Cannot reach RabbitMQ at {url}")  # unreachable


# ---------------------------------------------------------------------------
# Topology  (idempotent — safe to call on every startup)
# ---------------------------------------------------------------------------


def declare_topology(channel: Any) -> None:
    """Create the exchange, queues, and bindings.

    Call this once per connection before publishing or consuming.
    """
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # Shared results queue (workers/pools → NCT)
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=RESULTS_QUEUE, routing_key="result.*")

    # Worker registry queue (workers → NCT heartbeats & registration)
    channel.queue_declare(queue=WORKER_REGISTRY_QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=WORKER_REGISTRY_QUEUE, routing_key="worker.*")


# ---------------------------------------------------------------------------
# Coordinator (NCT) helpers
# ---------------------------------------------------------------------------


def publish_tasks(
    channel: Any,
    block_index: int,
    fingerprint: str,
    difficulty: int,
    num_workers: int = 3,
    range_size: int = 1_000_000_000,
) -> list[TaskMessage]:
    """Partition the nonce space and publish one task per partition.

    Used by pool coordinators to distribute sub-ranges to their workers.
    Returns the list of published messages.
    """
    chunk = range_size // num_workers
    tasks: list[TaskMessage] = []

    for i in range(num_workers):
        r_min = i * chunk
        r_max = range_size - 1 if i == num_workers - 1 else (i + 1) * chunk - 1

        task = TaskMessage.create(
            block_index=block_index,
            fingerprint=fingerprint,
            difficulty=difficulty,
            range_min=r_min,
            range_max=r_max,
        )
        tasks.append(task)

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=f"task.{i}",
            body=task.to_json(),
        )

    logger.info(
        "Published %d sub-tasks for block %d (difficulty=%d, range=[0, %d])",
        num_workers, block_index, difficulty, range_size,
    )
    return tasks


def publish_mining_task(
    channel: Any,
    block_index: int,
    fingerprint: str,
    difficulty: int,
    range_size: int = 1_000_000_000,
) -> TaskMessage:
    """Publish a single mining task to all consumers (fanout via topic).

    Pools and solo miners bind their own queues to ``task.mining``.
    One message → every subscriber gets a copy → they compete.
    """
    task = TaskMessage.create(
        block_index=block_index,
        fingerprint=fingerprint,
        difficulty=difficulty,
        range_min=0,
        range_max=range_size - 1,
    )
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="task.mining",
        body=task.to_json(),
    )
    logger.info("Published mining task for block %d (range=[0, %d])", block_index, range_size)
    return task


def declare_consumer_queue(channel: Any, queue_name: str, routing_key: str) -> None:
    """Declare a durable queue and bind it to a routing key.

    Called by pools and solo miners when they start up, so each consumer
    gets its own copy of broadcast messages.
    """
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=routing_key)
    logger.info("Declared consumer queue %s (bind: %s)", queue_name, routing_key)


def consume_result(
    channel: Any,
    timeout_seconds: float = 300.0,
    poll_interval: float = 0.1,
) -> ResultMessage | None:
    """Poll the results queue until a valid result arrives or timeout.

    Uses ``basic_get`` (polling) instead of ``basic_consume`` so the
    caller remains synchronous.  Suitable for the single-threaded NCT.
    """
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        method, _properties, body = channel.basic_get(
            queue=RESULTS_QUEUE, auto_ack=True
        )
        if method and body:
            result = ResultMessage.from_json(body)
            logger.info("Received result: worker=%s nonce=%d", result.worker_id, result.nonce)
            return result
        time.sleep(poll_interval)

    logger.warning("No result received within %ds", timeout_seconds)
    return None


def broadcast_abort(channel: Any, task_id: str) -> None:
    """Publish an abort signal so all workers stop searching for *task_id*."""
    msg = ControlMessage(action="abort", task_id=task_id)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=CONTROL_ROUTING_KEY,
        body=msg.to_json(),
    )
    logger.info("Broadcast abort for task %s", task_id)


# ---------------------------------------------------------------------------
# Worker helpers
# ---------------------------------------------------------------------------


def setup_control_listener(
    channel: Any,
    on_control: Callable[[ControlMessage], None],
) -> str:
    """Create an anonymous auto-delete queue bound to the control routing key.

    Returns the queue name (useful for debugging).
    """
    result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    queue_name: str = result.method.queue
    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=CONTROL_ROUTING_KEY)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=_make_control_callback(on_control),
        auto_ack=True,
    )
    logger.info("Control listener set up on queue %s", queue_name)
    return queue_name


def start_consuming_tasks(
    channel: Any,
    on_task: Callable[[TaskMessage], None],
) -> None:
    """Bind to the shared work queue and block forever, processing tasks.

    ``prefetch_count=1`` ensures the worker only gets one task at a time,
    so it finishes (or aborts) before receiving the next one.
    """
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=TASKS_QUEUE,
        on_message_callback=_make_task_callback(on_task),
        auto_ack=False,  # manual ack after successful processing
    )
    logger.info("Worker started consuming from %s", TASKS_QUEUE)
    channel.start_consuming()


def publish_result(channel: Any, result: ResultMessage) -> None:
    """Publish a PoW solution to the results queue."""
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=f"result.{result.worker_id}",
        body=result.to_json(),
    )
    logger.info("Published result: worker=%s nonce=%d", result.worker_id, result.nonce)


# ---------------------------------------------------------------------------
# Internal callbacks
# ---------------------------------------------------------------------------


def _make_task_callback(
    on_task: Callable[[TaskMessage], None],
) -> Callable[[Any, Any, Any, bytes], None]:
    """Wrap a user-provided callback so it acks the message after success."""

    def _callback(ch: Any, method: Any, _properties: Any, body: bytes) -> None:
        task = TaskMessage.from_json(body.decode())
        logger.debug("Received task %s (block %d, range [%d, %d])",
                      task.task_id, task.block_index, task.range_min, task.range_max)
        on_task(task)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    return _callback


def _make_control_callback(
    on_control: Callable[[ControlMessage], None],
) -> Callable[[Any, Any, Any, bytes], None]:
    """Wrap a user-provided control callback."""

    def _callback(_ch: Any, _method: Any, _properties: Any, body: bytes) -> None:
        msg = ControlMessage.from_json(body.decode())
        logger.debug("Received control: action=%s task_id=%s", msg.action, msg.task_id)
        on_control(msg)

    return _callback
