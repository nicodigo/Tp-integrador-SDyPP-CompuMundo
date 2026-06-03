"""Worker service — consumes mining tasks, runs CUDA miner, publishes results.

Usage::

    python -m worker.worker
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
import uuid
from typing import Any, Optional

from broker.broker import (
    CONTROL_ROUTING_KEY,
    EXCHANGE,
    TASKS_QUEUE,
    WORKER_REGISTRY_QUEUE,
    declare_topology,
    get_connection,
)
from broker.messages import ControlMessage, ResultMessage, TaskMessage
from miner.miner import MinerService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_HEARTBEAT_INTERVAL = 5.0

# ---------------------------------------------------------------------------
# WorkerService
# ---------------------------------------------------------------------------


class WorkerService:
    """Long-running process that mines blocks on demand.

    Connects to RabbitMQ, registers with the NCT, and processes mining
    tasks in a loop.  Control messages (abort) cancel in-flight work.
    """

    def __init__(
        self,
        worker_id: str,
        rmq_url: str,
        miner_binary: str = "./md5_range",
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
    ) -> None:
        self.worker_id = worker_id
        self.rmq_url = rmq_url
        self.miner = MinerService(binary_path=miner_binary)
        self.heartbeat_interval = heartbeat_interval

        # Mutable state
        self._current_task_id: Optional[str] = None
        self._aborted: threading.Event = threading.Event()
        self._shutdown: threading.Event = threading.Event()
        self._channel: Any = None

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        conn = get_connection(url=self.rmq_url)
        self._channel = conn.channel()
        declare_topology(self._channel)

        # Register immediately
        self._send_heartbeat()

        # Background heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat",
        )
        heartbeat_thread.start()

        # Control listener (abort signals)
        self._setup_control_listener()

        # Task consumer (blocking)
        self._setup_task_consumer()
        logger.info("Worker %s ready — waiting for mining tasks", self.worker_id)
        self._channel.start_consuming()

    def shutdown(self) -> None:
        self._shutdown.set()
        if self._channel is not None:
            try:
                self._channel.stop_consuming()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _send_heartbeat(self) -> None:
        msg = {
            "worker_id": self.worker_id,
            "action": "heartbeat",
            "timestamp": time.time(),
        }
        self._channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="worker.heartbeat",
            body=json.dumps(msg, sort_keys=True),
        )

    def _heartbeat_loop(self) -> None:
        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=self.heartbeat_interval)
            if not self._shutdown.is_set():
                try:
                    self._send_heartbeat()
                except Exception:
                    logger.warning("Heartbeat send failed (connection may be down)")

    # ------------------------------------------------------------------
    # Control listener (abort)
    # ------------------------------------------------------------------

    def _setup_control_listener(self) -> None:
        result = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        queue_name = result.method.queue
        self._channel.queue_bind(
            exchange=EXCHANGE, queue=queue_name, routing_key=CONTROL_ROUTING_KEY,
        )
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=self._on_control,
            auto_ack=True,
        )

    def _on_control(self, _ch: Any, _method: Any, _properties: Any, body: bytes) -> None:
        msg = ControlMessage.from_json(body.decode())
        if msg.action == "abort" and msg.task_id == self._current_task_id:
            logger.info("Abort received for task %s — cancelling mining", msg.task_id)
            self._aborted.set()

    # ------------------------------------------------------------------
    # Task consumer
    # ------------------------------------------------------------------

    def _setup_task_consumer(self) -> None:
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=TASKS_QUEUE,
            on_message_callback=self._on_task,
            auto_ack=False,
        )

    def _on_task(self, ch: Any, method: Any, _properties: Any, body: bytes) -> None:
        task = TaskMessage.from_json(body.decode())
        logger.info(
            "Task %s (block %d, difficulty=%d, range=[%d, %d])",
            task.task_id, task.block_index, task.difficulty,
            task.range_min, task.range_max,
        )

        self._current_task_id = task.task_id
        self._aborted.clear()

        # Convert difficulty (int) → target prefix (string)
        target_prefix = "0" * task.difficulty

        # Mine
        result = self.miner.mine(
            base_string=task.fingerprint,
            target_prefix=target_prefix,
            range_min=task.range_min,
            range_max=task.range_max,
        )

        # If aborted mid-mining, discard result
        if self._aborted.is_set():
            logger.info("Task %s aborted — discarding result", task.task_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if result is not None:
            msg = ResultMessage(
                task_id=task.task_id,
                block_index=task.block_index,
                worker_id=self.worker_id,
                nonce=result.nonce,
                hash=result.hash,
            )
            self._channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=f"result.{self.worker_id}",
                body=msg.to_json(),
            )
            logger.info("Nonce found: %d (hash=%s)", result.nonce, result.hash)
        else:
            logger.warning("No solution found in range [%d, %d]",
                           task.range_min, task.range_max)

        ch.basic_ack(delivery_tag=method.delivery_tag)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    worker_id = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    miner_binary = os.getenv("MINER_BINARY", "./md5_range")
    heartbeat = float(os.getenv("HEARTBEAT_INTERVAL", str(DEFAULT_HEARTBEAT_INTERVAL)))

    worker = WorkerService(
        worker_id=worker_id,
        rmq_url=rmq_url,
        miner_binary=miner_binary,
        heartbeat_interval=heartbeat,
    )

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        worker.shutdown()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    worker.run()


if __name__ == "__main__":
    main()
