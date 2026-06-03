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

import uvicorn
from fastapi import FastAPI

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
from shared.schemas import (
    HealthResponse,
    WorkerHealthResponse,
    WorkerStatusResponse,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_HEARTBEAT_INTERVAL = 5.0
DEFAULT_HEALTH_PORT = 8081

# ---------------------------------------------------------------------------
# FastAPI health application (runs in its own thread)
# ---------------------------------------------------------------------------


def _create_health_app(worker: WorkerService) -> FastAPI:
    """Build a FastAPI app wired to a single worker instance."""

    app = FastAPI(title=f"Worker {worker.worker_id}", version="1.0.0")

    def _uptime() -> float:
        return round(time.time() - worker.start_time, 1) if worker.start_time else 0.0

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/status", response_model=WorkerStatusResponse)
    def status() -> WorkerStatusResponse:
        return WorkerStatusResponse(
            worker_id=worker.worker_id,
            current_task=worker._current_task_id,
            tasks_processed=worker.tasks_processed,
            uptime_seconds=_uptime(),
        )

    return app


# ---------------------------------------------------------------------------
# WorkerService
# ---------------------------------------------------------------------------


class WorkerService:
    """Long-running process that mines blocks on demand.

    Connects to RabbitMQ, registers with the NCT, processes mining tasks,
    and exposes a health HTTP endpoint.
    """

    def __init__(
        self,
        worker_id: str,
        rmq_url: str,
        miner_binary: str = "./md5_range",
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        health_port: int = DEFAULT_HEALTH_PORT,
        pool_id: str | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.rmq_url = rmq_url
        self.miner = MinerService(binary_path=miner_binary)
        self.heartbeat_interval = heartbeat_interval
        self.health_port = health_port
        self.pool_id = pool_id

        # Mutable state
        self._current_task_id: Optional[str] = None
        self._aborted: threading.Event = threading.Event()
        self._shutdown: threading.Event = threading.Event()
        self._channel: Any = None
        self.tasks_processed: int = 0
        self.start_time: float = 0.0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        self.start_time = time.time()
        conn = get_connection(url=self.rmq_url)
        self._channel = conn.channel()
        declare_topology(self._channel)

        # Bind to task source: fanout inbox (solo) or pool queue
        if self.pool_id:
            inbox = f"pool.{self.pool_id}.inbox"
            from broker.broker import declare_consumer_queue
            declare_consumer_queue(self._channel, inbox, "task.mining")
            tasks_queue = f"pool.{self.pool_id}.tasks"
            self._channel.queue_declare(queue=tasks_queue, durable=True)
            self._channel.queue_bind(exchange=EXCHANGE, queue=tasks_queue,
                                     routing_key=f"pool.{self.pool_id}.task.*")
        else:
            inbox = f"worker.{self.worker_id}.inbox"
            from broker.broker import declare_consumer_queue
            declare_consumer_queue(self._channel, inbox, "task.mining")
            tasks_queue = inbox

        # Register immediately (safe — called from main thread before start_consuming)
        self._send_heartbeat(self._channel)

        # Background heartbeat thread
        threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat",
        ).start()

        # Health HTTP server thread (FastAPI via uvicorn)
        health_app = _create_health_app(self)
        threading.Thread(
            target=self._run_health_server, args=(health_app,), daemon=True,
            name="health-server",
        ).start()

        # Control listener (abort signals — NCT + pool)
        self._setup_control_listener()

        # Task consumer (blocking — must be last)
        self._setup_task_consumer(tasks_queue)
        logger.info("Worker %s ready (%s) — health on :%d",
                     self.worker_id,
                     f"pool={self.pool_id}" if self.pool_id else "solo",
                     self.health_port)
        self._channel.start_consuming()

    def shutdown(self) -> None:
        self._shutdown.set()
        if self._channel is not None:
            try:
                self._channel.stop_consuming()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Health HTTP server
    # ------------------------------------------------------------------

    def _run_health_server(self, app: FastAPI) -> None:
        logger.info("Worker health server listening on port %d", self.health_port)
        uvicorn.run(app, host="0.0.0.0", port=self.health_port, log_level="warning")

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _send_heartbeat(self, channel: Any) -> None:
        msg = {
            "worker_id": self.worker_id,
            "action": "heartbeat",
            "timestamp": time.time(),
        }
        key = f"worker.{self.pool_id}.heartbeat" if self.pool_id else "worker.heartbeat"
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=key,
            body=json.dumps(msg, sort_keys=True),
        )

    def _heartbeat_loop(self) -> None:
        # Open a dedicated connection so we never share a channel across threads.
        hb_conn = get_connection(url=self.rmq_url)
        hb_channel = hb_conn.channel()

        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=self.heartbeat_interval)
            if not self._shutdown.is_set():
                try:
                    self._send_heartbeat(hb_channel)
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
        if self.pool_id:
            self._channel.queue_bind(
                exchange=EXCHANGE, queue=queue_name,
                routing_key=f"pool.{self.pool_id}.control",
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

    def _setup_task_consumer(self, queue_name: str) -> None:
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=queue_name,
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

        target_prefix = "0" * task.difficulty

        result = self.miner.mine(
            base_string=task.fingerprint,
            target_prefix=target_prefix,
            range_min=task.range_min,
            range_max=task.range_max,
        )

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
            result_key = (
                f"pool.{self.pool_id}.result.{self.worker_id}"
                if self.pool_id
                else f"result.{self.worker_id}"
            )
            self._channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=result_key,
                body=msg.to_json(),
            )
            logger.info("Nonce found: %d (hash=%s)", result.nonce, result.hash)
        else:
            logger.warning("No solution found in range [%d, %d]",
                           task.range_min, task.range_max)

        self.tasks_processed += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(log_file: str | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=handlers,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    log_file = os.getenv("LOG_FILE")
    setup_logging(log_file)

    worker_id = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    miner_binary = os.getenv("MINER_BINARY", "./md5_range")
    heartbeat = float(os.getenv("HEARTBEAT_INTERVAL", str(DEFAULT_HEARTBEAT_INTERVAL)))
    health_port = int(os.getenv("HEALTH_PORT", str(DEFAULT_HEALTH_PORT)))
    pool_id = os.getenv("POOL_ID") or None

    worker = WorkerService(
        worker_id=worker_id,
        rmq_url=rmq_url,
        miner_binary=miner_binary,
        heartbeat_interval=heartbeat,
        health_port=health_port,
        pool_id=pool_id,
    )

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        worker.shutdown()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    worker.run()


if __name__ == "__main__":
    main()
