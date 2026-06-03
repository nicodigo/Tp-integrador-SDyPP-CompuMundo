"""Unit tests for broker messages and topology (mock pika)."""

import json
import unittest
from unittest.mock import MagicMock, call, patch

from broker.broker import (
    EXCHANGE,
    RESULTS_QUEUE,
    TASKS_QUEUE,
    broadcast_abort,
    consume_result,
    declare_topology,
    publish_result,
    publish_tasks,
    setup_control_listener,
    start_consuming_tasks,
)
from broker.messages import ControlMessage, ResultMessage, TaskMessage


# ---------------------------------------------------------------------------
# Messages (pure dataclasses — no mocking needed)
# ---------------------------------------------------------------------------


class TestTaskMessage(unittest.TestCase):
    def test_create_and_serialise(self):
        task = TaskMessage.create(
            block_index=2,
            fingerprint="abc123",
            difficulty=4,
            range_min=0,
            range_max=100,
        )
        self.assertIsNotNone(task.task_id)
        self.assertEqual(task.block_index, 2)
        self.assertEqual(task.difficulty, 4)

        raw = task.to_json()
        restored = TaskMessage.from_json(raw)
        self.assertEqual(restored.task_id, task.task_id)
        self.assertEqual(restored.fingerprint, "abc123")
        self.assertEqual(restored.range_max, 100)


class TestResultMessage(unittest.TestCase):
    def test_roundtrip(self):
        result = ResultMessage(
            task_id="t1",
            block_index=2,
            worker_id="w1",
            nonce=42,
            hash="0000abcd1234",
        )
        raw = result.to_json()
        restored = ResultMessage.from_json(raw)
        self.assertEqual(restored.worker_id, "w1")
        self.assertEqual(restored.nonce, 42)
        self.assertEqual(restored.hash, "0000abcd1234")


class TestControlMessage(unittest.TestCase):
    def test_roundtrip(self):
        msg = ControlMessage(action="abort", task_id="t1")
        raw = msg.to_json()
        restored = ControlMessage.from_json(raw)
        self.assertEqual(restored.action, "abort")
        self.assertEqual(restored.task_id, "t1")


# ---------------------------------------------------------------------------
# Topology declaration
# ---------------------------------------------------------------------------


class TestDeclareTopology(unittest.TestCase):
    def test_creates_exchange_and_queues(self):
        channel = MagicMock()
        declare_topology(channel)

        channel.exchange_declare.assert_called_once_with(
            exchange=EXCHANGE, exchange_type="topic", durable=True
        )
        channel.queue_declare.assert_has_calls(
            [call(queue=TASKS_QUEUE, durable=True),
             call(queue=RESULTS_QUEUE, durable=True)],
            any_order=True,
        )
        channel.queue_bind.assert_has_calls(
            [call(exchange=EXCHANGE, queue=TASKS_QUEUE, routing_key="task.*"),
             call(exchange=EXCHANGE, queue=RESULTS_QUEUE, routing_key="result.*")],
            any_order=True,
        )


# ---------------------------------------------------------------------------
# Coordinator operations
# ---------------------------------------------------------------------------


class TestPublishTasks(unittest.TestCase):
    def test_partitions_nonce_space(self):
        channel = MagicMock()
        tasks = publish_tasks(
            channel, block_index=1, fingerprint="f", difficulty=2,
            num_workers=3, range_size=300,
        )

        self.assertEqual(len(tasks), 3)
        # Ranges should be [0,99], [100,199], [200,299]
        self.assertEqual(tasks[0].range_min, 0)
        self.assertEqual(tasks[0].range_max, 99)
        self.assertEqual(tasks[1].range_min, 100)
        self.assertEqual(tasks[1].range_max, 199)
        self.assertEqual(tasks[2].range_min, 200)
        self.assertEqual(tasks[2].range_max, 299)

        self.assertEqual(channel.basic_publish.call_count, 3)

    def test_last_range_absorbs_remainder(self):
        channel = MagicMock()
        tasks = publish_tasks(
            channel, block_index=1, fingerprint="f", difficulty=2,
            num_workers=3, range_size=100,
        )
        self.assertEqual(tasks[-1].range_max, 99)  # 100-1


class TestConsumeResult(unittest.TestCase):
    def test_returns_result_when_found(self):
        channel = MagicMock()
        result = ResultMessage(
            task_id="t1", block_index=1, worker_id="w1",
            nonce=42, hash="0000abcd",
        )

        # First get returns a result
        method = MagicMock()
        channel.basic_get.return_value = (method, None, result.to_json().encode())

        got = consume_result(channel, timeout_seconds=1, poll_interval=0.01)
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got.nonce, 42)

    def test_returns_none_on_timeout(self):
        channel = MagicMock()
        channel.basic_get.return_value = (None, None, None)

        got = consume_result(channel, timeout_seconds=0.05, poll_interval=0.01)
        self.assertIsNone(got)


class TestBroadcastAbort(unittest.TestCase):
    def test_publishes_control_message(self):
        channel = MagicMock()
        broadcast_abort(channel, "task-abc")

        channel.basic_publish.assert_called_once()
        call_args = channel.basic_publish.call_args
        self.assertEqual(call_args[1]["exchange"], EXCHANGE)
        self.assertEqual(call_args[1]["routing_key"], "control")

        body = json.loads(call_args[1]["body"])
        self.assertEqual(body["action"], "abort")
        self.assertEqual(body["task_id"], "task-abc")


# ---------------------------------------------------------------------------
# Worker operations
# ---------------------------------------------------------------------------


class TestSetupControlListener(unittest.TestCase):
    def test_creates_anonymous_queue_and_binds(self):
        channel = MagicMock()
        mock_result = MagicMock()
        mock_result.method.queue = "amq.gen-fake"
        channel.queue_declare.return_value = mock_result

        received: list[ControlMessage] = []
        qname = setup_control_listener(channel, received.append)

        self.assertEqual(qname, "amq.gen-fake")
        channel.queue_bind.assert_called_once_with(
            exchange=EXCHANGE, queue="amq.gen-fake", routing_key="control"
        )

    def test_callback_receives_control_message(self):
        channel = MagicMock()
        mock_result = MagicMock()
        mock_result.method.queue = "amq.gen-xyz"
        channel.queue_declare.return_value = mock_result

        received: list[ControlMessage] = []
        setup_control_listener(channel, received.append)

        # Grab the registered callback and simulate a message
        consume_call = channel.basic_consume.call_args
        callback = consume_call[1]["on_message_callback"]

        msg = ControlMessage(action="abort", task_id="t42")
        callback(channel, MagicMock(), None, msg.to_json().encode())

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].action, "abort")
        self.assertEqual(received[0].task_id, "t42")


class TestPublishResult(unittest.TestCase):
    def test_publishes_to_results_routing_key(self):
        channel = MagicMock()
        result = ResultMessage(
            task_id="t1", block_index=1, worker_id="worker-x",
            nonce=7, hash="0000dead",
        )
        publish_result(channel, result)

        channel.basic_publish.assert_called_once()
        call_args = channel.basic_publish.call_args
        self.assertEqual(call_args[1]["routing_key"], "result.worker-x")


class TestStartConsumingTasks(unittest.TestCase):
    def test_sets_qos_and_consumes(self):
        channel = MagicMock()
        # Prevent start_consuming from blocking
        channel.start_consuming.side_effect = StopIteration

        on_task = MagicMock()
        with self.assertRaises(StopIteration):
            start_consuming_tasks(channel, on_task)

        channel.basic_qos.assert_called_once_with(prefetch_count=1)
        channel.basic_consume.assert_called_once()
        self.assertEqual(channel.basic_consume.call_args[1]["queue"], TASKS_QUEUE)

    def test_task_callback_acks_after_processing(self):
        channel = MagicMock()
        channel.start_consuming.side_effect = StopIteration

        processed: list[TaskMessage] = []

        with self.assertRaises(StopIteration):
            start_consuming_tasks(channel, processed.append)

        # Grab callback and simulate a message
        consume_call = channel.basic_consume.call_args
        callback = consume_call[1]["on_message_callback"]

        task = TaskMessage.create(
            block_index=3, fingerprint="f", difficulty=4,
            range_min=0, range_max=100,
        )
        delivery_tag = 123
        method = MagicMock()
        method.delivery_tag = delivery_tag

        callback(channel, method, None, task.to_json().encode())

        self.assertEqual(len(processed), 1)
        self.assertEqual(processed[0].block_index, 3)
        channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)


if __name__ == "__main__":
    unittest.main()
