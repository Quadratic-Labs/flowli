"""Unit tests for InMemoryQueue wake-up semantics (delay, claim window, ack)."""
from uuid import uuid4

import pytest

from flowlet.queue.config import InMemoryQueueConfig
from flowlet.queue.memory import InMemoryQueue


@pytest.fixture
def queue():
    return InMemoryQueue.setup(InMemoryQueueConfig())


class TestVisibility:
    def test_dequeued_job_is_invisible_during_claim_window(self, queue, make_flow_job):
        queue.enqueue(make_flow_job())
        job = queue.dequeue(timeout=60)
        assert job is not None
        assert queue.dequeue() is None

    def test_unacked_job_redelivers_after_claim_window(self, queue, make_flow_job):
        queue.enqueue(make_flow_job())
        job = queue.dequeue(timeout=0)  # claim window expires immediately
        assert job is not None
        redelivered = queue.dequeue()
        assert redelivered is not None
        assert redelivered.job_id == job.job_id

    def test_delayed_enqueue_is_not_immediately_visible(self, queue, make_flow_job):
        queue.enqueue(make_flow_job(), delay=60)
        assert queue.dequeue() is None
        assert queue.get_queue_size() == 1  # still queued, just not visible

    def test_zero_delay_is_immediately_visible(self, queue, make_flow_job):
        queue.enqueue(make_flow_job(), delay=0)
        assert queue.dequeue() is not None


class TestAck:
    def test_ack_removes_job_permanently(self, queue, make_flow_job):
        queue.enqueue(make_flow_job())
        job = queue.dequeue(timeout=0)
        queue.ack(job.job_id)
        assert queue.dequeue() is None

    def test_ack_unknown_job_is_noop(self, queue):
        queue.ack(uuid4())  # must not raise

    def test_queue_size_excludes_in_flight(self, queue, make_flow_job):
        queue.enqueue(make_flow_job())
        queue.enqueue(make_flow_job())
        assert queue.get_queue_size() == 2
        queue.dequeue(timeout=60)
        assert queue.get_queue_size() == 1
