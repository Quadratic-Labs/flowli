"""In-memory queue implementation for development and testing.

Provides a simple thread-safe queue implementation without external dependencies.
Suitable for local development, testing, and single-process deployments.
"""
import threading
from collections import deque
from datetime import UTC, datetime, timedelta
from uuid import UUID

from attrs import define, field

from flowlet.models import FlowJob
from flowlet.queue.config import InMemoryQueueConfig

_DEFAULT_CLAIM_WINDOW = 60  # seconds a dequeued message stays invisible


@define
class InMemoryQueue:
    """In-memory queue implementation for development/testing.

    Thread-safe queue using collections.deque and threading.Lock.
    Messages carry a ``visible_at`` timestamp: enqueue delays and dequeue
    claim windows are both expressed through it.

    Note:
        This implementation is not distributed and will not persist
        across process restarts. Use Azure Queue Storage for production.

    Attributes:
        config: In-memory queue configuration.
    """
    config: InMemoryQueueConfig
    _queue: deque = field(factory=deque, init=False)  # (job, visible_at)
    _in_flight: dict[UUID, tuple[FlowJob, datetime]] = field(factory=dict, init=False)
    _lock: threading.Lock = field(factory=threading.Lock, init=False)

    @classmethod
    def setup(cls, config: InMemoryQueueConfig) -> 'InMemoryQueue':
        """Factory method for dependency injection integration.

        Args:
            config: In-memory queue configuration.

        Returns:
            InMemoryQueue instance ready for use.
        """
        return cls(config=config)

    def enqueue(self, job: FlowJob, delay: int = 0) -> UUID:
        """Add a job to the queue, optionally delayed.

        Args:
            job: Flow job to enqueue.
            delay: Seconds before the message becomes visible to workers.

        Returns:
            UUID: The job_id of the enqueued job.

        Raises:
            RuntimeError: If queue is full and max_size is configured.
        """
        visible_at = datetime.now(UTC) + timedelta(seconds=delay)
        with self._lock:
            if self.config.max_size > 0 and len(self._queue) >= self.config.max_size:
                raise RuntimeError(
                    f"Queue full (max_size={self.config.max_size}). "
                    f"Cannot enqueue job {job.job_id}"
                )
            self._queue.append((job, visible_at))
        return job.job_id

    def dequeue(self, timeout: int | None = None) -> FlowJob | None:
        """Get the next visible job, making it invisible for the claim window.

        Args:
            timeout: Claim window in seconds. If None, uses the default
                (60 s — generous for resolve-and-ack, short enough that a
                crashed worker's message reappears quickly).

        Returns:
            FlowJob if a visible message exists, None otherwise.
        """
        now = datetime.now(UTC)
        claim_window = timeout if timeout is not None else _DEFAULT_CLAIM_WINDOW

        with self._lock:
            self._requeue_expired_locked(now)
            for i, (job, visible_at) in enumerate(self._queue):
                if visible_at <= now:
                    del self._queue[i]
                    self._in_flight[job.job_id] = (
                        job, now + timedelta(seconds=claim_window)
                    )
                    return job
            return None

    def ack(self, job_id: UUID) -> None:
        """Acknowledge a message, permanently removing it.

        Args:
            job_id: Unique identifier of the job to acknowledge.
        """
        with self._lock:
            self._in_flight.pop(job_id, None)

    def get_queue_size(self) -> int:
        """Get the number of queued (visible or delayed) jobs.

        Returns:
            int: Number of jobs in the queue, excluding in-flight ones.
        """
        with self._lock:
            return len(self._queue)

    def _requeue_expired_locked(self, now: datetime) -> None:
        """Return unacked in-flight jobs whose claim window expired.

        Must be called with the lock held.  This is the redelivery path for
        workers that crashed between dequeue and ack; the worker state
        machine makes the duplicate delivery harmless.
        """
        expired = [
            job_id for job_id, (_, visible_at) in self._in_flight.items()
            if now >= visible_at
        ]
        for job_id in expired:
            job, _ = self._in_flight.pop(job_id)
            self._queue.appendleft((job, now))
