"""In-memory queue implementation for development and testing.

Provides a simple thread-safe queue implementation without external dependencies.
Suitable for local development, testing, and single-process deployments.
"""
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from uuid import UUID
from attrs import define, field

from ..models import FlowJob
from .config import InMemoryQueueConfig


@define
class InMemoryQueue:
    """In-memory queue implementation for development/testing.

    Thread-safe queue using collections.deque and threading.Lock.
    Implements visibility timeout via delayed re-queueing.

    Features:
    - Thread-safe operations with lock-based synchronization
    - Configurable maximum queue size
    - Visibility timeout support (for future worker implementation)
    - No external dependencies

    Attributes:
        config: In-memory queue configuration.

    Note:
        This implementation is not distributed and will not persist
        across process restarts. Use Azure Queue Storage for production.

    Example:
        >>> from flowlet.queue.config import InMemoryQueueConfig
        >>> from flowlet.models import FlowJob
        >>>
        >>> config = InMemoryQueueConfig(max_size=100)
        >>> queue = InMemoryQueue.setup(config)
        >>>
        >>> job = FlowJob(flow_name="process_data", kwargs={"file": "data.csv"})
        >>> job_id = queue.enqueue(job)
    """
    config: InMemoryQueueConfig
    _queue: deque = field(factory=deque, init=False)
    _in_flight: dict[UUID, tuple[FlowJob, datetime]] = field(factory=dict, init=False)
    _lock: threading.Lock = field(factory=threading.Lock, init=False)

    @classmethod
    def setup(cls, config: InMemoryQueueConfig) -> 'InMemoryQueue':
        """Factory method for dependency injection integration.

        Args:
            config: In-memory queue configuration.

        Returns:
            InMemoryQueue instance ready for use.

        Example:
            >>> config = InMemoryQueueConfig(max_size=50)
            >>> queue = InMemoryQueue.setup(config)
        """
        return cls(config=config)

    def enqueue(self, job: FlowJob) -> UUID:
        """Add job to in-memory queue.

        Thread-safe operation with optional size limit enforcement.

        Args:
            job: Flow job to enqueue.

        Returns:
            UUID: The job_id of the enqueued job.

        Raises:
            RuntimeError: If queue is full and max_size is configured.

        Example:
            >>> job = FlowJob(flow_name="my_flow", kwargs={"x": 1})
            >>> job_id = queue.enqueue(job)
        """
        with self._lock:
            if self.config.max_size > 0 and len(self._queue) >= self.config.max_size:
                raise RuntimeError(
                    f"Queue full (max_size={self.config.max_size}). "
                    f"Cannot enqueue job {job.job_id}"
                )
            self._queue.append(job)
        return job.job_id

    def dequeue(self, timeout: int | None = None) -> FlowJob | None:
        """Get next job from queue with visibility timeout.

        Retrieves a job from the queue and marks it as in-flight with a visibility
        timeout. The job becomes invisible to other workers until the timeout expires
        or the job is acknowledged/rejected.

        Args:
            timeout: Visibility timeout in seconds. If None, uses job's configured
                    visibility_timeout.

        Returns:
            FlowJob if available, None if queue is empty.

        Example:
            >>> job = queue.dequeue(timeout=300)  # 5 minute visibility
            >>> if job:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
        """
        # First, requeue any expired in-flight jobs
        self._requeue_expired()

        with self._lock:
            if not self._queue:
                return None

            job = self._queue.popleft()
            visibility_timeout_seconds = timeout or job.visibility_timeout
            visible_at = datetime.now(timezone.utc) + timedelta(seconds=visibility_timeout_seconds)
            self._in_flight[job.job_id] = (job, visible_at)

        return job

    def ack(self, job_id: UUID) -> None:
        """Acknowledge successful job completion.

        Removes job from in-flight tracking, permanently removing it from the queue.

        Args:
            job_id: Unique identifier of the job to acknowledge.

        Example:
            >>> job = queue.dequeue()
            >>> try:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
            ... except Exception:
            ...     queue.nack(job.job_id)
        """
        with self._lock:
            self._in_flight.pop(job_id, None)

    def nack(self, job_id: UUID, requeue: bool = True) -> None:
        """Reject job and optionally requeue with exponential backoff.

        If requeue=True and retry count is under max_retries, increments retry_count
        and returns job to queue with visibility timeout for exponential backoff.
        If retry limit exceeded or requeue=False, discards the job.

        Exponential backoff formula: 2^retry_count seconds (1s, 2s, 4s, 8s, 16s...)

        Args:
            job_id: Unique identifier of the job to reject.
            requeue: If True, retry with exponential backoff. If False, discard job.

        Example:
            >>> job = queue.dequeue()
            >>> try:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
            ... except TemporaryError:
            ...     queue.nack(job.job_id, requeue=True)  # Retry later
            ... except PermanentError:
            ...     queue.nack(job.job_id, requeue=False)  # Discard
        """
        with self._lock:
            if job_id not in self._in_flight:
                return

            job, _ = self._in_flight.pop(job_id)

            if not requeue or job.retry_count >= job.max_retries:
                # Discard job - either explicitly not requeueing or max retries exceeded
                return

            # Increment retry count
            job.retry_count += 1

            # Calculate exponential backoff: 2^retry_count seconds
            backoff_seconds = 2 ** (job.retry_count - 1)  # Use retry_count - 1 since we just incremented
            visible_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)

            # Add back to in-flight with new visibility timeout (will be requeued when expired)
            self._in_flight[job.job_id] = (job, visible_at)

    def get_queue_size(self) -> int:
        """Get current queue size.

        Thread-safe operation that returns the exact number of jobs
        waiting in the queue (not including in-flight jobs).

        Returns:
            int: Number of jobs in the queue.

        Example:
            >>> size = queue.get_queue_size()
            >>> print(f"{size} jobs waiting")
        """
        with self._lock:
            return len(self._queue)

    def _requeue_expired(self) -> None:
        """Requeue jobs whose visibility timeout expired.

        Internal method that moves expired in-flight jobs back to the queue.
        Called automatically by dequeue operations to ensure jobs don't get stuck.

        This implements the visibility timeout behavior: if a worker dequeues a job
        but fails to ack/nack it before the timeout, the job becomes visible again
        and can be processed by another worker.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            expired = [
                job_id for job_id, (_, visible_at) in self._in_flight.items()
                if now >= visible_at
            ]
            for job_id in expired:
                job, _ = self._in_flight.pop(job_id)
                # Return to front of queue for immediate reprocessing
                self._queue.appendleft(job)
