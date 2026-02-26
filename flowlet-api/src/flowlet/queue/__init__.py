"""
Job queue implementations for asynchronous flow execution.

This module provides queue backends for submitting and processing flow jobs.
Supports multiple queue implementations following a common protocol.

Available implementations:
- AzureQueueStorage: Azure Queue Storage backend for production
- InMemoryQueue: In-memory queue for development and testing
"""
from typing import Protocol
from .config import AzureQueueStorageConfig, InMemoryQueueConfig, QueueConfig

__all__ = [
    "AzureQueueStorageConfig",
    "InMemoryQueueConfig",
    "QueueConfig",
    "JobQueueProtocol",
]


class JobQueueProtocol(Protocol):
    """
    Protocol for job queue implementations.

    Defines interface for enqueueing flow execution jobs and
    dequeuing them for processing by workers.

    Implementations must be thread-safe as they may be accessed
    concurrently from multiple API requests or worker threads.

    Example:
        >>> from flowlet.queue.memory import InMemoryQueue
        >>> from flowlet.models import FlowJob
        >>>
        >>> queue = InMemoryQueue.setup(config)
        >>> job = FlowJob(flow_name="my_flow", kwargs={"x": 1})
        >>> job_id = queue.enqueue(job)
        >>>
        >>> # Later, in worker:
        >>> job = queue.dequeue()
        >>> # ... execute job ...
        >>> queue.ack(job.job_id)
    """

    def enqueue(self, job: 'FlowJob') -> UUID:
        """
        Add a job to the queue.

        Serializes and stores the job for later processing by workers.
        Returns immediately after enqueueing.

        Args:
            job: Flow job to enqueue with flow name, arguments, and metadata.

        Returns:
            UUID: The job_id of the enqueued job (same as job.job_id).

        Raises:
            RuntimeError: If queue is full or unavailable.

        Example:
            >>> job = FlowJob(flow_name="process_data", kwargs={"file": "data.csv"})
            >>> job_id = queue.enqueue(job)
        """
        ...

    def dequeue(self, timeout: int | None = None) -> 'FlowJob | None':
        """
        Get next job from queue.

        Retrieves and marks a job as in-flight with visibility timeout.
        Job will be invisible to other workers until timeout expires or
        it's acknowledged/rejected.

        Args:
            timeout: Visibility timeout in seconds. If None, uses queue's
                    default timeout. Job becomes visible again after timeout
                    unless acknowledged.

        Returns:
            FlowJob if available, None if queue is empty.

        Note:
            Job must be acknowledged with ack() on success or nack() on failure
            to prevent reprocessing.

        Example:
            >>> job = queue.dequeue(timeout=300)  # 5 minute visibility
            >>> if job:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
        """
        ...

    def ack(self, job_id: UUID) -> None:
        """
        Acknowledge successful job completion.

        Permanently removes job from queue. Call after successful execution.

        Args:
            job_id: Unique identifier of the job to acknowledge.

        Example:
            >>> job = queue.dequeue()
            >>> try:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
            ... except Exception:
            ...     queue.nack(job.job_id, requeue=True)
        """
        ...

    def nack(self, job_id: UUID, requeue: bool = True) -> None:
        """
        Reject job, optionally requeueing for retry.

        Called when job processing fails. If requeue=True and retry count
        is under max_retries, job is returned to queue with incremented
        retry_count. Otherwise, job is discarded (or sent to dead letter queue).

        Args:
            job_id: Unique identifier of the job to reject.
            requeue: If True, return job to queue for retry (if under max_retries).
                    If False, discard job permanently.

        Example:
            >>> job = queue.dequeue()
            >>> try:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
            ... except TemporaryError:
            ...     queue.nack(job.job_id, requeue=True)  # Retry
            ... except PermanentError:
            ...     queue.nack(job.job_id, requeue=False)  # Discard
        """
        ...

    def get_queue_size(self) -> int:
        """
        Get approximate number of jobs in queue.

        Returns the number of jobs waiting to be processed (not including
        in-flight jobs that are currently being processed).

        Returns:
            int: Approximate queue depth. May be slightly inaccurate in
                distributed queue implementations.

        Note:
            This is an approximate count and may not be exact in distributed
            systems due to eventual consistency.

        Example:
            >>> size = queue.get_queue_size()
            >>> print(f"{size} jobs waiting")
        """
        ...
