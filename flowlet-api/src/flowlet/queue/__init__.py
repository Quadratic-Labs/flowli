"""
Job queue implementations for asynchronous flow execution.

The queue is a pure wake-up mechanism: it distributes FlowJob messages to
workers, nothing more.  Ownership, retry accounting, and failure handling
all live in the state store (ObligationSummary + CAS writes); workers ack a message
as soon as the obligation's state is resolved, and duplicate deliveries are dropped
by the worker state machine.

Available implementations:
- AzureQueueStorage: Azure Queue Storage backend for production
- InMemoryQueue: In-memory queue for development and testing
- AccountJobSource: queue-less mode — submissions are recorded directly in
  the account store and workers poll for claimable obligations
"""
from typing import Protocol
from uuid import UUID

from flowlet.queue.config import (
    AccountQueueConfig,
    AzureQueueStorageConfig,
    InMemoryQueueConfig,
    QueueConfig,
)

__all__ = [
    "AccountQueueConfig",
    "AzureQueueStorageConfig",
    "InMemoryQueueConfig",
    "JobQueueProtocol",
    "QueueConfig",
]


class JobQueueProtocol(Protocol):
    """
    Protocol for job queue implementations.

    Defines the minimal wake-up interface: enqueue a job (optionally
    delayed), dequeue one for processing, and ack it once the obligation's state
    has been resolved.

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
        >>> # resolve state, claim via CAS ...
        >>> queue.ack(job.job_id)
    """

    def enqueue(self, job: 'FlowJob', delay: int = 0) -> UUID:
        """
        Add a job to the queue.

        Args:
            job: Flow job to enqueue with flow name, arguments, and metadata.
            delay: Seconds before the message becomes visible to workers.
                Used for retry backoff; 0 means immediately visible.

        Returns:
            UUID: The job_id of the enqueued job (same as job.job_id).

        Raises:
            RuntimeError: If queue is full or unavailable.
        """
        ...

    def dequeue(self, timeout: int | None = None) -> 'FlowJob | None':
        """
        Get the next visible job from the queue.

        The message becomes invisible to other workers for a short claim
        window; the worker is expected to resolve the obligation's state and ack
        well within it.  An unacked message (crashed worker) simply becomes
        visible again — the state machine makes redelivery harmless.

        Args:
            timeout: Claim window in seconds. If None, uses the queue's
                default.

        Returns:
            FlowJob if available, None if the queue is empty.
        """
        ...

    def ack(self, job_id: UUID) -> None:
        """
        Acknowledge a message, permanently removing it from the queue.

        Called as soon as the obligation's state has been resolved (claimed,
        or recognised as closed/busy) — not after execution.

        Args:
            job_id: Unique identifier of the job to acknowledge.
        """
        ...

    def get_queue_size(self) -> int:
        """
        Get the approximate number of visible jobs in the queue.

        Returns:
            int: Approximate queue depth. May be slightly inaccurate in
                distributed queue implementations.
        """
        ...
