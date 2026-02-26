"""Azure Queue Storage implementation for job queue.

Provides production-ready queue backend using Azure Queue Storage
with support for visibility timeouts, TTL, and reliable message delivery.
"""
import json
from datetime import datetime
from uuid import UUID
from attrs import define, field

from ..models import FlowJob
from .config import AzureQueueStorageConfig


@define
class _MessageMetadata:
    """Internal metadata for tracking Azure Queue messages.

    Stores the message_id, pop_receipt, and job data needed for ack/nack operations.
    """
    message_id: str
    pop_receipt: str
    job: FlowJob


@define
class AzureQueueStorage:
    """Azure Queue Storage implementation of JobQueueProtocol.

    Uses Azure Queue Storage for reliable, distributed job queuing.
    Messages are JSON-serialized FlowJob objects.

    Features:
    - Automatic queue creation
    - Configurable visibility timeout for in-flight messages
    - Message TTL to prevent stale jobs
    - Thread-safe (Azure SDK handles concurrency)

    Attributes:
        config: Azure Queue Storage configuration with connection details.

    Example:
        >>> from flowlet.queue.config import AzureQueueStorageConfig
        >>> from flowlet.models import FlowJob
        >>>
        >>> config = AzureQueueStorageConfig(
        ...     connection_string="DefaultEndpointsProtocol=https;...",
        ...     queue_name="flowlet-jobs"
        ... )
        >>> queue = AzureQueueStorage.setup(config)
        >>>
        >>> job = FlowJob(flow_name="process_data", kwargs={"file": "data.csv"})
        >>> job_id = queue.enqueue(job)
    """
    config: AzureQueueStorageConfig
    _message_cache: dict[UUID, _MessageMetadata] = field(factory=dict, init=False)

    @classmethod
    def setup(cls, config: AzureQueueStorageConfig) -> 'AzureQueueStorage':
        """Factory method for dependency injection integration.

        Args:
            config: Azure Queue Storage configuration.

        Returns:
            AzureQueueStorage instance ready for use.

        Example:
            >>> config = AzureQueueStorageConfig(connection_string="...", queue_name="jobs")
            >>> queue = AzureQueueStorage.setup(config)
        """
        return cls(config=config)

    def enqueue(self, job: FlowJob) -> UUID:
        """Add job to Azure queue.

        Serializes the FlowJob to JSON and sends it to Azure Queue Storage.
        Returns immediately after successful enqueue.

        Args:
            job: Flow job to enqueue with flow name and arguments.

        Returns:
            UUID: The job_id of the enqueued job.

        Raises:
            Exception: If Azure Queue operation fails.

        Example:
            >>> job = FlowJob(flow_name="my_flow", kwargs={"x": 1})
            >>> job_id = queue.enqueue(job)
        """
        # Serialize job to JSON
        job_dict = {
            'job_id': str(job.job_id),
            'run_id': str(job.run_id),
            'flow_name': job.flow_name,
            'kwargs': job.kwargs,
            'submitted_at': job.submitted_at.isoformat(),
            'retry_count': job.retry_count,
            'max_retries': job.max_retries,
            'visibility_timeout': job.visibility_timeout,
        }
        message = json.dumps(job_dict)

        # Send to queue
        self.config.queue_client.send_message(
            message,
            time_to_live=self.config.message_ttl
        )

        return job.job_id

    def dequeue(self, timeout: int | None = None) -> FlowJob | None:
        """Get next job from queue with visibility timeout.

        Atomically retrieves a message and makes it invisible to other workers
        for the duration of the visibility timeout. Worker must call ack() or
        nack() before timeout expires, or the message becomes visible again.

        Args:
            timeout: Visibility timeout in seconds. If None, uses queue's default
                    or job's configured timeout.

        Returns:
            FlowJob if available, None if queue is empty.

        Example:
            >>> job = queue.dequeue(timeout=300)  # 5 minute visibility
            >>> if job:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
        """
        visibility_timeout = timeout or self.config.visibility_timeout

        # Receive one message with visibility timeout
        messages = self.config.queue_client.receive_messages(
            messages_per_page=1,
            visibility_timeout=visibility_timeout
        )

        for message in messages:
            # Deserialize job from JSON
            job_dict = json.loads(message.content)

            job = FlowJob(
                job_id=UUID(job_dict['job_id']),
                run_id=UUID(job_dict['run_id']),
                flow_name=job_dict['flow_name'],
                kwargs=job_dict['kwargs'],
                submitted_at=datetime.fromisoformat(job_dict['submitted_at']),
                retry_count=job_dict['retry_count'],
                max_retries=job_dict['max_retries'],
                visibility_timeout=job_dict['visibility_timeout'],
            )

            # Cache message metadata for ack/nack
            self._message_cache[job.job_id] = _MessageMetadata(
                message_id=message.id,
                pop_receipt=message.pop_receipt,
                job=job
            )

            return job

        return None

    def ack(self, job_id: UUID) -> None:
        """Acknowledge successful job completion.

        Permanently deletes the message from the queue. Call this after
        successfully executing the job.

        Args:
            job_id: Unique identifier of the job to acknowledge.

        Raises:
            KeyError: If job_id not found in message cache (not dequeued by this instance).

        Example:
            >>> job = queue.dequeue()
            >>> try:
            ...     execute_flow(job)
            ...     queue.ack(job.job_id)
            ... except Exception:
            ...     queue.nack(job.job_id)
        """
        metadata = self._message_cache.pop(job_id, None)
        if metadata is None:
            raise KeyError(
                f"Job {job_id} not found in message cache. "
                "Job must be dequeued before it can be acknowledged."
            )

        # Delete message from queue
        self.config.queue_client.delete_message(
            message=metadata.message_id,
            pop_receipt=metadata.pop_receipt
        )

    def nack(self, job_id: UUID, requeue: bool = True) -> None:
        """Reject job, optionally requeueing for retry with exponential backoff.

        If requeue=True and retry count is under max_retries, updates the message
        to become visible again after an exponential backoff delay. If retry limit
        exceeded or requeue=False, deletes the message (sends to dead letter queue).

        Exponential backoff formula: 2^retry_count seconds (1s, 2s, 4s, 8s, 16s...)

        Args:
            job_id: Unique identifier of the job to reject.
            requeue: If True, retry with exponential backoff. If False, discard job.

        Raises:
            KeyError: If job_id not found in message cache.

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
        metadata = self._message_cache.pop(job_id, None)
        if metadata is None:
            raise KeyError(
                f"Job {job_id} not found in message cache. "
                "Job must be dequeued before it can be rejected."
            )

        job = metadata.job
        current_retry_count = job.retry_count
        max_retries = job.max_retries

        if not requeue or current_retry_count >= max_retries:
            # Delete message - either explicitly not requeueing or max retries exceeded
            self.config.queue_client.delete_message(
                message=metadata.message_id,
                pop_receipt=metadata.pop_receipt
            )
            return

        # Increment retry count and rebuild message
        updated_job_dict = {
            'job_id': str(job.job_id),
            'run_id': str(job.run_id),
            'flow_name': job.flow_name,
            'kwargs': job.kwargs,
            'submitted_at': job.submitted_at.isoformat(),
            'retry_count': current_retry_count + 1,
            'max_retries': max_retries,
            'visibility_timeout': job.visibility_timeout,
        }
        updated_message = json.dumps(updated_job_dict)

        # Calculate exponential backoff: 2^retry_count seconds
        backoff_seconds = 2 ** current_retry_count

        # Update message with new content and visibility timeout (exponential backoff)
        self.config.queue_client.update_message(
            message=metadata.message_id,
            pop_receipt=metadata.pop_receipt,
            content=updated_message,
            visibility_timeout=backoff_seconds
        )

    def get_queue_size(self) -> int:
        """Get approximate queue depth.

        Returns:
            int: Approximate number of messages in the queue.

        Note:
            This is an approximate count and may not be exact due to
            Azure's eventual consistency model.

        Example:
            >>> size = queue.get_queue_size()
            >>> print(f"{size} jobs waiting")
        """
        properties = self.config.queue_client.get_queue_properties()
        return properties.approximate_message_count
