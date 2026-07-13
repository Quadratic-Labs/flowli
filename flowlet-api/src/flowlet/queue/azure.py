"""Azure Queue Storage implementation for job queue.

Provides production-ready queue backend using Azure Queue Storage.
Messages are JSON-serialized FlowJob objects used purely as wake-up
signals; ownership and retries live in the state store.
"""
from typing import TYPE_CHECKING
from uuid import UUID
from attrs import define, field

from ..models import FlowJob
from ..serdes import from_json, to_json
from .config import AzureQueueStorageConfig

if TYPE_CHECKING:
    from azure.storage.queue import QueueClient, QueueMessage


@define
class _MessageMetadata:
    """Internal metadata for tracking Azure Queue messages.

    Stores the message_id and pop_receipt needed to delete the message
    on ack.
    """
    message_id: str
    pop_receipt: str


@define
class AzureQueueStorage:
    """Azure Queue Storage implementation of JobQueueProtocol.

    Uses Azure Queue Storage for reliable, distributed job queuing.
    Messages are JSON-serialized FlowJob objects.

    Attributes:
        config: Azure Queue Storage configuration with connection details.

    Example:
        >>> config = AzureQueueStorageConfig(
        ...     connection_string="DefaultEndpointsProtocol=https;...",
        ...     queue_name="flowlet-jobs"
        ... )
        >>> queue = AzureQueueStorage.setup(config)
        >>> job_id = queue.enqueue(FlowJob(flow_name="process_data"))
    """
    config: AzureQueueStorageConfig
    _message_cache: dict[UUID, _MessageMetadata] = field(factory=dict, init=False)

    @classmethod
    def setup(cls, config: AzureQueueStorageConfig) -> AzureQueueStorage:
        """Factory method for dependency injection integration.

        Args:
            config: Azure Queue Storage configuration.

        Returns:
            AzureQueueStorage instance ready for use.
        """
        return cls(config=config)

    def enqueue(self, job: FlowJob, delay: int = 0) -> UUID:
        """Add a job to the Azure queue, optionally delayed.

        Args:
            job: Flow job to enqueue.
            delay: Seconds before the message becomes visible to workers
                (Azure send-time visibility timeout). 0 means immediately.

        Returns:
            UUID: The job_id of the enqueued job.
        """
        self.config.queue_client.send_message(
            to_json(job),
            time_to_live=self.config.message_ttl,
            visibility_timeout=delay or None,
        )
        return job.job_id

    def dequeue(self, timeout: int | None = None) -> FlowJob | None:
        """Get the next visible message with a short claim window.

        Args:
            timeout: Claim window in seconds. If None, uses the queue's
                configured visibility_timeout.

        Returns:
            FlowJob if available, None if the queue is empty.
        """
        visibility_timeout = timeout or self.config.visibility_timeout

        messages = self.config.queue_client.receive_messages(
            messages_per_page=1,
            visibility_timeout=visibility_timeout,
        )

        for message in messages:
            job = from_json(FlowJob)(message.content)
            self._message_cache[job.job_id] = _MessageMetadata(
                message_id=message.id,
                pop_receipt=message.pop_receipt,
            )
            return job

        return None

    def ack(self, job_id: UUID) -> None:
        """Acknowledge a message, permanently deleting it from the queue.

        No-op when the job is not in the local cache (already acked, or the
        claim window expired and the receipt is gone — the redelivered
        message will be acked by whoever dequeues it next).

        Args:
            job_id: Unique identifier of the job to acknowledge.
        """
        metadata = self._message_cache.pop(job_id, None)
        if metadata is None:
            return
        self.config.queue_client.delete_message(
            message=metadata.message_id,
            pop_receipt=metadata.pop_receipt,
        )

    def get_queue_size(self) -> int:
        """Get approximate queue depth.

        Returns:
            int: Approximate number of messages in the queue.
        """
        properties = self.config.queue_client.get_queue_properties()
        return properties.approximate_message_count
