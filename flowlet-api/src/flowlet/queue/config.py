"""Configuration classes for job queue backends.

Provides Pydantic configuration models for different queue implementations,
following the discriminated union pattern used in storage configuration.
"""
from functools import cached_property
from typing import Literal, TYPE_CHECKING, Union, Annotated

from pydantic import Field, Discriminator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from azure.storage.queue import QueueServiceClient, QueueClient


class AzureQueueStorageConfig(BaseSettings):
    """Configuration for Azure Queue Storage.

    Attributes:
        type: Queue type identifier, always "azure_queue".
        connection_string: Azure Storage connection string.
        queue_name: Name of the queue for flow jobs.
        visibility_timeout: Default visibility timeout in seconds (5 minutes).
        message_ttl: Message time-to-live in seconds (default 7 days).

    Environment Variables:
        FLOWLET_QUEUE_AZURE_CONNECTION_STRING: Azure connection string
        FLOWLET_QUEUE_AZURE_QUEUE_NAME: Queue name (default: "flowlet-jobs")
        FLOWLET_QUEUE_AZURE_VISIBILITY_TIMEOUT: Visibility timeout in seconds
        FLOWLET_QUEUE_AZURE_MESSAGE_TTL: Message TTL in seconds

    Example:
        >>> config = AzureQueueStorageConfig(
        ...     connection_string="DefaultEndpointsProtocol=https;...",
        ...     queue_name="flowlet-jobs"
        ... )
        >>> queue_client = config.queue_client
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_QUEUE_AZURE_',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
    )

    type: Literal["azure_queue"] = "azure_queue"
    connection_string: str = Field(
        description="Azure Storage connection string"
    )
    queue_name: str = Field(
        default="flowlet-jobs",
        description="Name of the queue"
    )
    visibility_timeout: int = Field(
        default=300,
        description="Visibility timeout in seconds (default 5 min)"
    )
    message_ttl: int = Field(
        default=604800,
        description="Message TTL in seconds (default 7 days)"
    )

    @cached_property
    def queue_service_client(self) -> 'QueueServiceClient':
        """Create and cache Azure Queue Service Client.

        The client is created once and reused for all queue operations.

        Returns:
            QueueServiceClient instance configured with connection string.
        """
        from azure.storage.queue import QueueServiceClient
        return QueueServiceClient.from_connection_string(
            self.connection_string
        )

    @cached_property
    def queue_client(self) -> 'QueueClient':
        """Get or create queue client (lazy-loaded and cached).

        Automatically creates the queue if it doesn't exist.

        Returns:
            QueueClient instance for the configured queue.

        Raises:
            Exception: If queue creation fails (except for AlreadyExists).
        """
        client = self.queue_service_client.get_queue_client(self.queue_name)
        # Ensure queue exists
        try:
            client.create_queue()
        except Exception as e:
            # Ignore if queue already exists
            if 'QueueAlreadyExists' not in str(type(e).__name__):
                raise
        return client


class InMemoryQueueConfig(BaseSettings):
    """Configuration for in-memory queue (development/testing).

    Provides a simple in-memory queue implementation for local development
    and testing without requiring external dependencies.

    Attributes:
        type: Queue type identifier, always "memory".
        max_size: Maximum queue size (0 = unlimited).

    Environment Variables:
        FLOWLET_QUEUE_MEMORY_MAX_SIZE: Maximum queue size (default: 0 = unlimited)

    Example:
        >>> config = InMemoryQueueConfig(max_size=100)
        >>> # Queue will reject enqueue operations when full
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_QUEUE_MEMORY_',
        env_nested_delimiter='_',
        env_nested_max_split=1
    )

    type: Literal["memory"] = "memory"
    max_size: int = Field(
        default=0,
        description="Maximum queue size (0 = unlimited)"
    )


# Discriminated union for queue configurations
QueueConfig = Annotated[
    Union[AzureQueueStorageConfig, InMemoryQueueConfig],
    Discriminator('type'),
]
"""Discriminated union type for all queue configurations.

Supports multiple queue backend types selected via the 'type' field.
Pydantic will automatically validate and parse the correct config type.

Example:
    >>> # Azure Queue
    >>> config: QueueConfig = {
    ...     "type": "azure_queue",
    ...     "connection_string": "...",
    ...     "queue_name": "my-queue"
    ... }
    >>>
    >>> # In-Memory Queue
    >>> config: QueueConfig = {
    ...     "type": "memory",
    ...     "max_size": 100
    ... }
"""
