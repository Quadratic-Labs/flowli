"""Configuration and dependencies for Flowlet framework.

This module defines the main configuration schema for Flowlet using Pydantic,
allowing configuration via Python objects, dictionaries, or environment variables.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

from flowlet.storage.config import StorageConfig
from flowlet.queue.config import QueueConfig


class FlowletConfig(BaseSettings):
    """Configuration schema for Flowlet framework.

    Centralizes all framework settings including storage and queue configuration.
    Can be loaded from dictionaries or environment variables with FLOWLET_ prefix.

    Attributes:
        storage: List of storage configurations for logs and runs.
                 Supports multiple exporters (filesystem, Azure Blob, SQLite).
        queue: Optional queue configuration for asynchronous flow execution.
               Supports Azure Queue Storage and in-memory queue.

    Example:
        >>> # Single filesystem storage
        >>> config = FlowletConfig(storage=[
        ...     {"type": "filesystem", "base_path": "./storage"}
        ... ])
        >>>
        >>> # Azure Queue for async execution
        >>> config = FlowletConfig(
        ...     storage=[{"type": "sqlite", "database_path": "./flowlet.db"}],
        ...     queue={"type": "azure_queue", "connection_string": "...", "queue_name": "jobs"}
        ... )
        >>>
        >>> # In-memory queue for development
        >>> config = FlowletConfig(
        ...     storage=[{"type": "filesystem", "base_path": "./storage"}],
        ...     queue={"type": "memory", "max_size": 100}
        ... )
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
    )

    storage: list[StorageConfig] = Field(
        default_factory=list,
        description="List of storage configurations for logs and runs. "
                    "Supports multiple exporters running concurrently."
    )
    queue: QueueConfig | None = Field(
        default=None,
        description="Optional queue configuration for asynchronous execution. "
                    "If not configured, only synchronous execution via /execute is available."
    )
