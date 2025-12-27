"""Configuration and dependencies for Flowlet framework.

This module defines the main configuration schema for Flowlet using Pydantic,
allowing configuration via Python objects, dictionaries, or environment variables.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

from flowlet.storage.config import StorageConfig


class FlowletConfig(BaseSettings):
    """Configuration schema for Flowlet framework.

    Centralizes all framework settings including database and storage configuration.
    Can be loaded from dictionaries or environment variables with FLOWLET_ prefix.

    Attributes:
        database: Database connection and settings for flow run tracking.
        storage: List of storage configurations for logs and runs.
                 Supports multiple exporters (filesystem, Azure Blob, SQLite).

    Example:
        >>> # Single filesystem storage
        >>> config = FlowletConfig(storage=[
        ...     {"type": "filesystem", "base_path": "./storage"}
        ... ])
        >>>
        >>> # Multiple exporters - Azure Blob + SQLite
        >>> config = FlowletConfig(storage=[
        ...     {
        ...         "type": "azure_blob",
        ...         "connection_string": "...",
        ...         "container_name": "logs"
        ...     },
        ...     {
        ...         "type": "sqlite",
        ...         "database_path": "./flowlet.db"
        ...     }
        ... ])
        >>>
        >>> # All three storage backends
        >>> config = FlowletConfig(storage=[
        ...     {"type": "filesystem", "base_path": "./storage"},
        ...     {"type": "azure_blob", "connection_string": "...", "container_name": "logs"},
        ...     {"type": "sqlite", "database_path": "./flowlet.db"}
        ... ])
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