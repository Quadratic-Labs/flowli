"""Configuration and dependencies for Flowlet framework.

This module defines the main configuration schema for Flowlet using Pydantic,
allowing configuration via Python objects, dictionaries, or environment variables.
"""
from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field

# from .database import DatabaseSettings


class FilesystemStorageConfig(BaseModel):
    """Configuration for filesystem-based storage.

    Attributes:
        type: Storage type identifier, always "filesystem".
        base_path: Base directory path for storing logs and runs.

    Example:
        >>> config = FilesystemStorageConfig(base_path="./storage")
    """
    type: Literal["filesystem"] = "filesystem"
    base_path: str = Field(
        default="./storage",
        description="Base directory path for storing logs and runs"
    )


class AzureBlobStorageConfig(BaseModel):
    """Configuration for Azure Blob Storage.

    Attributes:
        type: Storage type identifier, always "azure_blob".
        connection_string: Azure Storage connection string.
        container_name: Name of the blob container for logs.
        base_path: Optional base path/prefix within the container.

    Example:
        >>> config = AzureBlobStorageConfig(
        ...     connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        ...     container_name='flowlet-logs'
        ... )
    """
    type: Literal["azure_blob"] = "azure_blob"
    connection_string: str = Field(
        description="Azure Storage connection string"
    )
    container_name: str = Field(
        default="flowlet-logs",
        description="Name of the blob container"
    )
    base_path: str = Field(
        default="",
        description="Optional base path/prefix within the container"
    )


class SQLiteStorageConfig(BaseModel):
    """Configuration for SQLite database storage.

    Attributes:
        type: Storage type identifier, always "sqlite".
        database_path: Path to the SQLite database file.
        table_name: Name of the table to store logs.

    Example:
        >>> config = SQLiteStorageConfig(database_path="./flowlet.db")
    """
    type: Literal["sqlite"] = "sqlite"
    database_path: str = Field(
        default="./flowlet.db",
        description="Path to the SQLite database file"
    )
    logs_table_name: str = Field(
        default="logs",
        description="Name of the table for storing logs"
    )
    runs_table_name: str = Field(
        default="runs",
        description="Name of the table for storing run summaries"
    )


class FlowletConfig(BaseSettings):
    """Configuration schema for Flowlet framework.

    Centralizes all framework settings including database and storage configuration.
    Can be loaded from dictionaries or environment variables with FLOWLET_ prefix.

    Attributes:
        database: Database connection and settings for flow run tracking.
        storage: Storage configuration for logs and runs (filesystem, Azure Blob, or SQLite).

    Example:
        >>> # From dict with filesystem storage
        >>> config = FlowletConfig(storage={"type": "filesystem", "base_path": "./storage"})
        >>>
        >>> # With Azure Blob storage
        >>> config = FlowletConfig(storage={
        ...     "type": "azure_blob",
        ...     "connection_string": "...",
        ...     "container_name": "logs"
        ... })
        >>>
        >>> # With SQLite storage
        >>> config = FlowletConfig(storage={"type": "sqlite", "database_path": "./flowlet.db"})
        >>>
        >>> # From environment variables
        >>> # Set FLOWLET_STORAGE__TYPE=filesystem
        >>> # Set FLOWLET_STORAGE__BASE_PATH=./storage
        >>> config = FlowletConfig()
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_',
        env_nested_delimiter='__',
        arbitrary_types_allowed=True
    )

    # database: DatabaseSettings = Field(
    #     default_factory=DatabaseSettings,  # type: ignore
    #     description="Flow runs database settings",
    # )
    storage: Optional[FilesystemStorageConfig | AzureBlobStorageConfig | SQLiteStorageConfig] = Field(
        default=None,
        discriminator="type",
        description="Storage configuration for logs and runs"
    )