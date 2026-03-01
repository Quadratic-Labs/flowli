"""Configuration schema for the Flowlet framework.

Provides FlowletConfig, the single source of application-level settings.
Storage and queue backends are each configured once; computed path properties
derive the concrete StoragePath values consumed by the repository layer.
"""
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Union

from pydantic import Field, Discriminator
from pydantic_settings import BaseSettings, SettingsConfigDict

from flowlet.storage.config import AzureBlobStorageConfig, FilesystemStorageConfig
from flowlet.queue.config import QueueConfig

if TYPE_CHECKING:
    from flowlet.storage.types import StoragePath


# region @config
# ---
# role: core
# intent: single source of application-level configuration with derived storage path properties
# description: >
#   FlowletConfig holds a single storage backend (filesystem or Azure, never SQLite
#   directly — SQLite is used internally by SnapshotRepository) and the optional
#   queue backend.  Three read-only properties expose the concrete paths that the
#   repository layer consumes: storage_path, snapshot_storage_path,
#   snapshot_cache_path.
# rules:
#   - storage MUST be a single StorageRoot backend or None; never a list.
#   - SQLite MUST NOT appear in StorageRoot; it is an internal implementation detail.
#   - snapshot_cache_path MUST be None for local filesystem (no download needed).
#   - snapshot_cache_path MUST return a local Path for Azure (required for SQLite).
# dependencies:
#   - storage.config
#   - queue.config
# aliases:
#   - flowlet-config
# triggers:
#   - how to configure flowlet
#   - storage configuration
#   - queue configuration
# ---


StorageRoot = Annotated[
    Union[FilesystemStorageConfig, AzureBlobStorageConfig],
    Discriminator("type"),
]
"""Discriminated union of supported user-facing storage backends.

SQLite is intentionally excluded; it is used internally by SnapshotRepository
as the hot-snapshot engine and is not a user-facing storage option.
"""


class FlowletConfig(BaseSettings):
    """Configuration schema for the Flowlet framework.

    Centralises all framework settings. Can be loaded from Python objects,
    dicts, or environment variables with the ``FLOWLET_`` prefix.

    Attributes:
        storage: Single storage backend for logs and run state files, or None
            for in-memory-only operation (no persistence; query endpoints are
            unavailable).
        queue: Optional queue backend for asynchronous flow submission. When
            absent only synchronous execution via ``POST /execute`` is available.

    Example:
        >>> # Filesystem storage with in-memory queue
        >>> config = FlowletConfig(
        ...     storage={"type": "filesystem", "base_path": "./storage"},
        ...     queue={"type": "memory"},
        ... )
        >>>
        >>> # Azure Blob storage with Azure Queue
        >>> config = FlowletConfig(
        ...     storage={"type": "azure_blob", "connection_string": "...", "container_name": "flowlet"},
        ...     queue={"type": "azure_queue", "connection_string": "...", "queue_name": "jobs"},
        ... )
    """

    model_config = SettingsConfigDict(
        env_prefix="FLOWLET_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        arbitrary_types_allowed=True,
    )

    storage: StorageRoot | None = Field(
        default=None,
        description=(
            "Storage backend for logs and run state files. "
            "When None the framework runs without persistence and "
            "query endpoints are unavailable."
        ),
    )
    queue: QueueConfig | None = Field(
        default=None,
        description=(
            "Queue backend for asynchronous flow submission. "
            "When None only synchronous /execute is available."
        ),
    )

    @property
    def storage_path(self) -> "StoragePath | None":
        """Concrete storage root path derived from the configured backend.

        Returns:
            Path for filesystem storage; AzureBlobPath for Azure storage;
            None when no storage backend is configured.
        """
        if self.storage is None:
            return None
        if isinstance(self.storage, FilesystemStorageConfig):
            return self.storage.base_path
        return self.storage.azure_path  # AzureBlobStorageConfig

    @property
    def snapshot_storage_path(self) -> "StoragePath | None":
        """Storage root for SQLite snapshot files (under ``<root>/snapshots/``).

        Mirrors ``storage_path``; exposed as a dedicated property to allow
        future separation (e.g. a dedicated Azure container for snapshots).

        Returns:
            Same value as ``storage_path``.
        """
        return self.storage_path

    @property
    def snapshot_cache_path(self) -> Path | None:
        """Local filesystem directory for cached snapshot SQLite downloads.

        Azure storage requires downloading remote SQLite files before opening
        them with SQLAlchemy. This property provides a local cache directory
        for that purpose.

        Returns:
            None for local filesystem storage (direct file access, no cache
            needed). A local Path under ``~/.flowlet/snapshot-cache`` for
            Azure storage.
        """
        if self.storage is None or isinstance(self.storage, FilesystemStorageConfig):
            return None
        return Path.home() / ".flowlet" / "snapshot-cache"

# ---
# endregion
