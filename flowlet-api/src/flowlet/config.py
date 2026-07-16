"""Configuration schema for the Flowlet framework.

Provides FlowletConfig, the single source of application-level settings.
Storage and queue backends are each configured once; computed path properties
derive the concrete StoragePath values consumed by the repository layer.
"""
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Union

from pydantic import Field, Discriminator
from pydantic_settings import BaseSettings, SettingsConfigDict

from flowlet.storage.config import (
    AzureBlobStorageConfig,
    FilesystemStorageConfig,
    StorageConfig,
)
from flowlet.queue.config import QueueConfig

if TYPE_CHECKING:
    from chroniql.storage import BlobStorage

    from flowlet.storage.types import StoragePath


# region @config
# ---
# role: core
# intent: single source of application-level configuration with derived storage path properties
# description: >
#   FlowletConfig holds a single storage backend (filesystem or Azure, never SQLite
#   directly — SQLite is used internally by the CacheRepository), the optional
#   queue backend, and the history toggle.  Read-only properties expose what the
#   repository layer consumes: storage_path (concrete root path), object_store
#   (chroniql store for remote CAS state writes), and history_store (chroniql
#   store for the run-history event log under <root>/history/).
# rules:
#   - storage MUST be a single StorageRoot backend or None; never a list.
#   - SQLite MUST NOT appear in StorageRoot; it is an internal implementation detail.
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

    storage: StorageConfig | None = Field(
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
    history: bool = Field(
        default=False,
        description=(
            "Record archived runs to a durable ChroniQL event log under "
            "<storage root>/history/ so long-horizon run queries do not "
            "depend on rescanning state files. Requires storage."
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
    def object_store(self) -> "BlobStorage | None":
        """ChroniQL object store for backends that need remote CAS writes.

        Returns:
            A chroniql BlobStorage rooted at the storage location for remote
            backends (Azure); None for local filesystem storage, whose state
            writes use flock-based locking instead.
        """
        if isinstance(self.storage, AzureBlobStorageConfig):
            return self.storage.object_store
        return None

    @property
    def history_store(self) -> "BlobStorage | None":
        """ChroniQL store holding the run-history event log.

        Rooted at ``<storage root>/history/`` — separate from the ``runs/``
        and ``state/`` planes — on the same backend as the main storage.

        Returns:
            A chroniql BlobStorage when ``history`` is enabled and storage is
            configured; None otherwise.
        """
        if not self.history or self.storage is None:
            return None
        if isinstance(self.storage, FilesystemStorageConfig):
            from chroniql.storage.filesystem import FilesystemStorage

            return FilesystemStorage(self.storage.base_path / "history")
        if isinstance(self.storage, AzureBlobStorageConfig):
            from chroniql.storage.azure import AzureBlobStorage

            base = self.storage.base_path.strip("/")
            prefix = f"{base}/history" if base else "history"
            return AzureBlobStorage(
                container=self.storage.container_name,
                prefix=prefix,
                connection_string=self.storage.connection_string,
            )
        return None

# ---
# endregion
