"""Configuration schema for the Flowlet framework.

Provides FlowletConfig, the single source of application-level settings.
The storage backend resolves to one CairnDB blob store that every
repository shares; the queue backend is configured separately.
"""
from typing import TYPE_CHECKING

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from flowlet.queue.config import QueueConfig
from flowlet.storage.config import StorageConfig

if TYPE_CHECKING:
    from cairndb.storage.base import BlobStorage


# region @config
# ---
# role: core
# intent: single source of application-level configuration resolving to one blob store
# description: >
#   FlowletConfig holds a single storage backend (filesystem or Azure), the
#   optional queue backend, and the history toggle.  The store property
#   exposes the one cairndb BlobStorage every component consumes: state CAS
#   writes, dispatch claims, span/event streams, and the history commit log
#   (a cairndb named log, logs/history/, on the same store).
# rules:
#   - storage MUST be a single backend or None; never a list.
#   - SQLite MUST NOT appear in StorageConfig; it is internal to the
#     CacheRepository.
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
        storage: Single storage backend for the run record, run state, and
            history, or None for in-memory-only operation (no persistence;
            query endpoints are unavailable).
        queue: Optional queue backend for asynchronous flow submission. When
            absent only synchronous execution via ``POST /execute`` is available.
        history: Record archived runs to the durable history log.

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
            "Storage backend for run records and state. "
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
            "Record archived runs to a durable cairndb commit log "
            "(logs/history/ on the storage backend) so long-horizon run "
            "queries do not depend on rescanning state files. Requires "
            "storage."
        ),
    )

    @property
    def store(self) -> "BlobStorage | None":
        """The single CairnDB blob store all components share.

        Returns:
            The configured backend's store, or None when no storage backend
            is configured.
        """
        if self.storage is None:
            return None
        return self.storage.store

# ---
# endregion
