"""Configuration schema for the Flowlet framework.

Provides FlowletConfig, the single source of application-level settings.
Storage is cairndb's own :class:`~cairndb.storage.config.StorageConfig` —
flowlet defines no storage types of its own: dicts are dispatched through
``StorageConfig.from_dict`` and whatever backends cairndb supports
(filesystem, S3, Azure, GCS, and anything it grows next) are available
verbatim, with cairndb's field names.
"""
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Any

from cairndb.storage.config import StorageConfig
from pydantic import BeforeValidator, Field, InstanceOf
from pydantic_settings import BaseSettings, SettingsConfigDict

from flowlet.queue.config import QueueConfig

if TYPE_CHECKING:
    from cairndb.storage.base import BlobStorage


def _to_storage_config(value: Any) -> Any:
    """Dispatch dict input through cairndb's backend catalog."""
    if isinstance(value, dict):
        return StorageConfig.from_dict(value)
    return value


class FlowletConfig(BaseSettings):
    """Configuration schema for the Flowlet framework.

    Centralises all framework settings. Can be loaded from Python objects
    or dicts; queue settings also come from ``FLOWLET_``-prefixed
    environment variables, storage from ``CAIRNDB_*`` via
    :meth:`cairndb.storage.config.StorageConfig.from_env`.

    Attributes:
        storage: A cairndb StorageConfig (or a ``type``-discriminated dict
            for it), or None for in-memory-only operation (no persistence;
            query endpoints are unavailable).
        queue: Optional queue backend for asynchronous flow submission. When
            absent only synchronous execution via ``POST /execute`` is available.
        history: Record archived runs to the durable history log.
        history_db_path: Local path for the history log's SQLite projection.

    Example:
        >>> # Filesystem storage with in-memory queue
        >>> config = FlowletConfig(
        ...     storage={"type": "filesystem", "path": "./storage"},
        ...     queue={"type": "memory"},
        ... )
        >>>
        >>> # S3 with a prefix
        >>> config = FlowletConfig(
        ...     storage={"type": "s3", "bucket": "flowlet", "prefix": "prod/"},
        ... )
    """

    model_config = SettingsConfigDict(
        env_prefix="FLOWLET_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        arbitrary_types_allowed=True,
    )

    storage: Annotated[
        InstanceOf[StorageConfig], BeforeValidator(_to_storage_config)
    ] | None = Field(
        default=None,
        description=(
            "cairndb storage backend (StorageConfig or its dict form). "
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
    history_db_path: str = Field(
        default="./flowlet_history.sqlite",
        description=(
            "Local filesystem path for the history log's SQLite projection. "
            "Disposable — fully rebuildable by replaying the history log from "
            "scratch — but persisting it lets refresh() replay only the tail "
            "instead of the whole log on every process start."
        ),
    )

    @cached_property
    def store(self) -> "BlobStorage | None":
        """The single cairndb blob store all components share (cached).

        Returns:
            The configured backend's store, or None when no storage backend
            is configured.
        """
        if self.storage is None:
            return None
        return self.storage.create_storage()
