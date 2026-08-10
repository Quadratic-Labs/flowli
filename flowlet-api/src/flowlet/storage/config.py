"""Storage backend configuration.

Each backend config builds a CairnDB :class:`~cairndb.storage.base.BlobStorage`
rooted at the configured location. That store is the single storage and
concurrency primitive of the framework: conditional writes (put-if-absent,
etag compare-and-swap), listing, and reads all go through it, on local
filesystem and blob storage alike.
"""
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Union

from pydantic import Discriminator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from cairndb.storage.base import BlobStorage


# region @storage.config
# ---
# role: storage
# intent: map user-facing storage settings to a cairndb BlobStorage
# description: >
#   AzureBlobStorageConfig and FilesystemStorageConfig each expose .store,
#   a cached cairndb BlobStorage rooted at the configured container/prefix
#   or directory.  All repository-layer I/O — state CAS writes, dispatch
#   claims, span/event appends, the history log — runs on that one store,
#   so backends need nothing beyond what cairndb implements.
# rules:
#   - Configs MUST expose the backend only as a cairndb BlobStorage.
#   - SQLite MUST NOT appear here; it is the CacheRepository's internal
#     query engine, never a storage backend.
# dependencies:
#   - config
# aliases:
#   - storage-config
# triggers:
#   - how to configure storage
#   - azure storage configuration
# ---


class AzureBlobStorageConfig(BaseSettings):
    """Configuration for Azure Blob Storage.

    Attributes:
        type: Storage type identifier, always "azure_blob".
        connection_string: Azure Storage connection string.
        container_name: Name of the blob container.
        base_path: Optional base path/prefix within the container.

    Example:
        >>> config = AzureBlobStorageConfig(
        ...     connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        ...     container_name='flowlet-logs'
        ... )
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_STORAGE_AZURE_BLOB_',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True,
    )

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

    @cached_property
    def store(self) -> "BlobStorage":
        """CairnDB blob store rooted at the container/prefix (cached)."""
        from cairndb.storage.azure import AzureBlobStorage

        return AzureBlobStorage(
            container=self.container_name,
            prefix=self.base_path,
            connection_string=self.connection_string,
        )


class FilesystemStorageConfig(BaseSettings):
    """Configuration for filesystem-based storage.

    Attributes:
        type: Storage type identifier, always "filesystem".
        base_path: Base directory for all framework data.

    Example:
        >>> config = FilesystemStorageConfig(base_path="./storage")
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_STORAGE_FILESYSTEM_',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True,
    )
    type: Literal["filesystem"] = "filesystem"
    base_path: Path = Field(
        default=Path("./storage"),
        description="Base directory path for storing logs and runs"
    )

    @cached_property
    def store(self) -> "BlobStorage":
        """CairnDB filesystem store rooted at base_path (cached).

        The filesystem backend provides the same conditional-write
        semantics as the blob backends (flock-serialised compare-and-swap),
        so local and remote deployments share one code path.
        """
        from cairndb.storage.filesystem import FilesystemStorage

        return FilesystemStorage(self.base_path)


StorageConfig = Annotated[
    Union[
        AzureBlobStorageConfig,
        FilesystemStorageConfig,
    ],
    Discriminator('type'),
]
"""Discriminated union of supported user-facing storage backends."""

# ---
# endregion
