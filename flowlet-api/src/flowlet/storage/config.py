"""Storage backend configuration — a thin envelope over cairndb's.

CairnDB owns the backend catalog (filesystem, S3, Azure, GCS), its
validation, and construction; flowlet adds only the pydantic-settings
envelope and legacy aliases.  Whatever backends cairndb grows next are
available here without changes: the fields below are passed through to
:meth:`cairndb.storage.config.StorageConfig.from_dict`.
"""
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from cairndb.storage.base import BlobStorage
    from cairndb.storage.config import StorageConfig as CairnStorageConfig


# region @storage.config
# ---
# role: storage
# intent: delegate storage settings to cairndb's backend catalog
# description: >
#   StorageConfig collects backend fields (type + the union of cairndb's
#   backend fields) and delegates validation and construction to
#   cairndb.storage.config.StorageConfig.from_dict — flowlet never
#   re-implements a backend.  Legacy flowlet spellings are normalized:
#   type "azure_blob" → "azure", "base_path" → path (filesystem) or prefix
#   (blob backends), "container_name" → container.  .store is the single
#   cached BlobStorage every repository shares.
# rules:
#   - Backend validation and construction MUST live in cairndb; this
#     module only normalizes spellings and caches the store.
#   - Configs MUST expose the backend only as a cairndb BlobStorage.
#   - SQLite MUST NOT appear here; it is the CacheRepository's internal
#     query engine, never a storage backend.
# dependencies:
#   - config
# aliases:
#   - storage-config
# triggers:
#   - how to configure storage
#   - which storage backends are supported
# ---

_TYPE_ALIASES = {"azure_blob": "azure"}


class StorageConfig(BaseSettings):
    """User-facing storage settings, resolved through cairndb.

    Attributes:
        type: Backend discriminator — ``filesystem`` | ``s3`` | ``azure`` |
            ``gcs`` (legacy ``azure_blob`` accepted).
        base_path: Legacy spelling — filesystem root, or key prefix on blob
            backends.  Prefer ``path`` / ``prefix``.
        path: Filesystem root directory.
        prefix: Key prefix inside a bucket/container.
        bucket: S3/GCS bucket.
        region: AWS region.
        endpoint_url: S3-compatible endpoint (MinIO, localstack).
        container: Azure container (legacy ``container_name`` accepted).
        connection_string: Azure connection string.
        account_url: Azure account URL (DefaultAzureCredential auth).
        project: GCP project id.
        credentials_path: GCP service-account key file.

    Example:
        >>> StorageConfig(type="filesystem", base_path="./storage")
        >>> StorageConfig(type="s3", bucket="flowlet", prefix="prod/")
        >>> StorageConfig(type="azure", container="flowlet",
        ...               account_url="https://acct.blob.core.windows.net")
    """

    model_config = SettingsConfigDict(
        env_prefix="FLOWLET_STORAGE_",
        env_nested_delimiter="_",
        arbitrary_types_allowed=True,
        extra="allow",  # forward-compat: new cairndb backend fields pass through
    )

    type: str = "filesystem"
    base_path: str | None = Field(default=None, description="Legacy: path/prefix")
    path: str | None = None
    prefix: str | None = None
    bucket: str | None = None
    region: str | None = None
    endpoint_url: str | None = None
    container: str | None = None
    container_name: str | None = Field(default=None, description="Legacy: container")
    connection_string: str | None = None
    account_url: str | None = None
    project: str | None = None
    credentials_path: str | None = None

    def to_cairndb(self) -> "CairnStorageConfig":
        """Normalize spellings and delegate to cairndb's backend catalog."""
        from cairndb.storage.config import StorageConfig as CairnStorageConfig

        storage_type = _TYPE_ALIASES.get(self.type, self.type)
        data: dict[str, Any] = {
            key: value
            for key, value in self.model_dump().items()
            if value is not None
            and key not in ("type", "base_path", "container_name")
        }
        if self.container_name is not None:
            data.setdefault("container", self.container_name)
        if self.base_path is not None:
            legacy_target = "path" if storage_type == "filesystem" else "prefix"
            data.setdefault(legacy_target, str(self.base_path))
        data["type"] = storage_type
        return CairnStorageConfig.from_dict(data)

    @cached_property
    def store(self) -> "BlobStorage":
        """The cairndb blob store all repositories share (cached).

        Every backend provides the same conditional-write semantics
        (put-if-absent, etag compare-and-swap), so local and cloud
        deployments share one code path.
        """
        return self.to_cairndb().create_storage()


# Backward-compatible aliases: the discriminated union collapsed into one
# delegating class; old imports keep working.
FilesystemStorageConfig = StorageConfig
AzureBlobStorageConfig = StorageConfig

__all__ = [
    "AzureBlobStorageConfig",
    "FilesystemStorageConfig",
    "StorageConfig",
]

# ---
# endregion
