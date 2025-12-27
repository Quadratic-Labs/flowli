from functools import cached_property
from pathlib import Path
from typing import Literal, TYPE_CHECKING, Callable, Union, Annotated

from pydantic import Field, Discriminator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from flowlet.storage.azure.path import AzureBlobPath
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session


class AzureBlobStorageConfig(BaseSettings):
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
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_STORAGE_AZURE_BLOB',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
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
    def azure_path(self) -> AzureBlobPath:
        """
        Build an AzureBlobPath object from the configuration.

        This is computed once and cached for subsequent accesses.

        Returns:
            AzureBlobPath instance configured with connection string,
            container name, and base path.
        """
        from flowlet.storage.azure.path import AzureBlobPath

        return AzureBlobPath.from_connection_string(
            connection_string=self.connection_string,
            container=self.container_name,
            path=self.base_path
        )


class FilesystemStorageConfig(BaseSettings):
    """Configuration for filesystem-based storage.

    Attributes:
        type: Storage type identifier, always "filesystem".
        base_path: Base directory path for storing logs and runs.

    Example:
        >>> config = FilesystemStorageConfig(base_path="./storage")
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_STORAGE_FILESYSTEM',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
    )
    type: Literal["filesystem"] = "filesystem"
    base_path: Path = Field(
        default=Path("./storage"),
        description="Base directory path for storing logs and runs"
    )


class SQLiteStorageConfig(BaseSettings):
    """Configuration for SQLite database storage.

    Attributes:
        type: Storage type identifier, always "sqlite".
        database_path: Path to the SQLite database file.
        table_name: Name of the table to store logs.

    Example:
        >>> config = SQLiteStorageConfig(database_path="./flowlet.db")
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_STORAGE_SQLITE',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
    )
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

    @cached_property
    def engine(self) -> Engine:
        """
        Create and cache a SQLAlchemy engine.

        The engine is thread-safe and uses connection pooling.
        This is computed once and cached for subsequent accesses.

        Returns:
            SQLAlchemy Engine instance.
        """
        from sqlalchemy import create_engine

        # For SQLite, we need to use check_same_thread=False for multi-threading
        # The engine itself is thread-safe via connection pooling
        return create_engine(
            f"sqlite:///{self.database_path}",
            connect_args={"check_same_thread": False},
            echo=False,  # Set to True for SQL query logging
        )

    @cached_property
    def session_factory(self) -> Callable[[], Session]:
        """
        Create and cache a SQLAlchemy session factory (sessionmaker).

        The factory itself is thread-safe. Each call to the factory
        creates a new Session instance that should NOT be shared
        between threads.

        Returns:
            Session factory callable that creates new Session instances.

        Example:
            >>> config = SQLiteStorageConfig(database_path="./db.sqlite")
            >>> # Create a new session (not thread-safe, use per-thread)
            >>> session = config.session_factory()
            >>> try:
            ...     # Use session
            ...     session.query(...)
            ...     session.commit()
            ... finally:
            ...     session.close()
        """
        from sqlalchemy.orm import sessionmaker

        return sessionmaker(bind=self.engine, expire_on_commit=False)

    def create_session(self) -> 'Session':
        """
        Create a new SQLAlchemy session.

        This is a convenience method that uses the cached session_factory.
        Each session should be used in a single thread and properly closed.

        Returns:
            New Session instance.

        Example:
            >>> config = SQLiteStorageConfig(database_path="./db.sqlite")
            >>> with config.create_session() as session:
            ...     session.query(...)
            ...     session.commit()
        """
        return self.session_factory()


# Discriminated union type for all storage configurations
StorageConfig = Annotated[
    Union[
        AzureBlobStorageConfig,
        FilesystemStorageConfig,
        SQLiteStorageConfig,
    ],
    Discriminator('type'),
]