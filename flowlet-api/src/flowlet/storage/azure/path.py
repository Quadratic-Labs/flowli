"""
Pathlib-style interface for Azure Blob Storage.

Provides a Path-like API for working with Azure blobs, similar to pathlib.Path
for local filesystems. Supports blob operations like copy, move, delete, and
directory-like navigation.
"""

from __future__ import annotations
from typing import Optional, Iterator, TYPE_CHECKING, Literal, Any, cast
from pathlib import PurePosixPath

from .file import AzureBlobFile, DEFAULT_CHUNK_SIZE

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient


class AzureBlobPath:
    """
    Path-like interface for Azure Blob Storage.

    Similar to pathlib.Path but for Azure blobs. Supports navigation,
    file operations, and blob management with shared client configuration.

    Example:
        # Create a path with connection string
        root = AzureBlobPath.from_connection_string(
            connection_string,
            container='my-container'
        )

        # Navigate like pathlib
        data_dir = root / 'data'
        file_path = data_dir / 'file.txt'

        # File operations
        file_path.write_text('Hello, Azure!')
        content = file_path.read_text()

        # Blob operations
        file_path.copy_to(root / 'backup' / 'file.txt')
        file_path.move_to(root / 'archive' / 'file.txt')
        file_path.delete()

        # List blobs
        for blob in data_dir.iterdir():
            print(blob.name)
    """

    def __init__(
        self,
        container_name: str,
        blob_path: str = '',
        blob_service_client: Optional['BlobServiceClient'] = None,
        connection_string: Optional[str] = None
    ):
        """
        Initialize Azure Blob path.

        Args:
            container_name: Name of the blob container
            blob_path: Path within the container (blob name/prefix)
            blob_service_client: Shared BlobServiceClient instance
            connection_string: Azure Storage connection string (if no client provided)
        """
        from azure.storage.blob import BlobServiceClient

        self.container_name = container_name
        self._blob_path = blob_path.strip('/')  # Normalize path

        # Shared client configuration
        if blob_service_client is not None:
            self._blob_service_client = blob_service_client
            self._connection_string: Optional[str] = None
        elif connection_string is not None:
            self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self._connection_string = connection_string
        else:
            raise ValueError("Either blob_service_client or connection_string must be provided")

        # Lazy-loaded clients
        self._container_client: Optional['ContainerClient'] = None
        self._blob_client: Optional['BlobClient'] = None

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        container: str,
        path: str = ''
    ) -> 'AzureBlobPath':
        """
        Create an AzureBlobPath from a connection string.

        Args:
            connection_string: Azure Storage connection string
            container: Container name
            path: Optional blob path/prefix

        Returns:
            AzureBlobPath instance
        """
        return cls(
            container_name=container,
            blob_path=path,
            connection_string=connection_string
        )

    @classmethod
    def from_service_client(
        cls,
        blob_service_client: 'BlobServiceClient',
        container: str,
        path: str = ''
    ) -> 'AzureBlobPath':
        """
        Create an AzureBlobPath from an existing BlobServiceClient.

        Args:
            blob_service_client: BlobServiceClient instance
            container: Container name
            path: Optional blob path/prefix

        Returns:
            AzureBlobPath instance
        """
        return cls(
            container_name=container,
            blob_path=path,
            blob_service_client=blob_service_client
        )

    @property
    def _container(self) -> 'ContainerClient':
        """Get or create container client (lazy-loaded and cached)."""
        if self._container_client is None:
            self._container_client = self._blob_service_client.get_container_client(self.container_name)
        return self._container_client

    @property
    def _blob(self) -> 'BlobClient':
        """Get or create blob client (lazy-loaded and cached)."""
        if self._blob_client is None:
            self._blob_client = self._container.get_blob_client(self._blob_path)
        return self._blob_client

    def __truediv__(self, other: str | AzureBlobPath) -> AzureBlobPath:
        """
        Join paths using the / operator.

        Args:
            other: Path component to append

        Returns:
            New AzureBlobPath instance
        """
        if isinstance(other, AzureBlobPath):
            if other.container_name != self.container_name:
                raise ValueError("Cannot join paths from different containers")
            other_path = other._blob_path
        else:
            other_path = str(other)

        # Join paths
        if self._blob_path:
            new_path = f"{self._blob_path}/{other_path}".strip('/')
        else:
            new_path = other_path.strip('/')

        return AzureBlobPath(
            container_name=self.container_name,
            blob_path=new_path,
            blob_service_client=self._blob_service_client
        )

    def __str__(self) -> str:
        """String representation of the path."""
        return f"az://{self.container_name}/{self._blob_path}" if self._blob_path else f"az://{self.container_name}/"

    def __repr__(self) -> str:
        """Developer representation of the path."""
        return f"AzureBlobPath(container='{self.container_name}', path='{self._blob_path}')"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another path."""
        if not isinstance(other, AzureBlobPath):
            return False
        return (
            self.container_name == other.container_name and
            self._blob_path == other._blob_path
        )

    def __hash__(self) -> int:
        """Hash for using paths in sets/dicts."""
        return hash((self.container_name, self._blob_path))

    @property
    def name(self) -> str:
        """The final path component (blob name without directory prefix)."""
        if not self._blob_path:
            return ''
        return self._blob_path.split('/')[-1]

    @property
    def parent(self) -> 'AzureBlobPath':
        """The parent directory path."""
        if not self._blob_path or '/' not in self._blob_path:
            # Parent of root or single-level path is the container root
            return AzureBlobPath(
                container_name=self.container_name,
                blob_path='',
                blob_service_client=self._blob_service_client
            )

        parent_path = '/'.join(self._blob_path.split('/')[:-1])
        return AzureBlobPath(
            container_name=self.container_name,
            blob_path=parent_path,
            blob_service_client=self._blob_service_client
        )

    @property
    def parts(self) -> tuple[str, ...]:
        """Path components as a tuple."""
        if not self._blob_path:
            return (self.container_name,)
        return (self.container_name,) + tuple(self._blob_path.split('/'))

    @property
    def suffix(self) -> str:
        """File extension including the dot."""
        name = self.name
        if '.' not in name:
            return ''
        return '.' + name.rsplit('.', 1)[1]

    @property
    def stem(self) -> str:
        """File name without extension."""
        name = self.name
        if '.' not in name:
            return name
        return name.rsplit('.', 1)[0]

    def as_posix(self) -> str:
        """Return the path as a POSIX-style string."""
        return f"{self.container_name}/{self._blob_path}" if self._blob_path else self.container_name

    def exists(self) -> bool:
        """Check if the blob exists."""
        if not self._blob_path:
            # Check if container exists
            return self._container.exists()

        try:
            self._blob.get_blob_properties()
            return True
        except Exception as e:
            if 'BlobNotFound' in str(type(e).__name__):
                return False
            raise

    def is_file(self) -> bool:
        """
        Check if this is a blob (file).

        Note: In Azure Blob Storage, there's no true directory concept.
        This returns True if a blob exists at this path.
        """
        return self.exists()

    def is_dir(self) -> bool:
        """
        Check if this path represents a directory (prefix with blobs under it).

        Returns True if there are any blobs with this path as a prefix.
        """
        if not self._blob_path:
            # Container root is always a "directory"
            return self._container.exists()

        # Check if any blobs exist with this prefix
        prefix = self._blob_path + '/'
        blobs = self._container.list_blobs(name_starts_with=prefix, results_per_page=1)
        try:
            next(iter(blobs))
            return True
        except StopIteration:
            return False

    def iterdir(self, recursive: bool = False) -> Iterator['AzureBlobPath']:
        """
        Iterate over blobs in this directory.

        Args:
            recursive: If True, recursively list all blobs under this prefix

        Yields:
            AzureBlobPath instances for each blob
        """
        prefix = f"{self._blob_path}/" if self._blob_path else ""

        if recursive:
            # List all blobs under this prefix
            blobs = self._container.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                yield AzureBlobPath(
                    container_name=self.container_name,
                    blob_path=blob.name,
                    blob_service_client=self._blob_service_client
                )
        else:
            # List only immediate children (simulate directory listing)
            blobs = self._container.list_blobs(name_starts_with=prefix)
            seen_prefixes = set()

            for blob in blobs:
                # Remove prefix to get relative path
                relative = blob.name[len(prefix):]

                if '/' in relative:
                    # This is in a subdirectory, yield the subdirectory once
                    subdir = relative.split('/')[0]
                    if subdir not in seen_prefixes:
                        seen_prefixes.add(subdir)
                        yield AzureBlobPath(
                            container_name=self.container_name,
                            blob_path=f"{prefix}{subdir}",
                            blob_service_client=self._blob_service_client
                        )
                else:
                    # This is a direct child blob
                    yield AzureBlobPath(
                        container_name=self.container_name,
                        blob_path=blob.name,
                        blob_service_client=self._blob_service_client
                    )

    def glob(self, pattern: str) -> Iterator['AzureBlobPath']:
        """
        Glob for blobs matching a pattern.

        Args:
            pattern: Glob pattern (e.g., '*.txt', '**/*.json')

        Yields:
            AzureBlobPath instances matching the pattern
        """
        # Use PurePosixPath for pattern matching
        prefix = f"{self._blob_path}/" if self._blob_path else ""

        # List all blobs under this prefix
        blobs = self._container.list_blobs(name_starts_with=prefix)

        for blob in blobs:
            # Get relative path
            relative = blob.name[len(prefix):] if prefix else blob.name

            # Match against pattern
            if PurePosixPath(relative).match(pattern):
                yield AzureBlobPath(
                    container_name=self.container_name,
                    blob_path=blob.name,
                    blob_service_client=self._blob_service_client
                )

    def open(
        self,
        mode: Literal['r', 'w', 'rb', 'wb', 'a', 'ab'] = 'r',
        encoding: Optional[str] = 'utf-8',
        chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> AzureBlobFile:
        """
        Open the blob as a file.

        Args:
            mode: File mode
            encoding: Text encoding (for text modes)
            chunk_size: Chunk size for streaming reads

        Returns:
            AzureBlobFile instance
        """
        if not self._blob_path:
            raise ValueError("Cannot open container root as a file")

        return AzureBlobFile(
            path=self,
            mode=mode,
            encoding=encoding,
            chunk_size=chunk_size
        )

    def read_bytes(self) -> bytes:
        """Read blob contents as bytes."""
        with self.open('rb') as f:
            return cast(bytes, f.read())

    def read_text(self, encoding: str = 'utf-8') -> str:
        """Read blob contents as text."""
        with self.open('r', encoding=encoding) as f:
            return cast(str, f.read())

    def write_bytes(self, data: bytes) -> int:
        """Write bytes to blob."""
        with self.open('wb') as f:
            return f.write(data)

    def write_text(self, text: str, encoding: str = 'utf-8') -> int:
        """Write text to blob."""
        with self.open('w', encoding=encoding) as f:
            return f.write(text)

    def delete(self, missing_ok: bool = False) -> None:
        """
        Delete the blob.

        Args:
            missing_ok: If True, don't raise error if blob doesn't exist
        """
        if not self._blob_path:
            raise ValueError("Cannot delete container root")

        try:
            self._blob.delete_blob()
        except Exception as e:
            if 'BlobNotFound' in str(type(e).__name__):
                if not missing_ok:
                    raise FileNotFoundError(f"Blob '{self._blob_path}' not found")
            else:
                raise

    def unlink(self, missing_ok: bool = False) -> None:
        """Alias for delete() to match pathlib interface."""
        self.delete(missing_ok=missing_ok)

    def copy_to(
        self,
        destination: 'AzureBlobPath',
        overwrite: bool = True
    ) -> 'AzureBlobPath':
        """
        Copy blob to another location.

        Args:
            destination: Destination AzureBlobPath
            overwrite: Whether to overwrite if destination exists

        Returns:
            Destination AzureBlobPath
        """
        if not self._blob_path:
            raise ValueError("Cannot copy container root")

        if not destination._blob_path:
            raise ValueError("Cannot copy to container root")

        # Start copy operation
        source_url = self._blob.url
        destination._blob.start_copy_from_url(source_url)

        # Wait for copy to complete (for same-region copies, usually instant)
        # For large blobs or cross-region, this might take time

        return destination

    def move_to(
        self,
        destination: 'AzureBlobPath',
        overwrite: bool = True
    ) -> 'AzureBlobPath':
        """
        Move blob to another location (copy then delete).

        Args:
            destination: Destination AzureBlobPath
            overwrite: Whether to overwrite if destination exists

        Returns:
            Destination AzureBlobPath
        """
        self.copy_to(destination, overwrite=overwrite)
        self.delete()
        return destination

    def rename(self, new_name: str) -> 'AzureBlobPath':
        """
        Rename the blob (move to same directory with new name).

        Args:
            new_name: New name for the blob

        Returns:
            New AzureBlobPath
        """
        new_path = self.parent / new_name
        return self.move_to(new_path)

    def mkdir(self, parents: bool = True, exist_ok: bool = True) -> None:
        """
        Create a directory (marker blob).

        Note: Azure Blob Storage doesn't have true directories, but we can
        create a zero-byte marker blob to simulate directory creation.

        Args:
            parents: Not used (kept for pathlib compatibility)
            exist_ok: If True, don't raise error if already exists
        """
        if not self._blob_path:
            # Ensure container exists
            try:
                self._container.create_container()
            except Exception as e:
                if 'ContainerAlreadyExists' not in str(type(e).__name__):
                    raise
                if not exist_ok:
                    raise FileExistsError(f"Container '{self.container_name}' already exists")
        else:
            # Create a zero-byte marker blob
            marker_path = f"{self._blob_path}/.marker"
            marker_blob = self._container.get_blob_client(marker_path)
            try:
                marker_blob.upload_blob(b'', overwrite=exist_ok)
            except Exception as e:
                if 'BlobAlreadyExists' in str(type(e).__name__) and not exist_ok:
                    raise FileExistsError(f"Directory marker already exists at '{self._blob_path}'")

    def rmdir(self, recursive: bool = False) -> None:
        """
        Remove directory (delete all blobs with this prefix).

        Args:
            recursive: If True, delete all blobs under this prefix
        """
        if not self._blob_path:
            raise ValueError("Cannot delete container root. Use container client directly.")

        if recursive:
            # Delete all blobs with this prefix
            prefix = f"{self._blob_path}/"
            blobs = self._container.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                self._container.delete_blob(blob.name)

        # Delete marker blob if exists
        marker_path = f"{self._blob_path}/.marker"
        try:
            self._container.delete_blob(marker_path)
        except Exception:
            pass  # Marker might not exist

    def stat(self) -> Any:
        """
        Get blob properties (similar to os.stat).

        Returns:
            BlobProperties object from Azure SDK
        """
        if not self._blob_path:
            raise ValueError("Cannot stat container root")

        return self._blob.get_blob_properties()

    def get_size(self) -> int:
        """Get blob size in bytes."""
        properties = self.stat()
        return properties.size

    def get_content_type(self) -> Optional[str]:
        """Get blob content type."""
        properties = self.stat()
        return properties.content_settings.content_type if properties.content_settings else None

    def set_content_type(self, content_type: str) -> None:
        """Set blob content type."""
        if not self._blob_path:
            raise ValueError("Cannot set content type on container root")

        from azure.storage.blob import ContentSettings
        content_settings = ContentSettings(content_type=content_type)
        self._blob.set_http_headers(content_settings=content_settings)

    def get_metadata(self) -> dict[str, str]:
        """Get blob metadata."""
        properties = self.stat()
        return properties.metadata or {}

    def set_metadata(self, metadata: dict[str, str]) -> None:
        """Set blob metadata."""
        if not self._blob_path:
            raise ValueError("Cannot set metadata on container root")

        self._blob.set_blob_metadata(metadata)
