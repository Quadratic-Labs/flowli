# region @azure.prelude
# ---
# role: util
# intent: imports and constants
# description:
# rules:
# dependencies:
# aliases:
# triggers:
# ---
"""
Python path and file-like interface for Azure Blob Storage.

Provides a Path-like API for working with Azure blobs, similar to pathlib.Path
for local filesystems. Supports blob operations like copy, move, delete, and
directory-like navigation.

Implements the same interface as for local filesystem files, but for azure
blob storage. This makes it possible to use any of the storage backend
interchangeably.

This implementation supports streaming and partial loading of blobs using
Azure's range-based download capabilities for memory efficiency.

For append mode, uses Azure AppendBlob for efficient appending. Falls back
to buffered mode with warning for BlockBlobs.
"""
import io
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Callable, Iterator, Literal, Any, cast

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient


# Default chunk size for streaming reads (4MB)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Maximum size for append block operation (4MB for AppendBlob)
MAX_APPEND_BLOCK_SIZE = 4 * 1024 * 1024


@dataclass(frozen=True)
class AzureBlobStat:
    """os.stat_result-compatible result for Azure blob properties.

    Satisfies StatLike. Field mapping from BlobProperties:
        st_size  <- properties.size
        st_mtime <- properties.last_modified.timestamp()
        st_ctime <- properties.creation_time.timestamp()  (falls back to st_mtime)
        st_atime <- st_mtime  (Azure has no access-time concept)
    """
    st_size: int
    st_mtime: float
    st_ctime: float
    st_atime: float

# ---
# endregion


# region @azure.file
# ---
# role: util
# intent: file-like interface to azure blob storage
# description:
# rules:
# dependencies:
#   - azure.prelude
# aliases:
# triggers:
# ---

class AzureBlobFile:
    """
    File-like interface for Azure Blob Storage with streaming support.

    Supports reading and writing blobs with a Python file handle interface,
    including context manager support. For reading, uses chunked/streaming
    downloads to avoid loading entire blobs into memory.

    Append Mode:
        - For AppendBlobs: Uses native append_block() for efficient appending
        - For BlockBlobs/PageBlobs: Falls back to degraded mode (read-all, write-all)
          with a warning
        - New blobs are created as AppendBlobs by default in append mode

    Example (path-based API - recommended):
        from flowlet.storage.azure import AzureBlobPath

        # Create a root path
        root = AzureBlobPath.from_connection_string(conn_str, 'my-container')

        # Navigate and write
        file_path = root / 'data' / 'file.txt'
        with AzureBlobFile(file_path, mode='w') as f:
            f.write('Hello, Azure!')

        # Reading from a blob (streams in chunks)
        with AzureBlobFile(file_path, mode='r') as f:
            content = f.read()

        # Efficient appending to AppendBlob
        log_path = root / 'logs' / 'app.log'
        with AzureBlobFile(log_path, mode='a') as f:
            f.write('New log entry\n')  # Uses append_block()

    Legacy Example (still supported):
        # Writing to a blob
        with AzureBlobFile(
            connection_string=conn_str,
            container_name='container',
            blob_name='path/to/file.txt',
            mode='w'
        ) as f:
            f.write('Hello, Azure!')
    """

    def __init__(
        self,
        path: str | AzureBlobPath | None = None,
        mode: Literal['r', 'w', 'rb', 'wb', 'a', 'ab'] = 'r',
        encoding: str | None = 'utf-8',
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        """
        Initialize Azure Blob Storage file handle.

        Args:
            path: AzureBlobPath instance or blob path string (if using legacy params)
            mode: File mode ('r', 'w', 'rb', 'wb', 'a', 'ab')
            encoding: Text encoding for text modes (default: 'utf-8')
            chunk_size: Size of chunks for streaming reads (default: 4MB)

        Examples:
            # New path-based API (recommended)
            from flowlet.storage.azure import AzureBlobPath

            root = AzureBlobPath.from_connection_string(conn_str, 'my-container')
            file_path = root / 'data' / 'file.txt'

            with AzureBlobFile(file_path, mode='w') as f:
                f.write('Hello!')

            # Legacy API (still supported for backward compatibility)
            with AzureBlobFile(
                connection_string=conn_str,
                container_name='my-container',
                blob_name='data/file.txt',
                mode='w'
            ) as f:
                f.write('Hello!')
        """
        if BlobServiceClient is None:
            raise ImportError(
                "azure-storage-blob is required for AzureBlobFile. "
                "Install it with: pip install azure-storage-blob"
            )

        if not isinstance(path, AzureBlobPath):
            raise TypeError(f"path must be an AzureBlobPath instance, got {type(path).__name__}")

        if not path._blob_path:
            raise ValueError("Cannot open container root as a file")

        self.path = path
        self.mode = mode
        self.encoding = encoding if 'b' not in mode else None
        self.chunk_size = chunk_size

        # For write modes: buffer all writes locally
        self._write_buffer: io.BytesIO | None = None

        # For read modes: streaming state
        self._blob_size: int | None = None
        self._position: int = 0  # Current logical position in the blob
        self._read_buffer: bytes = b''  # Buffer for partial chunk data
        self._read_buffer_offset: int = 0  # Offset of read_buffer in the blob
        self._closed = False

        # For append mode: track blob type and append strategy
        self._is_append_blob: bool = False
        self._append_buffer: bytes = b''  # Buffer for append operations
        self._degraded_append_mode: bool = False

        # Initialize based on mode
        if 'r' in mode:
            self._init_read_mode()
        elif 'w' in mode:
            self._write_buffer = io.BytesIO()
        elif 'a' in mode:
            self._init_append_mode()

    def _init_read_mode(self) -> None:
        """Initialize read mode by fetching blob metadata."""
        try:
            properties = self.path._blob.get_blob_properties()
            self._blob_size = properties.size
            self._position = 0
        except Exception as e:
            if 'BlobNotFound' in str(type(e).__name__):
                raise FileNotFoundError(f"Blob '{self.path}' not found")
            raise

    def _init_append_mode(self) -> None:
        """
        Initialize append mode.

        Uses AppendBlob for efficient appending if possible.
        Falls back to buffered mode for BlockBlobs with a warning.
        """
        try:
            # Check if blob exists and get its type
            properties = self.path._blob.get_blob_properties()
            blob_type = properties.blob_type

            if blob_type == 'AppendBlob':
                # Use native append blob operations
                self._is_append_blob = True
                self._blob_size = properties.size
                self._position = properties.size
            else:
                # BlockBlob or PageBlob - use degraded mode
                warnings.warn(
                    f"Blob '{self.path}' is a {blob_type}, not an AppendBlob. "
                    f"Append operations will use degraded mode (read-all, write-all). "
                    f"For efficient appending, create the blob as an AppendBlob first.",
                    UserWarning,
                    stacklevel=3
                )
                self._degraded_append_mode = True
                # Download entire blob for modification
                blob_data = self.path._blob.download_blob()
                content = blob_data.readall()
                self._write_buffer = io.BytesIO(content)
                self._write_buffer.seek(0, io.SEEK_END)

        except Exception as e:
            if 'BlobNotFound' in str(type(e).__name__):
                # Blob doesn't exist - create as AppendBlob
                try:
                    self.path._blob.create_append_blob()
                    self._is_append_blob = True
                    self._blob_size = 0
                    self._position = 0
                except Exception:
                    # If append blob creation fails, fall back to degraded mode
                    warnings.warn(
                        f"Could not create AppendBlob '{self.path}'. "
                        f"Using degraded append mode.",
                        UserWarning,
                        stacklevel=3
                    )
                    self._degraded_append_mode = True
                    self._write_buffer = io.BytesIO()
            else:
                raise

    def _ensure_buffer_contains(self, start: int, length: int) -> None:
        """
        Ensure read buffer contains data from start to start+length.

        Downloads a chunk from Azure if needed.
        """
        if self._blob_size is None:
            return

        buffer_end = self._read_buffer_offset + len(self._read_buffer)

        # Check if requested range is already in buffer
        if (self._read_buffer and
            start >= self._read_buffer_offset and
            start + length <= buffer_end):
            return

        # Download chunk containing the requested position
        download_start = start
        download_length = max(self.chunk_size, length)

        # Don't read past end of blob
        download_length = min(download_length, self._blob_size - download_start)

        if download_length <= 0:
            self._read_buffer = b''
            self._read_buffer_offset = start
            return

        # Download the chunk
        blob_data = self.path._blob.download_blob(
            offset=download_start,
            length=download_length
        )
        self._read_buffer = blob_data.readall()
        self._read_buffer_offset = download_start

    def read(self, size: int = -1) -> str | bytes:
        """
        Read from the blob using streaming chunks.

        Args:
            size: Number of bytes/characters to read (-1 for all)

        Returns:
            Content as string (text mode) or bytes (binary mode)
        """
        self._check_readable()

        if self._blob_size is None:
            return '' if self.encoding else b''

        # Calculate how much to read
        if size == -1:
            size = self._blob_size - self._position
        else:
            size = min(size, self._blob_size - self._position)

        if size <= 0:
            return '' if self.encoding else b''

        # Read in chunks if necessary
        result_parts = []
        remaining = size

        while remaining > 0:
            # Ensure buffer has data at current position
            chunk_size_to_load = min(remaining, self.chunk_size)
            self._ensure_buffer_contains(self._position, chunk_size_to_load)

            if not self._read_buffer:
                break

            # Calculate how much we can read from current buffer
            buffer_position = self._position - self._read_buffer_offset
            available_in_buffer = len(self._read_buffer) - buffer_position
            to_read = min(remaining, available_in_buffer)

            if to_read <= 0:
                break

            # Extract data from buffer
            chunk = self._read_buffer[buffer_position:buffer_position + to_read]
            result_parts.append(chunk)

            self._position += to_read
            remaining -= to_read

        # Combine all chunks
        result = b''.join(result_parts)

        # Decode if in text mode
        if self.encoding:
            return result.decode(self.encoding)
        return result

    def readline(self, size: int = -1) -> str | bytes:
        """
        Read a single line from the blob.

        Args:
            size: Maximum number of bytes/characters to read

        Returns:
            Line as string (text mode) or bytes (binary mode)
        """
        self._check_readable()

        if self._blob_size is None or self._position >= self._blob_size:
            return '' if self.encoding else b''

        # For readline, we need to search for newline character
        # Read in chunks until we find a newline or reach size/end
        result_parts = []
        bytes_read = 0
        max_bytes = size if size > 0 else self._blob_size - self._position

        while bytes_read < max_bytes and self._position < self._blob_size:
            # Load chunk at current position
            chunk_to_load = min(self.chunk_size, max_bytes - bytes_read)
            self._ensure_buffer_contains(self._position, chunk_to_load)

            if not self._read_buffer:
                break

            # Find data in current buffer
            buffer_position = self._position - self._read_buffer_offset
            available = len(self._read_buffer) - buffer_position

            if available <= 0:
                break

            # Search for newline in available data
            to_search = min(available, max_bytes - bytes_read)
            search_data = self._read_buffer[buffer_position:buffer_position + to_search]

            newline_pos = search_data.find(b'\n')

            if newline_pos != -1:
                # Found newline, read up to and including it
                chunk = search_data[:newline_pos + 1]
                result_parts.append(chunk)
                self._position += len(chunk)
                break
            else:
                # No newline, add entire search data
                result_parts.append(search_data)
                self._position += len(search_data)
                bytes_read += len(search_data)

        result = b''.join(result_parts)

        if self.encoding:
            return result.decode(self.encoding)
        return result

    def readlines(self, hint: int = -1) -> list[str] | list[bytes]:
        """
        Read all lines from the blob.

        Args:
            hint: Optional size hint for optimization

        Returns:
            List of lines as strings (text mode) or bytes (binary mode)
        """
        self._check_readable()

        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            if hint > 0 and sum(len(line) for line in lines) >= hint:
                break

        return lines

    def write(self, data: str | bytes) -> int:
        """
        Write data to the blob buffer.

        For append mode with AppendBlob, data is buffered and flushed
        in chunks to Azure. For write mode or degraded append mode,
        data is buffered until close/flush.

        Args:
            data: Data to write (string in text mode, bytes in binary mode)

        Returns:
            Number of bytes/characters written
        """
        self._check_writable()

        # Convert string to bytes if in text mode
        if isinstance(data, str):
            if self.encoding is None:
                raise TypeError("Cannot write string in binary mode")
            bytes_data = data.encode(self.encoding)
        elif isinstance(data, bytes):
            bytes_data = data
        else:
            raise TypeError(f"Expected str or bytes, got {type(data).__name__}")

        if self._is_append_blob:
            # AppendBlob mode: buffer and flush when needed
            self._append_buffer += bytes_data

            # Flush if buffer exceeds max append block size
            if len(self._append_buffer) >= MAX_APPEND_BLOCK_SIZE:
                self._flush_append_buffer()

            return len(bytes_data)
        else:
            # Write or degraded append mode: use write buffer
            if self._write_buffer is None:
                self._write_buffer = io.BytesIO()
            return self._write_buffer.write(bytes_data)

    def writelines(self, lines: list[str] | list[bytes]) -> None:
        """
        Write a list of lines to the blob buffer.

        Args:
            lines: List of lines to write
        """
        for line in lines:
            self.write(line)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Change the stream position.

        Args:
            offset: Position offset
            whence: Reference point (SEEK_SET, SEEK_CUR, SEEK_END)

        Returns:
            New absolute position
        """
        self._check_closed()

        if 'w' in self.mode or 'a' in self.mode:
            # Write mode: use buffer's seek
            if self._write_buffer is None:
                return 0
            return self._write_buffer.seek(offset, whence)
        else:
            # Read mode: calculate new position
            if self._blob_size is None:
                return 0

            if whence == io.SEEK_SET:
                new_position = offset
            elif whence == io.SEEK_CUR:
                new_position = self._position + offset
            elif whence == io.SEEK_END:
                new_position = self._blob_size + offset
            else:
                raise ValueError(f"Invalid whence value: {whence}")

            # Clamp to valid range
            self._position = max(0, min(new_position, self._blob_size))
            return self._position

    def tell(self) -> int:
        """
        Return the current stream position.

        Returns:
            Current position in bytes
        """
        self._check_closed()

        if 'w' in self.mode or 'a' in self.mode:
            if self._write_buffer is None:
                return 0
            return self._write_buffer.tell()
        else:
            return self._position

    def _flush_append_buffer(self) -> None:
        """Flush append buffer to Azure AppendBlob."""
        if not self._append_buffer:
            return

        # Ensure container exists
        try:
            self.path._container.create_container()
        except Exception:
            pass  # Container may already exist

        # Append the data
        self.path._blob.append_block(self._append_buffer)

        # Update position
        if self._blob_size is not None:
            self._blob_size += len(self._append_buffer)
        self._position += len(self._append_buffer)

        # Clear buffer
        self._append_buffer = b''

    def flush(self) -> None:
        """Upload buffer contents to Azure Blob Storage."""
        self._check_closed()

        if 'r' in self.mode and 'w' not in self.mode and 'a' not in self.mode:
            # Read-only mode, nothing to flush
            return

        if self._is_append_blob:
            # Flush any pending append data
            self._flush_append_buffer()
        elif self._write_buffer is not None:
            # Get current position to restore later
            current_position = self._write_buffer.tell()

            # Upload to blob storage
            self._write_buffer.seek(0)
            data = self._write_buffer.read()

            # Ensure container exists
            try:
                self.path._container.create_container()
            except Exception:
                pass  # Container may already exist

            # Upload blob
            self.path._blob.upload_blob(data, overwrite=True)

            # Restore position
            self._write_buffer.seek(current_position)

    def close(self) -> None:
        """Close the file handle and upload any pending data."""
        if self._closed:
            return

        # Flush any pending writes
        if 'w' in self.mode or 'a' in self.mode:
            self.flush()

        # Clean up buffers
        if self._write_buffer is not None:
            self._write_buffer.close()
            self._write_buffer = None

        self._read_buffer = b''
        self._append_buffer = b''
        self._closed = True

    def __enter__(self) -> 'AzureBlobFile':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        _ = exc_type, exc_val, exc_tb
        self.close()

    def __iter__(self) -> 'AzureBlobFile':
        """Return self for iteration support."""
        self._check_readable()
        self.seek(0)
        return self

    def __next__(self) -> str | bytes:
        """Get next line for iteration."""
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def _check_closed(self) -> None:
        """Raise ValueError if the file is closed."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def _check_readable(self) -> None:
        """Raise ValueError if the file is not readable."""
        self._check_closed()
        if 'r' not in self.mode:
            # Note: append mode with AppendBlob supports reading, but degraded mode doesn't
            if 'a' in self.mode and self._is_append_blob:
                # Initialize read state if needed
                if self._blob_size is None:
                    try:
                        properties = self.path._blob.get_blob_properties()
                        self._blob_size = properties.size
                    except Exception:
                        pass
            else:
                raise io.UnsupportedOperation("File not open for reading")

    def _check_writable(self) -> None:
        """Raise ValueError if the file is not writable."""
        self._check_closed()
        if 'w' not in self.mode and 'a' not in self.mode:
            raise io.UnsupportedOperation("File not open for writing")

    @property
    def closed(self) -> bool:
        """Check if the file is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get the blob name."""
        return self.path.name

    def readable(self) -> bool:
        """Check if the file is readable."""
        return 'r' in self.mode or 'a' in self.mode

    def writable(self) -> bool:
        """Check if the file is writable."""
        return 'w' in self.mode or 'a' in self.mode

    def seekable(self) -> bool:
        """Check if the file is seekable."""
        return True

    @property
    def is_append_blob(self) -> bool:
        """Check if using native AppendBlob operations."""
        return self._is_append_blob

    @property
    def is_degraded_append_mode(self) -> bool:
        """Check if using degraded append mode (read-all, write-all)."""
        return self._degraded_append_mode

# ---
# endregion


# region @azure.path
# ---
# role: util
# intent: pathlib-like interface to azure blob storage
# description:
# rules:
# dependencies:
#   - azure.prelude
#   - azure.file
# aliases:
# triggers:
# ---

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
        blob_service_client: BlobServiceClient | None = None,
        connection_string: str | None = None
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
            self._connection_string: str | None = None
        elif connection_string is not None:
            self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self._connection_string = connection_string
        else:
            raise ValueError("Either blob_service_client or connection_string must be provided")

        # Lazy-loaded clients
        self._container_client: ContainerClient | None = None
        self._blob_client: BlobClient | None = None

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

    def relative_to(self, other: 'AzureBlobPath') -> str:
        """Return the blob path relative to *other* as a POSIX string.

        Args:
            other: Ancestor path within the same container.

        Returns:
            Relative path string (e.g. "2024-01-01/file.jsonl").

        Raises:
            ValueError: If paths belong to different containers or *other* is
                not an ancestor of this path.
        """
        if other.container_name != self.container_name:
            raise ValueError(
                f"Paths belong to different containers: "
                f"'{self.container_name}' and '{other.container_name}'"
            )
        if self._blob_path == other._blob_path:
            return ""
        other_prefix = f"{other._blob_path}/" if other._blob_path else ""
        if not self._blob_path.startswith(other_prefix):
            raise ValueError(
                f"'{self._blob_path}' is not relative to '{other._blob_path}'"
            )
        return self._blob_path[len(other_prefix):]

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

    def walk(
        self,
        top_down: bool = True,
        on_error: Callable[[OSError], object] | None = None,
    ) -> Iterator[tuple['AzureBlobPath', list[str], list[str]]]:
        """Walk the blob tree, yielding (dirpath, dirnames, filenames) tuples.

        Mirrors os.walk / pathlib.Path.walk semantics over Azure Blob Storage.
        Because Azure has no real directories, the tree is reconstructed from
        blob path separators.

        Args:
            top_down: If True (default), yield a directory before its children.
                If False, yield children before their parent (bottom-up).
            on_error: Optional callable invoked with the exception if listing
                fails. If None, errors are silently suppressed.

        Yields:
            (dirpath, dirnames, filenames) where dirpath is an AzureBlobPath,
            dirnames is a sorted list of immediate child "directory" names, and
            filenames is a sorted list of blob names in that directory.
        """
        prefix = f"{self._blob_path}/" if self._blob_path else ""
        root = self._blob_path

        try:
            blobs = list(self._container.list_blobs(name_starts_with=prefix))
        except OSError as e:
            if on_error is not None:
                on_error(e)
            return

        # Build virtual tree: blob_path -> (child dir names, file names)
        child_dirs: dict[str, set[str]] = {root: set()}
        child_files: dict[str, list[str]] = {root: []}

        for blob in blobs:
            rel = blob.name[len(prefix):]
            parts = rel.split('/')

            current = root
            for part in parts[:-1]:
                child = f"{current}/{part}" if current else part
                child_dirs[current].add(part)
                if child not in child_dirs:
                    child_dirs[child] = set()
                    child_files[child] = []
                current = child

            if parts[-1]:  # guard against trailing slash
                child_files[current].append(parts[-1])

        def _walk(path: str) -> Iterator[tuple['AzureBlobPath', list[str], list[str]]]:
            dirnames = sorted(child_dirs.get(path, set()))
            filenames = sorted(child_files.get(path, []))
            dir_path = AzureBlobPath(
                container_name=self.container_name,
                blob_path=path,
                blob_service_client=self._blob_service_client,
            )
            if top_down:
                yield dir_path, dirnames, filenames
                for subdir in dirnames:
                    child = f"{path}/{subdir}" if path else subdir
                    yield from _walk(child)
            else:
                for subdir in dirnames:
                    child = f"{path}/{subdir}" if path else subdir
                    yield from _walk(child)
                yield dir_path, dirnames, filenames

        yield from _walk(root)

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
        encoding: str = 'utf-8',
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

        # Wait for copy to complete (for local replication, usually instant)
        # For large blobs or georeplication, this might take time

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

    def stat(self) -> AzureBlobStat:
        """Return os.stat_result-compatible metadata for this blob.

        Returns:
            AzureBlobStat with st_size, st_mtime, st_ctime, st_atime populated
            from BlobProperties.

        Raises:
            ValueError: If called on the container root.
        """
        if not self._blob_path:
            raise ValueError("Cannot stat container root")

        props = self._blob.get_blob_properties()
        mtime = props.last_modified.timestamp() if props.last_modified else 0.0
        ctime = props.creation_time.timestamp() if props.creation_time else mtime
        return AzureBlobStat(
            st_size=props.size,
            st_mtime=mtime,
            st_ctime=ctime,
            st_atime=mtime,
        )

    def get_size(self) -> int:
        """Get blob size in bytes."""
        return self.stat().st_size

    def get_content_type(self) -> str | None:
        """Get blob content type."""
        properties = self._blob.get_blob_properties()
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
        properties = self._blob.get_blob_properties()
        return properties.metadata or {}

    def set_metadata(self, metadata: dict[str, str]) -> None:
        """Set blob metadata."""
        if not self._blob_path:
            raise ValueError("Cannot set metadata on container root")

        self._blob.set_blob_metadata(metadata)

# ---
# endregion
