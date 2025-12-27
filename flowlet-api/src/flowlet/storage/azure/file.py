"""
Python file-like interface for Azure Blob Storage.

Implements the same interface as for local filesystem Files, but for azure
blob storage. This makes it possible to use any of the storage backend
interchangeably.

This implementation supports streaming and partial loading of blobs using
Azure's range-based download capabilities for memory efficiency.

For append mode, uses Azure AppendBlob for efficient appending. Falls back
to buffered mode with warning for BlockBlobs.
"""
import io
import warnings
from typing import Optional, Literal, TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobType
    from .path import AzureBlobPath
else:
    try:
        from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobType
    except ImportError:
        BlobServiceClient = None  # type: ignore
        BlobClient = None  # type: ignore
        ContainerClient = None  # type: ignore
        BlobType = None  # type: ignore


# Default chunk size for streaming reads (4MB)
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

# Maximum size for append block operation (4MB for AppendBlob)
MAX_APPEND_BLOCK_SIZE = 4 * 1024 * 1024


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
        path: Optional[Union[str, 'AzureBlobPath']] = None,
        mode: Literal['r', 'w', 'rb', 'wb', 'a', 'ab'] = 'r',
        encoding: Optional[str] = 'utf-8',
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
        self._write_buffer: Optional[io.BytesIO] = None

        # For read modes: streaming state
        self._blob_size: Optional[int] = None
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