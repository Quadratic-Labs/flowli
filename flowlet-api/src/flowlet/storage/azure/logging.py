"""
Azure Blob Storage logging handler.

Provides a Python logging Handler that exports log records to Azure Blob Storage
using appendable blobs (AppendBlob) for efficient log streaming.
"""

import logging
import threading
from typing import Optional, TYPE_CHECKING, Any, Union, Callable

from .file import AzureBlobFile, MAX_APPEND_BLOCK_SIZE
from .path import AzureBlobPath

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient


class AzureBlobHandler(logging.Handler):
    """
    Logging handler that writes logs to Azure Blob Storage using AppendBlob.

    This handler efficiently appends log records to an Azure AppendBlob,
    making it ideal for centralized log collection and long-term storage.

    Features:
    - Uses AppendBlob for efficient appending without read-modify-write cycles
    - Thread-safe log writing with locks
    - Automatic blob creation if it doesn't exist
    - Supports custom formatters (JSON, text, etc.)
    - Configurable chunk-based buffering (default: 4MB)
    - Static or dynamic path routing (e.g., per run_id)

    Buffering Strategy:
        By default, logs are buffered in memory until the buffer reaches
        chunk_size (default: 4MB), then automatically flushed to Azure. This
        provides an excellent balance between performance and data persistence.

        For real-time streaming, use a smaller chunk_size (e.g., 64KB) or use
        AzureBlobStreamHandler which configures optimal settings automatically.

    Example (static path - recommended):
        from flowlet.storage.azure import AzureBlobPath

        # Create root path
        root = AzureBlobPath.from_connection_string(conn_str, 'logs')

        # Static path with default buffering (4MB chunks)
        handler = AzureBlobHandler(root / 'app.log')
        logger = logging.getLogger('myapp')
        logger.addHandler(handler)
        logger.info('Application started')

        # Clean up
        handler.close()

    Example (dynamic path per run_id):
        # Route logs to different blobs based on run_id
        def route_by_run_id(record: logging.LogRecord) -> str:
            run_id = getattr(record, 'run_id', 'default')
            return f'{run_id}/output.log'

        handler = AzureBlobHandler(root, router=route_by_run_id)
        logger.addHandler(handler)

        # Logs go to different blobs based on run_id
        logger.info('Task started', extra={'run_id': 'run-123'})  # -> logs/run-123/output.log
        logger.info('Task started', extra={'run_id': 'run-456'})  # -> logs/run-456/output.log

    Example (organize by date):
        import datetime
        today = datetime.date.today().isoformat()
        handler = AzureBlobHandler(root / 'myapp' / today / 'app.log')

    Example (real-time streaming with smaller chunks):
        # Flush every 64KB for near real-time visibility
        handler = AzureBlobHandler(root / 'realtime.log', chunk_size=64 * 1024)
    """

    def __init__(
        self,
        path: AzureBlobPath,
        level: int = logging.NOTSET,
        encoding: str = 'utf-8',
        chunk_size: int = MAX_APPEND_BLOCK_SIZE,
        router: Callable[[logging.LogRecord], str] | None = None
    ):
        """
        Initialize Azure Blob Storage logging handler.

        Args:
            path: AzureBlobPath instance where to send logs.
            level: Minimum log level to handle (default: NOTSET)
            encoding: Text encoding for log messages (default: 'utf-8')
            chunk_size: Size of buffer before auto-flush to Azure (default: 4MB).
                Smaller values provide more real-time visibility but more API calls.
                Larger values improve performance but delay log visibility.
            router: Optional function to route records to sub-blobs under path.
                Takes a LogRecord and returns a relative path string.

        Examples:
            # Static path with default buffering (recommended)
            from flowlet.storage.azure import AzureBlobPath

            root = AzureBlobPath.from_connection_string(conn_str, 'logs')
            log_path = root / 'app.log'

            handler = AzureBlobHandler(log_path)
            logger.addHandler(handler)

            # Real-time streaming with smaller chunks
            handler = AzureBlobHandler(log_path, chunk_size=64 * 1024)  # 64KB chunks

            # Dynamic routing based on context (e.g., per run_id)
            def route_by_run_id(record: logging.LogRecord) -> str:
                run_id = getattr(record, 'run_id', 'default')
                return f'{run_id}/output.log'

            handler = AzureBlobHandler(root, router=route_by_run_id)
            logger.addHandler(handler)

            # Now logs with different run_ids go to different blobs
            logger.info('Starting', extra={'run_id': 'run-123'})  # -> logs/run-123/output.log
            logger.info('Processing', extra={'run_id': 'run-456'})  # -> logs/run-456/output.log
        """
        super().__init__(level)
        self.path = path
        self.level = level
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.router = router

        # Thread safety
        self._lock = threading.Lock()
        self._blob_file_cache: dict[str, AzureBlobFile] = {}
        self._closed = False

    def _init_blob_file(self, path: AzureBlobPath) -> AzureBlobFile:
        """
        Initialize an Azure blob file in append mode.

        Args:
            path: AzureBlobPath to open

        Returns:
            Opened AzureBlobFile in append mode
        """
        try:
            return path.open(mode='a', encoding=self.encoding, chunk_size=self.chunk_size)
        except Exception as e:
            # Log initialization error but don't crash
            self.handleError(None)  # type: ignore
            raise RuntimeError(f"Failed to initialize Azure blob file: {e}") from e

    def _get_blob_file(self, record: logging.LogRecord) -> AzureBlobFile:
        """
        Get the appropriate blob file for this log record.

        For static paths, returns the single blob file.
        For dynamic paths, resolves the path and returns cached or new blob file.

        Args:
            record: The log record being emitted

        Returns:
            AzureBlobFile to write to
        """
        this_path = self.path
        if self.router is not None:
            this_path = this_path / self.router(record)

        # Get opened file from cache
        path_key = str(this_path)
        if path_key not in self._blob_file_cache:
            self._blob_file_cache[path_key] = self._init_blob_file(this_path)
        return self._blob_file_cache[path_key]

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the Azure blob.

        The log record is written to the blob file's internal buffer.
        When the buffer reaches chunk_size, it automatically flushes to Azure.

        For dynamic paths, routes the log record to the appropriate blob
        based on the router function.

        Args:
            record: The log record to emit
        """
        if self._closed:
            return

        try:
            # Format the log record
            msg = self.format(record)

            # Ensure message ends with newline
            if not msg.endswith('\n'):
                msg += '\n'

            # Thread-safe write to blob
            # AzureBlobFile will automatically flush when buffer reaches chunk_size
            with self._lock:
                blob_file = self._get_blob_file(record)

                if blob_file and not blob_file.closed:
                    blob_file.write(msg)

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """
        Flush any buffered log records to Azure Blob Storage.

        This forces any pending writes to be uploaded to Azure.
        For dynamic paths, flushes all cached blob files.
        """
        if self._closed:
            return

        try:
            with self._lock:
                # Flush all cached blob files
                for blob_file in self._blob_file_cache.values():
                    if blob_file and not blob_file.closed:
                        blob_file.flush()
        except Exception:
            self.handleError(None)  # type: ignore

    def close(self) -> None:
        """
        Close the handler and upload any pending log data.

        This should be called when the application shuts down to ensure
        all logs are written to Azure. For dynamic paths, closes all
        cached blob files.
        """
        if self._closed:
            return

        try:
            with self._lock:
                for blob_file in self._blob_file_cache.values():
                    if blob_file and not blob_file.closed:
                        blob_file.close()
                self._blob_file_cache.clear()
            self._closed = True
        except Exception:
            self.handleError(None)  # type: ignore
        finally:
            super().close()

    def __enter__(self) -> 'AzureBlobHandler':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        _ = exc_type, exc_val, exc_tb
        self.close()

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed


class AzureBlobStreamHandler(AzureBlobHandler):
    """
    Variant of AzureBlobHandler optimized for real-time log streaming.

    This handler uses a smaller chunk size (64KB) to provide near real-time
    log visibility in Azure, at the cost of more frequent API calls.

    For high-volume logging where performance is critical, use AzureBlobHandler
    with the default 4MB chunk size instead.

    Example:
        from flowlet.storage.azure import AzureBlobPath

        root = AzureBlobPath.from_connection_string(conn_str, 'logs')
        handler = AzureBlobStreamHandler(root / 'realtime.log')
        logger.addHandler(handler)

        # Logs appear in Azure within seconds
        logger.info('This appears quickly in Azure')
    """

    # Default chunk size for streaming: 64KB for near real-time visibility
    DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024

    def __init__(
        self,
        path: AzureBlobPath,
        level: int = logging.NOTSET,
        encoding: str = 'utf-8',
        chunk_size: int | None = None,
        router: Callable[[logging.LogRecord], str] | None = None
    ):
        """
        Initialize Azure Blob Storage streaming handler.

        Args:
            path: AzureBlobPath instance where to send logs
            level: Minimum log level to handle
            encoding: Text encoding for log messages
            chunk_size: Buffer size before flush (default: 64KB for real-time streaming)
            router: Optional function to route records to sub-blobs under path
        """
        super().__init__(
            path=path,
            level=level,
            encoding=encoding,
            chunk_size=chunk_size if chunk_size is not None else self.DEFAULT_STREAM_CHUNK_SIZE,
            router=router,
        )
