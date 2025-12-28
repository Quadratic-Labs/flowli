"""
Filesystem logging handler for Flowlet.

Provides Python logging Handlers that export log records to the local filesystem
or Azure Blob Storage, organizing logs by run ID or custom routing logic.
"""

import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypeAlias

if TYPE_CHECKING:
    from flowlet.storage.azure.path import AzureBlobPath
    PathLike: TypeAlias = Path | AzureBlobPath


class FilesystemHandler(logging.Handler):
    """
    Logging handler that writes logs to filesystem or Azure Blob Storage.

    This handler writes log records to files, with support for dynamic routing
    to organize logs by run ID or other attributes. Works seamlessly with both
    local filesystem paths (Path) and Azure Blob Storage paths (AzureBlobPath).

    Features:
    - Works with both Path and AzureBlobPath
    - Automatic directory creation
    - Thread-safe file writing with locks
    - Supports custom formatters (JSON, text, etc.)
    - Dynamic routing via router function
    - Backward compatible with run_id attribute

    Example (simple static file - local):
        from flowlet.storage.filesystem import FilesystemHandler

        handler = FilesystemHandler(Path('./logs') / 'app.log')
        logger.addHandler(handler)

    Example (simple static file - Azure):
        from flowlet.storage.azure.path import AzureBlobPath

        path = AzureBlobPath.from_connection_string(conn_str, 'logs', 'app.log')
        handler = FilesystemHandler(path)
        logger.addHandler(handler)

    Example (dynamic routing by run_id):
        def route_by_run_id(record: logging.LogRecord) -> str:
            run_id = getattr(record, 'run_id', 'default')
            return f'{run_id}/output.log'

        # Works with both Path and AzureBlobPath
        handler = FilesystemHandler(Path('./logs'), router=route_by_run_id)
        logger.addHandler(handler)

        # Logs routed to different files
        logger.info('Task started', extra={'run_id': 'run-123'})  # -> logs/run-123/output.log
        logger.info('Task started', extra={'run_id': 'run-456'})  # -> logs/run-456/output.log
    """

    def __init__(
        self,
        path: PathLike,
        level: int = logging.NOTSET,
        encoding: str = 'utf-8',
        router: Callable[[logging.LogRecord], str] | None = None,
        chunk_size: int | None = None
    ):
        """
        Initialize filesystem or Azure blob logging handler.

        Args:
            path: Base directory or file path for storing logs.
                  Can be a local Path or an AzureBlobPath.
            level: Minimum log level to handle (default: NOTSET)
            encoding: Text encoding for log messages (default: 'utf-8')
            router: Optional function to route records to sub-paths.
                    Takes a LogRecord and returns a relative path string.
            chunk_size: Buffer size for Azure blob storage (default: 4MB).
                       Only applies to AzureBlobPath. Smaller values provide
                       more real-time visibility but more API calls.
                       Ignored for local filesystem paths.

        Examples:
            # Static file - local
            handler = FilesystemHandler(Path('./logs/app.log'))

            # Static file - Azure with default buffering (4MB)
            path = AzureBlobPath.from_connection_string(conn_str, 'logs', 'app.log')
            handler = FilesystemHandler(path)

            # Azure with real-time streaming (64KB chunks)
            handler = FilesystemHandler(path, chunk_size=64 * 1024)

            # Dynamic routing by run_id
            def route_by_run_id(record: logging.LogRecord) -> str:
                run_id = getattr(record, 'run_id', 'default')
                return f'{run_id}/output.log'

            handler = FilesystemHandler(Path('./logs'), router=route_by_run_id)
        """
        super().__init__(level)

        self.path = path
        self.encoding = encoding
        self.router = router
        self.chunk_size = chunk_size

        # Thread safety
        self._lock = threading.Lock()
        self._file_handles: dict[str, Any] = {}
        self._closed = False

        # Create base directory if it doesn't exist
        if self.router is None:
            # Static file - create parent directory
            self.path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Dynamic routing - create base directory
            self.path.mkdir(parents=True, exist_ok=True)

    def _get_file_handle(self, filepath: PathLike):
        """Get or create a file handle for the given filepath."""
        path_key = str(filepath)
        if path_key not in self._file_handles:
            # Ensure parent directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Open file with appropriate parameters
            # Check if this is an AzureBlobPath (has chunk_size parameter)
            if self.chunk_size is not None and not isinstance(filepath, Path):
                # AzureBlobPath - supports chunk_size
                self._file_handles[path_key] = filepath.open(
                    mode='a', encoding=self.encoding, chunk_size=self.chunk_size,
                )
            else:
                self._file_handles[path_key] = filepath.open('a', encoding=self.encoding)
        return self._file_handles[path_key]

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the filesystem.

        For dynamic routing, routes the log record to the appropriate file
        based on the router function.

        Args:
            record: The log record to emit
        """
        if self._closed:
            return

        try:
            filepath = self.path
            if self.router is not None:
                filepath = self.path / self.router(record)

            msg = self.format(record)
            if not msg.endswith('\n'):
                msg += '\n'

            with self._lock:
                file_handle = self._get_file_handle(filepath)
                file_handle.write(msg)
                file_handle.flush()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush all open file handles."""
        if self._closed:
            return

        try:
            with self._lock:
                for file_handle in self._file_handles.values():
                    file_handle.flush()
        except Exception:
            self.handleError(None)  # type: ignore

    def close(self) -> None:
        """Close all file handles."""
        if self._closed:
            return

        try:
            with self._lock:
                for file_handle in self._file_handles.values():
                    file_handle.close()
                self._file_handles.clear()
                self._closed = True
        except Exception:
            self.handleError(None)  # type: ignore
        finally:
            super().close()

    def __enter__(self) -> 'FilesystemHandler':
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