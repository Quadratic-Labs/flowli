"""
Filesystem logging handler for Flowlet.

Provides Python logging Handlers that export log records to the local filesystem,
organizing logs by run ID or custom routing logic.
"""

import logging
import threading
from pathlib import Path
from typing import Any, Callable


class FilesystemHandler(logging.Handler):
    """
    Logging handler that writes logs to the filesystem.

    This handler writes log records to files, with support for dynamic routing
    to organize logs by run ID or other attributes.

    Features:
    - Automatic directory creation
    - Thread-safe file writing with locks
    - Supports custom formatters (JSON, text, etc.)
    - Dynamic routing via router function
    - Backward compatible with run_id attribute

    Example (simple static file):
        from flowlet.storage.filesystem import FilesystemHandler

        handler = FilesystemHandler(Path('./logs') / 'app.log')
        logger.addHandler(handler)

    Example (dynamic routing by run_id):
        def route_by_run_id(record: logging.LogRecord) -> str:
            run_id = getattr(record, 'run_id', 'default')
            return f'{run_id}/output.log'

        handler = FilesystemHandler(Path('./logs'), router=route_by_run_id)
        logger.addHandler(handler)

        # Logs routed to different files
        logger.info('Task started', extra={'run_id': 'run-123'})  # -> logs/run-123/output.log
        logger.info('Task started', extra={'run_id': 'run-456'})  # -> logs/run-456/output.log
    """

    def __init__(
        self,
        path: Path,
        level: int = logging.NOTSET,
        encoding: str = 'utf-8',
        router: Callable[[logging.LogRecord], str] | None = None
    ):
        """
        Initialize filesystem logging handler.

        Args:
            path: Base directory or file path for storing logs
            level: Minimum log level to handle (default: NOTSET)
            encoding: Text encoding for log messages (default: 'utf-8')
            router: Optional function to route records to sub-paths.
                    Takes a LogRecord and returns a relative path string.

        Examples:
            # Static file
            handler = FilesystemHandler(Path('./logs/app.log'))

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

    def _get_file_handle(self, filepath: Path):
        """Get or create a file handle for the given filepath."""
        path_key = str(filepath)
        if path_key not in self._file_handles:
            # Ensure parent directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            self._file_handles[path_key] = open(filepath, 'a', encoding=self.encoding)
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