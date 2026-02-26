"""
Flowlet structured logging configuration and run logging.

Provides JSON-formatted logging with automatic context injection (run_id, span_id, etc.)
and separate handlers for full logs and Flowlet-only logs.
"""
from datetime import datetime, timezone
import logging
from pathlib import Path
import threading
from typing import Any, Callable

from .context import ContextManager
from .models import SpanLog
from .serdes import to_json
from .storage import StoragePath


# Define custom SUCCESS logging level
SUCCESS = 25
logging.addLevelName(SUCCESS, 'SUCCESS')


# region @logging
# ---
# role: core
# intent: extend logging with execution context info
# description:
# rules:
#   - SHOULD extend standard logging library
# dependencies:
# aliases:
# triggers:
# ---

class FlowletLogger(logging.Logger):
    """Logger with custom SUCCESS level support."""

    def success(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log a message with severity 'SUCCESS' (level 25)."""
        if self.isEnabledFor(SUCCESS):
            self._log(SUCCESS, message, args, **kwargs)


# Set FlowletLogger as the default logger class for all loggers created after this point
logging.setLoggerClass(FlowletLogger)


class ContextInjectingFilter(logging.Filter):
    """
    Injects execution context (run_id, span_id, etc.) into log records.

    This filter automatically adds flowlet execution context to every log record,
    making it available to formatters and handlers.
    """
    def __init__(self, context: ContextManager):
        self.context = context

    def filter(self, record: logging.LogRecord) -> bool:
        """Add execution context to the log record."""
        current = self.context.get_current_span()
        span_data = {
            "flow_name": getattr(current, 'flow_name', None),
            "run_id": getattr(current, 'run_id', None),
            "span_type": getattr(current, 'span_type', None),
            "span_name": getattr(current, 'span_name', None),
            "span_id": getattr(current, 'span_id', None),
            "parent_span_id": getattr(current, 'parent_span_id', None),
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc),
            "message": record.getMessage(),
            "level": record.levelname,
            "extra": {},
        }
        setattr(record, "_span", SpanLog(**span_data))
        # if current:
        #     record = current.inject_as_str_into(record)
        return True


class JSONSpanFormatter(logging.Formatter):
    """
    Formats log records as JSON with execution context.

    Output format matches the example in examples/storage/hello-world/logs/*.jsonl
    """
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        span = getattr(record, "_span")
        if span is None:
            record.message = "JSONSpanFormatter error: no span to format"
            return super().format(record)

        # Add exception info if present
        if record.exc_info:
            span.extra["exception"] =  self.formatException(record.exc_info)

        # Add any custom extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'pathname', 'process', 'processName', 'relativeCreated',
                          'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info', ]:
                if not key.startswith('_'):
                    span.extra[key] = value

        return to_json(span)


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
        path: StoragePath,
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

    def _get_file_handle(self, filepath: StoragePath):
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


# This goes in the app...

def configure_context_logging(context_manager: ContextManager) -> None:
    """Attach a ContextInjectingFilter to the flowlet.log logger.

    Call once at application startup so every log record emitted through
    the flowlet.log logger automatically carries the active span's context.

    Args:
        context_manager: The ContextManager instance to inject context from.
    """
    log = logging.getLogger('flowlet.log')
    log.setLevel(logging.INFO)
    log.propagate = True
    log.addFilter(ContextInjectingFilter(context_manager))


def configure_run_file_logging(base_dir: StoragePath) -> None:
    """Attach a FilesystemHandler routing each span's logs to runs/<name>/<id>.jsonl.

    Must be called after configure_context_logging() so that the
    ContextInjectingFilter has already populated ``record._span`` by the time
    the router runs inside FilesystemHandler.emit().

    Each instrumented span writes to its own file:
    ``<base_dir>/runs/<span_name>/<span_id>.jsonl``

    Args:
        base_dir: Root directory under which the ``runs/`` tree is created.
    """
    def _router(record: logging.LogRecord) -> str:
        span = getattr(record, "_span", None)
        if span is None or span.span_id is None:
            return "unknown/unrouted.jsonl"
        return f"{span.span_name}/{span.span_id}.jsonl"

    log = logging.getLogger('flowlet.log')
    handler = FilesystemHandler(base_dir / "runs", router=_router)
    handler.setFormatter(JSONSpanFormatter())
    log.addHandler(handler)

# ---
# endregion
