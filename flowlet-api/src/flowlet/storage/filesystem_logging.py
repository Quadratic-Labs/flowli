"""
Filesystem logging handler for Flowlet.

Provides Python logging Handlers that export log records to the local filesystem,
organizing logs by run ID.
"""

import logging
import threading
from pathlib import Path
from typing import Optional


class FilesystemHandler(logging.Handler):
    """
    Logging handler that writes logs to the filesystem.

    This handler writes log records to files organized by run ID,
    creating a directory structure for easy navigation.

    Features:
    - Automatic directory creation
    - Thread-safe file writing with locks
    - Supports custom formatters (JSON, text, etc.)
    - Organizes logs by run ID

    Example:
        from flowlet.persistence.filesystem_logging import FilesystemHandler
        from flowlet.logging import JSONFormatter

        handler = FilesystemHandler(
            base_path='./storage/logs',
            filename_template='{run_id}.jsonl'
        )
        handler.setFormatter(JSONFormatter())

        logger = logging.getLogger('flowlet')
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    """

    def __init__(
        self,
        base_path: str,
        filename_template: str = '{run_id}.jsonl',
        level: int = logging.NOTSET,
        encoding: str = 'utf-8'
    ):
        """
        Initialize filesystem logging handler.

        Args:
            base_path: Base directory path for storing log files
            filename_template: Template for log filenames, supports {run_id} placeholder
            level: Minimum log level to handle (default: NOTSET)
            encoding: Text encoding for log messages (default: 'utf-8')
        """
        super().__init__(level)

        self.base_path = Path(base_path)
        self.filename_template = filename_template
        self.encoding = encoding

        # Thread safety
        self._lock = threading.Lock()
        self._file_handles: dict[str, any] = {}
        self._closed = False

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_file_handle(self, run_id: str):
        """Get or create a file handle for the given run_id."""
        if run_id not in self._file_handles:
            filename = self.filename_template.format(run_id=run_id)
            filepath = self.base_path / filename
            self._file_handles[run_id] = open(filepath, 'a', encoding=self.encoding)
        return self._file_handles[run_id]

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the filesystem.

        Args:
            record: The log record to emit
        """
        if self._closed:
            return

        try:
            # Get run_id from record
            run_id = getattr(record, 'run_id', None)
            if not run_id:
                return

            # Format the log record
            msg = self.format(record)

            # Ensure message ends with newline
            if not msg.endswith('\n'):
                msg += '\n'

            # Thread-safe write to file
            with self._lock:
                file_handle = self._get_file_handle(run_id)
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

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed


class FilesystemRunHandler(logging.Handler):
    """
    Logging handler that writes run summaries to the filesystem.

    This handler is designed for the run_logger and writes complete
    run summaries to individual files.

    Example:
        handler = FilesystemRunHandler(base_path='./storage/runs')
        handler.setFormatter(logging.Formatter("%(message)s"))

        run_logger = logging.getLogger('flowlet-run')
        run_logger.addHandler(handler)
    """

    def __init__(
        self,
        base_path: str,
        level: int = logging.NOTSET,
        encoding: str = 'utf-8'
    ):
        """
        Initialize filesystem run summary handler.

        Args:
            base_path: Base directory path for storing run summary files
            level: Minimum log level to handle (default: NOTSET)
            encoding: Text encoding for messages (default: 'utf-8')
        """
        super().__init__(level)

        self.base_path = Path(base_path)
        self.encoding = encoding

        # Thread safety
        self._lock = threading.Lock()
        self._closed = False

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a run summary to the filesystem.

        The message should be a JSON string containing the run summary.

        Args:
            record: The log record to emit
        """
        if self._closed:
            return

        try:
            import json

            # Parse the message to extract run_id
            msg = self.format(record)
            try:
                data = json.loads(msg)
                run_id = data.get('span_id')  # Root span_id is used as run identifier
            except (json.JSONDecodeError, AttributeError):
                # If we can't parse JSON or find run_id, skip
                return

            if not run_id:
                return

            # Write to file
            filename = f"{run_id}.json"
            filepath = self.base_path / filename

            with self._lock:
                with open(filepath, 'w', encoding=self.encoding) as f:
                    f.write(msg)
                    f.write('\n')

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush (no-op for this handler as writes are immediate)."""
        pass

    def close(self) -> None:
        """Close the handler."""
        if self._closed:
            return

        self._closed = True
        super().close()

    @property
    def closed(self) -> bool:
        """Check if the handler is closed."""
        return self._closed
