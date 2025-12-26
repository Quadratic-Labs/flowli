"""
Flowlet structured logging configuration.

Provides JSON-formatted logging with automatic context injection (run_id, span_id, etc.)
and separate handlers for full logs and Flowlet-only logs.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .interfaces.context import ContextManagerProtocol

# Define custom SUCCESS logging level
SUCCESS = 25
logging.addLevelName(SUCCESS, 'SUCCESS')


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
    def __init__(self, context: ContextManagerProtocol):
        self.context = context

    def filter(self, record: logging.LogRecord) -> bool:
        """Add execution context to the log record."""
        current = self.context.get_current_span()
        if current:
            record = current.inject_as_str_into(record)
        return True


class JSONFormatter(logging.Formatter):
    """
    Formats log records as JSON with execution context.

    Output format matches the example in examples/storage/hello-world/logs/*.jsonl
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data = {
            "flow_name": getattr(record, 'flow_name', None),
            "run_id": getattr(record, 'run_id', None),
            "span_type": getattr(record, 'span_type', None),
            "span_name": getattr(record, 'span_name', None),
            "span_id": getattr(record, 'span_id', None),
            "parent_span_id": getattr(record, 'parent_span_id', None),
            # "status": getattr(record, 'status', None),
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message": record.getMessage(),
            "level": record.levelname,
            "extra": {}
        }

        # Add exception info if present
        if record.exc_info:
            log_data["extra"]["exception"] = self.formatException(record.exc_info)

        # Add any custom extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'pathname', 'process', 'processName', 'relativeCreated',
                          'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info',
                          'run_id', 'span_id', 'parent_span_id', 'span_type', 'span_name', 'flow_name', 'status']:
                if not key.startswith('_'):
                    log_data["extra"][key] = value

        return json.dumps(log_data)


class FlowletLogBuffer(logging.Handler):
    """
    Buffers Flowlet-emitted logs for summary generation.

    Only captures logs from the 'flowlet' logger, filtering out user application logs.
    The buffered logs are used to generate the summary JSON in the runs/ folder.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.buffer: list[dict[str, Any]] = []
        self.run_buffers: dict[str, list[dict[str, Any]]] = {}

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer log records from the flowlet logger."""
        # Only buffer logs from flowlet logger
        if not record.name.startswith('flowlet'):
            return

        try:
            # Parse the JSON formatted log
            log_entry = json.loads(self.format(record))

            # Store in global buffer
            self.buffer.append(log_entry)

            # Store in per-run buffer
            run_id = log_entry.get('run_id')
            if run_id:
                if run_id not in self.run_buffers:
                    self.run_buffers[run_id] = []
                self.run_buffers[run_id].append(log_entry)
        except Exception:
            self.handleError(record)

    def get_run_logs(self, run_id: str) -> list[dict[str, Any]]:
        """Get all buffered logs for a specific run."""
        return self.run_buffers.get(run_id, []).copy()

    def clear_run_logs(self, run_id: str) -> None:
        """Clear buffered logs for a specific run."""
        if run_id in self.run_buffers:
            del self.run_buffers[run_id]

    def clear(self) -> None:
        """Clear all buffered logs."""
        self.buffer.clear()
        self.run_buffers.clear()