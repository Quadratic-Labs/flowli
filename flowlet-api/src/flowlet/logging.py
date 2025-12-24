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

from .interfaces.context import ExecutionContext


class ContextInjectingFilter(logging.Filter):
    """
    Injects execution context (run_id, span_id, etc.) into log records.

    This filter automatically adds flowlet execution context to every log record,
    making it available to formatters and handlers.
    """
    def __init__(self, context: ExecutionContext):
        self.context = context

    def filter(self, record: logging.LogRecord) -> bool:
        """Add execution context to the log record."""
        current_run = self.context.get_current_run()
        parent_run = self.context.get_parent_run()
        root_run = self.context.get_root_run()

        if current_run and root_run:
            record.run_id = str(root_run.run_id)
            record.span_id = str(current_run.run_id)
            record.parent_span_id = str(parent_run.run_id) if parent_run else None
            record.span_type = current_run.run_type  # "flow" or "task"
            # record.name = current_run.name
            record.flow_name = current_run.name  # Can be enhanced to track root flow name

            # Determine status from log level and message
            record.status = getattr(record, 'status', 'running')
        else:
            # No execution context available
            record.run_id = None
            record.span_id = None
            record.parent_span_id = None
            record.span_type = None
            # record.name = None
            record.flow_name = None
            record.status = None

        return True


class JSONFormatter(logging.Formatter):
    """
    Formats log records as JSON with execution context.

    Output format matches the example in examples/storage/hello-world/logs/*.jsonl
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data = {
            "logger": record.name,
            "run_id": getattr(record, 'run_id', None),
            "span_id": getattr(record, 'span_id', None),
            "parent_span_id": getattr(record, 'parent_span_id', None),
            "span_type": getattr(record, 'span_type', None),
            # "name": getattr(record, 'name', None),
            "flow": getattr(record, 'flow_name', None),
            "status": getattr(record, 'status', None),
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
                          'run_id', 'span_id', 'parent_span_id', 'span_type', 'flow_name', 'status']:
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


def setup_flowlet_logging(
    logs_dir: str | Path | None = None,
    log_level: int = logging.INFO,
    enable_file_logging: bool = True
) -> tuple[logging.Logger, FlowletLogBuffer]:
    """
    Configure structured logging for Flowlet execution.

    Sets up:
    1. JSON formatter with execution context injection
    2. File handler for full JSONL logs (logs/ folder)
    3. Buffering handler for Flowlet-only logs (for runs/ summaries)

    Args:
        logs_dir: Directory for log files. If None, file logging is disabled.
        log_level: Minimum log level to capture.
        enable_file_logging: Whether to enable file-based logging.

    Returns:
        Tuple of (flowlet_logger, buffer_handler) for use in execution context.
    """
    # Create formatter and filter
    json_formatter = JSONFormatter()
    context_filter = ContextInjectingFilter()

    # Get the flowlet logger (child of root)
    flowlet_logger = logging.getLogger('flowlet')
    flowlet_logger.setLevel(log_level)
    flowlet_logger.propagate = True  # Allow propagation to root logger

    # Add context filter to flowlet logger
    flowlet_logger.addFilter(context_filter)

    # Create buffer handler for Flowlet logs only
    buffer_handler = FlowletLogBuffer()
    buffer_handler.setFormatter(json_formatter)
    buffer_handler.setLevel(log_level)
    flowlet_logger.addHandler(buffer_handler)

    # Create file handler if requested
    if enable_file_logging and logs_dir:
        logs_path = Path(logs_dir)
        logs_path.mkdir(parents=True, exist_ok=True)

        # We'll create per-run log files, so we set up the root logger
        # to catch all logs (flowlet + user logs)
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        # Add context filter to root logger too
        root_logger.addFilter(context_filter)

        # Note: File handler will be added per-run in ExecutionContext
        # to create separate files for each run_id

    return flowlet_logger, buffer_handler


def create_run_log_handler(run_id: str, logs_dir: str | Path) -> JSONLFileHandler:
    """
    Create a file handler for a specific run.

    Args:
        run_id: The run ID (UUID7) to create the log file for.
        logs_dir: Directory where log files should be written.

    Returns:
        Configured file handler for this run.
    """
    logs_path = Path(logs_dir)
    logs_path.mkdir(parents=True, exist_ok=True)

    log_file = logs_path / f"{run_id}.jsonl"

    handler = JSONLFileHandler(log_file)
    handler.setFormatter(JSONFormatter())
    handler.setLevel(logging.INFO)

    return handler


def compact_logs_to_summary(logs: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    """
    Compact buffered Flowlet logs into a summary structure.

    Builds a hierarchical span structure from flat log entries,
    matching the format in examples/storage/hello-world/runs/*.json

    Args:
        logs: List of log entries from FlowletLogBuffer
        run_id: The run ID to build the summary for

    Returns:
        Summary dictionary ready to be written as JSON
    """
    # Group logs by span_id
    span_logs: dict[str, list[dict[str, Any]]] = {}
    for log in logs:
        span_id = log.get('span_id')
        if span_id:
            if span_id not in span_logs:
                span_logs[span_id] = []
            span_logs[span_id].append(log)

    # Build span objects
    spans: dict[str, dict[str, Any]] = {}
    for span_id, span_log_list in span_logs.items():
        # Find start and end logs
        start_log = None
        end_log = None

        for log in span_log_list:
            status = log.get('status', '')
            if status == 'starting':
                start_log = log
            elif status in ['success', 'failed']:
                end_log = log

        if start_log:
            span = {
                "span_id": span_id,
                "span_type": start_log.get('span_type'),
                "name": start_log.get('name'),
                "flow": start_log.get('flow'),
                "status": end_log.get('status', 'running') if end_log else 'running',
                "start_ts": start_log.get('ts'),
                "end_ts": end_log.get('ts') if end_log else None,
                "retry": 0,
                "children": []
            }

            # Calculate duration if we have both start and end
            if span["start_ts"] and span["end_ts"]:
                start_dt = datetime.fromisoformat(span["start_ts"].replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(span["end_ts"].replace('Z', '+00:00'))
                span["duration_ms"] = int((end_dt - start_dt).total_seconds() * 1000)

            spans[span_id] = span

    # Build hierarchy by linking parent-child relationships
    root_span = None
    for span_id, span in spans.items():
        # Find parent span ID from logs
        parent_span_id = None
        for log in span_logs[span_id]:
            if log.get('parent_span_id'):
                parent_span_id = log['parent_span_id']
                break

        if parent_span_id and parent_span_id in spans:
            # This is a child span
            spans[parent_span_id]["children"].append(span)
        else:
            # This is the root span
            root_span = span

    # Build final summary structure
    summary = {
        "run_id": run_id,
        "span": root_span if root_span else {}
    }

    return summary


def write_run_summary(summary: dict[str, Any], run_id: str, runs_dir: str | Path) -> None:
    """
    Write the run summary to the runs/ directory.

    Args:
        summary: The compacted summary dictionary
        run_id: The run ID
        runs_dir: Directory where run summaries should be written
    """
    runs_path = Path(runs_dir)
    runs_path.mkdir(parents=True, exist_ok=True)

    summary_file = runs_path / f"{run_id}.json"

    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
