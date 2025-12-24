"""
Flowlet logging manager - high-level API for configuring and managing execution logs.

This module provides a simplified interface for:
1. Setting up structured JSON logging with context injection
2. Managing per-run log files in the logs/ directory
3. Emitting run summaries to the runs/ directory
"""

from pathlib import Path
from typing import Any

from flowlet.context import ExecutionContext
from flowlet.logging import (
    FlowletLogBuffer,
    compact_logs_to_summary,
    create_run_log_handler,
    setup_flowlet_logging,
    write_run_summary,
)


class LoggingManager:
    """
    Manages Flowlet logging configuration and run summary generation.

    This class provides a high-level interface for:
    - Configuring structured logging with automatic context injection
    - Creating per-run log files
    - Generating and writing run summaries

    Example:
        >>> # Setup logging at application startup
        >>> log_manager = LoggingManager(
        ...     logs_dir="./storage/logs",
        ...     runs_dir="./storage/runs"
        ... )
        >>> log_manager.setup()
        >>>
        >>> # After a run completes, emit the summary
        >>> log_manager.emit_run_summary(run_id)
    """

    def __init__(
        self,
        logs_dir: str | Path | None = None,
        runs_dir: str | Path | None = None,
        enable_file_logging: bool = True
    ):
        """
        Initialize the logging manager.

        Args:
            logs_dir: Directory for JSONL log files. Defaults to "./logs"
            runs_dir: Directory for run summary JSON files. Defaults to "./runs"
            enable_file_logging: Whether to enable file-based logging
        """
        self.logs_dir = Path(logs_dir) if logs_dir else Path("./logs")
        self.runs_dir = Path(runs_dir) if runs_dir else Path("./runs")
        self.enable_file_logging = enable_file_logging

        self.flowlet_logger = None
        self.buffer_handler: FlowletLogBuffer | None = None
        self._run_handlers: dict[str, Any] = {}

    def setup(self) -> None:
        """
        Configure Flowlet logging system.

        Sets up:
        1. JSON formatter with execution context injection
        2. Buffering handler for Flowlet logs
        3. Root logger with context filter

        Call this once at application startup.
        """
        self.flowlet_logger, self.buffer_handler = setup_flowlet_logging(
            logs_dir=self.logs_dir,
            enable_file_logging=self.enable_file_logging
        )

    def start_run_logging(self, run_id: str) -> None:
        """
        Start file logging for a specific run.

        Creates a dedicated log file handler for this run in logs/{run_id}.jsonl.
        All logs (flowlet + user logs) during this run will be written to this file.

        Args:
            run_id: The run ID (UUID7) to create logging for
        """
        if not self.enable_file_logging:
            return

        # Create handler for this run
        import logging
        handler = create_run_log_handler(run_id, self.logs_dir)

        # Add to root logger to catch all logs
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        # Store for cleanup
        self._run_handlers[run_id] = handler

    def stop_run_logging(self, run_id: str) -> None:
        """
        Stop file logging for a specific run.

        Removes the log file handler for this run and cleans up resources.

        Args:
            run_id: The run ID to stop logging for
        """
        if run_id not in self._run_handlers:
            return

        import logging
        handler = self._run_handlers[run_id]

        # Remove from root logger
        root_logger = logging.getLogger()
        root_logger.removeHandler(handler)

        # Close handler
        handler.close()

        # Remove from tracking
        del self._run_handlers[run_id]

    def emit_run_summary(self, run_id: str, clear_buffer: bool = True) -> dict[str, Any]:
        """
        Generate and write a run summary from buffered logs.

        Takes the buffered Flowlet logs for the specified run, compacts them into
        a hierarchical summary structure, and writes the result to runs/{run_id}.json.

        Args:
            run_id: The run ID to generate summary for
            clear_buffer: Whether to clear the buffer after emitting (default: True)

        Returns:
            The generated summary dictionary

        Raises:
            ValueError: If buffer handler is not initialized
        """
        if self.buffer_handler is None:
            raise ValueError("LoggingManager not initialized. Call setup() first.")

        # Get buffered logs for this run
        logs = self.buffer_handler.get_run_logs(run_id)

        # Compact into summary structure
        summary = compact_logs_to_summary(logs, run_id)

        # Write to file
        write_run_summary(summary, run_id, self.runs_dir)

        # Clear buffer if requested
        if clear_buffer:
            self.buffer_handler.clear_run_logs(run_id)

        return summary

    def get_run_logs(self, run_id: str) -> list[dict[str, Any]]:
        """
        Get buffered logs for a specific run without emitting summary.

        Args:
            run_id: The run ID to get logs for

        Returns:
            List of log entries for this run

        Raises:
            ValueError: If buffer handler is not initialized
        """
        if self.buffer_handler is None:
            raise ValueError("LoggingManager not initialized. Call setup() first.")

        return self.buffer_handler.get_run_logs(run_id)


# Global instance for convenient access
_global_manager: LoggingManager | None = None


def get_logging_manager() -> LoggingManager:
    """
    Get the global logging manager instance.

    Returns:
        The global LoggingManager instance

    Raises:
        RuntimeError: If the manager has not been initialized
    """
    global _global_manager
    if _global_manager is None:
        raise RuntimeError(
            "Logging manager not initialized. Call initialize_logging() first."
        )
    return _global_manager


def initialize_logging(
    logs_dir: str | Path | None = None,
    runs_dir: str | Path | None = None,
    enable_file_logging: bool = True,
    flowlet_instance: Any = None
) -> LoggingManager:
    """
    Initialize the global logging manager.

    Convenience function that creates and sets up a LoggingManager instance.
    Optionally integrates with a Flowlet instance by setting the log_manager
    on its ExecutionObserver.

    Args:
        logs_dir: Directory for JSONL log files
        runs_dir: Directory for run summary JSON files
        enable_file_logging: Whether to enable file-based logging
        flowlet_instance: Optional Flowlet instance to integrate with

    Returns:
        The initialized LoggingManager instance

    Example:
        >>> # At application startup
        >>> from flowlet import configure
        >>> flowlet = configure()
        >>> initialize_logging(
        ...     logs_dir="./storage/logs",
        ...     runs_dir="./storage/runs",
        ...     flowlet_instance=flowlet
        ... )
    """
    global _global_manager
    _global_manager = LoggingManager(
        logs_dir=logs_dir,
        runs_dir=runs_dir,
        enable_file_logging=enable_file_logging
    )
    _global_manager.setup()

    # Integrate with Flowlet instance if provided
    if flowlet_instance is not None:
        flowlet_instance.observer.log_manager = _global_manager

    return _global_manager


def emit_current_run_summary() -> dict[str, Any] | None:
    """
    Emit summary for the current execution context's root run.

    Convenience function that automatically gets the root run from the current
    execution context and emits its summary.

    Returns:
        The generated summary dictionary, or None if no run is active

    Example:
        >>> # Inside a flow
        >>> @flowlet.flow()
        >>> def my_flow():
        ...     # Flow logic here
        ...     pass
        >>>
        >>> # After flow completes
        >>> emit_current_run_summary()
    """
    all_runs = ExecutionContext.get_all_runs()
    if not all_runs:
        return None

    # Get the root run (first in the stack)
    root_run = all_runs[0]
    run_id = str(root_run.run_id)

    manager = get_logging_manager()
    return manager.emit_run_summary(run_id)
