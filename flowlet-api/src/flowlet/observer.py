"""
Execution observer for tracking and logging flow/task lifecycle events.

This module separates the concern of observing execution (logging, tracking)
from managing execution context (run_id, span_id stack).

Provides both an abstract base class and concrete implementations for
different storage backends (relational DB, blob storage, etc.).
"""

import logging
import traceback
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

from .interfaces.observer import ExecutionObserver as ExecutionObserverProtocol
if TYPE_CHECKING:
    from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel
    from flowlet.interfaces.repository.protocols import FlowTrackerProtocol
else:
    # Runtime imports to avoid circular dependencies
    from .interfaces.repository.models import RunAttrModel, RunLogAttrModel


logger = logging.getLogger('flowlet')


class RelationalDBObserver(ExecutionObserverProtocol):
    """
    Execution observer for relational database backend.

    This implementation:
    1. Manages database sessions for each execution
    2. Records execution events to relational DB via FlowTracker
    3. Emits structured logs with execution context
    4. Manages per-run log file handlers (optional)

    Example:
        >>> observer = RelationalDBObserver(
        ...     tracker=tracker,
        ...     db_session_factory=session_factory
        ... )
        >>> observer.on_start(run, parent_run)
        >>> # ... execution happens ...
        >>> observer.on_success(run)
    """

    def __init__(
        self,
        tracker: "FlowTrackerProtocol | None" = None,
        db_session_factory: Any = None,
        log_manager: Any = None
    ):
        """
        Initialize the relational DB observer.

        Args:
            tracker: FlowTracker for database operations (required)
            db_session_factory: SQLAlchemy session factory (required)
            log_manager: LoggingManager for file-based logging (optional)
        """
        self.tracker = tracker
        self.db_session_factory = db_session_factory
        self.log_manager = log_manager
        self._sessions: dict[str, Any] = {}  # run_id -> session

    def on_start(
        self,
        run: "RunAttrModel",
        parent_run: "RunAttrModel | None"
    ) -> None:
        """
        Handle execution start event.

        Performs:
        1. Create database session
        2. Database tracking (create run, link to parent, log status)
        3. Start per-run log file handler (if log_manager configured)
        4. Emit structured log

        Args:
            run: The run that is starting
            parent_run: The parent run (if any)
        """
        run_id = str(run.run_id)

        # 1. Create and store database session
        if self.tracker and self.db_session_factory:
            session = self.db_session_factory()
            self._sessions[run_id] = session

            # 2. Database tracking
            from .interfaces.repository.models import RunLogAttrModel

            self.tracker.create_run(run, db=session)
            self.tracker.log(
                RunLogAttrModel(run_id=run.run_id, status="running"),
                db=session
            )
            self.tracker.link_runs(parent_run, run, db=session)

        # 3. Start per-run log file (if configured)
        if self.log_manager:
            self.log_manager.start_run_logging(run_id)

        # 4. Emit structured log
        logger.info(
            f"starting execution of {run.name} {run.run_type}",
            extra={"status": "starting"}
        )

    def on_success(self, run: "RunAttrModel") -> None:
        """
        Handle execution success event.

        Performs:
        1. Database tracking (log success status)
        2. Emit structured log
        3. Stop per-run log file handler (if log_manager configured)
        4. Close database session

        Args:
            run: The run that succeeded
        """
        run_id = str(run.run_id)
        session = self._sessions.get(run_id)

        # 1. Database tracking
        if self.tracker and session:
            from .interfaces.repository.models import RunLogAttrModel

            self.tracker.log(
                RunLogAttrModel(run_id=run.run_id, status="success"),
                db=session
            )

        # 2. Emit structured log
        logger.info(
            f"successfully executed {run.name} {run.run_type}",
            extra={"status": "success"}
        )

        # 3. Stop per-run log file (if configured)
        if self.log_manager:
            self.log_manager.stop_run_logging(run_id)

        # 4. Close and cleanup session
        if session:
            session.close()
            del self._sessions[run_id]

    def on_failure(
        self,
        run: "RunAttrModel",
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: Any
    ) -> None:
        """
        Handle execution failure event.

        Performs:
        1. Database tracking (log failure status with error details)
        2. Emit structured log with exception info
        3. Stop per-run log file handler (if log_manager configured)
        4. Close database session

        Args:
            run: The run that failed
            exc_type: Exception type
            exc_value: Exception instance
            exc_traceback: Exception traceback
        """
        run_id = str(run.run_id)
        session = self._sessions.get(run_id)

        # Format error for database
        error = "".join(
            traceback.format_exception(exc_type, exc_value, exc_traceback)
        )

        # 1. Database tracking
        if self.tracker and session:
            from .interfaces.repository.models import RunLogAttrModel

            self.tracker.log(
                RunLogAttrModel(run_id=run.run_id, status="failed", log=error),
                db=session
            )

        # 2. Emit structured log
        logger.error(
            f"{run.run_type} '{run.name}' failed: {exc_value}",
            exc_info=(exc_type, exc_value, exc_traceback),
            extra={"status": "failed"}
        )

        # 3. Stop per-run log file (if configured)
        if self.log_manager:
            self.log_manager.stop_run_logging(run_id)

        # 4. Close and cleanup session
        if session:
            session.close()
            del self._sessions[run_id]

    def emit_run_summary(self, run: "RunAttrModel") -> dict[str, Any] | None:
        """
        Emit run summary to storage.

        Only emits if log_manager is configured.

        Args:
            run: The run to emit summary for

        Returns:
            The summary dictionary, or None if no log_manager
        """
        if self.log_manager:
            return self.log_manager.emit_run_summary(str(run.run_id))
        return None


# Backwards compatibility alias
ExecutionObserver = RelationalDBObserver
