"""
Execution Context and Tracking

This module provides execution context tracking that is safe in both
multithreaded and async environments.
"""
# region Imports
# ============================================================================
# Key Design Decisions:
# - Uses contextvars for implicit context propagation (thread + async safe)
# - Context managers for automatic lifecycle management
# - Clean separation between flow and task contexts
# - Proper error handling and resource cleanup

import contextvars
import logging
import traceback
from datetime import datetime, UTC
from typing import Any, TYPE_CHECKING
from uuid import UUID, uuid4

from pydantic import BaseModel

if TYPE_CHECKING:
    from .repository import FlowTracker


logger = logging.getLogger()
# ============================================================================
# endregion

# region Flow Execution Context
# ============================================================================

class Ids(BaseModel):
    run_id: UUID
    name: str


class FlowContext:
    """
    Context manager for flow execution tracking.

    Automatically:
    - Creates a new flow run record
    - Sets the contextvar for child tasks
    - Tracks execution status and timing
    - Handles errors and cleanup

    Thread-safe and async-safe via contextvars.
    Supports both sync (with) and async (async with) usage.
    """
    current_ids: contextvars.ContextVar[Ids | None] = contextvars.ContextVar(
        "current_flow_ids", default=None
    )
    """Current flow execution identifiers"""

    def __init__(self, flow_name: str, *, tracker: "FlowTracker", **_):
        self.flow_name = flow_name
        self.tracker = tracker

        # variables initialized in __enter__
        self.session = None
        self.flow_run = None
        self._context_token = None

    @classmethod
    def get_current_ids(cls) -> Ids | None:
        """Get the current flow IDs from context (thread-safe and async-safe)."""
        return cls.current_ids.get()

    def __enter__(self):
        """Start flow execution tracking (sync context manager)."""
        # Generate unique run ID
        run_id = uuid4()

        # Create database session
        self.session = self.tracker.db_session_factory()

        # Create flow run record using tracker
        self.flow_run = self.tracker.create_flow_run(
            run_id=run_id,
            flow_name=self.flow_name,
            started_at=datetime.now(UTC),
            status="running",
            db=self.session
        )

        # Set context variable with token for proper cleanup
        ids = Ids(run_id=run_id, name=self.flow_name)
        self._context_token = self.current_ids.set(ids)

        logger.info(
            f"Started flow '{self.flow_name}' with run_id={run_id}"
        )
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete flow execution tracking and cleanup (sync context manager)."""
        assert self.flow_run is not None
        assert self.session is not None
        try:
            # Update flow run status using repository
            finished_at = datetime.now(UTC)

            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                self.tracker.update_flow_run(
                    self.flow_run,
                    finished_at=finished_at,
                    status="failed",
                    error=error,
                    db=self.session
                )
                logger.error(
                    f"Flow '{self.flow_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                self.tracker.update_flow_run(
                    self.flow_run,
                    finished_at=finished_at,
                    status="success",
                    db=self.session
                )
                logger.info(
                    f"Flow '{self.flow_name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.current_ids.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    async def __aenter__(self):
        """Start flow execution tracking (async context manager)."""
        # Generate unique run ID
        run_id = uuid4()

        # Create database session
        self.session = self.tracker.db_session_factory()

        # Create flow run record using tracker
        self.flow_run = self.tracker.create_flow_run(
            run_id=run_id,
            flow_name=self.flow_name,
            started_at=datetime.now(UTC),
            status="running",
            db=self.session
        )

        # Set context variable with token for proper cleanup
        ids = Ids(run_id=run_id, name=self.flow_name)
        self._context_token = self.current_ids.set(ids)

        logger.info(
            f"Started flow '{self.flow_name}' with run_id={run_id}"
        )
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        """Complete flow execution tracking and cleanup (async context manager)."""
        assert self.flow_run is not None
        assert self.session is not None
        try:
            # Update flow run status using repository
            finished_at = datetime.now(UTC)

            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                self.tracker.update_flow_run(
                    self.flow_run,
                    finished_at=finished_at,
                    status="failed",
                    error=error,
                    db=self.session
                )
                logger.error(
                    f"Flow '{self.flow_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                self.tracker.update_flow_run(
                    self.flow_run,
                    finished_at=finished_at,
                    status="success",
                    db=self.session
                )
                logger.info(
                    f"Flow '{self.flow_name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.current_ids.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

# ============================================================================
# endregion

# region Task Execution Context
# ============================================================================

class TaskContext:
    """
    Context manager for task execution tracking.

    Automatically:
    - Retrieves flow_run_id from contextvar (thread-safe)
    - Creates a new task run record
    - Tracks execution status and timing
    - Handles errors and cleanup

    Thread-safe and async-safe via contextvars.
    Supports both sync (with) and async (async with) usage.
    """
    # Class-level ContextVar for storing current task IDs
    current_ids: contextvars.ContextVar[Ids | None] = contextvars.ContextVar(
        "current_task_ids", default=None
    )

    def __init__(self, task_name: str, *, tracker: "FlowTracker", **_):
        self.task_name = task_name
        self.tracker = tracker

        # variables initialized in __enter__
        self.session = None
        self.task_run = None
        self._context_token = None

    @classmethod
    def get_current_ids(cls) -> Ids | None:
        """Get the current task IDs from context (thread-safe and async-safe)."""
        return cls.current_ids.get()

    def __enter__(self):
        """Start task execution tracking (sync context manager)."""
        # Generate unique task run ID
        task_run_id = uuid4()

        # Create database session
        self.session = self.tracker.db_session_factory()

        # Get flow context
        flow_ids = FlowContext.get_current_ids()
        if flow_ids is None:
            raise RuntimeError(
                f"TaskContext '{self.task_name}' must be used within a FlowContext"
            )

        # Create task run record using repository
        self.task_run = self.tracker.create_task_run(
            run_id=task_run_id,
            task_name=self.task_name,
            flow_run_id=flow_ids.run_id,
            flow_name=flow_ids.name,
            started_at=datetime.now(UTC),
            status="running",
            db=self.session
        )

        # Set task context with token for proper cleanup
        ids = Ids(run_id=task_run_id, name=self.task_name)
        self._context_token = self.current_ids.set(ids)

        logger.info(
            f"Started task '{self.task_name}' with task_run_id={task_run_id}"
        )

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete task execution tracking and cleanup (sync context manager)."""
        assert self.task_run is not None
        assert self.session is not None
        try:
            # Update task run status using repository
            finished_at = datetime.now(UTC)

            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                self.tracker.update_task_run(
                    self.task_run,
                    finished_at=finished_at,
                    status="failed",
                    error=error,
                    db=self.session
                )
                logger.error(
                    f"Task '{self.task_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                self.tracker.update_task_run(
                    self.task_run,
                    finished_at=finished_at,
                    status="success",
                    db=self.session
                )
                logger.info(
                    f"Task '{self.task_name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.current_ids.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    async def __aenter__(self):
        """Start task execution tracking (async context manager)."""
        # Generate unique task run ID
        task_run_id = uuid4()

        # Create database session
        self.session = self.tracker.db_session_factory()

        # Get flow context
        flow_ids = FlowContext.get_current_ids()
        if flow_ids is None:
            raise RuntimeError(
                f"TaskContext '{self.task_name}' must be used within a FlowContext"
            )

        # Create task run record using repository
        self.task_run = self.tracker.create_task_run(
            run_id=task_run_id,
            task_name=self.task_name,
            flow_run_id=flow_ids.run_id,
            flow_name=flow_ids.name,
            started_at=datetime.now(UTC),
            status="running",
            db=self.session
        )

        # Set task context with token for proper cleanup
        ids = Ids(run_id=task_run_id, name=self.task_name)
        self._context_token = self.current_ids.set(ids)

        logger.info(
            f"Started task '{self.task_name}' with task_run_id={task_run_id}"
        )

        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        """Complete task execution tracking and cleanup (async context manager)."""
        assert self.task_run is not None
        assert self.session is not None
        try:
            # Update task run status using repository
            finished_at = datetime.now(UTC)

            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                self.tracker.update_task_run(
                    self.task_run,
                    finished_at=finished_at,
                    status="failed",
                    error=error,
                    db=self.session
                )
                logger.error(
                    f"Task '{self.task_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                self.tracker.update_task_run(
                    self.task_run,
                    finished_at=finished_at,
                    status="success",
                    db=self.session
                )
                logger.info(
                    f"Task '{self.task_name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.current_ids.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    def set_result(self, result: Any):
        """Store the result of the task."""
        if self.task_run and hasattr(self.task_run, 'result'):
            self.task_run.result = repr(result)

# ============================================================================
# endregion