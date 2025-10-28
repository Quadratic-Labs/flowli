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
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import FlowRun, TaskRun


logger = logging.getLogger()
# ============================================================================
# endregion

# region Context Variables - These are thread-safe and async-safe
# ============================================================================

class Ids(BaseModel):
    run_id: UUID
    name: str

# Stores the current flow id, name
# Useful for logging and debugging - "What flow am I part of?"
current_flow_ids: contextvars.ContextVar[Ids | None] = contextvars.ContextVar(
    "current_flow_ids", default=None
)

# Stores the current task id, name
current_task_ids: contextvars.ContextVar[Ids | None] = contextvars.ContextVar(
    "current_task_ids", default=None
)


def get_current_flow_ids() -> Ids | None:
    """Get the current flow name from context (thread-safe)."""
    return current_flow_ids.get()


def get_current_task_ids() -> Ids | None:
    """Get the current flow run ID from context (thread-safe)."""
    return current_task_ids.get()


# ============================================================================
# endregion

# region Flow Execution Context
# ============================================================================

class FlowContext:
    """
    Context manager for flow execution tracking.

    Automatically:
    - Creates a new flow run record
    - Sets the contextvar for child tasks
    - Tracks execution status and timing
    - Handles errors and cleanup

    Thread-safe and async-safe via contextvars.
    """

    def __init__(self, flow_name: str, *, db_session_factory: Session, **_):
        self.flow_name = flow_name
        self.db_session_factory = db_session_factory

        # Create unique flow run ID
        self.flow_run_id = uuid4()

        # Session and model will be initialized in __enter__
        self.session = None
        self.flow_run = None
        self._flow_run_id_token = None
        self._flow_name_token = None

    def __enter__(self):
        """Start flow execution tracking."""

        # Create database session
        self.session = self.db_session_factory()

        # Create flow run record
        self.flow_run = FlowRun(
            run_id=self.flow_run_id,
            flow_name=self.flow_name,
            started_at=datetime.now(UTC),
            status="running",
        )
        # last_flow_run = LastFlowRun(
        #     run_id=self.flow_run_id,
        #     flow_name=self.flow_name,
        # )

        self.session.add(self.flow_run)
        # self.session.add(last_flow_run)
        self.session.commit()

        # Set contextvars for child tasks - CRITICAL for thread/async safety
        self._flow_ids_token = current_flow_ids.set(Ids(run_id=self.flow_run_id, name=self.flow_name))

        logger.info(
                f"Started flow '{self.flow_name}' with run_id={self.flow_run_id}"
            )

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete flow execution tracking and cleanup."""
        try:
            if exc_type is not None:
                # Flow failed with exception
                self.flow_run.finished_at = datetime.now(UTC)
                self.flow_run.status = "failed"
                self.flow_run.error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                logger.error(
                    f"Flow '{self.flow_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                # Flow succeeded
                self.flow_run.finished_at = datetime.now(UTC)
                self.flow_run.status = "success"

                logger.info(
                    f"Flow '{self.flow_name}' completed successfully"
                )

            self.session.add(self.flow_run)
            self.session.commit()

        finally:
            # Always reset contextvars and close session
            if self._flow_run_id_token is not None:
                current_flow_ids.reset(self._flow_run_id_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    def set_result(self, result: Any):
        """Optionally store a result for the flow."""
        if hasattr(self.flow_run, 'result'):
            self.flow_run.result = repr(result)

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
    """

    def __init__(
        self,
        task_name: str,
        db_session_factory,
        logger=None
    ):
        self.task_name = task_name
        self.db_session_factory = db_session_factory
        self.logger = logger

        # Create unique task run ID
        self.task_run_id = uuid4()

        # Session and model will be initialized in __enter__
        self.session = None
        self.task_run = None
        self._context_token = None

    def __enter__(self):
        """Start task execution tracking."""

        # Create database session
        self.session = self.db_session_factory()

        # Create task run record
        flow_ids = current_flow_ids.get()
        self.task_run = TaskRun(
            run_id=self.task_run_id,
            flow_run_id=flow_ids.run_id,
            task_name=self.task_name,
            flow_name=flow_ids.name,
            started_at=datetime.now(UTC),
            status="running",
        )

        self.session.add(self.task_run)
        self.session.commit()

        # Optionally set task context (for nested task tracking)
        self._context_token = current_task_ids.set(Ids(run_id=self.task_run_id, name=self.task_name))

        logger.info(
                f"Started task '{self.task_name}' with task_run_id={self.task_run_id}"
            )

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete task execution tracking and cleanup."""
        try:
            if exc_type is not None:
                # Task failed with exception
                self.task_run.finished_at = datetime.now(UTC)
                self.task_run.status = "failed"
                self.task_run.error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                logger.error(
                    f"Task '{self.task_name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                # Task succeeded
                self.task_run.finished_at = datetime.now(UTC)
                self.task_run.status = "success"

                logger.info(
                    f"Task '{self.task_name}' completed successfully"
                )

            self.session.add(self.task_run)
            self.session.commit()

        finally:
            # Always reset contextvar and close session
            if self._context_token is not None:
                current_task_ids.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    def set_result(self, result: Any):
        """Store the result of the task."""
        if hasattr(self.task_run, 'result'):
            self.task_run.result = repr(result)

# ============================================================================
# endregion