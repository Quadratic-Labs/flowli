"""
Execution Context and Tracking

This module provides unified execution context tracking for both flows and tasks
that is safe in both multithreaded and async environments.
"""
# region Imports
# ============================================================================
# Key Design Decisions:
# - Uses contextvars for implicit context propagation (thread + async safe)
# - Context managers for automatic lifecycle management
# - Unified context for both flows and tasks
# - Supports arbitrarily nested execution contexts
# - Proper error handling and resource cleanup

import contextvars
import logging
import traceback
from typing import  Literal

from .interfaces.repository.models import RunAttrModel, RunLogAttrModel

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .repositories.tracker import FlowTracker


logger = logging.getLogger()
# ============================================================================
# endregion

# region Unified Execution Context
# ============================================================================


class ExecutionContext:
    """
    Unified context manager for both flow and task execution tracking.

    Automatically:
    - Creates a new run record (flow or task)
    - Maintains a stack of nested execution contexts
    - Links child runs to parent runs automatically
    - Tracks execution status and timing
    - Handles errors and cleanup

    Thread-safe and async-safe via contextvars.
    Supports both sync (with) and async (async with) usage.
    """
    # Stack of current execution contexts (supports arbitrary nesting)
    runs_stack: contextvars.ContextVar[list[RunAttrModel]] = contextvars.ContextVar(
        "runs_stack", default=[]
    )
    """Stack of current execution runs (flows and tasks)"""

    def __init__(self, name: str, run_type: Literal["flow", "task"], *, tracker: "FlowTracker", **_):
        self.name = name
        self.run_type = run_type
        self.tracker = tracker

        # variables initialized in __enter__
        self.session = None
        self._context_token = None
        self._previous_stack = None

    @classmethod
    def get_current_run(cls) -> RunAttrModel | None:
        """Get the current (top of stack) execution IDs from context (thread-safe and async-safe)."""
        stack = cls.runs_stack.get()
        return stack[-1] if stack else None

    @classmethod
    def get_parent_run(cls) -> RunAttrModel | None:
        """Get the parent execution IDs from context (second from top of stack)."""
        stack = cls.runs_stack.get()
        return stack[-2] if len(stack) >= 2 else None

    @classmethod
    def get_all_runs(cls) -> list[RunAttrModel]:
        """Get all execution IDs in the current context stack."""
        return cls.runs_stack.get().copy()

    def __enter__(self):
        """Start execution tracking (sync context manager)."""
        self.session = self.tracker.db_session_factory()
        parent_run = self.get_current_run()
        if self.run_type == "task" and parent_run is None:
            raise RuntimeError(
                f"TaskContext '{self.name}' must be used within a parent context (flow or task)"
            )
        run = self.tracker.create_run(RunAttrModel(name=self.name, run_type=self.run_type))
        log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="running"))
        self.tracker.link_runs(parent_run, run)

        # Update context stack: save current stack and push new ID
        self._previous_stack = self.runs_stack.get().copy()
        new_stack = self._previous_stack + [run]
        self._context_token = self.runs_stack.set(new_stack)

        logger.info(f"Started {run}")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete execution tracking and cleanup (sync context manager)."""
        assert self.session is not None
        run = self.get_current_run()
        assert run is not None
        try:
            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="failed", log=error))
                logger.error(
                    f"{run.run_type.capitalize()} '{run.name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="success"))
                logger.info(
                    f"{run.run_type.capitalize()} '{run.name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.runs_stack.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

    async def __aenter__(self):
        """Start execution tracking (sync context manager)."""
        self.session = self.tracker.db_session_factory()
        parent_run = self.get_current_run()
        if self.run_type == "task" and parent_run is None:
            raise RuntimeError(
                f"TaskContext '{self.name}' must be used within a parent context (flow or task)"
            )
        run = self.tracker.create_run(RunAttrModel(name=self.name, run_type=self.run_type))
        log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="running"))
        self.tracker.link_runs(parent_run, run)

        # Update context stack: save current stack and push new ID
        self._previous_stack = self.runs_stack.get().copy()
        new_stack = self._previous_stack + [run]
        self._context_token = self.runs_stack.set(new_stack)

        logger.info(f"Started {run}")
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        """Complete execution tracking and cleanup (sync context manager)."""
        assert self.session is not None
        run = self.get_current_run()
        assert run is not None
        try:
            if exc_type is not None:
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="failed", log=error))
                logger.error(
                    f"{run.run_type.capitalize()} '{run.name}' failed: {exc_value}",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                log = self.tracker.log(RunLogAttrModel(run_id=run.run_id, status="success"))
                logger.info(
                    f"{run.run_type.capitalize()} '{run.name}' completed successfully"
                )

        finally:
            # Always reset contextvar using token and close session
            if self._context_token is not None:
                self.runs_stack.reset(self._context_token)

            if self.session:
                self.session.close()

        # Don't suppress exceptions
        return False

# ============================================================================
# endregion

# region Convenience Wrappers
# ============================================================================

class FlowContext(ExecutionContext):
    """Convenience wrapper for flow execution tracking."""
    def __init__(self, flow_name: str, *, tracker: "FlowTracker", **kwargs):
        super().__init__(name=flow_name, run_type="flow", tracker=tracker, **kwargs)


class TaskContext(ExecutionContext):
    """Convenience wrapper for task execution tracking."""
    def __init__(self, task_name: str, *, tracker: "FlowTracker", **kwargs):
        super().__init__(name=task_name, run_type="task", tracker=tracker, **kwargs)

# ============================================================================
# endregion