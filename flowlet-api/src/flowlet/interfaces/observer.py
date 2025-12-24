"""
Execution observer interface for tracking and logging flow/task lifecycle events.

This module separates the concern of observing execution (logging, tracking)
from managing execution context (run_id, span_id stack).

Provides both an abstract base class and concrete implementations for
different storage backends (relational DB, blob storage, etc.).
"""
from typing import Protocol, Any

from .repository.models import RunAttrModel


class ExecutionObserver(Protocol):
    """
    Protocol for execution observers.

    Observers handle lifecycle events (start, success, failure) for flows/tasks.
    Different implementations can use different storage backends.

    Subclasses must implement:
    - on_start: Handle execution start
    - on_success: Handle execution success
    - on_failure: Handle execution failure
    """
    def on_start(
        self,
        run: RunAttrModel,
        parent_run: RunAttrModel | None
    ) -> None:
        """
        Handle execution start event.

        Args:
            run: The run that is starting
            parent_run: The parent run (if any)
        """
        pass

    def on_success(self, run: RunAttrModel) -> None:
        """
        Handle execution success event.

        Args:
            run: The run that succeeded
        """
        pass

    def on_failure(
        self,
        run: RunAttrModel,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: Any
    ) -> None:
        """
        Handle execution failure event.

        Args:
            run: The run that failed
            exc_type: Exception type
            exc_value: Exception instance
            exc_traceback: Exception traceback
        """
        pass