"""Execution context management interface for flow and task runs.

This module defines the protocol for managing execution context during flow
and task runs, including run tracking, hierarchy management, and lifecycle control.
"""
from typing import Protocol, Self
from uuid import UUID, uuid7

from attrs import define, Factory, field


@define(slots=True, kw_only=True)
class SpanContextModel:
    """Core attributes identifying a flow or task run.

    Minimal model containing only the essential identifiers for a run.
    Used in contexts where full run details are not needed.

    Attributes:
        name: Name of the flow or task.
        run_type: Type of run ("flow" or "task").
        run_id: Unique identifier for this run.

    Example:
        >>> run_attrs = RunAttrModel(
        ...     name="my_flow",
        ...     run_type="flow",
        ...     run_id=uuid4()
        ... )
    """
    flow_name: str
    run_id: UUID = Factory(uuid7)
    span_name: str
    span_type: str
    span_id: UUID = Factory(lambda self: self.run_id, takes_self=True)
    parent_span_id: UUID | None = field(default=None)


class ExecutionContext(Protocol):
    """Protocol defining the interface for flow and task execution context management.

    ExecutionContext provides lifecycle management for flow and task execution,
    maintaining a hierarchical stack of nested execution contexts. It must be both
    thread-safe and async-safe.

    Responsibilities:
        - Manage the execution context stack (run_id, span_id hierarchy)
        - Control context lifecycle via enter/exit semantics
        - Coordinate database session lifecycle

    Note:
        Observation and tracking activities (logging, database writes) are
        delegated to the ExecutionObserver component.
    """
    @classmethod
    def get_current_span(cls) -> RunContextModel | None:
        """Retrieve the currently active execution run."""
        ...

    @classmethod
    def get_parent_run(cls) -> RunContextModel | None:
        """Retrieve the parent of the currently active execution run."""
        ...

    @classmethod
    def get_root_run(cls) -> RunContextModel | None:
        """Retrieve the root execution run from the context stack."""
        ...

    @classmethod
    def get_all_runs(cls) -> list[RunContextModel]:
        """Retrieve all execution runs from the current context stack."""
        ...

    def __enter__(self) -> Self:
        """Enter the execution context (synchronous context manager protocol).

        Initializes the execution context, pushes it onto the stack, and
        notifies the observer that execution has started.

        Returns:
            This ExecutionContext instance.

        Raises:
            RuntimeError: If entering a task context without an active parent context.
        """
        ...

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit the execution context (synchronous context manager protocol).

        Completes execution tracking by notifying the observer of success or
        failure, then performs cleanup of resources.

        Args:
            exc_type: Type of exception raised during execution, or None if successful.
            exc_value: Exception instance raised during execution, or None if successful.
            exc_traceback: Traceback object for the exception, or None if successful.

        Returns:
            Always False, allowing exceptions to propagate.
        """
        ...

    async def __aenter__(self) -> Self:
        """Enter the execution context (asynchronous context manager protocol).

        Initializes the execution context, pushes it onto the stack, and
        notifies the observer that execution has started.

        Returns:
            This ExecutionContext instance.

        Raises:
            RuntimeError: If entering a task context without an active parent context.
        """
        ...

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        """Exit the execution context (asynchronous context manager protocol).

        Completes execution tracking by notifying the observer of success or
        failure, then performs cleanup of resources.

        Args:
            exc_type: Type of exception raised during execution, or None if successful.
            exc_value: Exception instance raised during execution, or None if successful.
            exc_traceback: Traceback object for the exception, or None if successful.

        Returns:
            Always False, allowing exceptions to propagate.
        """
        ...