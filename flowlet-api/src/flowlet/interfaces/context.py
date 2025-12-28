"""Execution context management interface for flow and task runs.

This module defines the protocol for managing execution context during flow
and task runs, including run tracking, hierarchy management, and lifecycle control.
"""
from typing import AsyncContextManager, ContextManager, Protocol
from uuid import UUID, uuid7

from attrs import define, Factory, field, fields

from ..models import RunContext
from ..types import SpanType


class ContextManagerProtocol(Protocol):
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
    def get_all_spans(cls) -> tuple[RunContext, ...]:
        """Retrieve all execution spans from the current context stack."""
        ...

    @classmethod
    def get_current_span(cls) -> RunContext | None:
        """Retrieve the currently active execution span."""
        spans = cls.get_all_spans()
        return spans[-1] if spans else None

    @classmethod
    def get_parent_span(cls) -> RunContext | None:
        """Retrieve the parent of the currently active execution span."""
        spans = cls.get_all_spans()
        return spans[-2] if spans and len(spans) > 1 else None

    @classmethod
    def get_root_span(cls) -> RunContext | None:
        """Retrieve the root execution span from the context stack."""
        spans = cls.get_all_spans()
        return spans[0] if spans else None

    @classmethod
    def is_active(cls) -> bool:
        """Has current context?"""
        return bool(cls.get_all_spans())

    @classmethod
    def begin_span(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
        span_id: UUID | None = None,
    ) -> ContextManager[None]:
        """Enter the execution context (synchronous context manager protocol).

        Initializes the execution context, pushes it onto the stack, and
        notifies the observer that execution has started.

        Returns:
            This ExecutionContext instance.

        Raises:
            RuntimeError: If entering a task context without an active parent context.
        """
        ...

    @classmethod
    async def begin_span_async(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
        span_id: UUID | None = None,
    ) -> AsyncContextManager[None]:
        """Enter the execution context (synchronous context manager protocol).

        Initializes the execution context, pushes it onto the stack, and
        notifies the observer that execution has started.

        Returns:
            This ExecutionContext instance.

        Raises:
            RuntimeError: If entering a task context without an active parent context.
        """
        ...
