"""Execution context management for flow and task runs.

This module provides execution context tracking for both flows and tasks
that is safe in both multithreaded and async environments.

Key Design Decisions:
    - Uses contextvars for implicit context propagation (thread + async safe)
    - Context managers for automatic lifecycle management
    - Unified context for both flows and tasks
    - Supports arbitrarily nested execution contexts
    - Separation of concerns: context management vs. observation/tracking
"""
import contextvars
from typing import AsyncContextManager, ContextManager, cast
from uuid import UUID

from .interfaces.context import ContextManagerProtocol
from .models import RunContext
from .types import SpanType


class ExecutionContext(ContextManagerProtocol):
    """Unified context manager for flow and task execution.

    Provides automatic lifecycle management for flow and task execution,
    maintaining a stack of nested execution contexts. Thread-safe and
    async-safe via contextvars.

    Each ExecutionContext instance represents a single span in the execution hierarchy.
    When entering the context (__enter__/__aenter__), the span is pushed onto the stack.
    When exiting (__exit__/__aexit__), the span is popped from the stack.

    This class is responsible ONLY for:
        - Managing the execution context stack (run_id, span_id hierarchy)
        - Providing context lifecycle (enter/exit)
        - Pushing/popping spans onto/from the execution stack

    Observation/tracking (logging, database writes) is delegated to ExecutionObserver.

    Attributes:
        runs_stack: ContextVar maintaining the execution stack (thread/async-safe).
        run: The RunContext instance representing this execution span.
        observer: Execution observer for logging/tracking.

    Example:
        >>> # Sync usage
        >>> observer = ExecutionObserver()
        >>> with ExecutionContext("my_flow", observer=observer):
        ...     # Flow code here
        ...     pass
        >>>
        >>> # Async usage
        >>> async with ExecutionContext("my_flow", observer=observer):
        ...     # Flow code here
        ...     pass
    """
    runs_stack: contextvars.ContextVar[tuple[RunContext, ...]] = contextvars.ContextVar(
        "runs_stack", default=()
    )

    def __init__(self, **_):
        pass

    @classmethod
    def get_all_spans(cls) -> tuple[RunContext, ...]:
        """Get all execution runs in the current context stack.

        Returns a copy of the full execution hierarchy from root to current.

        Returns:
            list[RunContext]: List of all runs in the current execution stack.
        """
        return cls.runs_stack.get()

    @classmethod
    def append_new_span(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
        span_id: UUID | None = None,
    ) -> tuple[RunContext, ...]:
        parent = cls.get_current_span()
        if parent:
            ctx = parent.init_child_span(
                span_name=span_name, span_type=span_type, span_id=span_id)
        else:
            ctx = RunContext.init_root_span(
                span_name=span_name, span_type=span_type, span_id=span_id)
        return cls.get_all_spans() + (ctx,)

    @classmethod
    def begin_span(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
        span_id: UUID | None = None,
    ) -> ContextManager[None]:
        spans = cls.append_new_span(
            span_name=span_name, span_type=span_type, span_id=span_id)
        return cast(ContextManager[None], cls.runs_stack.set(spans))

    @classmethod
    async def begin_span_async(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
        span_id: UUID | None = None,
    ) -> AsyncContextManager[None]:
        spans = cls.append_new_span(
            span_name=span_name, span_type=span_type, span_id=span_id)
        return cast(AsyncContextManager[None], cls.runs_stack.set(spans))
