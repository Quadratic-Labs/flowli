"""
Execution context management for flow and task runs.

This module provides execution context tracking for both flows and tasks
that is safe in both multithreaded and async environments.

Key Design Decisions:
    - Uses contextvars for implicit context propagation (thread + async safe)
    - Context managers for automatic lifecycle management
    - Unified context for both flows and tasks
    - Supports arbitrarily nested execution contexts
    - Separation of concerns: context management vs. observation/tracking
"""
import contextlib
import contextvars
import typing
from uuid import UUID

from attrs import Factory, define, field
from .models import RunType
from .types import uuid7_desc


# region @context
# ---
# role: core
# intent: manage run context's data
# description: >
#   Defines what info to track during execution, handles uuids generation
#   and runs lifecycle including linking to parent runs.
# rules:
#   - SHOULD NOT handle logging/observation or consume context info.
# dependencies:
#   - models.run
# aliases:
# triggers:
# ---

@define(slots=True, kw_only=True)
class RunContext:
    """
    Core attributes identifying a flow or task run.

    Minimal model containing only the essential identifiers for a run.
    Used in contexts where full run details are not needed.

    Attributes:
        run_id: unique identifier for this run, the root's span id.
        span_name: Name of the flow or task.
        span_type: flow or task.
        span_id: span's unique identifier (sub-run). THe root span's id is run_id
        parent_span_id: parent's span id.
        flow_name: root span's name.
    """
    run_id: UUID = Factory(uuid7_desc)
    span_name: str
    span_type: RunType
    span_id: UUID = Factory(lambda self: self.run_id, takes_self=True)
    parent_span_id: UUID | None = field(default=None)
    flow_name: str = Factory(lambda self: self.span_name, takes_self=True)

    @classmethod
    def init_root_span(
        cls,
        span_name: str,
        span_type: RunType = RunType.task,
        span_id: UUID | None = None,
    ) -> RunContext:
        """Generate a valid root context.

        Invariants that need to be checked:

        1. span_type == "flow"
        2. run_id == span_id
        3. span_name == flow_name
        4. parent_span_id is None
        """
        if span_type == "task":
            raise RuntimeError(
                f"TaskContext '{span_name}' must be used within a parent context (flow or task)"
            )
        if span_id:
            ctx = RunContext(
                run_id = span_id,
                span_name = span_name,
                span_type = span_type,
            )
        else:
            ctx = RunContext(
                span_name = span_name,
                span_type = span_type,
            )
        return ctx

    def init_child_span(
        self,
        span_name: str,
        span_type: RunType = RunType.task,
        span_id: UUID | None = None,
    ) -> RunContext:
        """Spawn a valid child context from the current context.

        Invariants that needs to be verified:

        1. child.flow_name == self.flow_name
        2. child.run_id == self.run_id
        3. child.parent_span_id == self.span_id
        """
        return type(self)(
            flow_name = self.flow_name,
            run_id = self.run_id,
            span_name = span_name,
            span_type = span_type,
            span_id = span_id or uuid7_desc(),
            parent_span_id = self.span_id,
        )


class ContextManager:
    """
    Context manager for flow and task execution.

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
    def is_root(cls) -> bool:
        """Is current context the root?"""
        return len(cls.get_all_spans()) == 1

    @classmethod
    def append_new_span(
        cls,
        span_name: str,
        span_type: RunType = RunType.task,
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
    @contextlib.contextmanager
    def begin_span(
        cls,
        span_name: str,
        span_type: RunType = RunType.task,
        span_id: UUID | None = None,
    ) -> typing.Iterator[None]:
        spans = cls.append_new_span(
            span_name=span_name, span_type=span_type, span_id=span_id)
        token = cls.runs_stack.set(spans)
        try:
            yield
        finally:
            cls.runs_stack.reset(token)

    @classmethod
    @contextlib.asynccontextmanager
    async def begin_span_async(
        cls,
        span_name: str,
        span_type: RunType = RunType.task,
        span_id: UUID | None = None,
    ) -> typing.AsyncIterator[None]:
        spans = cls.append_new_span(
            span_name=span_name, span_type=span_type, span_id=span_id)
        token = cls.runs_stack.set(spans)
        try:
            yield
        finally:
            cls.runs_stack.reset(token)

# ---
# endregion
