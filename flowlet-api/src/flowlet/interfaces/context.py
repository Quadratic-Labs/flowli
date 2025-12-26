"""Execution context management interface for flow and task runs.

This module defines the protocol for managing execution context during flow
and task runs, including run tracking, hierarchy management, and lifecycle control.
"""
from typing import Any, AsyncContextManager, ContextManager, Literal, Protocol
from uuid import UUID, uuid7

from attrs import define, Factory, field, fields


@define(slots=True, kw_only=True)
class RunContextModel:
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
    run_id: UUID = Factory(uuid7)
    span_name: str
    span_type: str
    span_id: UUID = Factory(lambda self: self.run_id, takes_self=True)
    parent_span_id: UUID | None = field(default=None)
    flow_name: str = Factory(lambda self: self.span_name, takes_self=True)

    @classmethod
    def init_root_span(
        cls,
        span_name: str,
        span_type: Literal["task"] | Literal["flow"] = "task",
        span_id: UUID | None = None,
    ) -> RunContextModel:
        if span_type == "task":
            raise RuntimeError(
                f"TaskContext '{span_name}' must be used within a parent context (flow or task)"
            )
        if span_id:
            ctx = RunContextModel(
                span_name=span_name,
                span_type=span_type,
                span_id=span_id,
            )
        else:
            ctx = RunContextModel(
                span_name=span_name,
                span_type=span_type,
            )
        return ctx

    def init_child_span(
            self,
            span_name: str,
            span_type: Literal["flow"] | Literal["task"] = "task",
            span_id: UUID | None = None,
    ) -> RunContextModel:
        return type(self)(
            flow_name = self.flow_name,
            run_id = self.run_id,
            span_name = span_name,
            span_type = span_type,
            span_id = span_id or uuid7(),
            parent_span_id = self.span_id,
        )

    def inject_as_str_into(self, obj: Any) -> Any:
        if obj is None:
            for att in fields(type(self)):
                setattr(obj, att.name, None)
        else:
            for att in fields(type(self)):
                val = getattr(self, att.name, None)
                val = str(val) if val is not None else ""
                setattr(obj, att.name, val)
        return obj


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
    def get_all_spans(cls) -> tuple[RunContextModel, ...]:
        """Retrieve all execution spans from the current context stack."""
        ...

    @classmethod
    def get_current_span(cls) -> RunContextModel | None:
        """Retrieve the currently active execution span."""
        spans = cls.get_all_spans()
        return spans[-1] if spans else None

    @classmethod
    def get_parent_span(cls) -> RunContextModel | None:
        """Retrieve the parent of the currently active execution span."""
        spans = cls.get_all_spans()
        return spans[-2] if spans and len(spans) > 1 else None

    @classmethod
    def get_root_span(cls) -> RunContextModel | None:
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
        span_type: Literal["task"] | Literal["flow"] = "task",
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
        span_type: Literal["task"] | Literal["flow"] = "task",
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
