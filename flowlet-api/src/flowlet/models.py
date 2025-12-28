"""Data models for flow execution tracking.

This module defines attrs dataclasses for representing flow execution data
in a type-safe, immutable way. These models are used throughout the repository
layer for data transfer between components.
"""
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Self
from uuid import UUID, uuid7

from attrs import Factory, define, field, fields

from .types import SpanType, RunStatus


@define(slots=True, kw_only=True)
class RunContext:
    """Core attributes identifying a flow or task run.

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
    run_id: UUID = Factory(uuid7)
    span_name: str
    span_type: SpanType
    span_id: UUID = Factory(lambda self: self.run_id, takes_self=True)
    parent_span_id: UUID | None = field(default=None)
    flow_name: str = Factory(lambda self: self.span_name, takes_self=True)

    @classmethod
    def init_root_span(
        cls,
        span_name: str,
        span_type: SpanType = SpanType.task,
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
        span_type: SpanType = SpanType.task,
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
            span_id = span_id or uuid7(),
            parent_span_id = self.span_id,
        )

    def inject_as_str_into(self, obj: Any) -> Any:
        """Injects self attributes into `obj` as attributes.
        
        Useful to inject context into log records as attributes.
        These attributes become acccessible for Formatters and Handlers.
        """
        if obj is None:
            return None
            # for att in fields(type(self)):
            #     setattr(obj, att.name, None)
        else:
            for att in fields(type(self)):
                val = getattr(self, att.name, None)
                val = str(val) if val is not None else ""
                setattr(obj, att.name, val)
        return obj


@define(slots=True, kw_only=True)
class SpanLog:
    flow_name: str
    run_id: UUID
    span_type: SpanType
    span_name: str
    span_id: UUID
    parent_span_id: UUID | None
    ts: datetime
    message: str
    level: str
    extra: Mapping[str, Any]


@define(slots=True, kw_only=True)
class RunSummary:
    """
    A run's span's summary stats.

    Aggregates a run's span's logs into a single stat summary.
    Recursively embeds children's spans as well.
    The root span summary gives the entire run summary.

    Attributes:
        span_id: span's identifier. 
        span_name: span's name.
        status: span's most recent status.
        start_ts: span's starting time.
        end_ts: span's ending time.
        children: span's children span's summaries.
    """
    span_id: str
    span_name: str
    status: RunStatus
    start_ts: datetime
    end_ts: datetime
    children: list[RunSummary]

    @property
    def duration(self) -> timedelta | None:
        """Calculate execution duration of the latest run.

        Returns:
            timedelta | None: Duration from start to end, or None if incomplete.
        """
        if self.start_ts is None or self.end_ts is None:
            return None
        return self.end_ts - self.start_ts


@define(slots=True, kw_only=True)
class FlowSummary:
    """Summary of a flow's execution history.

    Aggregates information from recent runs of a flow for display in lists.
    Uses attrs for immutability and efficient memory usage.

    Attributes:
        name: Flow name.
        status: Status of the most recent run ("running", "success", "failed", etc.).
        started_at: Timestamp when the most recent run started.
        ended_at: Timestamp when the most recent run ended.
        doc: Optional docstring or description of the flow.

    Example:
        >>> summary = FlowSummary(
        ...     name="data_pipeline",
        ...     status="success",
        ...     started_at=datetime.now(UTC),
        ...     ended_at=datetime.now(UTC)
        ... )
        >>> print(summary.duration)
    """
    name: str
    status: str | None = field(default=None)
    started_at: datetime | None = field(default=None)
    ended_at: datetime | None = field(default=None)
    doc: str | None = field(default=None)

    @property
    def duration(self) -> timedelta | None:
        """Calculate execution duration of the latest run.

        Returns:
            timedelta | None: Duration from start to end, or None if incomplete.
        """
        if self.started_at is None or self.ended_at is None:
            return None
        return self.ended_at - self.started_at


@define(slots=True, kw_only=True)
class FlowRunSummary:
    """Summary of a single flow run.

    Provides essential information about an individual flow execution
    for display in run lists.

    Attributes:
        name: Flow name.
        run_id: Unique identifier for this run.
        status: Run status ("running", "success", "failed", etc.).
        ended_at: Timestamp when the run ended.

    Example:
        >>> run = FlowRunSummary(
        ...     name="data_pipeline",
        ...     run_id=uuid7(),
        ...     status="success"
        ... )
    """
    name: str
    run_id: UUID = Factory(uuid7)
    status: str | None = field(default=None)
    ended_at: datetime | None = field(default=None)


@define(slots=True, kw_only=True)
class RunAttrModel:
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
        ...     run_id=uuid7()
        ... )
    """
    name: str
    run_type: str
    run_id: UUID = Factory(uuid7)


@define(slots=True, kw_only=True)
class RunLogAttrModel:
    """A log entry for a run.

    Represents a single status change or message logged during execution.
    Logs are immutable and append-only.

    Attributes:
        run_id: ID of the run this log belongs to.
        status: Status at this point ("running", "success", "failed").
        log: Optional log message or error details.
        timestamp: When this log entry was created.
        log_id: Unique identifier for this log entry.

    Example:
        >>> log = RunLogAttrModel(
        ...     run_id=run.run_id,
        ...     status="success",
        ...     log="Flow completed successfully"
        ... )
    """
    run_id: UUID
    status: str
    log: str = field(default="")
    timestamp: datetime = Factory(lambda: datetime.now(UTC))
    log_id: UUID = Factory(uuid7)


@define(slots=True, kw_only=True)
class RunModel:
    """Complete model of a run with full hierarchy and logs.

    Contains all information about a run including its attributes,
    all log entries, parent run (if it's a task), and child runs
    (if it's a flow).

    Attributes:
        run: Core run attributes (name, type, ID).
        logs: All log entries for this run, in chronological order.
        parent: Parent run attributes if this is a task run.
        children: List of child run models if this is a flow run.

    Example:
        >>> run = RunModel(
        ...     run=RunAttrModel(name="my_flow", run_type="flow"),
        ...     logs=[RunLogAttrModel(run_id=run_id, status="running")],
        ...     children=[task_run1, task_run2]
        ... )
    """
    run: RunAttrModel
    logs: list[RunLogAttrModel] = Factory(list)
    parent: RunAttrModel | None = field(default=None)
    children: list[Self] = Factory(list)