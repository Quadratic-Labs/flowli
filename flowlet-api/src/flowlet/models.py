"""Data models for flow execution tracking.

This module defines attrs dataclasses for representing flow execution data
in a type-safe, immutable way. These models are used throughout the repository
layer for data transfer between components.
"""
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Self, TypedDict, NotRequired
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
    extra: Mapping[str, str]


class SpanResult(TypedDict):
    flow_name: NotRequired[str]
    run_id: NotRequired[UUID]
    span_type: NotRequired[SpanType]
    span_name: NotRequired[str]
    span_id: NotRequired[UUID]
    parent_span_id: NotRequired[UUID | None]
    ts: NotRequired[datetime]
    message: NotRequired[str]
    level: NotRequired[str]
    extra: NotRequired[Mapping[str, Any]]


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
    span_id: UUID
    span_name: str
    status: RunStatus
    start_ts: datetime
    end_ts: datetime
    children: list[RunSummary]


class RunSummaryResult(TypedDict):
    span_id: NotRequired[str]
    span_name: NotRequired[str]
    status: NotRequired[RunStatus]
    start_ts: NotRequired[datetime]
    end_ts: NotRequired[datetime]
    children: NotRequired[list[RunSummaryResult]]


class RunResult(RunSummaryResult):
    """
    A complete run with its logs and computed summary.

    Combines a run's logs with its hierarchical summary for efficient querying.

    Attributes:
        summary: Hierarchical summary computed from the logs.
        logs: All log entries for this run, in chronological order.
    """
    logs: NotRequired[list[SpanLog]]


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
