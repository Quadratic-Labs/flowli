"""Data models for flow execution tracking.

This module defines attrs dataclasses for representing flow execution data
in a type-safe, immutable way. These models are used throughout the repository
layer for data transfer between components.
"""
from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID, uuid4

from attrs import Factory, define, field


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
        ...     run_id=uuid4(),
        ...     status="success"
        ... )
    """
    name: str
    run_id: UUID = Factory(uuid4)
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
        ...     run_id=uuid4()
        ... )
    """
    name: str
    run_type: str
    run_id: UUID = Factory(uuid4)


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
    log_id: UUID = Factory(uuid4)


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