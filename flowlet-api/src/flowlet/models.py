"""
Data models for flow execution tracking.

This module defines attrs dataclasses for representing flow execution data
in a type-safe, immutable way. These models are used throughout the repository
layer for data transfer between components.
"""
from datetime import timedelta
from enum import StrEnum
from typing import Any, Mapping
from uuid import UUID, uuid7

from attrs import Factory, define, field

from .types import JsonData, Timestamp


# region @models.run
# ---
# role: datatype
# intent: define logs data models for the domain layer.
# description:
#   - Runs are like runtime execution context and likewise is hierarchically
#   structured.
#   - Runs are like spans in opentelemetry, they happen over a time-range.
#   - RunSummary represents the current status of a Run.
#   - RunLog represents a single event during a Run's span.
# rules:
#   - Models SHOULD be attrs define
#   - For inheritance, models SHOULD use kw_only=True
#   - For optimisation, models SHOULD use slots=True
# dependencies:
#    - types
# aliases:
# triggers:
#    - what traces do runs leave ?
# ---

class RunType(StrEnum):
    flow = "flow"
    task = "task"


class RunStatus(StrEnum):
    pending = "pending"
    running = "running"
    retry = "retry"
    failed = "failed"
    completed = "completed"
    warning = "warning"
    canceled = "canceled"
    stopped = "stopped"

    @classmethod
    def from_log_level(cls, level: str) -> "RunStatus":
        """Map a logging level to a RunStatus.

        Args:
            level: The logging level name (e.g., 'INFO', 'SUCCESS', 'WARNING', 'ERROR')

        Returns:
            The corresponding RunStatus value
        """
        level_upper = level.upper()
        if level_upper == 'SUCCESS':
            return cls.completed
        elif level_upper == 'WARNING':
            return cls.warning
        elif level_upper in ('ERROR', 'CRITICAL'):
            return cls.failed
        else:
            return cls.running

    def is_closed(self) -> bool:
        return self.value in ("completed", "canceled", "failed", "warning")


@define(slots=True, kw_only=True)
class RunLog:
    """
    A run's log event.

    Runs span a time range within which events can be recorded.
    RunLog models such events fields, attaching it to its span.
    """
    flow_name: str
    run_id: UUID
    span_type: RunType
    span_name: str
    span_id: UUID
    parent_span_id: UUID | None
    ts: Timestamp
    message: str
    level: str
    extra: Mapping[str, str]


@define(slots=True, kw_only=True)
class RunSummary:
    """
    A run's status.

    Aggregates a run's logs into a single stat summary.
    Recursively embeds children's spans as well.
    The root span summary gives the entire run summary.

    Attributes:
        span_id: span's identifier.
        span_name: span's name.
        span_type: span's type.
        status: span's most recent status.
        start_ts: span's starting time.
        end_ts: span's ending time.
        children: span's children span's summaries.
    """
    span_id: UUID
    span_name: str
    span_type: RunType
    status: RunStatus
    start_ts: Timestamp | None = field(default=None)
    end_ts: Timestamp | None = field(default=None)
    children: list[RunSummary] = Factory(list)

    @property
    def duration(self) -> timedelta | None:
        """
        Calculate execution duration of the latest run.

        Returns:
            timedelta | None: Duration from start to end, or None if incomplete.
        """
        if self.start_ts is None or self.end_ts is None:
            return None
        return self.end_ts.value - self.start_ts.value


@define(slots=True, kw_only=True)
class Run(RunSummary):
    """
    A complete run with its logs and computed summary.

    Combines a run's logs with its hierarchical summary for efficient querying.

    Attributes:
        summary: Hierarchical summary computed from the logs.
        logs: All log entries for this run, in chronological order.
    """
    logs: JsonData


@define(slots=True, kw_only=True)
class RunState:
    """
    Worker-owned state for a single flow execution.

    Written atomically to the state store on every transition.
    Acts as the source of truth for liveness detection and retry logic.

    Attributes:
        run_id: Unique identifier for the run.
        flow_name: Name of the flow being executed.
        status: Current execution status.
        worker_id: Identifier of the owning worker process.
        started_at: Timestamp when the run was first started.
        heartbeat_at: Timestamp of the last heartbeat from the worker.
        ended_at: Timestamp when the run finished (completed or failed).
        attempt: Current attempt number (1-based).
        max_retries: Maximum number of retry attempts allowed.
    """
    run_id: UUID
    flow_name: str
    status: RunStatus
    worker_id: str
    started_at: Timestamp
    heartbeat_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    attempt: int = 1
    max_retries: int = 3

# ---
# endregion


# region @models.job
# ---
# role: datatype
# intent: describe metadata for the job queue
# description: >
#   The job queue serves for workers' synchronisation. Flows' requiring
#   execution submit a job to the queue with metadata to be picked up
#   independently by workers.
# rules:
#   - run_id MUST identify uniquely a run
#   - Each message on the queue MUST have a unique job_id (job = message)
# dependencies:
#   - types
# aliases:
# triggers:
# ---

@define(slots=True, kw_only=True)
class FlowJob:
    """
    A flow execution job for queue processing.

    Represents a queued flow execution with all necessary metadata
    for scheduling, tracking, and retry logic.

    Attributes:
        job_id: Unique job identifier in the queue.
        run_id: Pre-generated run ID for tracking execution.
        flow_name: Name of the flow to execute.
        kwargs: Validated keyword arguments to pass to the flow.
        submitted_at: Timestamp when job was submitted to queue.
        retry_count: Number of times this job has been retried.
        max_retries: Maximum number of retry attempts allowed.
        visibility_timeout: Seconds before job becomes visible again if not acknowledged.

    Example:
        >>> job = FlowJob(
        ...     flow_name="process_data",
        ...     kwargs={"file": "data.csv", "mode": "batch"}
        ... )
        >>> queue.enqueue(job)
    """
    job_id: UUID = Factory(uuid7)
    run_id: UUID = Factory(uuid7)
    flow_name: str
    kwargs: dict[str, Any] = Factory(dict)
    submitted_at: Timestamp = Factory(Timestamp.now)
    retry_count: int = 0
    max_retries: int = 3
    visibility_timeout: int = 300  # 5 minutes default

# ---
# endregion
