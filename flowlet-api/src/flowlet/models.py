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
# intent: define run/span data models for the domain layer.
# description:
#   - Runs are hierarchically structured executions recorded as OTel spans;
#   run_id is the trace_id, span ids are OTel 64-bit ids as 16-char hex.
#   - SpanRecord is one finished span read back from the run folder's
#   spans-<attempt>.jsonl files; SpanEvent is a log event inside a span.
#   - RunSummary represents the derived status tree of a Run.
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
class SpanEvent:
    """
    A log event recorded inside a span.

    Attributes:
        ts: When the event was recorded.
        message: The event/log message.
        attributes: Structured event attributes (e.g. log.level).
    """
    ts: Timestamp
    message: str
    attributes: Mapping[str, Any] = Factory(dict)


@define(slots=True, kw_only=True)
class SpanRecord:
    """
    One finished span read back from a run's spans-<attempt>.jsonl file.

    Attributes:
        run_id: The run's UUID — equal to the OTel trace_id.
        span_id: OTel span id, 16-char hex string.
        parent_span_id: Parent span id, or None for the root span.
        name: Span (flow/task) name.
        flow_name: Root flow name (identical for all spans of a run).
        attempt: Execution attempt this span belongs to (1-based).
        span_type: flow or task.
        status: Terminal status of the span (completed or failed).
        status_message: Error description when failed.
        start_ts: Span start time.
        end_ts: Span end time.
        events: Log events recorded inside the span.
        attributes: Remaining span attributes.
    """
    run_id: UUID
    span_id: str
    parent_span_id: str | None = field(default=None)
    name: str
    flow_name: str
    attempt: int = 1
    span_type: RunType
    status: RunStatus
    status_message: str | None = field(default=None)
    start_ts: Timestamp
    end_ts: Timestamp | None = field(default=None)
    events: list[SpanEvent] = Factory(list)
    attributes: dict[str, Any] = Factory(dict)


@define(slots=True, kw_only=True)
class RunSummary:
    """
    A run's status.

    Aggregates a run's spans into a single stat summary.
    Recursively embeds children's spans as well.
    The root span summary gives the entire run summary.

    Attributes:
        span_id: span's identifier (16-char hex OTel span id).
        span_name: span's name.
        span_type: span's type.
        status: span's most recent status.
        start_ts: span's starting time.
        end_ts: span's ending time.
        children: span's children span's summaries.
    """
    span_id: str
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
    Acts as the source of truth for ownership and retry logic: a worker owns
    a run by CAS-writing ``status=running`` with a lease ``deadline_at``;
    anything running past its deadline is reclaimable (by another worker or
    the sweeper).  The lease is the liveness signal; flows may renew it
    mid-run via ``flowlet.heartbeat()``, which also observes
    ``cancel_requested``.

    Attributes:
        run_id: Unique identifier for the run.
        flow_name: Name of the flow being executed.
        status: Current execution status.
        worker_id: Identifier of the owning worker process.
        started_at: Timestamp when the run was first started.
        ended_at: Timestamp when the run finished (completed or failed).
        deadline_at: Lease expiry — reset on every claim to now + timeout,
            and renewed by heartbeats.
        attempt: Current attempt number (1-based); incremented on each claim
            of an existing state.
        max_retries: Maximum number of execution attempts allowed.
        kwargs: Flow keyword arguments, copied from the job at first claim so
            the sweeper can re-enqueue a crashed run without the original
            queue message.
        cancel_requested: Cooperative-cancellation flag set through the API.
            The owning worker observes it on its next heartbeat and finalizes
            the run as ``canceled``; a claim of a flagged state cancels
            without executing.
    """
    run_id: UUID
    flow_name: str
    status: RunStatus
    worker_id: str
    started_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    deadline_at: Timestamp | None = field(default=None)
    attempt: int = 1
    max_retries: int = 3
    kwargs: dict[str, Any] = Factory(dict)
    cancel_requested: bool = False

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
    A flow execution wake-up message for queue processing.

    The queue is purely a work-distribution signal: retry accounting and
    ownership live in ``RunState``, never in the message.  A worker acks the
    message as soon as the run's state is resolved; duplicate deliveries are
    harmless because the state machine drops them (busy/closed).

    Attributes:
        job_id: Unique job identifier in the queue (one per message).
        run_id: Pre-generated run ID for tracking execution.
        flow_name: Name of the flow to execute.
        kwargs: Validated keyword arguments to pass to the flow.
        submitted_at: Timestamp when job was submitted to queue.
        max_retries: Maximum number of execution attempts allowed.
        timeout_seconds: Per-attempt lease duration; None uses the worker's
            default.

    Example:
        >>> job = FlowJob(
        ...     flow_name="process_data",
        ...     kwargs={"file": "data.csv", "mode": "batch"}
        ... )
        >>> queue.enqueue(job)
    """
    # Standard UUIDv7 — the single ID convention across spans, log paths,
    # PeriodUUID range checks, and snapshot run_id ordering. run_id doubles
    # as the OTel trace_id (both are 128-bit).
    job_id: UUID = Factory(uuid7)
    run_id: UUID = Factory(uuid7)
    flow_name: str
    kwargs: dict[str, Any] = Factory(dict)
    submitted_at: Timestamp = Factory(Timestamp.now)
    max_retries: int = 3
    timeout_seconds: int | None = field(default=None)

# ---
# endregion
