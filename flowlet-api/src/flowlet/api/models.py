"""
FastAPI DTO models for API boundary.
"""
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, computed_field
from pydantic.functional_validators import BeforeValidator

from flowlet.models import RunStatus, RunType


class Base(BaseModel):
    """Base Pydantic model with common configuration.

    Enables automatic conversion from SQLAlchemy ORM objects.
    """
    model_config = ConfigDict(from_attributes=True)


def humanize_timedelta(td: timedelta) -> str:
    """Convert a timedelta into a human-friendly relative time string.

    Args:
        td: Timedelta to humanize.

    Returns:
        Human-readable string like ``"5s"``, ``"3m"``, ``"2h 15m"``, or ``"1d 3h"``.

    Example:
        >>> from datetime import timedelta
        >>> humanize_timedelta(timedelta(seconds=45))
        '45s'
        >>> humanize_timedelta(timedelta(minutes=5, seconds=30))
        '5m'
        >>> humanize_timedelta(timedelta(hours=2, minutes=15))
        '2h 15m'
    """
    seconds = int(td.total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h"


HumanDuration = Annotated[timedelta, AfterValidator(humanize_timedelta)]
"""Type alias for timedelta that is automatically humanized when validated."""


def _coerce_timestamp(v: object) -> datetime:
    """Extract a UTC datetime from a Timestamp attrs object or pass through a datetime."""
    if hasattr(v, "value") and isinstance(getattr(v, "value", None), datetime):
        v = v.value  # type: ignore[assignment]
    if not isinstance(v, datetime):
        raise ValueError(f"Expected Timestamp or datetime, got {type(v).__name__!r}")
    return v if v.tzinfo is not None else v.replace(tzinfo=UTC)


TimestampDTO = Annotated[datetime, BeforeValidator(_coerce_timestamp)]
"""Pydantic-compatible mirror of :class:`~flowlet.types.Timestamp`.

Accepts a ``Timestamp`` attrs object (unwrapping ``.value``), a
timezone-aware ``datetime``, or an ISO 8601 string.  Always validates to
a UTC-aware ``datetime`` and serialises as a standard datetime string.
"""


class FlowArguments(Base):
    """API model for flow execution request.

    Accepts keyword arguments to pass to the flow function.

    Attributes:
        kwargs: Dictionary of keyword arguments for flow execution.
        dispatch_key: Optional idempotency key.  Submissions of the same
            flow with the same key all resolve to the same run: the first
            one creates it, later ones are deduplicated (see the
            ``deduplicated`` response field).  Use a webhook delivery id,
            an event id, or ``"<flow>:<schedule tick>"`` for cron overlap
            protection.

    Example:
        >>> flow_input = FlowArguments(kwargs={"user_id": 123, "mode": "test"})
        >>> idempotent = FlowArguments(
        ...     kwargs={"campaign": 7}, dispatch_key="webhook-delivery-42"
        ... )
    """
    kwargs: dict[str, Any] = Field(default_factory=dict)
    dispatch_key: str | None = Field(
        None,
        min_length=1,
        max_length=512,
        description=(
            "Idempotency key: repeated submissions with the same key "
            "collapse onto one run"
        ),
    )
    parent_run_id: UUID | None = Field(
        None,
        description=(
            "Submit as a sub-obligation of this run: the child gets its own "
            "state document and failure domain, linked through "
            "parent_id/root_id"
        ),
    )
    timeout_seconds: int | None = Field(
        None, ge=10, le=86400,
        description="Per-attempt lease duration; None uses the worker default",
    )
    max_retries: int | None = Field(
        None, ge=1, le=100,
        description="Attempt budget; None uses the kernel default (3)",
    )


class ExecutorClaimRequest(Base):
    """Claim an existing obligation for a detached executor.

    Attributes:
        flow_name: Flow the obligation belongs to (part of its key).
        executor: Stable identifier of the claiming executor (harness /
            session id) — recorded as the attempt's executor and required
            for every subsequent fenced call.
        ttl_seconds: Lease duration per renewal; renew well within it.
    """
    flow_name: str = Field(min_length=1)
    executor: str = Field(min_length=1, max_length=256)
    ttl_seconds: int | None = Field(None, ge=10, le=86400)


class ExecutorClaimResponse(Base):
    """The claimed obligation and the fencing token.

    Attributes:
        run_id: The obligation claimed.
        epoch: Fence token — pass it to every subsequent call; a 409 on a
            later call means the lease was stolen and the outcome must be
            discarded.
        deadline_at: Current lease expiry (ISO).
        attempt: This attempt's number.
        kwargs: The obligation's contract inputs.
        adjudication: ``auto`` or ``gated`` — a gated obligation suspends
            on a returned outcome instead of self-discharging.
        signals: Signals already pending at claim time.
    """
    run_id: UUID
    epoch: int
    deadline_at: str
    attempt: int
    kwargs: dict[str, Any]
    adjudication: str
    signals: dict[str, Any]


class ExecutorRenewRequest(Base):
    """Heartbeat: renew the lease and observe signals."""
    flow_name: str = Field(min_length=1)
    executor: str = Field(min_length=1, max_length=256)
    epoch: int = Field(ge=1)
    ttl_seconds: int | None = Field(None, ge=10, le=86400)


class ExecutorRenewResponse(Base):
    """Renewal outcome: new deadline and pending signals."""
    run_id: UUID
    deadline_at: str
    signals: dict[str, Any]


class ExecutorEffectRequest(Base):
    """Record a side-effect exactly once per occurrence.

    The occurrence key derives from (obligation, name, occurrence) — never
    the attempt — so retried executors converge on the recorded result.
    """
    flow_name: str = Field(min_length=1)
    executor: str = Field(min_length=1, max_length=256)
    epoch: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=256)
    occurrence: str = Field("1", min_length=1, max_length=256)
    result: Any = None


class ExecutorEffectResponse(Base):
    """The recorded effect result.

    ``produced`` is False when a previous execution's result was returned.
    """
    run_id: UUID
    name: str
    occurrence: str
    result: Any
    produced: bool


class ExecutorOutcomeRequest(Base):
    """Report how the attempt's execution ended.

    Attributes:
        outcome: ``returned`` (normal end — auto-verdict or gate applies),
            ``raised`` (failure — retry budget decides), or ``interrupted``
            (a honoured cancel/interrupt signal).
        error: Short machine-readable error class when ``raised``.
    """
    flow_name: str = Field(min_length=1)
    executor: str = Field(min_length=1, max_length=256)
    epoch: int = Field(ge=1)
    outcome: str = Field(pattern="^(returned|raised|interrupted)$")
    error: str | None = Field(None, max_length=256)


class ExecutorOutcomeResponse(Base):
    """The route the account took: completed/gated/pending/failed/canceled."""
    run_id: UUID
    status: str


class AdjudicationRequest(Base):
    """API model for resolving a gated obligation.

    Attributes:
        decision: ``accepted`` discharges the obligation; ``rejected``
            reopens it when the attempt budget allows, else abandons it.
        actor: Principal rendering the verdict — recorded in the account
            and checked against the flow's gate policy.
        reason: Optional short ground for the decision.
    """
    decision: str = Field(pattern="^(accepted|rejected)$")
    actor: str = Field(min_length=1, max_length=256)
    reason: str | None = Field(None, max_length=1024)


class AdjudicationResponse(Base):
    """API model for the adjudication outcome.

    Attributes:
        run_id: The adjudicated obligation.
        status: Projection status after the verdict was applied.
        decision: The recorded decision.
    """
    run_id: UUID
    status: str
    decision: str


class FlowSubmissionResponse(Base):
    """API model for flow submission response.

    Returned when a flow is successfully submitted to the queue for
    asynchronous execution.

    Attributes:
        job_id: Unique job identifier in the queue.
        run_id: Identifier of the run this submission maps to — the
            pre-existing run when ``deduplicated`` is True.
        status: Initial status, always ``RunStatus.pending``.
        submitted_at: Timestamp when job was submitted.
        deduplicated: True when a dispatch_key resolved to a run created by
            an earlier submission; no new run was created.

    Example:
        >>> response = FlowSubmissionResponse(
        ...     job_id=UUID("..."),
        ...     run_id=UUID("..."),
        ...     submitted_at=datetime.now()
        ... )
    """
    job_id: UUID
    run_id: UUID
    status: RunStatus = Field(default=RunStatus.pending)
    submitted_at: TimestampDTO
    deduplicated: bool = False


class CancelRunResponse(Base):
    """API model for a run-cancellation request's outcome.

    Attributes:
        run_id: The targeted run.
        status: Run status after the request — ``canceled`` when the run
            was closed directly (it was not executing), ``running`` when a
            cooperative cancel was flagged for the owning worker, or the
            pre-existing terminal status when the run was already closed.
        cancel_requested: Whether the cooperative-cancellation flag is set.
    """
    run_id: UUID
    status: RunStatus
    cancel_requested: bool


class RunQueryRequest(Base):
    """API model for querying recent run states.

    Attributes:
        names: Optional list of flow names to filter by; ``None`` means all
            registered flows.
        last_n: Maximum number of most-recent runs to return per flow.

    Example:
        >>> request = RunQueryRequest(names=["ingest", "transform"], last_n=10)
    """
    names: list[str] | None = Field(
        None,
        description="Optional list of flow names to filter by"
    )
    last_n: int = Field(
        5,
        description="Most recent runs per flow to return"
    )


class LogQueryRequest(Base):
    """API model for fetching a single run with its log entries.

    Attributes:
        flow_name: Name of the flow that owns the run.
        run_id: UUID of the run to fetch.
        with_logs: When ``True`` (default) the response includes all log
            entries recorded during the run.

    Example:
        >>> request = LogQueryRequest(
        ...     flow_name="ingest",
        ...     run_id=UUID("018f..."),
        ... )
    """
    flow_name: str = Field(description="Name of the flow that owns the run")
    run_id: UUID = Field(description="UUID of the run to fetch")
    with_logs: bool = Field(True, description="Include log entries in the response")


class SpanEventDTO(Base):
    """API DTO for a log event recorded inside a span.

    Mirrors :class:`~flowlet.models.SpanEvent`.

    Attributes:
        ts: Wall-clock timestamp of the event.
        message: Event/log message body.
        attributes: Structured event attributes (e.g. ``log.level``).
    """
    ts: TimestampDTO | None = None
    message: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class SpanRecordDTO(Base):
    """API DTO for one finished span of a run.

    Mirrors :class:`~flowlet.models.SpanRecord`.

    Attributes:
        run_id: Identifier of the enclosing run (equals the trace id).
        span_id: OTel span id, 16-char hex string.
        parent_span_id: Parent span id, or ``None`` for the root span.
        name: Span (flow/task) name.
        flow_name: Root flow name.
        attempt: Execution attempt this span belongs to.
        span_type: Whether the span is a flow or a task.
        status: Terminal status of the span.
        status_message: Error description when failed.
        start_ts: Span start time.
        end_ts: Span end time.
        events: Log events recorded inside the span.
        attributes: Remaining span attributes.
    """
    run_id: UUID | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    name: str | None = None
    flow_name: str | None = None
    attempt: int = 1
    span_type: RunType | None = None
    status: RunStatus | None = None
    status_message: str | None = None
    start_ts: TimestampDTO | None = None
    end_ts: TimestampDTO | None = None
    events: list[SpanEventDTO] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class RunSummaryDTO(Base):
    """API DTO for a single span's summary within a run.

    Mirrors :class:`~flowlet.models.RunSummary`. The tree of ``children``
    recursively represents the full span hierarchy of a run.

    Attributes:
        span_id: Unique identifier of the span.
        span_name: Human-readable name of the span.
        span_type: Whether this span is a flow or a task.
        status: Most recent execution status of the span.
        start_ts: Wall-clock start time, or ``None`` if not yet started.
        end_ts: Wall-clock end time, or ``None`` if still running.
        children: Nested summaries of child spans.
        duration: Computed human-readable elapsed time, or ``None`` if
            start or end is missing.
    """
    span_id: str
    span_name: str
    span_type: RunType
    status: RunStatus
    start_ts: TimestampDTO | None
    end_ts: TimestampDTO | None
    children: list[RunSummaryDTO]

    @computed_field
    @property
    def duration(self) -> str | None:
        """Human-readable elapsed time between start and end.

        Returns:
            Formatted string such as ``"5s"`` or ``"2h 15m"``, or ``None``
            if either timestamp is absent.
        """
        if self.start_ts is None or self.end_ts is None:
            return None
        return humanize_timedelta(self.end_ts - self.start_ts)


class RunDTO(RunSummaryDTO):
    """API DTO for a complete run including its log entries.

    Extends :class:`RunSummaryDTO` with the full list of recorded spans.

    Attributes:
        logs: All span records for this run (all attempts), in file order.
    """
    logs: list[SpanRecordDTO]


class RunStateDTO(Base):
    """API DTO for the worker-owned execution state of a run.

    Mirrors :class:`~flowlet.models.RunState`. Exposes liveness and retry
    metadata written atomically by the owning worker on every state transition.

    Attributes:
        run_id: Unique identifier of the run.
        flow_name: Name of the flow being executed.
        status: Current execution status.
        worker_id: Identifier of the owning worker process.
        started_at: Timestamp when the run was first started.
        ended_at: Timestamp when the run finished, or ``None`` if still active.
        attempt: Current attempt number (1-based).
        max_retries: Maximum number of retry attempts allowed.
    """
    run_id: UUID
    flow_name: str
    status: RunStatus
    worker_id: str
    started_at: TimestampDTO
    ended_at: TimestampDTO | None
    attempt: int
    max_retries: int

