"""
FastAPI DTO models for API boundary.
"""
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, computed_field
from pydantic.functional_validators import BeforeValidator

from ..models import RunStatus, RunType


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
    return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)


TimestampDTO = Annotated[datetime, BeforeValidator(_coerce_timestamp)]
"""Pydantic-compatible mirror of :class:`~flowlet.types.Timestamp`.

Accepts a ``Timestamp`` attrs object (unwrapping ``.value``), a
timezone-aware ``datetime``, or an ISO 8601 string.  Always validates to
a UTC-aware ``datetime`` and serialises as a standard datetime string.
"""


class Base(BaseModel):
    """Base Pydantic model with common configuration.

    Enables automatic conversion from SQLAlchemy ORM objects.
    """
    model_config = ConfigDict(from_attributes=True)


# region @api.models.request
# ---
# role: api
# intent: define request and response envelope models for the API boundary
# description: >
#   Incoming request bodies and outgoing submission envelopes.
#   Validated by Pydantic at the FastAPI boundary before reaching the domain.
# rules:
#   - MUST validate all external inputs via Pydantic
#   - SHOULD use Field() for descriptions exposed in the OpenAPI schema
# dependencies:
#   - models.run
#   - models.job
# ---

class FlowArguments(Base):
    """API model for flow execution request.

    Accepts keyword arguments to pass to the flow function.

    Attributes:
        kwargs: Dictionary of keyword arguments for flow execution.

    Example:
        >>> flow_input = FlowArguments(kwargs={"user_id": 123, "mode": "test"})
    """
    kwargs: dict[str, Any] = Field(default_factory=dict)


class FlowSubmissionResponse(Base):
    """API model for flow submission response.

    Returned when a flow is successfully submitted to the queue for
    asynchronous execution.

    Attributes:
        job_id: Unique job identifier in the queue.
        status: Initial status, always ``RunStatus.pending``.
        submitted_at: Timestamp when job was submitted.

    Example:
        >>> response = FlowSubmissionResponse(
        ...     job_id=UUID("..."),
        ...     submitted_at=datetime.now()
        ... )
    """
    job_id: UUID
    status: RunStatus = Field(default=RunStatus.pending)
    submitted_at: TimestampDTO


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

# ---
# endregion


# region @api.models.dto
# ---
# role: api
# intent: define read DTOs that mirror domain models for API responses
# description: >
#   Pydantic models returned by GET endpoints. Each DTO mirrors a domain
#   model from models.py, mapping domain types (Timestamp, JsonData) to
#   JSON-serialisable primitives (datetime, dict, list).
#   TimestampDTO is the annotated datetime type that coerces Timestamp attrs
#   objects by unwrapping .value, so domain objects can be validated directly
#   without a manual serialisation step.
#   RunSummaryDTO nests children recursively, matching RunSummary's tree
#   structure. RunStateDTO exposes worker-owned execution state.
# rules:
#   - Fields MUST match their domain counterpart in name and nullability
#   - All timestamp fields MUST use TimestampDTO (not bare datetime)
#   - Extra log metadata MUST be forwarded as dict[str, str]
# dependencies:
#   - models.run
# ---

class RunLogDTO(Base):
    """API DTO for a single log event recorded during a run.

    Mirrors :class:`~flowlet.models.RunLog`. All fields are optional to
    accommodate partial projections returned by log queries.

    Attributes:
        flow_name: Name of the flow that produced this log.
        run_id: Identifier of the enclosing run.
        span_type: Whether the emitting span is a flow or a task.
        span_name: Name of the emitting span.
        span_id: Identifier of the emitting span.
        parent_span_id: Identifier of the parent span, or ``None`` for the root.
        ts: Wall-clock timestamp of the event.
        message: Log message body.
        level: Logging level (e.g. ``"INFO"``, ``"ERROR"``).
        extra: Arbitrary key-value metadata attached to the log record.
    """
    flow_name: str | None = None
    run_id: UUID | None = None
    span_type: RunType | None = None
    span_name: str | None = None
    span_id: UUID | None = None
    parent_span_id: UUID | None = None
    ts: TimestampDTO | None = None
    message: str | None = None
    level: str | None = None
    extra: dict[str, str] = Field(default_factory=dict)


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
    span_id: UUID
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

    Extends :class:`RunSummaryDTO` with the full ordered list of log events
    recorded during the run.

    Attributes:
        logs: All log entries for this run in chronological order.
    """
    logs: list[RunLogDTO]


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
        heartbeat_at: Timestamp of the last heartbeat from the worker.
        ended_at: Timestamp when the run finished, or ``None`` if still active.
        attempt: Current attempt number (1-based).
        max_retries: Maximum number of retry attempts allowed.
    """
    run_id: UUID
    flow_name: str
    status: RunStatus
    worker_id: str
    started_at: TimestampDTO
    heartbeat_at: TimestampDTO
    ended_at: TimestampDTO | None
    attempt: int
    max_retries: int

# ---
# endregion
