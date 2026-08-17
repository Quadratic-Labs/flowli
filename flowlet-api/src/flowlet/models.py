"""
Data models for flow execution tracking.

This module defines attrs dataclasses for representing flow execution data
in a type-safe, immutable way. These models are used throughout the repository
layer for data transfer between components.
"""
from collections.abc import Mapping
from datetime import timedelta
from enum import StrEnum
from typing import Any
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
    """Projection statuses for the API/history surface.

    Derived from the account (see :meth:`ObligationRecord.summary`), never
    stored as authority.  ``retry``/``warning``/``stopped`` survive only so
    archived pre-account records still parse.
    """
    pending = "pending"
    running = "running"
    gated = "gated"
    retry = "retry"
    failed = "failed"
    completed = "completed"
    warning = "warning"
    canceled = "canceled"
    stopped = "stopped"

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
    """A flat projection of one obligation's account, for the read surface.

    Derived from :class:`ObligationRecord` via :meth:`ObligationRecord.summary`
    — never authoritative.  The API, SQLite caches, and the history log keep
    consuming this stable shape while the durable payload is the full
    account.

    Attributes:
        run_id: The obligation's id.
        flow_name: Name of the flow.
        status: Derived projection status (see RunStatus).
        worker_id: Executor of the most recent attempt ("" if none).
        started_at: When the obligation was created.
        ended_at: When the obligation closed (discharged/abandoned).
        attempt: Number of attempts recorded so far.
        max_retries: Maximum number of execution attempts allowed.
        kwargs: Flow keyword arguments.
        cancel_requested: Whether cancellation was requested.
    """
    run_id: UUID
    flow_name: str
    status: RunStatus
    worker_id: str
    started_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    attempt: int = 1
    max_retries: int = 3
    kwargs: dict[str, Any] = Factory(dict)
    cancel_requested: bool = False

# ---
# endregion


# region @models.account
# ---
# role: datatype
# intent: the account model — obligation, attempts, verdicts as durable record
# description: >
#   An ObligationRecord is the payload of one obligation's lease document:
#   the obligation (unit of intent — what was promised, including its place
#   in a parent/root hierarchy for sub-obligations), its attempts (units of
#   execution, each with an explicit outcome), and per-attempt verdicts
#   (units of judgment — how the outcome was adjudicated).  Execution only
#   proposes entries into this account; the account is the system of
#   record.  Done-ness is a recorded verdict, not a return code: the
#   auto-verdict (returned ⇒ accepted) is merely the default adjudication
#   policy, and gates/human adjudication slot into the same fields.
# rules:
#   - The account MUST be append-only in spirit: attempts are appended and
#     their outcome recorded once; never rewritten.
#   - Every attempt MUST end with an explicit outcome — crash accounting
#     happens at the fenced steal that discovers the crash.
#   - Obligation status transitions MUST go through the record methods so
#     closed_at/cause stay consistent.
#   - RunState/RunStatus are projections via summary(); they MUST NOT be
#     stored as authority.
# dependencies:
#   - types
# aliases:
#   - account
#   - obligation
#   - attempt
#   - verdict
# triggers:
#   - what constitutes a workflow
#   - how is done-ness decided
#   - how are attempts recorded
# ---

class ObligationStatus(StrEnum):
    """Lifecycle of an obligation — the unit of intent."""
    open = "open"
    awaiting_adjudication = "awaiting_adjudication"
    discharged = "discharged"
    abandoned = "abandoned"

    def is_closed(self) -> bool:
        return self.value in ("discharged", "abandoned")


class AttemptOutcome(StrEnum):
    """How an attempt's execution ended — fact, prior to judgment."""
    returned = "returned"        # the callable returned normally
    raised = "raised"            # the callable raised an exception
    crashed = "crashed"          # the executor died; lease expired mid-flight
    interrupted = "interrupted"  # deliberately stopped (cancel)


class VerdictDecision(StrEnum):
    """The adjudication of an attempt's outcome against the contract."""
    accepted = "accepted"
    rejected = "rejected"


@define(slots=True, kw_only=True)
class Verdict:
    """A recorded adjudication of one attempt.

    Attributes:
        decision: Accepted or rejected.
        by: Who adjudicated — ``auto`` for the default policy (returned ⇒
            accepted), a gate name, or a principal for human adjudication.
        rendered_at: When the verdict was recorded.
        reason: Optional short machine-readable ground for the decision.
    """
    decision: VerdictDecision
    by: str = "auto"
    rendered_at: Timestamp
    reason: str | None = field(default=None)


@define(slots=True, kw_only=True)
class Attempt:
    """One execution attempt — the unit of execution, failure, and cost.

    Attributes:
        n: Attempt number, 1-based.
        executor: Worker that ran (or is running) this attempt.
        started_at: When the attempt was claimed.
        ended_at: When its outcome was recorded.
        outcome: Explicit end of execution; None while in flight.
        error: Exception type name when the outcome is ``raised``.
        verdict: The adjudication of this attempt's outcome, when rendered.
    """
    n: int
    executor: str
    started_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    outcome: AttemptOutcome | None = field(default=None)
    error: str | None = field(default=None)
    verdict: Verdict | None = field(default=None)


@define(slots=True, kw_only=True)
class Obligation:
    """The unit of intent: what was promised, durable and adjudicable.

    Attributes:
        id: The obligation's stable identity (uuid7; doubles as run_id and
            OTel trace id on the read surface).
        flow_name: Name of the flow that discharges the obligation.
        kwargs: Validated flow arguments — the contract's inputs.
        parent_id: Parent obligation for sub-obligations; None for roots.
        root_id: Root of the obligation tree; None for roots.
        max_retries: Attempt budget.
        adjudication: How done-ness is decided — ``auto`` applies the
            default policy (returned ⇒ accepted) at the end of each attempt;
            ``gated`` suspends the obligation as awaiting_adjudication until
            an authorized verdict arrives through the adjudication API.
        caused_by: Provenance of this obligation — what event or decision
            spawned it (e.g. ``dispatch:<key>``, ``plan:<delta>``); account
            data, never scheduling data.
        created_at: When the obligation was recorded.
        closed_at: When it was discharged or abandoned.
        status: Current lifecycle state.
        cause: Machine-readable ground for abandonment (e.g. ``canceled``,
            ``max_retries_exceeded``, ``rejected``).
        cancel_requested: Whether cancellation was requested (record; the
            live channel is the cancel signal object).
    """
    id: UUID
    flow_name: str
    kwargs: dict[str, Any] = Factory(dict)
    parent_id: UUID | None = field(default=None)
    root_id: UUID | None = field(default=None)
    max_retries: int = 3
    adjudication: str = "auto"
    caused_by: str | None = field(default=None)
    created_at: Timestamp
    closed_at: Timestamp | None = field(default=None)
    status: ObligationStatus = ObligationStatus.open
    cause: str | None = field(default=None)
    cancel_requested: bool = False


@define(slots=True, kw_only=True)
class Effect:
    """A recorded side-effect — the unit of value, idempotent by occurrence.

    The effect's *occurrence* identity is derived from the obligation and
    the effect's name, never from the attempt that happened to execute it —
    so retries converge on the recorded effect instead of re-firing it.
    The exactly-once guarantee is the claim on the effect's key; this entry
    is the account's reference to it.

    Attributes:
        name: What the effect is (e.g. ``send_email``).
        occurrence: Distinguishes deliberate repetitions of the same effect
            (a manual re-send mints a new occurrence); defaults to "1".
        attempt_n: Which attempt executed (or resolved) the effect.
        produced_at: When the effect was recorded.
        result_ref: JSON-safe result or content-addressed reference to it.
    """
    name: str
    occurrence: str = "1"
    attempt_n: int
    produced_at: Timestamp
    result_ref: Any = field(default=None)


@define(slots=True, kw_only=True)
class ObligationRecord:
    """The account of one obligation: intent plus the record of its discharge.

    Carried as the state payload of the obligation's lease document.
    Sub-obligations are separate records (own lease document, own failure
    domain) linked through ``obligation.parent_id``/``root_id``.

    Attributes:
        obligation: The unit of intent.
        attempts: Every execution attempt, in order, outcomes explicit.
        effects: Recorded side-effects, idempotent by occurrence key.
    """
    obligation: Obligation
    attempts: list[Attempt] = Factory(list)
    effects: list[Effect] = Factory(list)

    # -- reading the account ------------------------------------------------

    @property
    def last_attempt(self) -> Attempt | None:
        """The most recent attempt, or None before any claim."""
        return self.attempts[-1] if self.attempts else None

    @property
    def open_attempt(self) -> Attempt | None:
        """The in-flight attempt (no outcome recorded yet), if any."""
        last = self.last_attempt
        return last if last is not None and last.outcome is None else None

    def retries_left(self) -> bool:
        """Whether the attempt budget allows another attempt."""
        return len(self.attempts) < self.obligation.max_retries

    # -- account transitions -------------------------------------------------

    def begin_attempt(self, executor: str) -> Attempt:
        """Append a fresh in-flight attempt for *executor*."""
        attempt = Attempt(
            n=len(self.attempts) + 1,
            executor=executor,
            started_at=Timestamp.now(),
        )
        self.attempts.append(attempt)
        return attempt

    def record_outcome(
        self,
        outcome: AttemptOutcome,
        *,
        error: str | None = None,
        verdict: Verdict | None = None,
    ) -> None:
        """Record the open attempt's outcome (and optionally its verdict)."""
        attempt = self.open_attempt
        if attempt is None:
            return  # nothing in flight — outcome already accounted
        attempt.outcome = outcome
        attempt.ended_at = Timestamp.now()
        attempt.error = error
        attempt.verdict = verdict

    @property
    def pending_verdict_attempt(self) -> Attempt | None:
        """The attempt awaiting adjudication (outcome recorded, no verdict)."""
        last = self.last_attempt
        if last is not None and last.outcome is not None and last.verdict is None:
            return last
        return None

    def suspend_for_adjudication(self) -> None:
        """Park the obligation until an authorized verdict arrives.

        The attempt's outcome is already recorded; its verdict stays pending
        — waiting is an obligation state, never an attempt outcome.
        """
        self.obligation.status = ObligationStatus.awaiting_adjudication

    def adjudicate(self, verdict: Verdict) -> None:
        """Record an external verdict on the pending attempt and route it.

        Accepted discharges the obligation.  Rejected reopens it when the
        attempt budget allows another attempt, and abandons it (cause
        ``rejected``) otherwise.
        """
        attempt = self.pending_verdict_attempt
        if attempt is None:
            raise ValueError("no attempt is awaiting adjudication")
        attempt.verdict = verdict
        if verdict.decision == VerdictDecision.accepted:
            self.discharge()
        elif self.retries_left():
            self.obligation.status = ObligationStatus.open
        else:
            self.abandon("rejected")

    def record_effect(self, effect: Effect) -> None:
        """Append a recorded side-effect to the account."""
        self.effects.append(effect)

    def discharge(self) -> None:
        """Close the obligation as discharged (its contract was met)."""
        self.obligation.status = ObligationStatus.discharged
        self.obligation.closed_at = Timestamp.now()

    def abandon(self, cause: str) -> None:
        """Close the obligation as abandoned, recording why."""
        self.obligation.status = ObligationStatus.abandoned
        self.obligation.cause = cause
        self.obligation.closed_at = Timestamp.now()

    # -- projection -----------------------------------------------------------

    def summary(self) -> RunState:
        """Project the account onto the flat RunState read surface."""
        obligation = self.obligation
        if obligation.status == ObligationStatus.discharged:
            status = RunStatus.completed
        elif obligation.status == ObligationStatus.abandoned:
            status = (
                RunStatus.canceled
                if obligation.cause == "canceled"
                else RunStatus.failed
            )
        elif obligation.status == ObligationStatus.awaiting_adjudication:
            status = RunStatus.gated
        elif self.open_attempt is not None:
            status = RunStatus.running
        else:
            status = RunStatus.pending
        last = self.last_attempt
        return RunState(
            run_id=obligation.id,
            flow_name=obligation.flow_name,
            status=status,
            worker_id=last.executor if last is not None else "",
            started_at=obligation.created_at,
            ended_at=obligation.closed_at,
            attempt=len(self.attempts),
            max_retries=obligation.max_retries,
            kwargs=obligation.kwargs,
            cancel_requested=obligation.cancel_requested,
        )

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
        run_id: Pre-generated obligation id for tracking execution.
        flow_name: Name of the flow to execute.
        kwargs: Validated keyword arguments to pass to the flow.
        submitted_at: Timestamp when job was submitted to queue.
        max_retries: Maximum number of execution attempts allowed.
        timeout_seconds: Per-attempt lease duration; None uses the worker's
            default.
        parent_id: Parent obligation when submitting a sub-obligation.
        root_id: Root of the obligation tree (parent's root, or the parent
            itself); None for roots.

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
    parent_id: UUID | None = field(default=None)
    root_id: UUID | None = field(default=None)
    caused_by: str | None = field(default=None)

# ---
# endregion
