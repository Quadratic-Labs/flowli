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

from attrs import Factory, define, field, resolve_types

from flowlet.types import JsonData, Timestamp


class ReportedStatus(StrEnum):
    """Projection statuses for the API/history surface.

    Derived from the account (see :meth:`ObligationRecord.summary`), never
    stored as authority.  ``retry``/``warning``/``stopped`` survive only so
    archived pre-account records still parse.
    """
    pending = "pending"
    running = "running"
    gated = "gated"
    held = "held"
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
        span_type: Open label for the span's kind (taskflow emits
            ``flow`` / ``task``); None when the span carried none.
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
    span_type: str | None = field(default=None)
    status: ReportedStatus
    status_message: str | None = field(default=None)
    start_ts: Timestamp
    end_ts: Timestamp | None = field(default=None)
    events: list[SpanEvent] = Factory(list)
    attributes: dict[str, Any] = Factory(dict)


@define(slots=True, kw_only=True)
class TraceSummary:
    """
    A run's status.

    Aggregates a run's spans into a single stat summary.
    Recursively embeds children's spans as well.
    The root span summary gives the entire run summary.

    Attributes:
        span_id: span's identifier (16-char hex OTel span id).
        span_name: span's name.
        span_type: span's open kind label, when recorded.
        status: span's most recent status.
        start_ts: span's starting time.
        end_ts: span's ending time.
        children: span's children span's summaries.
    """
    span_id: str
    span_name: str
    span_type: str | None = field(default=None)
    status: ReportedStatus
    start_ts: Timestamp | None = field(default=None)
    end_ts: Timestamp | None = field(default=None)
    children: list[TraceSummary] = Factory(list)

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
class Trace(TraceSummary):
    """
    A complete run with its logs and computed summary.

    Combines a run's logs with its hierarchical summary for efficient querying.

    Attributes:
        summary: Hierarchical summary computed from the logs.
        logs: All log entries for this run, in chronological order.
    """
    logs: JsonData


# `children: list[TraceSummary]` is self-referential: under lazy annotations the
# name is unbound while @define runs, so the field type is captured as
# list[ForwardRef(...)] — unhashable on 3.14, which breaks cattrs' hook cache.
# Resolving eagerly replaces it with the real class on both fields tuples.
resolve_types(TraceSummary)
resolve_types(Trace)


@define(slots=True, kw_only=True)
class ObligationSummary:
    """A flat projection of one obligation's account, for the read surface.

    Derived from :class:`ObligationRecord` via :meth:`ObligationRecord.summary`
    — never authoritative.  The API, SQLite caches, and the history log keep
    consuming this stable shape while the durable payload is the full
    account.

    Attributes:
        run_id: The obligation's id.
        flow_name: Name of the flow.
        status: Derived projection status (see ReportedStatus).
        worker_id: Executor of the most recent attempt ("" if none).
        started_at: When the obligation was created.
        ended_at: When the obligation closed (discharged/abandoned).
        attempt: Number of attempts recorded so far.
        max_retries: Budget of consuming attempts allowed (``raised`` /
            ``rejected``; crashed and interrupted attempts are free).
        kwargs: Flow keyword arguments.
        cancel_requested: Whether cancellation was requested.
    """
    run_id: UUID
    flow_name: str
    status: ReportedStatus
    worker_id: str
    started_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    attempt: int = 1
    max_retries: int = 3
    kwargs: dict[str, Any] = Factory(dict)
    cancel_requested: bool = False


class ObligationStatus(StrEnum):
    """Lifecycle of an obligation — the unit of intent.

    ``held`` is the entry gate, symmetric to ``awaiting_review`` on
    the exit (abstractions v0.3, delta 4): the obligation is durably on the
    books but not claimable until a fenced admission releases it.  The
    kernel records the state "not yet eligible"; *when* to release is
    controller logic (a dependency completing, a human approving).
    """
    held = "held"
    open = "open"
    awaiting_review = "awaiting_review"
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


class Decision(StrEnum):
    """The review of an attempt's outcome against the contract."""
    approved = "approved"
    rejected = "rejected"


@define(slots=True, kw_only=True)
class Review:
    """A recorded review of one attempt.

    Attributes:
        decision: Approved or rejected.
        by: Who reviewed — ``auto`` for the default policy (returned ⇒
            approved), a gate name, or a principal for human review.
        decided_at: When the review was recorded.
        reason: Optional short machine-readable ground for the decision.
        evidence_ref: JSON-safe reference to the grounds of the decision
            (review findings, gate evidence) — a content-addressed ref or
            small JSON, never the body itself; symmetric to
            :attr:`Effect.result_ref`.  A rejecting review's evidence is
            what seeds the next attempt's envelope.
    """
    decision: Decision
    by: str = "auto"
    decided_at: Timestamp
    reason: str | None = field(default=None)
    evidence_ref: Any = field(default=None)


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
        review: The review of this attempt's outcome, once recorded.
        resumed_from: Prior attempt number whose substrate (session,
            worktree, resume artifact) this attempt continued; None for a
            fresh start.  Cost/provenance legibility — the account shows
            continuation instead of faking fresh starts.
    """
    n: int
    executor: str
    started_at: Timestamp
    ended_at: Timestamp | None = field(default=None)
    outcome: AttemptOutcome | None = field(default=None)
    error: str | None = field(default=None)
    review: Review | None = field(default=None)
    resumed_from: int | None = field(default=None)

    def consumes_budget(self) -> bool:
        """Whether this attempt bills the obligation's attempt budget.

        ``raised`` consumes; ``returned`` consumes once its review is
        ``rejected``; ``crashed``/``interrupted`` attempts (and in-flight
        ones) are free — infrastructure death and deliberate suspension are
        not failures of the work, so crash, interrupt/resume, and rework
        must not bill identically (abstractions v0.3, delta 1).  This holds
        regardless of any review recorded on a crashed attempt (crash
        accounting stamps ``rejected``/``lease_expired`` as a fact of
        review, not as a budget event).
        """
        if self.outcome == AttemptOutcome.raised:
            return True
        if self.outcome == AttemptOutcome.returned:
            return (
                self.review is not None
                and self.review.decision == Decision.rejected
            )
        return False


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
        max_retries: Attempt budget, counted over *consuming* attempts —
            ``raised`` outcomes and ``rejected`` reviews on returned
            attempts; ``crashed``/``interrupted`` attempts are free.
        review_policy: How done-ness is decided — ``auto`` applies the
            default policy (returned ⇒ approved) at the end of each attempt;
            ``gated`` suspends the obligation as awaiting_review until
            an authorized review arrives through the review API.
        admission: How eligibility to run is decided — ``auto`` obligations
            are claimable from creation; ``gated`` obligations are born
            ``held`` and become claimable only when a fenced admission
            releases them (the entry gate, mirror of ``review``).
        admitted_at: When a held obligation's admission was released.
        admitted_by: Who released it — a principal, or a controller acting
            on a completion event.
        caused_by: Provenance of this obligation — what event or decision
            spawned it (e.g. ``dispatch:<key>``, ``plan:<delta>``,
            ``review:<run_id>`` for an reviewer obligation); account
            data, never scheduling data.
        reviewer_id: While ``awaiting_review``, the obligation
            minted to record this one's review — the parked account
            answers "who owes me the review", not just "I'm waiting"
            (abstractions v0.3, delta 5).  Cleared when a review lands;
            re-minting after an abandoned reviewer overwrites it.
        created_at: When the obligation was recorded.
        closed_at: When it was discharged or abandoned.
        status: Current lifecycle state.
        cause: Machine-readable ground for abandonment (e.g. ``canceled``,
            ``max_retries_exceeded``, ``rejected``).
        cancel_requested: Whether cancellation was requested (record; the
            live channel is the cancel signal object).
        flow_version: Version of the flow's code/config the obligation was
            created against (a release tag, git sha, or harness config
            hash) — provenance for replay legibility: later attempts may
            run newer code, and the account shows against what the
            obligation was minted.  Recording only; matching is policy.
    """
    id: UUID
    flow_name: str
    flow_version: str | None = field(default=None)
    kwargs: dict[str, Any] = Factory(dict)
    parent_id: UUID | None = field(default=None)
    root_id: UUID | None = field(default=None)
    max_retries: int = 3
    review_policy: str = "auto"
    admission: str = "auto"
    admitted_at: Timestamp | None = field(default=None)
    admitted_by: str | None = field(default=None)
    reviewer_id: UUID | None = field(default=None)
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
class Consumption:
    """A recorded message consumption — the recv checkpoint.

    Identity derives from the obligation, the topic, and the consumption's
    ordinal within that topic — never from the attempt that happened to
    execute the recv — so a retried attempt *replays* recorded consumptions
    in order (receiving identical messages) before consuming fresh ones:
    the effect discipline applied to the message channel.

    Attributes:
        topic: Channel the message was consumed from.
        message_id: The consumed message's id (uuid7 hex — the topic's
            ordering key; the last entry per topic is the topic's cursor).
        attempt_n: Which attempt recorded the consumption.
        consumed_at: When it was recorded.
    """
    topic: str
    message_id: str
    attempt_n: int
    consumed_at: Timestamp


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
        consumptions: Recorded message consumptions, in recv order — the
            per-topic cursor and replay log of the message channel.
    """
    obligation: Obligation
    attempts: list[Attempt] = Factory(list)
    effects: list[Effect] = Factory(list)
    consumptions: list[Consumption] = Factory(list)

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

    def consumed_attempts(self) -> int:
        """The number of attempts that consumed the budget.

        Counts ``raised`` outcomes and ``rejected`` reviews on returned
        attempts (see :meth:`Attempt.consumes_budget`); ``crashed`` and
        ``interrupted`` attempts are free.
        """
        return sum(1 for attempt in self.attempts if attempt.consumes_budget())

    def retries_left(self) -> bool:
        """Whether the budget allows another attempt.

        The budget is counted over *consuming* attempts only, so a
        resumption-heavy healthy obligation (crashes recovered, suspensions
        resumed) never reads as a retry storm.  Crash-loop protection is
        policy (pause/escalate), never budget; :meth:`backoff_seconds`
        still paces every attempt.
        """
        return self.consumed_attempts() < self.obligation.max_retries

    def backoff_seconds(self) -> int:
        """Retry backoff before the next attempt: 2, 4, 8, ... capped at 300.

        Derived from the account (the attempt count), never from a message,
        so every wake-up channel — queue delay, sweeper grace, account
        polling — applies the same window.  Deliberately counts every
        attempt, consuming or not: free crashed attempts must still back
        off, or a crash loop spins at full speed.
        """
        return min(2 ** len(self.attempts), 300)

    # -- account transitions -------------------------------------------------

    def begin_attempt(
        self, executor: str, *, resumed_from: int | None = None
    ) -> Attempt:
        """Append a fresh in-flight attempt for *executor*.

        Args:
            executor: Who runs the attempt.
            resumed_from: Prior attempt whose substrate this attempt
                continues; must reference a recorded attempt.

        Raises:
            ValueError: *resumed_from* does not reference a prior attempt.
        """
        if resumed_from is not None and not 1 <= resumed_from <= len(self.attempts):
            raise ValueError(
                f"resumed_from={resumed_from} does not reference a prior attempt"
            )
        attempt = Attempt(
            n=len(self.attempts) + 1,
            executor=executor,
            started_at=Timestamp.now(),
            resumed_from=resumed_from,
        )
        self.attempts.append(attempt)
        return attempt

    def record_outcome(
        self,
        outcome: AttemptOutcome,
        *,
        error: str | None = None,
        review: Review | None = None,
    ) -> None:
        """Record the open attempt's outcome (and optionally its review)."""
        attempt = self.open_attempt
        if attempt is None:
            return  # nothing in flight — outcome already accounted
        attempt.outcome = outcome
        attempt.ended_at = Timestamp.now()
        attempt.error = error
        attempt.review = review

    @property
    def pending_review_attempt(self) -> Attempt | None:
        """The attempt awaiting review (outcome recorded, no review)."""
        last = self.last_attempt
        if last is not None and last.outcome is not None and last.review is None:
            return last
        return None

    def suspend_for_review(self) -> None:
        """Park the obligation until an authorized review arrives.

        The attempt's outcome is already recorded; its review stays pending
        — waiting is an obligation state, never an attempt outcome.
        """
        self.obligation.status = ObligationStatus.awaiting_review

    def assign_reviewer(self, reviewer_id: UUID) -> None:
        """Record which obligation owes this one its review.

        The review-as-work linkage: ``awaiting_review`` is the
        lock; the reviewer obligation is the job.  The parked account
        should answer "who owes me the review", not just "I'm waiting".
        Overwriting is allowed — re-minting after an abandoned reviewer
        replaces the debt-holder; the event log keeps the history.

        Args:
            reviewer_id: The obligation minted to record the review.

        Raises:
            ValueError: The obligation is not awaiting review.
        """
        if self.obligation.status != ObligationStatus.awaiting_review:
            raise ValueError(
                "obligation is not awaiting review "
                f"(status: {self.obligation.status})"
            )
        self.obligation.reviewer_id = reviewer_id

    def decide(self, review: Review, *, extend_budget: int = 0) -> None:
        """Record an external review on the pending attempt and route it.

        Approved discharges the obligation.  Rejected first grants
        *extend_budget* extra attempts (the resume path for an obligation
        gated on exhaustion: fix the environment, extend, re-run), then
        reopens the obligation when the budget allows another attempt, and
        abandons it (cause ``rejected``) otherwise.  Either way the review
        settles the debt: ``reviewer_id`` is cleared.
        """
        attempt = self.pending_review_attempt
        if attempt is None:
            raise ValueError("no attempt is awaiting review")
        attempt.review = review
        self.obligation.reviewer_id = None
        if review.decision == Decision.approved:
            self.discharge()
            return
        self.obligation.max_retries += extend_budget
        if self.retries_left():
            self.obligation.status = ObligationStatus.open
        else:
            self.abandon("rejected")

    def admit(self, by: str) -> None:
        """Release a held obligation's admission — the entry-gate mirror of
        :meth:`decide`.

        The kernel records the state change and who caused it; *why* now is
        the right moment (a dependency discharged, a human approved) is the
        caller's — a controller's — knowledge, carried in the event log.

        Args:
            by: Who released the hold — a principal, or a controller acting
                on a completion event.

        Raises:
            ValueError: The obligation is not held.
        """
        if self.obligation.status != ObligationStatus.held:
            raise ValueError(
                f"obligation is not held (status: {self.obligation.status})"
            )
        self.obligation.status = ObligationStatus.open
        self.obligation.admitted_at = Timestamp.now()
        self.obligation.admitted_by = by

    def record_effect(self, effect: Effect) -> None:
        """Append a recorded side-effect to the account."""
        self.effects.append(effect)

    def consumptions_for(self, topic: str) -> list[Consumption]:
        """The topic's recorded consumptions, in recv order."""
        return [c for c in self.consumptions if c.topic == topic]

    def record_consumption(self, topic: str, message_id: str) -> Consumption:
        """Append a message consumption — the recv checkpoint.

        Written under the lease fence by whoever holds the attempt, so only
        the current attempt can advance a topic's cursor.
        """
        entry = Consumption(
            topic=topic,
            message_id=message_id,
            attempt_n=len(self.attempts),
            consumed_at=Timestamp.now(),
        )
        self.consumptions.append(entry)
        return entry

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

    def summary(self) -> ObligationSummary:
        """Project the account onto the flat ObligationSummary read surface."""
        obligation = self.obligation
        if obligation.status == ObligationStatus.discharged:
            status = ReportedStatus.completed
        elif obligation.status == ObligationStatus.abandoned:
            status = (
                ReportedStatus.canceled
                if obligation.cause == "canceled"
                else ReportedStatus.failed
            )
        elif obligation.status == ObligationStatus.awaiting_review:
            status = ReportedStatus.gated
        elif obligation.status == ObligationStatus.held:
            status = ReportedStatus.held
        elif self.open_attempt is not None:
            status = ReportedStatus.running
        else:
            status = ReportedStatus.pending
        last = self.last_attempt
        return ObligationSummary(
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


@define(slots=True, kw_only=True)
class FlowJob:
    """
    A flow execution wake-up message for queue processing.

    The queue is purely a work-distribution signal: retry accounting and
    ownership live in ``ObligationSummary``, never in the message.  A worker acks the
    message as soon as the run's state is resolved; duplicate deliveries are
    harmless because the state machine drops them (busy/closed).

    Attributes:
        job_id: Unique job identifier in the queue (one per message).
        run_id: Pre-generated obligation id for tracking execution.
        flow_name: Name of the flow to execute.
        flow_version: Version of the flow's code/config, stamped on the
            obligation at creation (provenance; see Obligation).
        kwargs: Validated keyword arguments to pass to the flow.
        submitted_at: Timestamp when job was submitted to queue.
        max_retries: Budget of consuming attempts allowed (``raised`` /
            ``rejected``; crashed and interrupted attempts are free).
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
    flow_version: str | None = field(default=None)
    kwargs: dict[str, Any] = Factory(dict)
    submitted_at: Timestamp = Factory(Timestamp.now)
    max_retries: int = 3
    timeout_seconds: int | None = field(default=None)
    parent_id: UUID | None = field(default=None)
    root_id: UUID | None = field(default=None)
    caused_by: str | None = field(default=None)
