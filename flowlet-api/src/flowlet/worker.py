"""
Worker for executing one queued job against the obligation account model.

Ownership is a cairndb lease on the obligation's document: a worker claims
an obligation by acquiring the lease, and the claim transition — crash
accounting for a predecessor's in-flight attempt, then appending its own
attempt — is written atomically with the acquisition itself.  Every
subsequent write is epoch-fenced: a worker whose lease expired and was
stolen gets ``LeaseLost`` and discards its outcome.  There are no
background threads; a run past its lease deadline is simply reclaimable, by
another worker dequeuing a duplicate message or by the sweeper (see
``flowlet.sweeper``).  Flows renew the lease cooperatively via
``flowlet.heartbeat()`` (see ``flowlet.lease``), which also observes the
obligation's cancel signal.

The account discipline: execution never *is* the record — it *proposes*
entries into it.  An attempt is appended at claim, its outcome recorded
exactly once (``returned``/``raised``/``interrupted`` by its own executor,
``crashed`` by whoever discovers the corpse), and done-ness is a recorded
review.  The auto-review (returned ⇒ approved ⇒ discharged) is the
default review policy; gates and human review slot into the
same fields.

The queue is a pure wake-up signal.  Every dequeued message is acked as
soon as the obligation's state is resolved; retry accounting lives
exclusively in the account's attempt list.  Duplicate deliveries are
harmless by construction.

State Machine
-------------
``existing_state_case()`` maps the current :class:`StateView` (or ``None``)
to one of the ``JobState`` values:

    JobState  Condition                                        Action
    --------- ------------------------------------------------ ------------------------
    new       No document exists                               Acquire (attempt 1) → execute
    closed    Obligation discharged or abandoned               Ack, done (idempotent)
    busy      Lease actively held (unexpired holder)           Ack — another worker owns it
    expired   In-flight attempt, lease expired, budget left    Acquire (crash + new attempt) → execute
    failed    In-flight attempt, lease expired, budget spent   Acquire → crash + abandon → release
                                                               (gated: crash + park for review)
    ready     Released open obligation (parked after failure)  Acquire (new attempt) → execute

The classification is advisory (early-outs without churning the lease
epoch); the acquisition's ``state_fn`` re-derives the transition under CAS,
so races between the read and the acquire resolve correctly — including an
obligation that closed in between, which aborts the acquisition via
:class:`AlreadyClosed`.

On flow failure with budget left, the worker releases the lease with the
rejected attempt recorded and the obligation still open, then self-enqueues
a fresh wake-up message with exponential backoff.

Exhaustion is a judgment point: when the last attempt spends a *gated*
obligation's budget, the obligation parks ``awaiting_review`` with
the final outcome recorded and its review pending — a human can extend the
budget through the review endpoint (fix the environment, resume) or
close it with a rejected review.  Only auto-reviewed obligations
abandon themselves (``max_retries_exceeded``).
"""
import logging
from collections.abc import Callable
from enum import StrEnum
from typing import Protocol

from flowlet.events import EventLog
from flowlet.lease import LeaseLost, RunCancelled, RunLease, bind_lease, unbind_lease
from flowlet.models import (
    AttemptOutcome,
    Decision,
    FlowJob,
    Obligation,
    ObligationRecord,
    ObligationStatus,
    Review,
)
from flowlet.queue import JobQueueProtocol
from flowlet.repository import (
    AlreadyClosed,
    EffectRepository,
    MessageRepository,
    SignalRepository,
    StateRepository,
    StateView,
    Unclaimable,
)
from flowlet.repository.signals import CANCEL
from flowlet.types import Timestamp

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 900  # seconds — default lease per execution attempt


class Executor(Protocol):
    """The seam between the kernel and whatever does the work.

    An executor runs one attempt's work under an already-bound
    :class:`~flowlet.lease.RunLease` (so ``flowlet.heartbeat()`` and
    ``flowlet.effect()`` are ambient).  The kernel interprets its end:

    - return            → outcome ``returned`` (auto-review or gate)
    - raise RunCancelled → outcome ``interrupted``
    - raise LeaseLost    → the outcome is discarded (fenced)
    - raise anything     → outcome ``raised`` (retry budget decides)

    Controllers supply implementations: taskflow's RegistryExecutor invokes
    a decorated Python callable; CodeFlow's harness is the same role over
    the HTTP claim surface (it never reaches this in-process seam).
    """

    def execute(self, obligation, attempt: int) -> None:
        """Run the attempt's work for *obligation*."""
        ...


class JobState(StrEnum):
    """JobState determines the worker's action for this job."""
    new = "new"
    ready = "ready"
    busy = "busy"
    failed = "failed"
    expired = "expired"
    closed = "closed"
    gated = "gated"
    held = "held"


def existing_state_case(
    view: StateView | None, now: Timestamp | None = None
) -> JobState:
    """Determine the job's state from its obligation's lease document.

    Args:
        view: The current StateView, or None when no document exists.
        now: Reference time for lease expiry (defaults to Timestamp.now()).

    Returns:
        The JobState driving the worker's next action.
    """
    if view is None:
        return JobState.new
    record = view.record
    if record.obligation.status.is_closed():
        return JobState.closed
    if record.obligation.status == ObligationStatus.awaiting_review:
        return JobState.gated
    if record.obligation.status == ObligationStatus.held:
        return JobState.held
    if view.held(now):
        return JobState.busy
    if record.open_attempt is not None:
        # An in-flight attempt whose lease died — the executor crashed.
        if record.retries_left():
            return JobState.expired
        return JobState.failed
    return JobState.ready


def _ack_safely(queue: JobQueueProtocol, job: FlowJob) -> None:
    """Acknowledge a job, logging instead of raising on failure.

    An ack can fail when the claim window expired (stale pop receipt).
    The redelivered message is dropped by the state machine, so the
    failure must never abort the worker.

    Args:
        queue: Job queue the job was dequeued from.
        job: The job to acknowledge.
    """
    try:
        queue.ack(job.job_id)
    except Exception:
        logger.warning(
            "job_ack_failed",
            extra={"job_id": str(job.job_id), "run_id": str(job.run_id)},
            exc_info=True,
        )


def _emit(
    events: EventLog | None,
    record: ObligationRecord,
    event: str,
    actor: str,
    **kwargs,
) -> None:
    """Append a lifecycle event for *record* when an event log is configured."""
    if events is None:
        return
    events.append(
        flow_name=record.obligation.flow_name,
        run_id=record.obligation.id,
        event=event,
        actor=actor,
        attempt=len(record.attempts),
        **kwargs,
    )


def _auto_review(decision: Decision, reason: str | None = None) -> Review:
    """The default review policy, recorded like any other review."""
    return Review(
        decision=decision, by="auto", decided_at=Timestamp.now(), reason=reason
    )


class _ClaimCase(StrEnum):
    """What the atomic claim transition decided to do."""
    execute = "execute"
    canceled = "canceled"
    exhausted = "exhausted"
    gated = "gated"  # budget spent on a gated obligation: parked for judgment


def _claim_transition(
    existing: ObligationRecord | None,
    *,
    job: FlowJob,
    worker_id: str,
    cancel_pending: bool,
    gated: bool,
    decision: dict,
    resumed_from: int | None = None,
) -> ObligationRecord:
    """Pure transition applied atomically with the lease acquisition.

    Re-derives the claim decision from the account the CAS actually
    observed (the advisory pre-read may be stale): creates the obligation
    on first claim, does crash accounting for a dead in-flight attempt,
    honours a pending cancel, closes a spent budget, or appends this
    worker's attempt.  Records which branch it took in *decision*; a re-run
    after a lost CAS race simply overwrites it.

    Args:
        existing: Account currently in the lease document (None when the
            document is being created).
        job: The wake-up message being processed.
        worker_id: The acquiring worker.
        cancel_pending: Whether the obligation's cancel signal was observed.
        decision: Out-parameter receiving {"case": _ClaimCase}.
        resumed_from: Prior attempt whose substrate this attempt continues
            (external executors resuming from a resume artifact); recorded
            on the appended attempt, validated against the account.

    Returns:
        The account to write with the acquisition.

    Raises:
        AlreadyClosed: The obligation closed between the pre-read and the
            acquire.
    """
    if existing is None:
        record = ObligationRecord(
            obligation=Obligation(
                id=job.run_id,
                flow_name=job.flow_name,
                flow_version=job.flow_version,
                kwargs=job.kwargs,
                parent_id=job.parent_id,
                root_id=job.root_id,
                max_retries=job.max_retries,
                review_policy="gated" if gated else "auto",
                caused_by=job.caused_by,
                created_at=Timestamp.now(),
            )
        )
    else:
        record = existing

    if record.obligation.status.is_closed():
        raise AlreadyClosed(record)
    if record.obligation.status == ObligationStatus.awaiting_review:
        # Nothing to execute — the obligation waits on a review, not a worker.
        raise Unclaimable(record)
    if record.obligation.status == ObligationStatus.held:
        # Entry gate: the obligation waits on an admission, not a worker.
        raise Unclaimable(record)

    # Crash accounting: a dead holder's in-flight attempt ends here, judged
    # by whoever discovers it — never silently overwritten.  A crash never
    # spends the budget (crashed attempts are free); when it lands on an
    # already-exhausted gated obligation, its review stays pending: the
    # crashed attempt is what the human gate below will decide.
    if existing is not None and record.open_attempt is not None:
        parks = (
            not cancel_pending
            and not record.retries_left()
            and record.obligation.review_policy == "gated"
        )
        record.record_outcome(
            AttemptOutcome.crashed,
            review=None if parks else _auto_review(
                Decision.rejected, reason="lease_expired"
            ),
        )

    if cancel_pending:
        record.obligation.cancel_requested = True
        record.record_outcome(AttemptOutcome.interrupted)  # no-op if none open
        record.abandon("canceled")
        decision["case"] = _ClaimCase.canceled
        return record

    if not record.retries_left():
        # Exhaustion is a judgment point: a gated obligation parks for
        # human review (extend the budget to resume, reject to
        # close); only auto obligations close themselves.
        if record.obligation.review_policy == "gated":
            record.suspend_for_review()
            decision["case"] = _ClaimCase.gated
            return record
        record.abandon("max_retries_exceeded")
        decision["case"] = _ClaimCase.exhausted
        return record

    record.begin_attempt(worker_id, resumed_from=resumed_from)
    decision["case"] = _ClaimCase.execute
    return record


def conclude_attempt(
    lease,
    *,
    outcome: AttemptOutcome,
    error: str | None = None,
    queue: JobQueueProtocol | None = None,
    events: EventLog | None = None,
    actor: str,
    timeout_seconds: int | None = None,
) -> str:
    """Record an attempt's end and route the obligation — the shared epilogue.

    Used by the in-process worker after the flow call and by the
    external-executor surface when a detached harness reports its outcome.
    Applies the account discipline: record the outcome (with the
    auto-review where the review policy allows), then discharge,
    suspend for review, park for retry (self-enqueueing a wake-up
    when a queue is available — the sweeper is the fallback), or abandon.

    Args:
        lease: The held StateLease for the obligation.
        outcome: How execution ended — ``returned``, ``raised``, or
            ``interrupted`` (a honoured cancel).
        error: Exception type name when the outcome is ``raised``.
        queue: Optional queue for the retry wake-up.
        events: Optional run event log.
        actor: The executor concluding the attempt.
        timeout_seconds: Per-attempt lease override forwarded to the retry
            wake-up message.

    Returns:
        The resulting route — ``"canceled"``, ``"gated"``, ``"completed"``,
        ``"pending"``, ``"failed"`` — or ``"lost"`` when the lease was
        fenced and the outcome belongs to the new owner.
    """
    record = lease.record

    if outcome == AttemptOutcome.interrupted:
        record.obligation.cancel_requested = True
        record.record_outcome(AttemptOutcome.interrupted)
        record.abandon("canceled")
        if _release_terminal(lease, events, record, "canceled", actor,
                             from_status="running"):
            return "canceled"
        return "lost"

    if outcome == AttemptOutcome.returned:
        if record.obligation.review_policy == "gated":
            # No auto-review: the outcome is a fact, judgment is pending.
            record.record_outcome(AttemptOutcome.returned)
            record.suspend_for_review()
            try:
                lease.release(record)
            except LeaseLost:
                return "lost"
            _emit(
                events, record, "awaiting_review", actor,
                from_status="running", to_status="gated",
            )
            return "gated"
        record.record_outcome(
            AttemptOutcome.returned,
            review=_auto_review(Decision.approved),
        )
        record.discharge()
        if _release_terminal(lease, events, record, "completed", actor,
                             from_status="running"):
            return "completed"
        return "lost"

    # raised — retry accounting lives in the account, never the queue.
    # Record the outcome first: a raised attempt consumes the budget by
    # outcome alone, so exhaustion is only known once it is accounted.
    attempt = record.open_attempt
    record.record_outcome(AttemptOutcome.raised, error=error)
    if not record.retries_left() and record.obligation.review_policy == "gated":
        # Exhaustion is a judgment point, not an auto-review: the outcome
        # is a fact, judgment is pending.  A human can extend the budget
        # and resume (decide rejected + extend_budget) or close it.
        record.suspend_for_review()
        try:
            lease.release(record)
        except LeaseLost:
            return "lost"
        _emit(
            events, record, "awaiting_review", actor,
            from_status="running", to_status="gated", cause=error,
        )
        return "gated"

    if attempt is not None:
        attempt.review = _auto_review(Decision.rejected, reason=error)
    if record.retries_left():
        try:
            lease.release(record)
        except LeaseLost:
            return "lost"
        _emit(
            events, record, "retry_scheduled", actor,
            from_status="running", to_status="pending",
            cause=error,
        )
        if queue is not None:
            retry_job = FlowJob(
                run_id=record.obligation.id,
                flow_name=record.obligation.flow_name,
                kwargs=record.obligation.kwargs,
                max_retries=record.obligation.max_retries,
                timeout_seconds=timeout_seconds,
                parent_id=record.obligation.parent_id,
                root_id=record.obligation.root_id,
                caused_by=f"retry_of_attempt:{len(record.attempts)}",
            )
            try:
                queue.enqueue(retry_job, delay=record.backoff_seconds())
            except Exception:
                # The account already shows a rejected attempt with budget
                # left; the sweeper re-enqueues the parked obligation.
                logger.warning(
                    "job_retry_enqueue_failed",
                    extra={"run_id": str(record.obligation.id)},
                    exc_info=True,
                )
        return "pending"

    record.abandon("max_retries_exceeded")
    if _release_terminal(lease, events, record, "failed", actor,
                         from_status="running", cause=error):
        return "failed"
    return "lost"


def execute_job(
    queue: JobQueueProtocol,
    executor: Executor,
    state_repo: StateRepository,
    signals: SignalRepository,
    worker_id: str,
    default_timeout: int = DEFAULT_TIMEOUT,
    events: EventLog | None = None,
    review_policy_for: "Callable[[str], str] | None" = None,
) -> int:
    """Execute a single job from the queue under the account state machine.

    Dequeues one message, resolves the ``JobState``, acquires the
    obligation's lease with an atomic claim transition, acks the message,
    runs the flow, then records the attempt's outcome and review and
    releases the lease.  See the module docstring for the full transition
    table.

    While the flow runs, a :class:`~flowlet.lease.RunLease` is bound to the
    context so ``flowlet.heartbeat()`` can renew the lease and observe the
    cancel signal.  A cancel finalizes the obligation as abandoned
    (``canceled``); a lost lease discards the outcome without writing.

    Args:
        queue: Job queue to dequeue from.
        executor: What runs the attempt's work (the controller's half).
        state_repo: State repository owning the obligation lease documents.
        signals: Signal repository the cancel signal is read from.
        worker_id: Unique identifier for this worker instance.
        default_timeout: Lease seconds used when the job carries no
            timeout_seconds.
        events: Optional run event log receiving lifecycle events.
        review_policy_for: Per-flow review policy (``"auto"``/
            ``"gated"``) stamped on obligations *created* by this claim;
            None means auto.  Existing obligations keep the policy fixed
            at their creation.

    Returns:
        Exit code — 0 success or deliberate cancel, 1 failure, 2 no job or
        skipped.
    """
    job = queue.dequeue()
    if job is None:
        logger.info("no_jobs_available")
        return 2

    logger.info(
        "job_dequeued",
        extra={
            "job_id": str(job.job_id),
            "flow_name": job.flow_name,
            "run_id": str(job.run_id),
        },
    )

    # Admission: a pause at any covering scope blocks new claims; in-flight
    # attempts run to completion.  The parked obligation is re-enqueued by
    # the sweeper once the pause is revoked.
    paused = signals.paused_scopes(
        flow_name=job.flow_name,
        run_id=job.run_id,
        parent_id=job.parent_id,
        root_id=job.root_id,
    )
    if paused:
        _ack_safely(queue, job)
        logger.info(
            "job_admission_paused",
            extra={"run_id": str(job.run_id), "scopes": paused},
        )
        return 2

    # Advisory early-outs: skip closed/busy runs without bumping the epoch.
    view = state_repo.read(job.flow_name, job.run_id)
    job_state = existing_state_case(view)
    if job_state == JobState.closed:
        assert view is not None
        _ack_safely(queue, job)
        logger.info(
            "job_already_finished",
            extra={
                "run_id": str(job.run_id),
                "status": view.record.obligation.status,
            },
        )
        return 0
    if job_state in (JobState.busy, JobState.gated, JobState.held):
        assert view is not None
        # Duplicate wake-up for an actively-owned, review-parked, or
        # admission-held run: drop it.  Crashed owners are the sweeper's
        # job; gated runs resume through the review endpoint and held
        # runs through the admission endpoint, never a wake-up.
        _ack_safely(queue, job)
        logger.info(
            "job_not_claimable",
            extra={
                "run_id": str(job.run_id),
                "case": str(job_state),
                "holder": view.holder,
            },
        )
        return 2

    cancel_pending = signals.get(job.flow_name, job.run_id, CANCEL) is not None
    gated = (
        review_policy_for is not None
        and review_policy_for(job.flow_name) == "gated"
    )
    timeout = job.timeout_seconds or default_timeout
    decision: dict = {}
    try:
        lease = state_repo.acquire(
            job.flow_name,
            job.run_id,
            ttl=timeout,
            holder=worker_id,
            state_fn=lambda existing: _claim_transition(
                existing,
                job=job,
                worker_id=worker_id,
                cancel_pending=cancel_pending,
                gated=gated,
                decision=decision,
            ),
        )
    except Unclaimable:
        _ack_safely(queue, job)
        logger.info("job_awaiting_review", extra={"run_id": str(job.run_id)})
        return 2
    except AlreadyClosed as closed:
        _ack_safely(queue, job)
        logger.info(
            "job_already_finished",
            extra={
                "run_id": str(job.run_id),
                "status": closed.record.obligation.status,
            },
        )
        return 0
    if lease is None:
        # Another worker holds (or won) the lease; they own the run now.
        _ack_safely(queue, job)
        logger.info("job_claim_lost", extra={"run_id": str(job.run_id)})
        return 2

    record = lease.record
    case = decision["case"]

    # Message is spent the moment the claim lands — crash recovery is the
    # sweeper's job from here on, not the queue's.
    _ack_safely(queue, job)

    if case == _ClaimCase.canceled:
        _release_terminal(lease, events, record, "canceled", worker_id,
                          cause="cancel_requested_before_claim")
        logger.info("job_canceled_before_claim", extra={"run_id": str(job.run_id)})
        return 0

    if case == _ClaimCase.exhausted:
        _release_terminal(lease, events, record, "failed", worker_id,
                          cause="max_retries_exceeded")
        logger.warning(
            "job_max_retries_exceeded",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
        return 1

    if case == _ClaimCase.gated:
        # Budget spent on a gated obligation: parked for human judgment,
        # resumable through the review endpoint (extend_budget).
        try:
            lease.release(record)
        except LeaseLost:
            return 2
        _emit(
            events, record, "awaiting_review", worker_id,
            to_status="gated", cause="max_retries_exceeded",
        )
        logger.warning(
            "job_gated_on_exhaustion",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
        return 2

    _emit(
        events, record, "claimed", worker_id,
        from_status=str(view.state.status) if view is not None else None,
        to_status="running",
        details={"case": str(job_state)},
    )

    run_lease = RunLease(
        lease=lease,
        signals=signals,
        effects=EffectRepository(store=state_repo.store),
        messages=MessageRepository(store=state_repo.store),
    )
    flow_exc: Exception | None = None
    cancelled = False
    lease_lost = False
    lease_token = bind_lease(run_lease)
    try:
        executor.execute(record.obligation, len(record.attempts))
    except RunCancelled:
        cancelled = True
        logger.info("job_cancelled", extra={"run_id": str(job.run_id)})
    except LeaseLost:
        lease_lost = True
        logger.warning("job_lease_lost", extra={"run_id": str(job.run_id)})
    except Exception as exc:
        flow_exc = exc
        logger.exception("job_failed", extra={"run_id": str(job.run_id)})
    finally:
        unbind_lease(lease_token)

    if lease_lost:
        # The run was reclaimed mid-flight; the new owner records the outcome.
        return 2

    if cancelled:
        route = conclude_attempt(
            lease, outcome=AttemptOutcome.interrupted,
            events=events, actor=worker_id,
        )
        if route == "canceled":
            logger.info("job_canceled", extra={"run_id": str(job.run_id)})
            return 0
        logger.warning(
            "job_cancel_ownership_lost", extra={"run_id": str(job.run_id)}
        )
        return 2

    if flow_exc is None:
        route = conclude_attempt(
            lease, outcome=AttemptOutcome.returned,
            events=events, actor=worker_id,
        )
        if route == "gated":
            logger.info(
                "job_awaiting_review", extra={"run_id": str(job.run_id)}
            )
            return 0
        if route == "completed":
            logger.info("job_completed", extra={"run_id": str(job.run_id)})
            return 0
        # Lease expired mid-run and someone reclaimed: they own the outcome.
        logger.warning(
            "job_completion_ownership_lost",
            extra={"run_id": str(job.run_id)},
        )
        return 2

    route = conclude_attempt(
        lease, outcome=AttemptOutcome.raised,
        error=type(flow_exc).__name__,
        queue=queue, events=events, actor=worker_id,
        timeout_seconds=job.timeout_seconds,
    )
    if route == "pending":
        logger.info(
            "job_requeued",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
    elif route == "gated":
        # Budget spent on a gated obligation: parked for human judgment.
        logger.warning(
            "job_gated_on_exhaustion",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
    elif route == "failed":
        logger.warning(
            "job_failed_permanently",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
    else:
        logger.warning(
            "job_conclusion_ownership_lost",
            extra={"run_id": str(job.run_id)},
        )

    return 1


def _release_terminal(
    lease,
    events: EventLog | None,
    record: ObligationRecord,
    event: str,
    actor: str,
    **event_kwargs,
) -> bool:
    """Release the lease with a routed (closed or parked) account and emit the event.

    Returns:
        True when the release landed; False when the lease was fenced (the
        outcome belongs to the new owner and nothing is emitted).
    """
    try:
        lease.release(record)
    except LeaseLost:
        return False
    _emit(
        events, record, event, actor,
        to_status=str(record.summary().status), **event_kwargs,
    )
    return True
