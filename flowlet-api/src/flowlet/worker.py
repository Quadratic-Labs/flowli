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
verdict.  The auto-verdict (returned ⇒ accepted ⇒ discharged) is the
default adjudication policy; gates and human adjudication slot into the
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
    ready     Released open obligation (parked after failure)  Acquire (new attempt) → execute

The classification is advisory (early-outs without churning the lease
epoch); the acquisition's ``state_fn`` re-derives the transition under CAS,
so races between the read and the acquire resolve correctly — including an
obligation that closed in between, which aborts the acquisition via
:class:`AlreadyClosed`.

On flow failure with budget left, the worker releases the lease with the
rejected attempt recorded and the obligation still open, then self-enqueues
a fresh wake-up message with exponential backoff.
"""
import logging
from enum import StrEnum

from . import tracing
from .events import RunEventLog
from .lease import LeaseLost, RunCancelled, RunLease, bind_lease, unbind_lease
from .models import (
    AttemptOutcome,
    FlowJob,
    Obligation,
    ObligationRecord,
    ObligationStatus,
    Verdict,
    VerdictDecision,
)
from .queue import JobQueueProtocol
from .registry import Registry
from .repository import (
    AlreadyClosed,
    EffectRepository,
    SignalRepository,
    StateRepository,
    StateView,
    Unclaimable,
)
from .repository.signals import CANCEL
from .types import Timestamp

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 900  # seconds — default lease per execution attempt


# region @worker.state
# ---
# role: computation
# intent: Define JobState and the account evaluation that drives the worker
# description: >
#   Contains the JobState enum whose values drive the worker state machine
#   (see module docstring for the full transition table) and
#   existing_state_case(), which maps a StateView (or None) to a JobState.
#   Liveness is judged from the lease envelope — holder and deadline —
#   execution progress from the account (open attempt, budget), and
#   closed-ness from the obligation's status.
# rules:
#   - existing_state_case() MUST be a pure function (no side-effects).
#   - Liveness MUST be judged by the envelope's holder/deadline_at alone; a
#     released or deadline-less lease is reclaimable, never busy forever.
# dependencies:
#   - models.account
#   - state_repository
# aliases:
#   - job-state
# triggers:
#   - how is job state determined
#   - what are the worker states
#   - how does the lease work
# ---

class JobState(StrEnum):
    """JobState determines the worker's action for this job."""
    new = "new"
    ready = "ready"
    busy = "busy"
    failed = "failed"
    expired = "expired"
    closed = "closed"
    gated = "gated"


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
    if record.obligation.status == ObligationStatus.awaiting_adjudication:
        return JobState.gated
    if view.held(now):
        return JobState.busy
    if record.open_attempt is not None:
        # An in-flight attempt whose lease died — the executor crashed.
        if record.retries_left():
            return JobState.expired
        return JobState.failed
    return JobState.ready


# ---
# endregion


# region @worker.execute
# ---
# role: computation
# intent: Execute one queued job — acquire the obligation, run, account the outcome
# description: >
#   Dequeues one job, early-outs on closed/busy via a plain read, then
#   acquires the obligation's lease with a state_fn transition that does
#   crash accounting for any dead in-flight attempt and appends this
#   worker's attempt — atomically with the ownership transfer (or refuses
#   via AlreadyClosed / short-circuits to abandoned for cancel and spent
#   budgets).  Acks the message, runs the flow under a bound RunLease, then
#   records the attempt's outcome plus its verdict and releases the lease.
#   Every write is epoch-fenced: LeaseLost means the obligation was
#   reclaimed and the outcome belongs to the new owner.
# rules:
#   - Ownership MUST be taken via state_repo.acquire's atomic transition.
#   - MUST ack the queue message as soon as the obligation is resolved.
#   - Every claim of an existing open obligation MUST append an attempt;
#     crash accounting for the predecessor happens in the same transition.
#   - On retryable failure MUST release with the rejected attempt recorded
#     and self-enqueue with backoff.
#   - MUST NOT execute when the obligation is closed, busy, or
#     cancel-signalled.
#   - MUST NOT write state after LeaseLost — the outcome belongs to the
#     new owner.
# dependencies:
#   - worker.state
#   - state_repository
#   - signals_repository
#   - models.account
#   - registry.registry
#   - lease
#   - events.log
# aliases:
#   - execute-job
# triggers:
#   - how does a worker execute a job
#   - how are retries handled
#   - what happens when a job fails
# ---

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


def _retry_backoff(attempt: int) -> int:
    """Exponential backoff delay before the next attempt's wake-up message.

    Args:
        attempt: The attempt number that just failed (1-based).

    Returns:
        Delay in seconds: 2, 4, 8, ... capped at 300.
    """
    return min(2 ** attempt, 300)


def _emit(
    events: RunEventLog | None,
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


def _auto_verdict(decision: VerdictDecision, reason: str | None = None) -> Verdict:
    """The default adjudication policy, recorded like any other verdict."""
    return Verdict(
        decision=decision, by="auto", rendered_at=Timestamp.now(), reason=reason
    )


class _ClaimCase(StrEnum):
    """What the atomic claim transition decided to do."""
    execute = "execute"
    canceled = "canceled"
    exhausted = "exhausted"


def _claim_transition(
    existing: ObligationRecord | None,
    *,
    job: FlowJob,
    worker_id: str,
    cancel_pending: bool,
    gated: bool,
    decision: dict,
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
                kwargs=job.kwargs,
                parent_id=job.parent_id,
                root_id=job.root_id,
                max_retries=job.max_retries,
                adjudication="gated" if gated else "auto",
                caused_by=job.caused_by,
                created_at=Timestamp.now(),
            )
        )
    else:
        record = existing

    if record.obligation.status.is_closed():
        raise AlreadyClosed(record)
    if record.obligation.status == ObligationStatus.awaiting_adjudication:
        # Nothing to execute — the obligation waits on a verdict, not a worker.
        raise Unclaimable(record)

    # Crash accounting: a dead holder's in-flight attempt ends here, judged
    # by whoever discovers it — never silently overwritten.
    if existing is not None and record.open_attempt is not None:
        record.record_outcome(
            AttemptOutcome.crashed,
            verdict=_auto_verdict(VerdictDecision.rejected, reason="lease_expired"),
        )

    if cancel_pending:
        record.obligation.cancel_requested = True
        record.record_outcome(AttemptOutcome.interrupted)  # no-op if none open
        record.abandon("canceled")
        decision["case"] = _ClaimCase.canceled
        return record

    if not record.retries_left():
        record.abandon("max_retries_exceeded")
        decision["case"] = _ClaimCase.exhausted
        return record

    record.begin_attempt(worker_id)
    decision["case"] = _ClaimCase.execute
    return record


def execute_job(
    queue: JobQueueProtocol,
    registry: Registry,
    state_repo: StateRepository,
    signals: SignalRepository,
    worker_id: str,
    default_timeout: int = DEFAULT_TIMEOUT,
    events: RunEventLog | None = None,
) -> int:
    """Execute a single job from the queue under the account state machine.

    Dequeues one message, resolves the ``JobState``, acquires the
    obligation's lease with an atomic claim transition, acks the message,
    runs the flow, then records the attempt's outcome and verdict and
    releases the lease.  See the module docstring for the full transition
    table.

    While the flow runs, a :class:`~flowlet.lease.RunLease` is bound to the
    context so ``flowlet.heartbeat()`` can renew the lease and observe the
    cancel signal.  A cancel finalizes the obligation as abandoned
    (``canceled``); a lost lease discards the outcome without writing.

    Args:
        queue: Job queue to dequeue from.
        registry: Flow registry for retrieving flow functions.
        state_repo: State repository owning the obligation lease documents.
        signals: Signal repository the cancel signal is read from.
        worker_id: Unique identifier for this worker instance.
        default_timeout: Lease seconds used when the job carries no
            timeout_seconds.
        events: Optional run event log receiving lifecycle events.

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
    if job_state in (JobState.busy, JobState.gated):
        assert view is not None
        # Duplicate wake-up for an actively-owned or adjudication-parked
        # run: drop it.  Crashed owners are the sweeper's job; gated runs
        # resume through the adjudication endpoint, never a wake-up.
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
    get_options = getattr(registry, "get_flow_options", None)
    options = get_options(job.flow_name) if get_options is not None else None
    gated = bool(options is not None and getattr(options, "gated", False))
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
        logger.info("job_awaiting_adjudication", extra={"run_id": str(job.run_id)})
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
    )
    flow_exc: Exception | None = None
    cancelled = False
    lease_lost = False
    lease_token = bind_lease(run_lease)
    try:
        fn = registry.get_flow(job.flow_name)
        with tracing.run_root(
            record.obligation.id,
            record.obligation.flow_name,
            attempt=len(record.attempts),
        ):
            fn(**record.obligation.kwargs)
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
        # Persist buffered spans before the terminal write so the run
        # record is complete by the time the state reports it closed.
        tracing.force_flush()

    if lease_lost:
        # The run was reclaimed mid-flight; the new owner records the outcome.
        return 2

    if cancelled:
        record.obligation.cancel_requested = True
        record.record_outcome(AttemptOutcome.interrupted)
        record.abandon("canceled")
        if _release_terminal(lease, events, record, "canceled", worker_id,
                             from_status="running"):
            logger.info("job_canceled", extra={"run_id": str(job.run_id)})
            return 0
        logger.warning(
            "job_cancel_ownership_lost", extra={"run_id": str(job.run_id)}
        )
        return 2

    if flow_exc is None:
        if record.obligation.adjudication == "gated":
            # No auto-verdict: the outcome is a fact, judgment is pending.
            record.record_outcome(AttemptOutcome.returned)
            record.suspend_for_adjudication()
            try:
                lease.release(record)
            except LeaseLost:
                logger.warning(
                    "job_completion_ownership_lost",
                    extra={"run_id": str(job.run_id)},
                )
                return 2
            _emit(
                events, record, "awaiting_adjudication", worker_id,
                from_status="running", to_status="gated",
            )
            logger.info(
                "job_awaiting_adjudication", extra={"run_id": str(job.run_id)}
            )
            return 0
        record.record_outcome(
            AttemptOutcome.returned,
            verdict=_auto_verdict(VerdictDecision.accepted),
        )
        record.discharge()
        if _release_terminal(lease, events, record, "completed", worker_id,
                             from_status="running"):
            logger.info("job_completed", extra={"run_id": str(job.run_id)})
            return 0
        # Lease expired mid-run and someone reclaimed: they own the outcome.
        logger.warning(
            "job_completion_ownership_lost",
            extra={"run_id": str(job.run_id)},
        )
        return 2

    # Failure path — retry accounting lives in the account, never the queue.
    record.record_outcome(
        AttemptOutcome.raised,
        error=type(flow_exc).__name__,
        verdict=_auto_verdict(
            VerdictDecision.rejected, reason=type(flow_exc).__name__
        ),
    )
    if record.retries_left():
        try:
            lease.release(record)
        except LeaseLost:
            logger.warning(
                "job_requeue_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )
            return 1
        _emit(
            events, record, "retry_scheduled", worker_id,
            from_status="running", to_status="pending",
            cause=type(flow_exc).__name__,
        )
        retry_job = FlowJob(
            run_id=record.obligation.id,
            flow_name=record.obligation.flow_name,
            kwargs=record.obligation.kwargs,
            max_retries=record.obligation.max_retries,
            timeout_seconds=job.timeout_seconds,
            parent_id=record.obligation.parent_id,
            root_id=record.obligation.root_id,
            caused_by=f"retry_of_attempt:{len(record.attempts)}",
        )
        try:
            queue.enqueue(retry_job, delay=_retry_backoff(len(record.attempts)))
        except Exception:
            # The account already shows a rejected attempt with budget left;
            # the sweeper will notice the parked obligation and re-enqueue.
            logger.warning(
                "job_retry_enqueue_failed",
                extra={"run_id": str(job.run_id)},
                exc_info=True,
            )
        logger.info(
            "job_requeued",
            extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
        )
    else:
        record.abandon("max_retries_exceeded")
        if _release_terminal(lease, events, record, "failed", worker_id,
                             from_status="running",
                             cause=type(flow_exc).__name__):
            logger.warning(
                "job_failed_permanently",
                extra={"run_id": str(job.run_id), "attempt": len(record.attempts)},
            )
        else:
            logger.warning(
                "job_failure_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )

    return 1


def _release_terminal(
    lease,
    events: RunEventLog | None,
    record: ObligationRecord,
    event: str,
    actor: str,
    **event_kwargs,
) -> bool:
    """Release the lease with a closed account and emit the event.

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

# ---
# endregion
