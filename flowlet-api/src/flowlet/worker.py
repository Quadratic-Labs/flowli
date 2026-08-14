"""
Worker for executing a single queued flow job under the cairndb lease model.

Ownership is a cairndb lease: a worker claims a run by acquiring the lease
on its state document, and the claim transition (attempt increment, status
to ``running``) is written atomically with the acquisition itself.  Every
subsequent write is epoch-fenced — a worker whose lease expired and was
stolen gets ``LeaseLost`` and discards its outcome.  There are no
background threads: a run past its lease deadline is simply reclaimable, by
another worker dequeuing a duplicate message or by the sweeper (see
``flowlet.sweeper``).  Flows renew the lease cooperatively via
``flowlet.heartbeat()`` (see ``flowlet.lease``), which also observes the
run's cancel signal.

The queue is a pure wake-up signal.  Every dequeued message is acked as soon
as the run's state is resolved; retry accounting lives exclusively in
``RunState.attempt``.  Duplicate deliveries are harmless by construction.

State Machine
-------------
``existing_state_case()`` maps the current :class:`StateView` (or ``None``)
to one of the ``JobState`` values:

    JobState  Condition                                       Action
    --------- ----------------------------------------------- ------------------------
    new       No state document exists                        Acquire (attempt=1) → execute
    closed    Payload status is terminal                      Ack, done (idempotent)
    busy      Lease actively held (unexpired holder)          Ack — another worker owns it
    expired   Running, lease expired, attempt < max_retries   Acquire (attempt+1) → execute
    failed    Running, lease expired, attempt ≥ max_retries   Acquire → failed → release
    ready     Released (pending after a failure)              Acquire (attempt+1) → execute

The classification is advisory (early-outs without churning the lease
epoch); the acquisition's ``state_fn`` re-derives the transition under CAS,
so races between the read and the acquire resolve correctly — including a
run that closed in between, which aborts the acquisition via
:class:`AlreadyClosed`.

On flow failure with retries left, the worker releases the lease with a
``pending`` payload and self-enqueues a fresh wake-up message with
exponential backoff.  kwargs are copied into the state at first claim so
the sweeper can re-enqueue a crashed run without the original message.
"""
import logging
from enum import StrEnum

from attrs import evolve

from . import tracing
from .events import RunEventLog
from .lease import LeaseLost, RunCancelled, RunLease, bind_lease, unbind_lease
from .models import FlowJob, RunState, RunStatus
from .queue import JobQueueProtocol
from .registry import Registry
from .repository import AlreadyClosed, SignalRepository, StateRepository, StateView
from .repository.signals import CANCEL
from .types import Timestamp

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 900  # seconds — default lease per execution attempt


# region @worker.state
# ---
# role: computation
# intent: Define JobState and the lease evaluation that drives the worker
# description: >
#   Contains the JobState enum whose values drive the worker state machine
#   (see module docstring for the full transition table) and
#   existing_state_case(), which maps a StateView (or None) to a JobState.
#   Liveness is judged from the lease envelope — holder and deadline — never
#   from RunState fields.
# rules:
#   - existing_state_case() MUST be a pure function (no side-effects).
#   - Liveness MUST be judged by the envelope's holder/deadline_at alone; a
#     released or deadline-less lease is reclaimable, never busy forever.
# dependencies:
#   - models.run
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


def existing_state_case(
    view: StateView | None, now: Timestamp | None = None
) -> JobState:
    """Determine the job's state from its run's lease document.

    Args:
        view: The current StateView, or None when no document exists.
        now: Reference time for lease expiry (defaults to Timestamp.now()).

    Returns:
        The JobState driving the worker's next action.
    """
    if view is None:
        return JobState.new
    if view.state.status.is_closed():
        return JobState.closed
    if view.held(now):
        return JobState.busy
    if view.state.status == RunStatus.running:
        # Lease expired mid-flight — the attempt was consumed by a crash.
        if view.state.attempt >= view.state.max_retries:
            return JobState.failed
        return JobState.expired
    return JobState.ready


# ---
# endregion


# region @worker.execute
# ---
# role: computation
# intent: Execute a single queued job — acquire lease, run, release with outcome
# description: >
#   Dequeues one job, early-outs on closed/busy via a plain read, then
#   acquires the run's lease with a state_fn transition that increments the
#   attempt atomically with the ownership transfer (or refuses via
#   AlreadyClosed / short-circuits to canceled/failed).  Acks the message,
#   runs the flow under a bound RunLease, and finalizes by releasing the
#   lease with the terminal payload.  Every write is epoch-fenced: LeaseLost
#   means the run was reclaimed and the outcome belongs to the new owner.
# rules:
#   - Ownership MUST be taken via state_repo.acquire's atomic transition.
#   - MUST ack the queue message as soon as the run's state is resolved.
#   - MUST increment attempt on every claim of an existing run.
#   - MUST copy job.kwargs into the state at first claim (sweeper re-enqueue).
#   - On retryable failure MUST release with pending and self-enqueue with
#     backoff.
#   - MUST NOT execute when the run is closed, busy, or cancel-signalled.
#   - MUST NOT write state after LeaseLost — the outcome belongs to the
#     new owner.
# dependencies:
#   - worker.state
#   - state_repository
#   - signals_repository
#   - models.run
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
    state: RunState,
    event: str,
    actor: str,
    **kwargs,
) -> None:
    """Append a lifecycle event for *state* when an event log is configured."""
    if events is None:
        return
    events.append(
        flow_name=state.flow_name,
        run_id=state.run_id,
        event=event,
        actor=actor,
        attempt=state.attempt,
        **kwargs,
    )


class _ClaimCase(StrEnum):
    """What the atomic claim transition decided to do."""
    execute = "execute"
    canceled = "canceled"
    exhausted = "exhausted"


def _claim_transition(
    existing: RunState | None,
    *,
    job: FlowJob,
    worker_id: str,
    cancel_pending: bool,
    decision: dict,
) -> RunState:
    """Pure transition applied atomically with the lease acquisition.

    Re-derives the claim decision from the payload the CAS actually
    observed (the advisory pre-read may be stale).  Records which branch it
    took in *decision* so the caller can act on the outcome; a re-run after
    a lost CAS race simply overwrites it.

    Args:
        existing: Payload currently in the lease document (None when the
            document is being created).
        job: The wake-up message being processed.
        worker_id: The acquiring worker.
        cancel_pending: Whether the run's cancel signal was observed.
        decision: Out-parameter receiving {"case": _ClaimCase}.

    Returns:
        The payload to write with the acquisition.

    Raises:
        AlreadyClosed: The run closed between the pre-read and the acquire.
    """
    now = Timestamp.now()
    if existing is None:
        if cancel_pending:
            # A cancel signal with no state cannot occur through the API
            # (it requires an active run), but honour it defensively.
            decision["case"] = _ClaimCase.canceled
            return RunState(
                run_id=job.run_id,
                flow_name=job.flow_name,
                status=RunStatus.canceled,
                worker_id=worker_id,
                started_at=now,
                ended_at=now,
                attempt=1,
                max_retries=job.max_retries,
                kwargs=job.kwargs,
                cancel_requested=True,
            )
        decision["case"] = _ClaimCase.execute
        return RunState(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=now,
            attempt=1,
            max_retries=job.max_retries,
            kwargs=job.kwargs,
        )

    if existing.status.is_closed():
        raise AlreadyClosed(existing)

    if cancel_pending:
        decision["case"] = _ClaimCase.canceled
        return evolve(
            existing,
            status=RunStatus.canceled,
            ended_at=now,
            cancel_requested=True,
        )

    if existing.status == RunStatus.running and existing.attempt >= existing.max_retries:
        # Crashed final attempt: the lease expired with no retries left.
        decision["case"] = _ClaimCase.exhausted
        return evolve(existing, status=RunStatus.failed, ended_at=now)

    decision["case"] = _ClaimCase.execute
    return evolve(
        existing,
        status=RunStatus.running,
        worker_id=worker_id,
        attempt=existing.attempt + 1,
        kwargs=existing.kwargs or job.kwargs,
    )


def execute_job(
    queue: JobQueueProtocol,
    registry: Registry,
    state_repo: StateRepository,
    signals: SignalRepository,
    worker_id: str,
    default_timeout: int = DEFAULT_TIMEOUT,
    events: RunEventLog | None = None,
) -> int:
    """Execute a single job from the queue under the lease state machine.

    Dequeues one message, resolves the ``JobState``, acquires the run's
    lease with an atomic claim transition, acks the message, runs the flow,
    then releases the lease with the terminal payload.  See the module
    docstring for the full transition table.

    While the flow runs, a :class:`~flowlet.lease.RunLease` is bound to the
    context so ``flowlet.heartbeat()`` can renew the lease and observe the
    cancel signal.  A cancel finalizes the run as ``canceled``; a lost
    lease discards the outcome without writing.

    Args:
        queue: Job queue to dequeue from.
        registry: Flow registry for retrieving flow functions.
        state_repo: State repository owning the run lease documents.
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
            extra={"run_id": str(job.run_id), "status": view.state.status},
        )
        return 0
    if job_state == JobState.busy:
        assert view is not None
        # Duplicate wake-up for an actively-owned run: drop it.  If the owner
        # crashes, the sweeper re-enqueues after the lease expires.
        _ack_safely(queue, job)
        logger.info(
            "job_active_elsewhere",
            extra={"run_id": str(job.run_id), "holder": view.holder},
        )
        return 2

    cancel_pending = signals.get(job.flow_name, job.run_id, CANCEL) is not None
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
                decision=decision,
            ),
        )
    except AlreadyClosed as closed:
        _ack_safely(queue, job)
        logger.info(
            "job_already_finished",
            extra={"run_id": str(job.run_id), "status": closed.state.status},
        )
        return 0
    if lease is None:
        # Another worker holds (or won) the lease; they own the run now.
        _ack_safely(queue, job)
        logger.info("job_claim_lost", extra={"run_id": str(job.run_id)})
        return 2

    state = lease.state
    case = decision["case"]

    # Message is spent the moment the claim lands — crash recovery is the
    # sweeper's job from here on, not the queue's.
    _ack_safely(queue, job)

    if case == _ClaimCase.canceled:
        _release_terminal(lease, events, state, "canceled", worker_id,
                          cause="cancel_requested_before_claim")
        logger.info("job_canceled_before_claim", extra={"run_id": str(job.run_id)})
        return 0

    if case == _ClaimCase.exhausted:
        _release_terminal(lease, events, state, "failed", worker_id,
                          cause="max_retries_exceeded")
        logger.warning(
            "job_max_retries_exceeded",
            extra={"run_id": str(job.run_id), "attempt": state.attempt},
        )
        return 1

    _emit(
        events, state, "claimed", worker_id,
        from_status=str(view.state.status) if view is not None else None,
        to_status=str(RunStatus.running),
        details={"case": str(job_state)},
    )

    run_lease = RunLease(lease=lease, signals=signals)
    flow_exc: Exception | None = None
    cancelled = False
    lease_lost = False
    lease_token = bind_lease(run_lease)
    try:
        fn = registry.get_flow(job.flow_name)
        with tracing.run_root(state.run_id, state.flow_name, attempt=state.attempt):
            fn(**state.kwargs)
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
        final = evolve(
            state,
            status=RunStatus.canceled,
            ended_at=Timestamp.now(),
            cancel_requested=True,
        )
        if _release_terminal(lease, events, final, "canceled", worker_id,
                             from_status=str(RunStatus.running)):
            logger.info("job_canceled", extra={"run_id": str(job.run_id)})
            return 0
        logger.warning(
            "job_cancel_ownership_lost", extra={"run_id": str(job.run_id)}
        )
        return 2

    if flow_exc is None:
        final = evolve(state, status=RunStatus.completed, ended_at=Timestamp.now())
        if _release_terminal(lease, events, final, "completed", worker_id,
                             from_status=str(RunStatus.running)):
            logger.info("job_completed", extra={"run_id": str(job.run_id)})
            return 0
        # Lease expired mid-run and someone reclaimed: they own the outcome.
        logger.warning(
            "job_completion_ownership_lost",
            extra={"run_id": str(job.run_id)},
        )
        return 2

    # Failure path — retry accounting lives here, never in the queue.
    if state.attempt < state.max_retries:
        final = evolve(state, status=RunStatus.pending)
        try:
            lease.release(final)
        except LeaseLost:
            logger.warning(
                "job_requeue_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )
            return 1
        _emit(
            events, final, "retry_scheduled", worker_id,
            from_status=str(RunStatus.running), to_status=str(RunStatus.pending),
            cause=type(flow_exc).__name__,
        )
        retry_job = FlowJob(
            run_id=final.run_id,
            flow_name=final.flow_name,
            kwargs=final.kwargs,
            max_retries=final.max_retries,
            timeout_seconds=job.timeout_seconds,
        )
        try:
            queue.enqueue(retry_job, delay=_retry_backoff(final.attempt))
        except Exception:
            # State is already pending; the sweeper will notice the run
            # is neither running nor closed and re-enqueue it.
            logger.warning(
                "job_retry_enqueue_failed",
                extra={"run_id": str(job.run_id)},
                exc_info=True,
            )
        logger.info(
            "job_requeued",
            extra={"run_id": str(job.run_id), "attempt": final.attempt},
        )
    else:
        final = evolve(state, status=RunStatus.failed, ended_at=Timestamp.now())
        if _release_terminal(lease, events, final, "failed", worker_id,
                             from_status=str(RunStatus.running),
                             cause=type(flow_exc).__name__):
            logger.warning(
                "job_failed_permanently",
                extra={"run_id": str(job.run_id), "attempt": final.attempt},
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
    state: RunState,
    event: str,
    actor: str,
    **event_kwargs,
) -> bool:
    """Release the lease with a terminal payload and emit the event.

    Returns:
        True when the release landed; False when the lease was fenced (the
        outcome belongs to the new owner and nothing is emitted).
    """
    try:
        lease.release(state)
    except LeaseLost:
        return False
    _emit(events, state, event, actor, to_status=str(state.status), **event_kwargs)
    return True

# ---
# endregion
