"""
Worker for executing a single queued flow job under a lease-based state machine.

Ownership is a lease: a worker claims a run by CAS-writing
``status=running`` with ``deadline_at = now + timeout``.  There are no
background threads — a run past its deadline is simply reclaimable, by
another worker dequeuing a duplicate message or by the sweeper (see
``flowlet.sweeper``), which re-enqueues expired runs on a cron cadence.
Flows may renew the lease cooperatively via ``flowlet.heartbeat()`` (see
``flowlet.lease``), which is also how a cancel request reaches a running
flow.

The queue is a pure wake-up signal.  Every dequeued message is acked as soon
as the run's state is resolved; retry accounting lives exclusively in
``RunState.attempt``.  Duplicate deliveries are harmless by construction.

State Machine
-------------
``existing_state_case()`` maps the current ``RunState`` (or ``None``) to one
of the five ``JobState`` values:

    JobState  Condition                                     Action
    --------- --------------------------------------------- --------------------------
    new       No state file exists                          Claim (attempt=1) → execute
    closed    Status is completed or failed                 Ack, done (idempotent)
    busy      Running with an unexpired lease               Ack — another worker owns it
    expired   Running past deadline, attempt < max_retries  Claim (attempt+1) → execute
    failed    Running past deadline, attempt ≥ max_retries  Mark failed → ack → done

``ready`` states (e.g. pending after a failure) are claimed like ``expired``:
every claim of an existing state increments ``attempt``.

On flow failure with retries left, the worker CAS-writes ``pending`` and
self-enqueues a fresh wake-up message with exponential backoff.  kwargs are
copied into the state at first claim so the sweeper can re-enqueue a crashed
run without the original message.
"""
from datetime import timedelta
from enum import StrEnum
import logging

from . import tracing
from .events import RunEventLog
from .lease import LeaseLost, RunCancelled, RunLease, bind_lease, unbind_lease
from .models import FlowJob, RunState, RunStatus
from .queue import JobQueueProtocol
from .registry import Registry
from .repository import StateRepository
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
#   existing_state_case(), which maps a RunState (or None) to a JobState
#   using only the lease deadline — no heartbeats.
# rules:
#   - existing_state_case() MUST be a pure function (no side-effects).
#   - Liveness MUST be judged by deadline_at alone; a running state with no
#     deadline is treated as expired (reclaimable), never as busy forever.
# dependencies:
#   - models.run
#   - state_repository
# aliases:
#   - job-state
#   - lease
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


def existing_state_case(existing: RunState | None) -> JobState:
    """Determine the job's state from its run's state.

    Args:
        existing: The current RunState, or None when no state file exists.

    Returns:
        The JobState driving the worker's next action.
    """
    if existing is None:
        return JobState.new
    if existing.status.is_closed():
        return JobState.closed
    if existing.status != RunStatus.running:
        return JobState.ready
    if not _is_lease_expired(existing):
        return JobState.busy
    if existing.attempt >= existing.max_retries:
        return JobState.failed
    return JobState.expired


def _is_lease_expired(state: RunState) -> bool:
    """Return True when the run's lease deadline has passed.

    A running state without a deadline is treated as expired: leases are
    always set at claim time, so a missing one means a malformed or legacy
    state that must stay reclaimable.

    Args:
        state: The run state to inspect.
    """
    if state.deadline_at is None:
        return True
    return Timestamp.now().value > state.deadline_at.value


# ---
# endregion


# region @worker.execute
# ---
# role: computation
# intent: Execute a single queued job — claim lease, run, finalize via CAS
# description: >
#   Dequeues one job, resolves state via @worker.state, claims ownership with
#   a CAS write carrying a fresh lease deadline, acks the message immediately,
#   runs the flow, then CAS-finalizes the state.  No threads, no event bus:
#   the lease is the only liveness mechanism, crash recovery belongs to the
#   sweeper, and the API reads state files directly.
# rules:
#   - MUST acquire ownership via a conditional state_repo.write() before executing.
#   - MUST ack the queue message as soon as the run's state is resolved.
#   - MUST increment attempt on every claim of an existing state.
#   - MUST copy job.kwargs into the state at first claim (sweeper re-enqueue).
#   - On retryable failure MUST CAS to pending and self-enqueue with backoff.
#   - MUST NOT execute when job_state is closed, failed, or busy.
#   - MUST NOT execute a claimed state whose cancel_requested flag is set;
#     finalize it as canceled instead.
#   - Terminal writes MUST use the lease's latest etag and, on conflict,
#     retry once when the run is still owned (a concurrent cancel-flag
#     write must not orphan a finished run).
#   - MUST NOT write state after LeaseLost — the outcome belongs to the
#     new owner.
# dependencies:
#   - worker.state
#   - state_repository
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

def _compute_deadline(timeout_seconds: int) -> Timestamp:
    """Compute the lease deadline from a timeout duration.

    Args:
        timeout_seconds: Lease duration in seconds for this attempt.

    Returns:
        A Timestamp representing ``now + timeout_seconds``.
    """
    return Timestamp(Timestamp.now().value + timedelta(seconds=timeout_seconds))


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


def _finalize(
    state_repo: StateRepository,
    state: RunState,
    etag: str | None,
) -> bool:
    """Write a terminal (or pending-retry) state, surviving a cancel-flag race.

    A conflict on the terminal write can only mean one of two things: the
    cancel endpoint bumped the version (we still own the run — merge the
    flag and retry once on the fresh etag), or the lease expired and the
    run was reclaimed (the outcome is no longer ours to record).

    Args:
        state_repo: State repository for conditional writes.
        state: The finalized state to persist.
        etag: Version from the lease's last successful write.

    Returns:
        True when the write landed; False when ownership was lost.
    """
    ok, _ = state_repo.write(state, etag)
    if ok:
        return True
    read = state_repo.read(state.flow_name, state.run_id)
    if read is None:
        return False
    current, current_etag = read
    if (
        current.status != RunStatus.running
        or current.worker_id != state.worker_id
        or current.attempt != state.attempt
    ):
        return False  # reclaimed — new owner records the outcome
    state.cancel_requested = state.cancel_requested or current.cancel_requested
    ok, _ = state_repo.write(state, current_etag)
    return ok


def execute_job(
    queue: JobQueueProtocol,
    registry: Registry,
    state_repo: StateRepository,
    worker_id: str,
    default_timeout: int = DEFAULT_TIMEOUT,
    events: RunEventLog | None = None,
) -> int:
    """Execute a single job from the queue under the lease state machine.

    Dequeues one message, resolves the ``JobState``, claims ownership via a
    conditional (ETag-verified) write with a fresh lease deadline, acks the
    message, runs the flow, then CAS-finalizes the state.  See the module
    docstring for the full transition table.

    While the flow runs, a :class:`~flowlet.lease.RunLease` is bound to the
    context so ``flowlet.heartbeat()`` can renew the lease and observe
    cancellation.  A cancel request finalizes the run as ``canceled``; a
    lost lease discards the outcome without writing.

    Args:
        queue: Job queue to dequeue from.
        registry: Flow registry for retrieving flow functions.
        state_repo: State repository for conditional state R/W.
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

    read_result = state_repo.read(job.flow_name, job.run_id)
    existing = read_result[0] if read_result is not None else None
    read_etag = read_result[1] if read_result is not None else None
    job_state = existing_state_case(existing)

    # Do NOT execute states — the message is spent in every one of them.
    if job_state == JobState.closed:
        assert existing is not None
        _ack_safely(queue, job)
        logger.info(
            "job_already_finished",
            extra={"run_id": str(job.run_id), "status": existing.status},
        )
        return 0
    if job_state == JobState.busy:
        assert existing is not None
        # Duplicate wake-up for an actively-owned run: drop it.  If the owner
        # crashes, the sweeper re-enqueues after the lease expires.
        _ack_safely(queue, job)
        logger.info(
            "job_active_elsewhere",
            extra={"run_id": str(job.run_id), "worker_id": existing.worker_id},
        )
        return 2
    if job_state == JobState.failed:
        assert existing is not None
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.failed,
            worker_id=worker_id,
            started_at=existing.started_at,
            ended_at=Timestamp.now(),
            attempt=existing.attempt,
            max_retries=existing.max_retries,
            kwargs=existing.kwargs,
            cancel_requested=existing.cancel_requested,
        )
        ok, _ = state_repo.write(state, read_etag)
        if ok:
            _emit(
                events, state, "failed", worker_id,
                from_status=str(existing.status), to_status=str(RunStatus.failed),
                cause="max_retries_exceeded",
            )
        _ack_safely(queue, job)
        logger.warning(
            "job_max_retries_exceeded",
            extra={"run_id": str(job.run_id), "attempt": existing.attempt},
        )
        return 1

    # A cancel that arrived while the run was off-lease (pending after a
    # failure, or flagged just before its lease expired): honour it instead
    # of executing another attempt.
    if existing is not None and existing.cancel_requested:
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.canceled,
            worker_id=worker_id,
            started_at=existing.started_at,
            ended_at=Timestamp.now(),
            attempt=existing.attempt,
            max_retries=existing.max_retries,
            kwargs=existing.kwargs,
            cancel_requested=True,
        )
        ok, _ = state_repo.write(state, read_etag)
        if ok:
            _emit(
                events, state, "canceled", worker_id,
                from_status=str(existing.status), to_status=str(RunStatus.canceled),
                cause="cancel_requested_before_claim",
            )
            logger.info("job_canceled_before_claim", extra={"run_id": str(job.run_id)})
        _ack_safely(queue, job)
        return 0

    # Execute states — claim with a fresh lease.
    timeout = job.timeout_seconds or default_timeout
    deadline_at = _compute_deadline(timeout)
    if job_state == JobState.new:
        assert existing is None
        state = RunState(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=Timestamp.now(),
            deadline_at=deadline_at,
            attempt=1,
            max_retries=job.max_retries,
            kwargs=job.kwargs,
        )
    else:  # ready or expired — every claim of an existing state is a new attempt
        assert existing is not None
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=existing.started_at,
            deadline_at=deadline_at,
            attempt=existing.attempt + 1,
            max_retries=existing.max_retries,
            kwargs=existing.kwargs or job.kwargs,
            cancel_requested=existing.cancel_requested,
        )
        logger.info(
            "job_claimed_existing",
            extra={
                "run_id": str(job.run_id),
                "attempt": existing.attempt + 1,
                "case": str(job_state),
            },
        )

    write_etag = None if job_state == JobState.new else read_etag
    ok, claim_etag = state_repo.write(state, write_etag)
    if not ok:
        # Another worker won the claim race; they own the run now.
        _ack_safely(queue, job)
        logger.info("job_claim_lost", extra={"run_id": str(job.run_id)})
        return 2
    assert claim_etag is not None
    _emit(
        events, state, "claimed", worker_id,
        from_status=str(existing.status) if existing is not None else None,
        to_status=str(RunStatus.running),
        details={"case": str(job_state)},
    )

    # Message is spent the moment the claim lands — crash recovery is the
    # sweeper's job from here on, not the queue's.
    _ack_safely(queue, job)

    lease = RunLease(
        state=state,
        etag=claim_etag,
        state_repo=state_repo,
        timeout_seconds=timeout,
    )
    flow_exc: Exception | None = None
    cancelled = False
    lease_lost = False
    lease_token = bind_lease(lease)
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
        # Persist buffered spans before the terminal CAS write so the run
        # record is complete by the time the state reports it closed.
        tracing.force_flush()

    if lease_lost:
        # The run was reclaimed mid-flight; the new owner records the outcome.
        return 2

    if cancelled:
        state.status = RunStatus.canceled
        state.ended_at = Timestamp.now()
        state.cancel_requested = True
        if _finalize(state_repo, state, lease.etag):
            _emit(
                events, state, "canceled", worker_id,
                from_status=str(RunStatus.running), to_status=str(RunStatus.canceled),
            )
            logger.info("job_canceled", extra={"run_id": str(job.run_id)})
            return 0
        logger.warning(
            "job_cancel_ownership_lost", extra={"run_id": str(job.run_id)}
        )
        return 2

    if flow_exc is None:
        state.status = RunStatus.completed
        state.ended_at = Timestamp.now()
        if _finalize(state_repo, state, lease.etag):
            _emit(
                events, state, "completed", worker_id,
                from_status=str(RunStatus.running), to_status=str(RunStatus.completed),
            )
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
        state.status = RunStatus.pending
        if _finalize(state_repo, state, lease.etag):
            _emit(
                events, state, "retry_scheduled", worker_id,
                from_status=str(RunStatus.running), to_status=str(RunStatus.pending),
                cause=type(flow_exc).__name__,
            )
            retry_job = FlowJob(
                run_id=state.run_id,
                flow_name=state.flow_name,
                kwargs=state.kwargs,
                max_retries=state.max_retries,
                timeout_seconds=job.timeout_seconds,
            )
            try:
                queue.enqueue(retry_job, delay=_retry_backoff(state.attempt))
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
                extra={"run_id": str(job.run_id), "attempt": state.attempt},
            )
        else:
            logger.warning(
                "job_requeue_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )
    else:
        state.status = RunStatus.failed
        state.ended_at = Timestamp.now()
        if _finalize(state_repo, state, lease.etag):
            _emit(
                events, state, "failed", worker_id,
                from_status=str(RunStatus.running), to_status=str(RunStatus.failed),
                cause=type(flow_exc).__name__,
            )
            logger.warning(
                "job_failed_permanently",
                extra={"run_id": str(job.run_id), "attempt": state.attempt},
            )
        else:
            logger.warning(
                "job_failure_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )

    return 1

# ---
# endregion
