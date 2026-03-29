"""
Worker for executing a single queued flow job with full state machine and heartbeat.

When a job is dequeued the worker resolves it against the shared state store to
determine a ``JobState``, then takes the appropriate action.  A heartbeat thread
runs during execution to signal liveness and enable stale-takeover detection.

State Machine
-------------
``existing_state_case()`` maps the current ``RunState`` (or ``None``) to one of
the six ``JobState`` values:

    JobState  Condition                                        Action
    --------- ------------------------------------------------ ---------------------------
    new       No state file exists                             Create state(running, attempt=1) → execute
    closed    Status is completed or failed                    Ack queue, done (idempotent)
    busy      Running with a live heartbeat                    Skip — another worker is active
    ready     Exists but not running (e.g. pending)            Transition to running → execute
    stale     Running, stale hb, attempt < max_retries         Take over, increment attempt → execute
    failed    Running, stale hb, attempt ≥ max_retries         Mark failed → ack → done

State is written to the ``StateRepository`` on every transition and published on
the ``PubSub`` bus so the API and dashboards can observe liveness.
"""
from datetime import timedelta
from enum import StrEnum
import logging
import threading

from .context import ContextManager
from .models import RunState, RunStatus, RunType
from .pubsub import PubSubProtocol
from .queue import JobQueueProtocol
from .registry import Registry
from .repository import StateRepository
from .types import Timestamp

logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL = 30  # seconds


# region @worker.state
# ---
# role: computation
# intent: Define JobState and the helpers that evaluate and manage worker state
# description: >
#   Contains the JobState enum whose six values drive the worker state machine
#   (see module docstring for the full transition table).
#   existing_state_case() maps a RunState (or None) to a JobState so the caller
#   knows what action to take without inspecting raw RunState fields directly.
#   The heartbeat helpers (_heartbeat_loop, _is_heartbeat_stale) track worker
#   liveness and support stale-takeover detection.
# rules:
#   - existing_state_case() MUST be a pure function (no side-effects).
#   - _heartbeat_loop MUST stop immediately when stop_event is set.
# dependencies:
#   - models.run
#   - state_repository
#   - pubsub
# aliases:
#   - job-state
#   - heartbeat
# triggers:
#   - how is job state determined
#   - what are the worker states
#   - how does heartbeat work
# ---

class JobState(StrEnum):
    """JobState determines workers' action for this job."""
    new = "new"
    ready = "ready"
    busy = "busy"
    failed = "failed"
    stale = "stale"
    closed = "closed"


def existing_state_case(existing, stale_threshold) -> JobState:
    """Determine the job's state from its run's state."""
    if existing is None:
        return JobState.new
    if existing.status.is_closed():
        return JobState.closed
    if existing.status != RunStatus.running:
        return JobState.ready
    if not _is_heartbeat_stale(existing, stale_threshold):
        # Live heartbeat — still check wall-clock deadline so a stuck-but-alive
        # worker can be preempted even when its heartbeat is fresh.
        if _is_deadline_exceeded(existing):
            if existing.attempt >= existing.max_retries:
                return JobState.failed
            return JobState.stale
        return JobState.busy
    if existing.attempt >= existing.max_retries:
        return JobState.failed
    return JobState.stale


def _is_deadline_exceeded(state: RunState) -> bool:
    """Return True if the run has exceeded its wall-clock deadline.

    Args:
        state: The run state to inspect.

    Returns:
        True when ``deadline_at`` is set and the current time is past it.
    """
    if state.deadline_at is None:
        return False
    return Timestamp.now().value > state.deadline_at.value


def _publish_state(pubsub: PubSubProtocol, state: RunState) -> None:
    """Fire-and-forget publish of a state transition event.

    Args:
        pubsub: PubSub bus implementation.
        state: Current run state to broadcast.
    """
    try:
        pubsub.publish(f"state/{state.flow_name}", state)
    except Exception:
        logger.exception("pubsub_publish_failed", extra={"run_id": str(state.run_id)})


def _heartbeat_loop(
    state_repo: StateRepository,
    pubsub: PubSubProtocol,
    state: RunState,
    stop_event: threading.Event,
    abort_event: threading.Event,
    etag_holder: list[str | None],
    interval: int = _HEARTBEAT_INTERVAL,
) -> None:
    """Background thread that refreshes heartbeat_at periodically.

    Mutates ``state.heartbeat_at`` in place, writes with the tracked ETag,
    and updates ``etag_holder[0]`` on success.  If the write is rejected
    (another worker has taken ownership), sets ``abort_event`` so the main
    thread can detect the loss after the current flow call returns.
    Runs until ``stop_event`` is set.

    Args:
        state_repo: State repository for conditional writes.
        pubsub: PubSub bus for broadcasting heartbeats.
        state: Mutable RunState owned by the current worker.
        stop_event: Signal to stop the heartbeat loop.
        abort_event: Set when ownership is lost; signals the main thread.
        etag_holder: Single-element list holding the current ETag.
            Updated in place on each successful write.
        interval: Seconds between heartbeats.
    """
    while not stop_event.wait(interval):
        state.heartbeat_at = Timestamp.now()
        ok, new_etag = state_repo.write(state, etag_holder[0])
        if ok:
            etag_holder[0] = new_etag
            _publish_state(pubsub, state)
        else:
            logger.warning(
                "heartbeat_ownership_lost",
                extra={"run_id": str(state.run_id)},
            )
            abort_event.set()
            return


def _is_heartbeat_stale(state: RunState, stale_threshold: int) -> bool:
    """Return True if the heartbeat is older than *stale_threshold* seconds.

    Args:
        state: The run state to inspect.
        stale_threshold: Maximum acceptable heartbeat age in seconds.

    Returns:
        True when the heartbeat is stale.
    """
    now = Timestamp.now().value
    age = (now - state.heartbeat_at.value).total_seconds()
    return age > stale_threshold

# ---
# endregion


# region @worker.execute
# ---
# role: computation
# intent: Execute a single queued job using the full state machine and heartbeat
# description: >
#   Dequeues one job, delegates state resolution to @worker.state, then
#   acquires ownership via an atomic state_repo.write() before running the
#   flow.  A heartbeat thread refreshes heartbeat_at for the lifetime of
#   the execution.  See the module docstring for the full state transition
#   table.
# rules:
#   - MUST acquire ownership via state_repo.write() before execution.
#   - MUST run a heartbeat thread that refreshes heartbeat_at every interval.
#   - MUST stop the heartbeat thread (via stop_event) in the finally block.
#   - MUST requeue (nack) on failure when retries remain; ack otherwise.
#   - MUST NOT execute when job_state is closed, failed, or busy.
# dependencies:
#   - worker.state
#   - state_repository
#   - pubsub
#   - models.run
#   - registry.registry
# aliases:
#   - execute-job
# triggers:
#   - how does a worker execute a job
#   - how are retries handled
#   - what happens when a job fails
# ---

def _compute_deadline(timeout_seconds: int | None) -> Timestamp | None:
    """Compute a wall-clock deadline from a timeout duration.

    Args:
        timeout_seconds: Maximum allowed execution time in seconds, or None
            for no deadline.

    Returns:
        A Timestamp representing ``now + timeout_seconds``, or None.
    """
    if timeout_seconds is None:
        return None
    return Timestamp(Timestamp.now().value + timedelta(seconds=timeout_seconds))


def execute_job(
    queue: JobQueueProtocol,
    registry: Registry,
    state_repo: StateRepository,
    pubsub: PubSubProtocol,
    worker_id: str,
    timeout: int | None = None,
    stale_threshold: int = _HEARTBEAT_INTERVAL * 3,
) -> int:
    """Execute a single job from the queue using the full state machine.

    Dequeues one job, resolves the ``JobState``, acquires ownership via a
    conditional write (ETag-verified), runs the flow under a heartbeat thread,
    then transitions the state on completion or failure.  See the module
    docstring for the full state transition table.

    The heartbeat thread tracks ownership via a shared ETag.  If another
    worker takes over (stale or deadline takeover), the heartbeat write will
    be rejected, ``abort_event`` will be set, and any subsequent state writes
    by this worker will also be rejected — preventing duplicate ACKs and
    inconsistent state.

    Args:
        queue: Job queue to dequeue from.
        registry: Flow registry for retrieving flow functions.
        state_repo: State repository for conditional state R/W.
        pubsub: PubSub bus for broadcasting state transitions.
        worker_id: Unique identifier for this worker instance.
        timeout: Optional visibility timeout override in seconds.
        stale_threshold: Seconds after which a running heartbeat is considered
            stale.  Defaults to three heartbeat intervals.

    Returns:
        Exit code — 0 success, 1 failure, 2 no job or skipped.
    """
    job = queue.dequeue(timeout=timeout)
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
    job_state = existing_state_case(existing, stale_threshold)

    # Do NOT execute states.
    if job_state == JobState.closed:
        assert existing is not None
        queue.ack(job.job_id)
        logger.info(
            "job_already_finished",
            extra={"run_id": str(job.run_id), "status": existing.status},
        )
        return 0
    if job_state == JobState.failed:
        assert existing is not None
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.failed,
            worker_id=worker_id,
            started_at=existing.started_at,
            heartbeat_at=Timestamp.now(),
            ended_at=Timestamp.now(),
            attempt=existing.attempt,
            max_retries=existing.max_retries,
        )
        state_repo.write(state, read_etag)
        _publish_state(pubsub, state)
        queue.ack(job.job_id)
        logger.warning(
            "job_max_retries_exceeded",
            extra={"run_id": str(job.run_id), "attempt": existing.attempt},
        )
        return 1
    if job_state == JobState.busy:
        assert existing is not None
        logger.info(
            "job_active_elsewhere",
            extra={"run_id": str(job.run_id), "worker_id": existing.worker_id},
        )
        return 2

    # Execute states — build the new RunState for this worker.
    # deadline_at is always reset per execution attempt so that a takeover
    # worker gets a fresh window (timeout is per attempt, not per job).
    deadline_at = _compute_deadline(job.timeout_seconds)
    if job_state == JobState.stale:
        assert existing is not None
        attempt = existing.attempt + 1
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=existing.started_at,
            heartbeat_at=Timestamp.now(),
            deadline_at=deadline_at,
            attempt=attempt,
            max_retries=existing.max_retries,
        )
        logger.info(
            "job_takeover",
            extra={"run_id": str(job.run_id), "attempt": attempt},
        )
    elif job_state == JobState.ready:
        assert existing is not None
        state = RunState(
            run_id=existing.run_id,
            flow_name=existing.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=existing.started_at,
            heartbeat_at=Timestamp.now(),
            deadline_at=deadline_at,
            attempt=existing.attempt,
            max_retries=existing.max_retries,
        )
    else:  # job_state == JobState.new
        assert existing is None
        state = RunState(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id=worker_id,
            started_at=Timestamp.now(),
            heartbeat_at=Timestamp.now(),
            deadline_at=deadline_at,
            attempt=1,
            max_retries=job.max_retries,
        )

    # Try to acquire ownership via conditional write.
    # etag=None for new files; read_etag for takeover of existing state.
    write_etag = None if job_state == JobState.new else read_etag
    ok, current_etag = state_repo.write(state, write_etag)
    if not ok:
        logger.info(
            "job_ownership_lost",
            extra={"run_id": str(job.run_id)},
        )
        return 2

    _publish_state(pubsub, state)

    # Shared ETag — updated by heartbeat thread on every successful write.
    # The main thread reads it for final state transitions.
    etag_holder: list[str | None] = [current_etag]
    abort_event = threading.Event()
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(state_repo, pubsub, state, stop_event, abort_event, etag_holder),
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        fn = registry.get_flow(job.flow_name)
        with ContextManager.begin_span(job.flow_name, RunType.flow, span_id=job.run_id):
            fn(**job.kwargs)

        # If the heartbeat detected ownership loss while the flow was running,
        # do not write final state or ACK — another worker owns this run.
        if abort_event.is_set():
            logger.warning(
                "job_aborted_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )
            return 2

        # Success path — conditional write guards against last-moment takeover.
        state.status = RunStatus.completed
        state.ended_at = Timestamp.now()
        state.heartbeat_at = Timestamp.now()
        ok, _ = state_repo.write(state, etag_holder[0])
        if ok:
            _publish_state(pubsub, state)
            queue.ack(job.job_id)
            logger.info("job_completed", extra={"run_id": str(job.run_id)})
            return 0
        else:
            logger.warning(
                "job_completion_ownership_lost",
                extra={"run_id": str(job.run_id)},
            )
            return 2

    except Exception:
        logger.exception("job_failed", extra={"run_id": str(job.run_id)})

        if abort_event.is_set():
            return 2

        if state.attempt < state.max_retries:
            state.status = RunStatus.pending
            state.heartbeat_at = Timestamp.now()
            ok, _ = state_repo.write(state, etag_holder[0])
            if ok:
                queue.nack(job.job_id, requeue=True)
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
            state.heartbeat_at = Timestamp.now()
            ok, _ = state_repo.write(state, etag_holder[0])
            if ok:
                _publish_state(pubsub, state)
                queue.ack(job.job_id)
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

    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=5)

# ---
# endregion
