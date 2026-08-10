"""
Sweeper: crash recovery and state-directory hygiene for the lease model.

Runs on a cron cadence (Container Apps cron job or any scheduler).  Each
sweep lists the active state directory and:

1. **Expired running runs** — lease deadline passed.  With attempts left the
   run is CAS-written to ``pending`` and a fresh wake-up message is enqueued
   (kwargs come from the state file, not the original message).  With
   attempts exhausted it is CAS-written to terminal ``failed``.
2. **Stuck pending runs** — pending long past their backoff window (their
   retry message was lost, or the failing worker died before enqueueing).
   Re-enqueued; a duplicate message is harmless because the state machine
   drops wake-ups for busy/closed runs.
3. **Closed runs past the grace window** — archived out of ``state/`` so the
   active directory stays O(active runs) and sweep cost never grows with
   history.  When a run-history log is configured, each run is durably
   recorded there *before* its state file is archived; a crash in between
   re-records the run on the next sweep, which the idempotent history
   projection absorbs.

Sweeps are idempotent and overlap-safe: ownership is transferred by CAS
writes, never by the act of sweeping, so concurrent sweepers (or a sweeper
racing a worker) cannot double-claim a run.

Failure-detection latency is ``lease deadline + sweep interval`` — tune the
per-flow ``@flow(timeout=...)`` for faster takeover, not the sweep cadence.
"""
import logging
from typing import TYPE_CHECKING

from attrs import define

from .events import RunEventLog
from .models import FlowJob, RunState, RunStatus
from .queue import JobQueueProtocol
from .repository import StateRepository
from .types import Timestamp

if TYPE_CHECKING:
    from .history import RunHistory

logger = logging.getLogger(__name__)

DEFAULT_PENDING_GRACE = 600    # seconds before a pending run is re-enqueued
DEFAULT_ARCHIVE_GRACE = 3600   # seconds a closed run stays in state/


# region @sweeper
# ---
# role: computation
# intent: recover expired leases and keep the active state directory small
# description: >
#   sweep() is the single crash-recovery mechanism of the lease model: it
#   re-enqueues runs whose lease expired (or whose retry message was lost),
#   fails runs that exhausted their attempts, and archives closed state
#   files after a grace window.  It is a pure scan-and-CAS pass — safe to
#   run concurrently with workers and with other sweepers.  When a
#   RunHistory is provided, archive candidates are group-committed to the
#   history event log before any state file is removed; if recording fails,
#   archiving is skipped for the pass and retried on the next sweep.
# rules:
#   - MUST transfer ownership only via CAS state writes, never by deletion.
#   - MUST be idempotent: sweeping twice in a row changes nothing new.
#   - MUST NOT raise on individual runs; log and continue the scan.
#   - Archived state MUST keep its JSON content byte-for-byte.
#   - History recording MUST happen before archiving, never after.
# dependencies:
#   - worker.state
#   - state_repository
#   - models.job
#   - history
#   - events.log
# aliases:
#   - sweeper
#   - crash-recovery
# triggers:
#   - how are crashed workers recovered
#   - what re-enqueues expired runs
#   - how is the state directory kept small
# ---


@define(slots=True, kw_only=True)
class SweepStats:
    """Counters describing what a single sweep pass did.

    Attributes:
        scanned: Number of state files examined.
        requeued: Expired or stuck runs re-enqueued for another attempt.
        failed: Runs marked terminally failed (attempts exhausted).
        archived: Closed state files moved out of the active directory.
        errors: Runs skipped because of read/write errors (logged).
    """
    scanned: int = 0
    requeued: int = 0
    failed: int = 0
    archived: int = 0
    errors: int = 0


def sweep(
    state_repo: StateRepository,
    queue: JobQueueProtocol,
    *,
    pending_grace: int = DEFAULT_PENDING_GRACE,
    archive_grace: int = DEFAULT_ARCHIVE_GRACE,
    history: "RunHistory | None" = None,
    events: RunEventLog | None = None,
) -> SweepStats:
    """Run one sweep pass over the active state directory.

    Args:
        state_repo: State repository holding the active runs.
        queue: Job queue to enqueue wake-up messages on.
        pending_grace: Seconds a pending run may wait before it is assumed
            stuck (lost retry message) and re-enqueued.
        archive_grace: Seconds a closed run stays in the active directory
            before being archived.
        history: Optional run-history log; when given, archive candidates
            are durably recorded there before their state files are removed.
        events: Optional run event log receiving recovery events.

    Returns:
        SweepStats describing the pass.
    """
    stats = SweepStats()
    now = Timestamp.now().value
    to_archive: list[RunState] = []

    for listed in state_repo.list_states():
        stats.scanned += 1
        try:
            # Re-read individually to get the CAS etag; list_states() is a
            # bulk read without version tracking.
            read = state_repo.read(listed.flow_name, listed.run_id)
            if read is None:
                continue
            state, etag = read

            if state.status.is_closed():
                if _past_archive_grace(state, now, archive_grace):
                    to_archive.append(state)
            elif state.status == RunStatus.running:
                _maybe_recover(state_repo, queue, state, etag, now, stats, events)
            elif state.status == RunStatus.pending:
                _maybe_requeue_pending(queue, state, now, pending_grace, stats, events)
        except Exception:
            stats.errors += 1
            logger.exception(
                "sweep_run_error",
                extra={"flow_name": listed.flow_name, "run_id": str(listed.run_id)},
            )

    _archive_all(state_repo, history, to_archive, stats)

    logger.info(
        "sweep_done",
        extra={
            "scanned": stats.scanned,
            "requeued": stats.requeued,
            "failed": stats.failed,
            "archived": stats.archived,
            "errors": stats.errors,
        },
    )
    return stats


def _maybe_recover(
    state_repo: StateRepository,
    queue: JobQueueProtocol,
    state: RunState,
    etag: str,
    now,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Recover a running run whose lease deadline has passed.

    A live lease is left alone.  An expired one is either sent back to
    pending + re-enqueued (attempts left) or terminally failed.  The CAS
    write is the ownership transfer: if it loses (owner finished or another
    sweeper won), nothing else happens.
    """
    if state.deadline_at is not None and now <= state.deadline_at.value:
        return  # lease still live — owner is (presumed) working

    if state.attempt >= state.max_retries:
        state.status = RunStatus.failed
        state.ended_at = Timestamp.now()
        ok, _ = state_repo.write(state, etag)
        if ok:
            stats.failed += 1
            _emit(events, state, "failed", cause="lease_expired_max_retries")
            logger.warning(
                "sweep_run_failed_permanently",
                extra={"run_id": str(state.run_id), "attempt": state.attempt},
            )
        return

    state.status = RunStatus.pending
    ok, _ = state_repo.write(state, etag)
    if not ok:
        return  # lost the race — current owner acted first
    _enqueue_wakeup(queue, state)
    stats.requeued += 1
    _emit(events, state, "requeued", cause="lease_expired")
    logger.info(
        "sweep_run_requeued",
        extra={"run_id": str(state.run_id), "attempt": state.attempt},
    )


def _maybe_requeue_pending(
    queue: JobQueueProtocol,
    state: RunState,
    now,
    pending_grace: int,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Re-enqueue a pending run whose retry message appears lost.

    ``deadline_at`` still holds the failed attempt's lease expiry, which
    approximates the failure time; a pending run much older than its backoff
    window has no live message.  Re-enqueueing is idempotent — if a message
    does still exist, the second delivery hits busy/closed and is dropped.
    No state write is needed: pending is already the correct status.
    """
    reference = state.deadline_at.value if state.deadline_at is not None else None
    if reference is not None and (now - reference).total_seconds() < pending_grace:
        return
    _enqueue_wakeup(queue, state)
    stats.requeued += 1
    _emit(events, state, "requeued", cause="pending_grace_expired")
    logger.info(
        "sweep_pending_requeued",
        extra={"run_id": str(state.run_id), "attempt": state.attempt},
    )


def _past_archive_grace(state: RunState, now, archive_grace: int) -> bool:
    """Whether a closed run has outlived the grace window in state/."""
    if state.ended_at is not None:
        age = (now - state.ended_at.value).total_seconds()
        if age < archive_grace:
            return False
    return True


def _archive_all(
    state_repo: StateRepository,
    history: "RunHistory | None",
    to_archive: list[RunState],
    stats: SweepStats,
) -> None:
    """Record archive candidates to the history log, then archive them.

    Recording happens strictly before any state file is removed: if the
    history write fails, all candidates stay in ``state/`` and the whole
    step is retried on the next sweep.  Re-recording is harmless — the
    history projection upserts by run_id.
    """
    if not to_archive:
        return
    if history is not None:
        try:
            history.record_many(to_archive)
        except Exception:
            stats.errors += 1
            logger.exception(
                "sweep_history_record_error",
                extra={"count": len(to_archive)},
            )
            return  # keep state files; retry recording on the next sweep
    for state in to_archive:
        state_repo.archive(state.flow_name, state.run_id)
        stats.archived += 1


def _emit(
    events: RunEventLog | None, state: RunState, event: str, cause: str
) -> None:
    """Append a sweeper lifecycle event when an event log is configured."""
    if events is None:
        return
    events.append(
        flow_name=state.flow_name,
        run_id=state.run_id,
        event=event,
        actor="sweeper",
        attempt=state.attempt,
        to_status=str(state.status),
        cause=cause,
    )


def _enqueue_wakeup(queue: JobQueueProtocol, state: RunState) -> None:
    """Enqueue a wake-up message rebuilt entirely from the state file."""
    queue.enqueue(
        FlowJob(
            run_id=state.run_id,
            flow_name=state.flow_name,
            kwargs=state.kwargs,
            max_retries=state.max_retries,
        )
    )


# ---
# endregion
