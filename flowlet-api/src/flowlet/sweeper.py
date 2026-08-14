"""
Sweeper: crash recovery and state-directory hygiene for the lease model.

Runs on a cron cadence (Container Apps cron job or any scheduler).  Each
sweep lists the active state directory and:

1. **Expired running runs** — lease deadline passed with the holder gone.
   With attempts left the lease is stolen with a ``pending`` transition,
   released, and a fresh wake-up message is enqueued (kwargs come from the
   state payload, not the original message).  With attempts exhausted it is
   transitioned to terminal ``failed``.
2. **Stuck pending runs** — released ``pending`` long past their backoff
   window (their retry message was lost, or the failing worker died before
   enqueueing).  Re-enqueued; a duplicate message is harmless because the
   state machine drops wake-ups for busy/closed runs.
3. **Closed runs past the grace window** — archived out of ``state/`` (and
   their signals cleared) so the active directory stays O(active runs) and
   sweep cost never grows with history.  When a run-history log is
   configured, each run is durably recorded there *before* its state file
   is archived; a crash in between re-records the run on the next sweep,
   which the idempotent history projection absorbs.

Sweeps are idempotent and overlap-safe: ownership is transferred by the
epoch-fenced lease acquisition, never by the act of sweeping, so concurrent
sweepers (or a sweeper racing a worker) cannot double-claim a run.

Failure-detection latency is ``lease deadline + sweep interval`` — tune the
per-flow ``@flow(timeout=...)`` for faster takeover, not the sweep cadence.
"""
import logging
from typing import TYPE_CHECKING

from attrs import define, evolve

from .events import RunEventLog
from .models import FlowJob, RunState, RunStatus
from .queue import JobQueueProtocol
from .repository import AlreadyClosed, SignalRepository, StateRepository, StateView
from .types import Timestamp

if TYPE_CHECKING:
    from .history import RunHistory

logger = logging.getLogger(__name__)

DEFAULT_PENDING_GRACE = 600    # seconds before a pending run is re-enqueued
DEFAULT_ARCHIVE_GRACE = 3600   # seconds a closed run stays in state/

SWEEPER_TTL = 60  # seconds — recovery transitions release immediately


# region @sweeper
# ---
# role: computation
# intent: recover expired leases and keep the active state directory small
# description: >
#   sweep() is the single crash-recovery mechanism of the lease model: it
#   steals expired leases with an atomic pending/failed transition
#   (releasing immediately), re-enqueues runs whose retry message was lost,
#   and archives closed state documents (clearing their signals) after a
#   grace window.  All ownership transfers go through the epoch-fenced
#   acquire — safe to run concurrently with workers and other sweepers.
#   When a RunHistory is provided, archive candidates are group-committed
#   to the history event log before any state document is removed; if
#   recording fails, archiving is skipped for the pass and retried on the
#   next sweep.
# rules:
#   - Ownership MUST be transferred only via the fenced lease acquisition,
#     never by deletion or unguarded writes.
#   - MUST be idempotent: sweeping twice in a row changes nothing new.
#   - MUST NOT raise on individual runs; log and continue the scan.
#   - Expiry MUST be judged from the lease envelope, never the payload.
#   - History recording MUST happen before archiving, never after.
# dependencies:
#   - worker.state
#   - state_repository
#   - signals_repository
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
        scanned: Number of state documents examined.
        requeued: Expired or stuck runs re-enqueued for another attempt.
        failed: Runs marked terminally failed (attempts exhausted).
        archived: Closed state documents moved out of the active directory.
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
    signals: SignalRepository | None = None,
    pending_grace: int = DEFAULT_PENDING_GRACE,
    archive_grace: int = DEFAULT_ARCHIVE_GRACE,
    history: "RunHistory | None" = None,
    events: RunEventLog | None = None,
) -> SweepStats:
    """Run one sweep pass over the active state directory.

    Args:
        state_repo: State repository holding the active runs.
        queue: Job queue to enqueue wake-up messages on.
        signals: Optional signal repository; when given, an archived run's
            signals are cleared with it.
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
    now = Timestamp.now()
    to_archive: list[RunState] = []

    for view in state_repo.list_views():
        stats.scanned += 1
        try:
            state = view.state
            if state.status.is_closed():
                if _past_archive_grace(state, now, archive_grace):
                    to_archive.append(state)
            elif view.held(now):
                continue  # lease still live — owner is (presumed) working
            elif state.status == RunStatus.running:
                _recover_expired(state_repo, queue, view, stats, events)
            elif state.status == RunStatus.pending:
                _maybe_requeue_pending(
                    queue, view, now, pending_grace, stats, events
                )
        except Exception:
            stats.errors += 1
            logger.exception(
                "sweep_run_error",
                extra={
                    "flow_name": view.state.flow_name,
                    "run_id": str(view.state.run_id),
                },
            )

    _archive_all(state_repo, signals, history, to_archive, stats)

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


def _recover_expired(
    state_repo: StateRepository,
    queue: JobQueueProtocol,
    view: StateView,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Recover a running run whose lease expired (crashed holder).

    Steals the lease with an atomic transition — ``pending`` when attempts
    remain, terminal ``failed`` otherwise — and releases it immediately.
    The epoch fence is the ownership transfer: if the acquisition loses
    (owner finished or another sweeper won), nothing else happens.
    """
    state = view.state
    exhausted = state.attempt >= state.max_retries

    def transition(existing: RunState | None) -> RunState:
        if existing is None:
            raise LookupError(f"state for run {state.run_id} disappeared")
        if existing.status.is_closed():
            raise AlreadyClosed(existing)
        if existing.status != RunStatus.running:
            # Someone else already recovered it (pending) — keep as-is;
            # the requeue below is harmless if duplicated.
            return existing
        if existing.attempt >= existing.max_retries:
            return evolve(existing, status=RunStatus.failed, ended_at=Timestamp.now())
        return evolve(existing, status=RunStatus.pending)

    try:
        lease = state_repo.acquire(
            state.flow_name,
            state.run_id,
            ttl=SWEEPER_TTL,
            holder="sweeper",
            state_fn=transition,
        )
    except (AlreadyClosed, LookupError):
        return  # closed (or vanished) between the scan and the steal
    if lease is None:
        return  # actively held again — a worker reclaimed it first

    recovered = lease.state
    lease.release()

    if recovered.status == RunStatus.failed and exhausted:
        stats.failed += 1
        _emit(events, recovered, "failed", cause="lease_expired_max_retries")
        logger.warning(
            "sweep_run_failed_permanently",
            extra={"run_id": str(recovered.run_id), "attempt": recovered.attempt},
        )
        return

    _enqueue_wakeup(queue, recovered)
    stats.requeued += 1
    _emit(events, recovered, "requeued", cause="lease_expired")
    logger.info(
        "sweep_run_requeued",
        extra={"run_id": str(recovered.run_id), "attempt": recovered.attempt},
    )


def _maybe_requeue_pending(
    queue: JobQueueProtocol,
    view: StateView,
    now: Timestamp,
    pending_grace: int,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Re-enqueue a pending run whose retry message appears lost.

    A released lease's ``deadline_at`` is the release time — the moment the
    failing worker (or a recovering sweeper) parked the run — so a pending
    run much older than its backoff window has no live message.
    Re-enqueueing is idempotent: if a message does still exist, the second
    delivery hits busy/closed and is dropped.  No state write is needed —
    pending is already the correct status.
    """
    reference = view.deadline_at.value if view.deadline_at is not None else None
    if reference is not None and (now.value - reference).total_seconds() < pending_grace:
        return
    _enqueue_wakeup(queue, view.state)
    stats.requeued += 1
    _emit(events, view.state, "requeued", cause="pending_grace_expired")
    logger.info(
        "sweep_pending_requeued",
        extra={"run_id": str(view.state.run_id), "attempt": view.state.attempt},
    )


def _past_archive_grace(state: RunState, now: Timestamp, archive_grace: int) -> bool:
    """Whether a closed run has outlived the grace window in state/."""
    if state.ended_at is not None:
        age = (now.value - state.ended_at.value).total_seconds()
        if age < archive_grace:
            return False
    return True


def _archive_all(
    state_repo: StateRepository,
    signals: SignalRepository | None,
    history: "RunHistory | None",
    to_archive: list[RunState],
    stats: SweepStats,
) -> None:
    """Record archive candidates to the history log, then archive them.

    Recording happens strictly before any state document is removed: if the
    history write fails, all candidates stay in ``state/`` and the whole
    step is retried on the next sweep.  Re-recording is harmless — the
    history projection upserts by run_id.  Each archived run's signals are
    cleared after its state document is gone.
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
        if signals is not None:
            signals.clear(state.flow_name, state.run_id)
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
    """Enqueue a wake-up message rebuilt entirely from the state payload."""
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
