"""
Sweeper: crash recovery and state-directory hygiene for the account model.

Runs on a cron cadence (Container Apps cron job or any scheduler).  Each
sweep lists the active state directory and:

1. **Expired in-flight attempts** — lease deadline passed with the holder
   gone.  The sweeper steals the lease and does the crash accounting: the
   dead attempt's outcome is recorded as ``crashed`` (auto-verdict
   rejected).  With budget left the obligation is released open and a fresh
   wake-up message is enqueued (kwargs come from the account, not the
   original message); with the budget spent it is abandoned.
2. **Stuck parked obligations** — released and open long past their backoff
   window (their retry message was lost, or the failing worker died before
   enqueueing).  Re-enqueued; a duplicate message is harmless because the
   state machine drops wake-ups for busy/closed runs.
3. **Closed obligations past the grace window** — archived out of
   ``state/`` (and their signals cleared) so the active directory stays
   O(active runs) and sweep cost never grows with history.  When a
   run-history log is configured, each run is durably recorded there
   *before* its state document is archived; a crash in between re-records
   the run on the next sweep, which the idempotent history projection
   absorbs.

Sweeps are idempotent and overlap-safe: ownership is transferred by the
epoch-fenced lease acquisition, never by the act of sweeping, so concurrent
sweepers (or a sweeper racing a worker) cannot double-claim a run.

Failure-detection latency is ``lease deadline + sweep interval`` — tune the
per-flow ``@flow(timeout=...)`` for faster takeover, not the sweep cadence.
"""
import logging
from typing import TYPE_CHECKING

from attrs import define

from flowlet.events import RunEventLog
from flowlet.models import (
    AttemptOutcome,
    FlowJob,
    ObligationRecord,
    ObligationStatus,
    Verdict,
    VerdictDecision,
)
from flowlet.queue import JobQueueProtocol
from flowlet.repository import (
    AlreadyClosed,
    SignalRepository,
    StateRepository,
    StateView,
    TimerRepository,
)
from flowlet.types import Timestamp

if TYPE_CHECKING:
    from flowlet.history import RunHistory

logger = logging.getLogger(__name__)

DEFAULT_PENDING_GRACE = 600    # seconds before a parked run is re-enqueued
DEFAULT_ARCHIVE_GRACE = 3600   # seconds a closed run stays in state/

SWEEPER_TTL = 60  # seconds — recovery transitions release immediately


@define(slots=True, kw_only=True)
class SweepStats:
    """Counters describing what a single sweep pass did.

    Attributes:
        scanned: Number of state documents examined.
        requeued: Expired or stuck runs re-enqueued for another attempt.
        failed: Obligations abandoned (attempt budget spent).
        archived: Closed state documents moved out of the active directory.
        timers_fired: Due timers fired (signal sent and/or wake-up enqueued).
        errors: Runs skipped because of read/write errors (logged).
    """
    scanned: int = 0
    requeued: int = 0
    failed: int = 0
    archived: int = 0
    timers_fired: int = 0
    errors: int = 0


def sweep(
    state_repo: StateRepository,
    queue: JobQueueProtocol,
    *,
    signals: SignalRepository | None = None,
    timers: TimerRepository | None = None,
    pending_grace: int = DEFAULT_PENDING_GRACE,
    archive_grace: int = DEFAULT_ARCHIVE_GRACE,
    history: "RunHistory | None" = None,
    events: RunEventLog | None = None,
) -> SweepStats:
    """Run one sweep pass over the active state directory.

    Args:
        state_repo: State repository holding the active obligations.
        queue: Job queue to enqueue wake-up messages on.
        signals: Optional signal repository; when given, an archived run's
            signals are cleared with it, pause modes gate re-enqueues, and
            due timers can deliver their signals.
        timers: Optional timer repository; when given, due timers fire in
            this pass (bounded by the sweep cadence).
        pending_grace: Seconds a parked obligation may wait before it is
            assumed stuck (lost retry message) and re-enqueued.
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
    to_archive: list[ObligationRecord] = []

    for view in state_repo.list_views():
        stats.scanned += 1
        try:
            record = view.record
            if record.obligation.status.is_closed():
                if _past_archive_grace(record, now, archive_grace):
                    to_archive.append(record)
            elif view.held(now):
                continue  # lease still live — owner is (presumed) working
            elif record.open_attempt is not None:
                _recover_crashed(state_repo, queue, view, stats, events)
            elif record.obligation.status == ObligationStatus.open:
                if signals is not None and signals.paused_scopes(
                    flow_name=record.obligation.flow_name,
                    run_id=record.obligation.id,
                    parent_id=record.obligation.parent_id,
                    root_id=record.obligation.root_id,
                ):
                    continue  # paused: stays parked until RESUME revokes
                _maybe_requeue_parked(
                    queue, view, now, pending_grace, stats, events
                )
            # awaiting_adjudication: waits passively for a verdict signal —
            # never re-enqueued; a wake-up could not execute anything.
        except Exception:
            stats.errors += 1
            logger.exception(
                "sweep_run_error",
                extra={
                    "flow_name": view.record.obligation.flow_name,
                    "run_id": str(view.record.obligation.id),
                },
            )

    _archive_all(state_repo, signals, history, to_archive, stats)

    if timers is not None:
        _fire_due_timers(state_repo, timers, signals, queue, now, stats)

    logger.info(
        "sweep_done",
        extra={
            "scanned": stats.scanned,
            "requeued": stats.requeued,
            "failed": stats.failed,
            "archived": stats.archived,
            "timers_fired": stats.timers_fired,
            "errors": stats.errors,
        },
    )
    return stats


def _recover_crashed(
    state_repo: StateRepository,
    queue: JobQueueProtocol,
    view: StateView,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Recover an obligation whose in-flight attempt lost its lease.

    Steals the lease with an atomic transition that records the dead
    attempt as ``crashed`` (auto-verdict rejected), then either parks the
    obligation open (budget left) or abandons it — and releases
    immediately.  The epoch fence is the ownership transfer: if the
    acquisition loses (owner finished or another sweeper won), nothing else
    happens.
    """
    obligation = view.record.obligation

    def transition(existing: ObligationRecord | None) -> ObligationRecord:
        if existing is None:
            raise LookupError(f"account for {obligation.id} disappeared")
        if existing.obligation.status.is_closed():
            raise AlreadyClosed(existing)
        if existing.open_attempt is not None:
            existing.record_outcome(
                AttemptOutcome.crashed,
                verdict=Verdict(
                    decision=VerdictDecision.rejected,
                    by="auto",
                    rendered_at=Timestamp.now(),
                    reason="lease_expired",
                ),
            )
        if not existing.retries_left():
            existing.abandon("max_retries_exceeded")
        return existing

    try:
        lease = state_repo.acquire(
            obligation.flow_name,
            obligation.id,
            ttl=SWEEPER_TTL,
            holder="sweeper",
            state_fn=transition,
        )
    except (AlreadyClosed, LookupError):
        return  # closed (or vanished) between the scan and the steal
    if lease is None:
        return  # actively held again — a worker reclaimed it first

    recovered = lease.record
    lease.release()

    if recovered.obligation.status.is_closed():
        stats.failed += 1
        _emit(events, recovered, "failed", cause="lease_expired_max_retries")
        logger.warning(
            "sweep_run_failed_permanently",
            extra={
                "run_id": str(recovered.obligation.id),
                "attempt": len(recovered.attempts),
            },
        )
        return

    _enqueue_wakeup(queue, recovered)
    stats.requeued += 1
    _emit(events, recovered, "requeued", cause="lease_expired")
    logger.info(
        "sweep_run_requeued",
        extra={
            "run_id": str(recovered.obligation.id),
            "attempt": len(recovered.attempts),
        },
    )


def _maybe_requeue_parked(
    queue: JobQueueProtocol,
    view: StateView,
    now: Timestamp,
    pending_grace: int,
    stats: SweepStats,
    events: RunEventLog | None = None,
) -> None:
    """Re-enqueue a parked obligation whose retry message appears lost.

    A released lease's ``deadline_at`` is the release time — the moment the
    failing worker (or a recovering sweeper) parked the obligation — so one
    much older than its backoff window has no live message.  Re-enqueueing
    is idempotent: if a message does still exist, the second delivery hits
    busy/closed and is dropped.  No state write is needed — the account is
    already correct.
    """
    reference = view.deadline_at.value if view.deadline_at is not None else None
    if reference is not None and (now.value - reference).total_seconds() < pending_grace:
        return
    _enqueue_wakeup(queue, view.record)
    stats.requeued += 1
    _emit(events, view.record, "requeued", cause="pending_grace_expired")
    logger.info(
        "sweep_pending_requeued",
        extra={
            "run_id": str(view.record.obligation.id),
            "attempt": len(view.record.attempts),
        },
    )


def _past_archive_grace(
    record: ObligationRecord, now: Timestamp, archive_grace: int
) -> bool:
    """Whether a closed obligation has outlived the grace window in state/."""
    closed_at = record.obligation.closed_at
    if closed_at is not None:
        age = (now.value - closed_at.value).total_seconds()
        if age < archive_grace:
            return False
    return True


def _archive_all(
    state_repo: StateRepository,
    signals: SignalRepository | None,
    history: "RunHistory | None",
    to_archive: list[ObligationRecord],
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
            history.record_many([record.summary() for record in to_archive])
        except Exception:
            stats.errors += 1
            logger.exception(
                "sweep_history_record_error",
                extra={"count": len(to_archive)},
            )
            return  # keep state files; retry recording on the next sweep
    for record in to_archive:
        obligation = record.obligation
        state_repo.archive(obligation.flow_name, obligation.id)
        if signals is not None:
            signals.clear(obligation.flow_name, obligation.id)
        stats.archived += 1


def _fire_due_timers(
    state_repo: StateRepository,
    timers: TimerRepository,
    signals: SignalRepository | None,
    queue: JobQueueProtocol,
    now: Timestamp,
    stats: SweepStats,
) -> None:
    """Fire every due timer: deliver its signal and/or wake-up, then clear.

    Idempotent by construction — signal sends converge on the first,
    duplicate wake-ups are dropped by the worker state machine — so a crash
    between firing and clearing merely re-fires harmlessly on the next
    pass.  A timer for a closed or vanished obligation is cleared without
    firing.
    """
    for timer in timers.due(now):
        try:
            view = state_repo.read(timer.flow_name, timer.run_id)
            closed = (
                view is None or view.record.obligation.status.is_closed()
            )
            if not closed:
                if timer.signal is not None and signals is not None:
                    signals.send(
                        timer.flow_name,
                        timer.run_id,
                        timer.signal,
                        actor="timer",
                        details={"timer": timer.name, **(timer.details or {})},
                    )
                if timer.wakeup:
                    obligation = view.record.obligation
                    queue.enqueue(
                        FlowJob(
                            run_id=obligation.id,
                            flow_name=obligation.flow_name,
                            kwargs=obligation.kwargs,
                            max_retries=obligation.max_retries,
                            parent_id=obligation.parent_id,
                            root_id=obligation.root_id,
                            caused_by=f"timer:{timer.name}",
                        )
                    )
                stats.timers_fired += 1
                logger.info(
                    "timer_fired",
                    extra={"run_id": str(timer.run_id), "timer": timer.name},
                )
            timers.clear(timer.flow_name, timer.run_id, timer.name)
        except Exception:
            stats.errors += 1
            logger.exception(
                "timer_fire_error",
                extra={"run_id": str(timer.run_id), "timer": timer.name},
            )


def _emit(
    events: RunEventLog | None, record: ObligationRecord, event: str, cause: str
) -> None:
    """Append a sweeper lifecycle event when an event log is configured."""
    if events is None:
        return
    events.append(
        flow_name=record.obligation.flow_name,
        run_id=record.obligation.id,
        event=event,
        actor="sweeper",
        attempt=len(record.attempts),
        to_status=str(record.summary().status),
        cause=cause,
    )


def _enqueue_wakeup(queue: JobQueueProtocol, record: ObligationRecord) -> None:
    """Enqueue a wake-up message rebuilt entirely from the account."""
    obligation = record.obligation
    queue.enqueue(
        FlowJob(
            run_id=obligation.id,
            flow_name=obligation.flow_name,
            kwargs=obligation.kwargs,
            max_retries=obligation.max_retries,
            parent_id=obligation.parent_id,
            root_id=obligation.root_id,
            caused_by="sweeper_recovery",
        )
    )

