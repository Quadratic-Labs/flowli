"""Sweeper: fire timers, recover dead executions, repair the control log, clear orphan waits.

See specs/05-protocols.md section 9. Cron-shaped: no lease, every action idempotent.

Every enqueue here passes `repair=True`. The sweeper is the last thing that
will ever put these tasks back, so it must not be refused by an enqueue marker
whose task was lost (`Queue.ensure`).
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Protocol

from cairndb import Timestamp

from flowli.domain import (
    ROOT_FID,
    Actor,
    Condition,
    Eid,
    Entry,
    EntryType,
    ExecutionStatus,
    FrameRef,
    Task,
    TaskKind,
    parse_eid,
)
from flowli.log import get_logger

from .engine import Engine, UnknownExecution

log = get_logger("flowli.sweeper")

_STATUS_OF_ENTRY: dict[str, ExecutionStatus] = {
    EntryType.EXECUTION_CREATED: ExecutionStatus.PENDING,
    EntryType.EXECUTION_STARTED: ExecutionStatus.RUNNING,
    EntryType.EXECUTION_RESUMED: ExecutionStatus.RUNNING,
    EntryType.EXECUTION_SUSPENDED: ExecutionStatus.SUSPENDED,
    EntryType.EXECUTION_COMPLETED: ExecutionStatus.COMPLETED,
    EntryType.EXECUTION_FAILED: ExecutionStatus.FAILED,
    EntryType.EXECUTION_CANCELLED: ExecutionStatus.CANCELLED,
}


@dataclass
class KnownExecution:
    status: ExecutionStatus
    last_type: str
    last_at: Timestamp
    queue: str = "default"
    archived: bool = False


class ControlSource(Protocol):
    """Where the sweeper learns execution statuses: the in-memory fold or a projection."""

    async def snapshot(self) -> dict[Eid, KnownExecution]:
        """Return the known executions, terminal ones optional."""
        ...

    async def terminal_before(self, before: Timestamp) -> dict[Eid, KnownExecution]:
        """Return terminal executions whose last entry is older than `before`. For retention."""
        ...


class ControlView:
    """A fold of the control log: eid -> last known lifecycle entry. Keeps a cursor."""

    def __init__(self, engine: Engine | None = None) -> None:
        self.cursor = 0
        self.executions: dict[Eid, KnownExecution] = {}
        self._engine = engine

    async def snapshot(self) -> dict[Eid, KnownExecution]:
        if self._engine is not None:
            for s in await self._engine.ports.control.read(after=self.cursor):
                self.apply(s.seq, s.item)
        return self.executions

    async def terminal_before(self, before: Timestamp) -> dict[Eid, KnownExecution]:
        await self.snapshot()
        return {
            eid: k
            for eid, k in self.executions.items()
            if k.status.is_terminal and k.last_at < before and not k.archived
        }

    def apply(self, seq: int, entry: Entry) -> None:
        self.cursor = max(self.cursor, seq)
        raw = entry.payload.get("eid")
        if not isinstance(raw, str):
            return
        eid = parse_eid(raw)
        if entry.type == EntryType.EXECUTION_ARCHIVED:
            known = self.executions.get(eid)
            if known is not None:
                known.archived = True
            return
        status = _STATUS_OF_ENTRY.get(entry.type)
        if status is None:
            return
        known = self.executions.get(eid)
        queue = entry.payload.get("queue", known.queue if known else "default")
        self.executions[eid] = KnownExecution(status, entry.type, entry.provenance.at, queue)


@dataclass
class SweepReport:
    timers_fired: list[str] = field(default_factory=list)
    recovered: list[Eid] = field(default_factory=list)
    restarted: list[Eid] = field(default_factory=list)
    repaired: list[Eid] = field(default_factory=list)
    waits_cleared: list[tuple[str, Eid]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return (
            len(self.timers_fired)
            + len(self.recovered)
            + len(self.restarted)
            + len(self.repaired)
            + len(self.waits_cleared)
        )


class Sweeper:
    def __init__(
        self,
        engine: Engine,
        *,
        repair_window: timedelta = timedelta(seconds=60),
        sweeper_id: str = "sweeper",
        source: ControlSource | None = None,
    ) -> None:
        self.engine = engine
        self.repair_window = repair_window
        self.actor = Actor.system(sweeper_id)
        self.view = ControlView(engine)
        self.source: ControlSource = source or self.view
        self._known: dict[Eid, KnownExecution] = {}

    async def run_once(self) -> SweepReport:
        report = SweepReport()
        now = self.engine.clock()
        await self._refresh_view()
        await self._fire_timers(now, report)
        await self._recover_dead(now, report)
        await self._restart_lost(now, report)
        await self._repair_control_log(now, report)
        await self._clear_orphan_waits(report)
        if report.total:
            log.info(
                "sweep_done",
                timers_fired=len(report.timers_fired),
                recovered=report.recovered,
                restarted=report.restarted,
                repaired=report.repaired,
                waits_cleared=len(report.waits_cleared),
            )
        return report

    async def run_forever(self, stop: asyncio.Event | None = None, interval: float = 60.0) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            await self.run_once()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval)

    # --- steps -------------------------------------------------------------------------

    async def _refresh_view(self) -> None:
        self._known = dict(await self.source.snapshot())

    async def _fire_timers(self, now: Timestamp, report: SweepReport) -> None:
        prov = self.engine.provenance(self.actor, frame_name="sweeper.timer")
        for timer in await self.engine.ports.timers.due(now):
            eid = timer.target.eid
            with suppress(UnknownExecution):
                await self.engine.enqueue_resume(
                    eid, f"timer:{timer.timer_id}", "timer", prov, repair=True
                )
                report.timers_fired.append(timer.timer_id)
                log.debug("timer_fired", eid=str(eid), timer_id=timer.timer_id)
            await self.engine.ports.timers.remove(timer)

    async def _recover_dead(self, now: Timestamp, report: SweepReport) -> None:
        prov = self.engine.provenance(self.actor, frame_name="sweeper.recovery")
        for eid, known in list(self._known.items()):
            if known.status is not ExecutionStatus.RUNNING:
                continue
            info = await self.engine.ports.ownership.inspect(eid)
            if info is None or not info.is_expired(now):
                continue
            with suppress(UnknownExecution):
                if await self.engine.enqueue_resume(
                    eid, f"recovery:{info.epoch}", "recovery", prov, repair=True
                ):
                    report.recovered.append(eid)
                    log.warning("execution_recovered", eid=str(eid), dead_epoch=info.epoch)

    async def _restart_lost(self, now: Timestamp, report: SweepReport) -> None:
        prov = self.engine.provenance(self.actor, frame_name="sweeper.restart")
        for eid, known in list(self._known.items()):
            if known.status is not ExecutionStatus.PENDING:
                continue
            if known.last_at + self.repair_window > now:
                continue
            task = Task(
                queue=known.queue,
                kind=TaskKind.START,
                target=FrameRef(eid, ROOT_FID),
                reason="start",
                enqueued_by=prov,
            )
            if await self.engine.enqueue(task, repair=True):
                report.restarted.append(eid)
                log.warning("start_reenqueued", eid=str(eid))

    async def _repair_control_log(self, now: Timestamp, report: SweepReport) -> None:
        for eid, known in list(self._known.items()):
            if known.status.is_terminal or known.last_at + self.repair_window > now:
                continue
            last: Entry | None = None
            for s in await self.engine.ports.journal.read(eid):
                if s.item.type in _STATUS_OF_ENTRY and s.item.type != EntryType.EXECUTION_CREATED:
                    last = s.item
            if last is None or last.type == known.last_type:
                continue
            if last.provenance.at + self.repair_window > now:
                continue
            entry = Entry(last.type, ROOT_FID, {"eid": str(eid), **last.payload}, last.provenance)
            seq = await self.engine.ports.control.announce(entry)
            self.view.apply(seq, entry)
            self._known[eid] = self.view.executions[eid]  # pragma: no mutate
            report.repaired.append(eid)
            log.warning("control_log_repaired", eid=str(eid), entry=last.type)

    async def _clear_orphan_waits(self, report: SweepReport) -> None:
        for channel, ref in await self.engine.ports.channel.all_waits():
            memo = await self.engine.memo(ref.eid)
            expected = Condition.channel(channel)
            if not memo.is_terminal and memo.suspended.get(ref.fid) == expected:
                continue
            await self.engine.ports.channel.clear_wait(channel, ref)
            report.waits_cleared.append((channel, ref.eid))
