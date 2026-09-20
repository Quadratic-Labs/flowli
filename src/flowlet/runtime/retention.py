"""Retention: archive finished executions and delete their live state.

See docs/specs/05-protocols.md section 11. Cron-shaped, idempotent, no lease.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from flowlet.codec import unstructure
from flowlet.domain import ROOT_FID, Actor, Eid, Entry, EntryType
from flowlet.log import get_logger

from .engine import Engine
from .sweeper import ControlSource, ControlView

log = get_logger("flowlet.retention")


@dataclass
class RetentionReport:
    archived: list[Eid] = field(default_factory=list)
    cleaned: list[Eid] = field(default_factory=list)  # archive existed already, live state removed


class Retention:
    def __init__(
        self,
        engine: Engine,
        *,
        delay: timedelta = timedelta(days=30),
        source: ControlSource | None = None,
        retention_id: str = "retention",
    ) -> None:
        self.engine = engine
        self.delay = delay
        self.source: ControlSource = source or ControlView(engine)
        self.actor = Actor.system(retention_id)

    async def run_once(self) -> RetentionReport:
        report = RetentionReport()
        before = self.engine.clock() - self.delay
        for eid in sorted(await self.source.terminal_before(before), key=str):  # pragma: no mutate
            await self.fold(eid, report)
        if report.archived or report.cleaned:
            log.info("retention_done", archived=report.archived, cleaned=report.cleaned)
        return report

    async def run_forever(
        self, stop: asyncio.Event | None = None, interval: float = 3600.0
    ) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            await self.run_once()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval)

    async def fold(self, eid: Eid, report: RetentionReport | None = None) -> bool:
        """Archive one execution, then delete its live state. Return True when archived now.

        Order: archive (put-if-absent), deletes, then the announcement. A rerun after a
        crash finds the archive, finishes the deletes and announces. The announcement can
        repeat after a crash between the last delete and the announcement. That is harmless.
        """
        report = report or RetentionReport()
        ports = self.engine.ports
        record = await ports.executions.read(eid)
        journal = await ports.journal.read(eid)
        channels = await ports.channel.scoped(eid)
        archived = await ports.archive.read(eid) is not None
        if record is None and not journal and not channels:
            if archived:
                await self._announce(eid)
                report.cleaned.append(eid)
            return False

        written = False
        if journal and not archived:
            data: dict[str, Any] = {
                "eid": str(eid),
                "execution": None if record is None else unstructure(record),
                "archived_at": self.engine.clock().to_iso(),
                "journal": [{"seq": s.seq, "entry": unstructure(s.item)} for s in journal],
                "channels": {
                    name: [unstructure(m) for m in await ports.channel.read(name)]
                    for name in channels
                },
            }
            written = await ports.archive.write(eid, data)
        elif journal and archived:
            log.warning("archive_exists_with_journal", eid=str(eid))

        await ports.journal.delete(eid)
        for name in channels:
            await ports.channel.delete_channel(name)
        await ports.timers.remove_for(eid)
        for channel, ref in await ports.channel.all_waits():
            if ref.eid == eid:
                await ports.channel.clear_wait(channel, ref)
        # The archive does not hold evidence: an agent transcript can be large
        # and the archive is one object (05-protocols.md, section 11).
        await ports.evidence.delete_for(eid)
        await ports.ownership.delete(eid)
        await ports.executions.delete(eid)
        await self._announce(eid)

        (report.archived if written else report.cleaned).append(eid)
        log.info("execution_archived" if written else "execution_cleaned", eid=str(eid))
        return written

    async def _announce(self, eid: Eid) -> None:
        prov = self.engine.provenance(self.actor, frame_name="retention")
        await self.engine.ports.control.announce(
            Entry(EntryType.EXECUTION_ARCHIVED, ROOT_FID, {"eid": str(eid)}, prov)
        )
