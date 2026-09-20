"""The board. See flowlet/docs/specs/11-codeflow.md section 11.

An issue tracker is a projection of the account and an ingress for people. It
is never the system of record: it has no compare-and-set, no fencing and no
atomic transition.

Outbound is this reconciler: it follows the projection and materializes
executions as items. Inbound is the HTTP service, which is where a webhook
turns into `signal`, `cancel` or a review decision.

It never ends, so it is a process and not a workflow (section 2.1).
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol

from flowlet.log import get_logger

log = get_logger("flowlet_codeflow.board")


@dataclass(frozen=True, slots=True)
class Item:
    """What one execution looks like on a board."""

    key: str  # the eid: stable, and what the claim pins
    title: str
    status: str
    workflow: str
    body: str = ""
    labels: tuple[str, ...] = ()


class Board(Protocol):
    """The tracker. An implementation wraps an API; this side knows nothing."""

    async def create(self, item: Item) -> str:
        """Create it, and return the external id."""
        ...

    async def update(self, external_id: str, item: Item) -> None: ...

    async def comment(self, external_id: str, text: str) -> None: ...


@dataclass
class MemoryBoard:
    """A board in a dict, for a test and for a dry run."""

    items: dict[str, Item] = field(default_factory=dict)
    comments: dict[str, list[str]] = field(default_factory=dict)
    _next: int = 1

    async def create(self, item: Item) -> str:
        external_id = f"#{self._next}"
        self._next += 1
        self.items[external_id] = item
        return external_id

    async def update(self, external_id: str, item: Item) -> None:
        self.items[external_id] = item

    async def comment(self, external_id: str, text: str) -> None:
        self.comments.setdefault(external_id, []).append(text)


@dataclass
class Reconciler:
    """Mirrors executions onto a board. Drift resolves toward the account.

    The external id is pinned with a claim, so a second reconciler, or a
    restarted one, converges on the item that exists instead of creating a
    second one.
    """

    engine: Any
    projection: Any
    board: Board
    workflows: tuple[str, ...] = ("codeflow.task", "codeflow.feature", "codeflow.milestone")
    interval: float = 10.0
    _mirrored: dict[str, str] = field(default_factory=dict)

    async def run_once(self) -> int:
        await self.projection.refresh()
        changed = 0
        for workflow in self.workflows:
            for row in self.projection.executions(workflow=workflow, limit=500):
                if await self._reconcile(row):
                    changed += 1
        return changed

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("board_pass_failed")
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.interval)

    async def _reconcile(self, row: Any) -> bool:
        eid = str(row.eid)
        item = Item(
            key=eid,
            title=f"{row.workflow} {eid[:8]}",
            status=row.status.value,
            workflow=row.workflow,
            body=_body(row),
            labels=(row.status.value, row.queue),
        )
        external_id = self._mirrored.get(eid)
        if external_id is None:
            external_id = await self._pin(item)
            self._mirrored[eid] = external_id
            return True
        if self._same(external_id, item):
            return False
        await self.board.update(external_id, item)
        return True

    async def _pin(self, item: Item) -> str:
        """One item per execution, whoever gets there first."""
        proposed = await self.board.create(item)
        won, value = await self.engine.ports.dispatch.claim(
            f"board/{item.key}", {"external_id": proposed}
        )
        if won:
            return proposed
        # Another reconciler created it first. Ours is a duplicate, and the
        # account decides which one is real.
        log.info("board_item_raced", eid=item.key, kept=value["external_id"])
        return str(value["external_id"])

    def _same(self, external_id: str, item: Item) -> bool:
        current = getattr(self.board, "items", {}).get(external_id)
        return current == item


def _body(row: Any) -> str:
    lines = [f"workflow: {row.workflow} v{row.version}", f"queue: {row.queue}"]
    if row.suspended_on:
        lines.append(f"waiting for: {', '.join(row.suspended_on)}")
    if row.error_type:
        lines.append(f"error: {row.error_type}: {row.error_message}")
    if row.result is not None:
        lines.append(f"result: {row.result}")
    return "\n".join(lines)


__all__ = ["Board", "Item", "MemoryBoard", "Reconciler"]
