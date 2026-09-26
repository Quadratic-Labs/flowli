"""The board reconciler. Spec 11 section 11.

The tracker is a projection: drift resolves toward the account, and the item
is pinned by a claim so a second reconciler converges instead of duplicating.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from flowli.domain import ExecutionStatus

from flowli_codeflow import MemoryBoard, Reconciler


@dataclass
class Row:
    """A row as the projection gives it."""

    eid: str
    workflow: str = "codeflow.task"
    version: str = "1"
    status: ExecutionStatus = ExecutionStatus.RUNNING
    queue: str = "default"
    suspended_on: list[str] | None = None
    error_type: str | None = None
    error_message: str | None = None
    result: Any = None


class FakeProjection:
    def __init__(self, rows: list[Row]) -> None:
        self.rows = rows
        self.refreshed = 0

    async def refresh(self) -> int:
        self.refreshed += 1
        return self.refreshed

    def executions(self, *, workflow: str | None = None, limit: int = 100) -> list[Row]:
        return [r for r in self.rows if workflow is None or r.workflow == workflow][:limit]


@pytest.fixture
def engine(backend):
    from flowli.domain import Site
    from flowli.runtime import Engine

    return Engine(backend.ports, Site.local("w-1"))


def reconciler(engine, rows, board=None) -> Reconciler:
    return Reconciler(
        engine=engine,
        projection=FakeProjection(rows),
        board=board or MemoryBoard(),
        workflows=("codeflow.task",),
    )


async def test_an_execution_becomes_an_item(engine):
    board = MemoryBoard()
    rec = reconciler(engine, [Row(eid="e1")], board)

    assert await rec.run_once() == 1

    (item,) = board.items.values()
    assert item.key == "e1"
    assert item.status == "running"
    assert "codeflow.task" in item.body


async def test_a_second_pass_changes_nothing(engine):
    board = MemoryBoard()
    rows = [Row(eid="e1")]
    rec = reconciler(engine, rows, board)
    await rec.run_once()

    assert await rec.run_once() == 0
    assert len(board.items) == 1


async def test_the_account_decides_what_the_item_says(engine):
    board = MemoryBoard()
    rows = [Row(eid="e1")]
    rec = reconciler(engine, rows, board)
    await rec.run_once()
    (external_id,) = board.items

    # Someone edited the item on the tracker; the account wins.
    board.items[external_id] = board.items[external_id].__class__(
        key="e1",
        title="edited by a person",
        status="anything",
        workflow="x",
    )
    rows[0] = Row(eid="e1", status=ExecutionStatus.COMPLETED, result={"ok": True})

    assert await rec.run_once() == 1
    assert board.items[external_id].status == "completed"
    assert "result" in board.items[external_id].body


async def test_a_failure_shows_on_the_board(engine):
    board = MemoryBoard()
    rows = [
        Row(
            eid="e1",
            status=ExecutionStatus.FAILED,
            error_type="StepFailed",
            error_message="the gate failed",
        )
    ]
    await reconciler(engine, rows, board).run_once()

    (item,) = board.items.values()
    assert "StepFailed: the gate failed" in item.body


async def test_two_reconcilers_converge_on_one_item(engine):
    """The external id is pinned by a claim, so the loser keeps the winner's."""
    rows = [Row(eid="e1")]
    first, second = MemoryBoard(), MemoryBoard()
    await reconciler(engine, rows, first).run_once()
    await reconciler(engine, rows, second).run_once()

    won, value = await engine.ports.dispatch.claim("board/e1", {"external_id": "#99"})
    assert not won
    assert value["external_id"] == "#1"  # whoever got there first


async def test_a_workflow_outside_the_list_is_ignored(engine):
    board = MemoryBoard()
    rows = [Row(eid="e1", workflow="something.else")]
    assert await reconciler(engine, rows, board).run_once() == 0
    assert board.items == {}
