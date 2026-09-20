"""Retention and migrate on filesystem CairnDB, plus the projection's archived_at column."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowlet.adapters.cairndb import CairnBackend
from flowlet.adapters.memory import ManualClock
from flowlet.domain import Actor, Execution, ExecutionStatus, FrameRef, Site, Timer
from flowlet.runtime import Engine, EngineConfig, UnknownExecution
from tests.ids import E_ABC, E_DEF, E_NOPE, E_ZZZ

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


@pytest.fixture
async def backend(tmp_path, clock):
    b = CairnBackend.configure(
        {"storage": {"type": "filesystem", "path": str(tmp_path / "bucket")}}, now=clock
    )
    yield b
    await b.close()


@pytest.fixture
def engine(backend, clock) -> Engine:
    return Engine(backend.ports, Site("h", 1, "w-1"), clock=clock, config=EngineConfig())


async def drain(worker, limit=30):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit
    return n


async def test_new_port_methods_on_cairndb(backend, engine):
    s = backend.executions
    ex = Execution(E_ABC, "w", "1", None, engine.provenance(HUMAN))
    assert await s.create(ex)
    assert (await s.replace(E_ABC, lambda e: replace(e, version="2"))).version == "2"
    assert (await s.read(E_ABC)).version == "2"
    assert await s.replace(E_ZZZ, lambda e: e) is None
    await s.delete(E_ABC)
    assert await s.read(E_ABC) is None

    a = backend.archive
    assert await a.write(E_ABC, {"eid": E_ABC, "journal": [{"seq": 1, "event": {"x": 1}}]})
    assert not await a.write(E_ABC, {})
    assert (await a.read(E_ABC))["journal"][0]["event"] == {"x": 1}
    assert await a.read(E_NOPE) is None

    t = backend.timers
    await t.schedule(Timer(T0, FrameRef(E_ABC, "root/a#0")))
    await t.schedule(Timer(T0, FrameRef(E_ABC, "root/b#0")))
    await t.schedule(Timer(T0, FrameRef(E_DEF, "root/a#0")))
    assert await t.remove_for(E_ABC) == 2
    assert [x.target.eid for x in await t.due(T0)] == [E_DEF]

    lease = await backend.ownership.acquire(E_ABC, "w", 5)
    await lease.release()
    await backend.ownership.delete(E_ABC)
    assert await backend.ownership.inspect(E_ABC) is None


async def test_retention_end_to_end_on_cairndb(backend, engine, clock, tmp_path):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.send("out", 1)
        return "done"

    worker = engine.worker()
    projection = backend.projection(db_path=str(tmp_path / "view.sqlite"))
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert await backend.channel.scoped(eid) == [f"{eid}.out"]
    assert await backend.journal.tail(eid) > 0

    clock.advance(timedelta(days=31))
    report = await engine.retention(delay=timedelta(days=30), source=projection).run_once()
    assert report.archived == [eid]
    with pytest.raises(UnknownExecution):
        await engine.status(eid)
    assert await backend.journal.read(eid) == [] and await backend.journal.tail(eid) == 0
    assert await backend.channel.scoped(eid) == []
    assert await backend.channel.read(f"{eid}.out") == []
    archived = await engine.archive(eid)
    assert archived["channels"][f"{eid}.out"][0]["payload"] == 1
    assert [s.item.type for s in await engine.journal(eid)][-1] == "execution.completed"

    await projection.refresh()
    row = projection.execution(eid)
    assert row.status == ExecutionStatus.COMPLETED and row.archived_at is not None
    assert await projection.terminal_before(clock() + timedelta(days=1)) == {}
    assert (
        await engine.retention(delay=timedelta(days=30), source=projection).run_once()
    ).archived == []
    await projection.stop()


async def test_migrate_on_cairndb_updates_record_and_projection(backend, engine, clock, tmp_path):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")
        return 1

    @engine.workflow("w", "2")
    async def w2(ctx):
        await ctx.receive("x")
        return 2

    worker = engine.worker()
    projection = backend.projection(db_path=str(tmp_path / "view.sqlite"))
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    await engine.migrate(eid, "2", by=HUMAN)
    await drain(worker)  # resume task: still suspended on x
    await engine.signal(eid, "x", None, by=HUMAN)
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": 2}
    await projection.refresh()
    assert projection.execution(eid).version == "2"
    await projection.stop()
