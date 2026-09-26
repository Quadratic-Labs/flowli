from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.domain import (
    ROOT_FID,
    Actor,
    ExecutionStatus,
    FrameRef,
    Site,
    Timer,
    execution_channel,
)
from flowli.runtime import Engine, EngineConfig, RetentionReport, UnknownExecution
from tests.ids import E_NOPE

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")
OPS = Actor.human("ops@example.com")


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend(clock=ManualClock(T0))


@pytest.fixture
def engine(backend) -> Engine:
    return Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(),
    )


async def drain(worker, limit=30):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit
    return n


# --- migrate ------------------------------------------------------------------------


async def test_migrate_unblocks_nondeterministic_execution(backend, engine):
    @engine.workflow("w", "1")
    async def w1(ctx):
        a = await ctx.step(lambda: 1, name="a")
        m = await ctx.receive("go")
        return a + m.payload

    # v2 keeps frame "a" with the same digest, and adds a new step after the receive
    @engine.workflow("w", "2")
    async def w2(ctx):
        a = await ctx.step(lambda: 1, name="a")
        m = await ctx.receive("go")
        b = await ctx.step(lambda: 100, name="b")
        return a + m.payload + b

    worker = engine.worker()
    eid = await engine.start(w1, by=HUMAN)
    await drain(worker)
    # the code of v1 changes under the live execution: frame "a" now has other args
    engine.registry._by_key.pop(("w", "1"))

    @engine.workflow("w", "1")
    async def w1_changed(ctx):
        a = await ctx.step(lambda x: x, 5, name="a")
        m = await ctx.receive("go")
        return a + m.payload

    await engine.signal(eid, "go", 10, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    assert backend.ownership.lease_doc(eid).state["blocked"] == "nondeterminism"

    after = await engine.migrate(eid, "2", by=OPS)
    assert after.version == "2" and (await engine.execution(eid)).version == "2"
    migrated = [s.item for s in await engine.journal(eid) if s.item.type == "execution.migrated"]
    assert migrated[0].payload == {"from_version": "1", "version": "2"}
    assert migrated[0].provenance.actor == OPS
    assert migrated[0].provenance.code.workflow == "w"
    assert migrated[0].provenance.code.version == "2"
    assert migrated[0].provenance.code.frame_name == "migrate"
    control_migrated = next(
        s.item for s in backend.control.entries if s.item.type == "execution.migrated"
    )
    assert control_migrated.fid == ROOT_FID
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED  # migrated does not change status
    tasks = await backend.queue.pending("default")
    assert [t.task_id for t in tasks] == [f"resume:{eid}:migrate:2"]
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["task_id"] == f"resume:{eid}:migrate:2"
    ]
    assert enqueued[-1].payload["reason"] == "migrate:2"

    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": 111}
    # frame "a" was not run again: one frame.started for it in the whole journal
    starts = [s.item for s in await engine.journal(eid) if s.item.type == "frame.started"]
    assert [e.fid for e in starts].count("root/a#0") == 1


async def test_migrate_refuses_terminal_and_is_noop_on_same_version(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")
        return 1

    eid = await engine.start(w, by=HUMAN)
    worker = engine.worker()
    await drain(worker)
    same = await engine.migrate(eid, "1", by=OPS)
    assert same.version == "1" and await backend.queue.pending("default") == []
    await engine.signal(eid, "x", None, by=HUMAN)
    await drain(worker)
    with pytest.raises(ValueError):
        await engine.migrate(eid, "2", by=OPS)
    with pytest.raises(UnknownExecution) as exc_info:
        await engine.migrate(E_NOPE, "2", by=OPS)
    assert exc_info.value.eid == E_NOPE


async def test_migrate_raises_unknown_execution_when_cas_races_away(backend, engine, monkeypatch):
    """`ports.executions.replace` returning None (record vanished mid-CAS) surfaces the eid."""

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)

    async def vanished(eid, fn):
        return None

    monkeypatch.setattr(backend.executions, "replace", vanished)
    with pytest.raises(UnknownExecution) as exc_info:
        await engine.migrate(eid, "2", by=OPS)
    assert exc_info.value.eid == eid


# --- retention -------------------------------------------------------------------------


async def test_retention_archives_old_terminal_executions_only(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx, x):
        await ctx.send("out", {"x": x})
        return await ctx.step(lambda: x * 2, name="dbl")

    @engine.workflow("waits", "1")
    async def waits(ctx):
        await ctx.receive("never", timeout=timedelta(days=90))

    worker = engine.worker()
    old = await engine.start(w, 1, by=HUMAN)
    live = await engine.start(waits, by=HUMAN)
    await drain(worker)
    backend.clock.advance(timedelta(days=40))
    recent = await engine.start(w, 2, by=HUMAN)
    await drain(worker)
    # a leftover timer and a stale wait marker for the old execution
    await backend.timers.schedule(Timer(T0 + timedelta(days=100), FrameRef(old, "root/x#0")))
    await backend.channel.register_wait("some.channel", FrameRef(old, "root/x#0"))

    retention = engine.retention(delay=timedelta(days=30))
    report = await retention.run_once()
    assert report.archived == [old] and report.cleaned == []

    # live state of `old` is gone
    with pytest.raises(UnknownExecution):
        await engine.status(old)
    assert await backend.journal.read(old) == []
    assert await backend.channel.scoped(old) == []
    assert backend.ownership.lease_doc(old) is None
    assert all(t.target.eid != old for t in backend.timers.timers.values())
    assert all(ref.eid != old for _, ref in await backend.channel.all_waits())
    archived_entry = backend.control.entries[-1].item
    assert archived_entry.type == "execution.archived"
    assert archived_entry.fid == ROOT_FID
    assert archived_entry.provenance.actor == Actor.system("retention")
    assert archived_entry.provenance.code.frame_name == "retention"

    # the archive holds the journal, the record and the scoped channels
    data = await engine.archive(old)
    assert data["eid"] == str(old)
    assert data["execution"]["eid"] == str(old)
    assert data["archived_at"] == "2026-10-17T09:00:00.000000Z"
    types = [item["entry"]["type"] for item in data["journal"]]
    assert types[0] == "execution.started" and types[-1] == "execution.completed"
    assert list(data["channels"]) == [execution_channel(old, "out")]
    assert data["channels"][execution_channel(old, "out")][0]["payload"] == {"x": 1}
    # engine.journal falls back to the archive
    replayed = await engine.journal(old)
    assert [s.item.type for s in replayed] == types
    assert replayed[0].item.provenance.site.worker_id == "w-1"

    # the recent terminal and the live suspended executions are untouched
    assert await engine.status(recent) == ExecutionStatus.COMPLETED
    assert await engine.status(live) == ExecutionStatus.SUSPENDED
    assert (await retention.run_once()).archived == []


async def test_retention_rerun_after_partial_failure_finishes_cleanup(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    backend.clock.advance(timedelta(days=31))
    retention = engine.retention(delay=timedelta(days=30))

    # crash simulation: archive written and announced, journal deleted, record still there
    original_delete = backend.executions.delete

    async def crash(eid_):
        raise RuntimeError("disk on fire")

    backend.executions.delete = crash
    with pytest.raises(RuntimeError):
        await retention.run_once()
    backend.executions.delete = original_delete
    assert await engine.archive(eid) is not None
    assert await backend.executions.read(eid) is not None
    assert await backend.journal.read(eid) == []
    assert [s.item.type for s in backend.control.entries].count("execution.archived") == 0

    # fold() itself, not just the report it fills in, must report exactly False here
    report = RetentionReport()
    result = await retention.fold(eid, report)
    assert result is False
    assert report.archived == [] and report.cleaned == [eid]
    assert await backend.executions.read(eid) is None
    assert [s.item.type for s in backend.control.entries].count("execution.archived") == 1
    assert (await retention.run_once()).cleaned == []


async def test_fold_on_unknown_eid_is_a_true_noop(backend, engine):
    """fold() must not announce or touch the report for an eid with no record, no journal,
    no channels and no archive: there is nothing to archive or clean up."""
    report = RetentionReport()
    result = await engine.retention().fold(E_NOPE, report)
    assert result is False
    assert report.archived == [] and report.cleaned == []
    assert backend.control.entries == []


async def test_fold_reannounces_when_only_the_archive_is_left(backend, engine):
    """Simulates a crash between the last delete and the announcement: a rerun of fold()
    finds only the archive and must re-announce, recording the eid as cleaned."""
    eid = E_NOPE  # no record, journal or channels ever existed for it
    await backend.archive.write(eid, {"eid": str(eid), "journal": []})

    report = RetentionReport()
    result = await engine.retention().fold(eid, report)
    assert result is False
    assert report.archived == [] and report.cleaned == [eid]
    assert backend.control.entries[-1].item.type == "execution.archived"
    assert backend.control.entries[-1].item.payload["eid"] == str(eid)


async def test_journal_of_an_archive_missing_the_journal_key_is_empty(backend, engine):
    """`engine.journal` falls back to the archive when the live journal is gone. A
    malformed or legacy archive without a "journal" key must not crash it -- the
    defensive default is an empty list, not None (which the entry-parsing comprehension
    would then reject as not iterable)."""
    eid = E_NOPE
    await backend.archive.write(eid, {"eid": str(eid)})  # no "journal" key at all

    assert await engine.journal(eid) == []


async def test_worker_drops_task_for_archived_execution(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    worker = engine.worker()
    await drain(worker)
    await engine.enqueue_resume(eid, "late", "late", engine.provenance(HUMAN))
    backend.clock.advance(timedelta(days=31))
    await engine.retention(delay=timedelta(days=30)).run_once()
    assert len(await backend.queue.pending("default")) == 1
    assert await worker.run_once()  # task dropped, not crashed
    assert await backend.queue.pending("default") == []


async def test_retention_deletes_the_evidence_of_an_execution(backend, engine):
    """The archive does not hold evidence, so retention must remove it."""
    from flowli.domain import EvidenceRef

    @engine.workflow("plain", "1")
    async def plain(ctx):
        return await ctx.step(lambda: "done", name="work")

    worker = engine.worker()
    eid = await engine.start(plain, by=HUMAN)
    await drain(worker)

    ref = EvidenceRef(eid, "root/work#0", 1)
    await backend.evidence.append_log(ref, b'{"event": "worked"}\n')
    assert await backend.evidence.list(eid) != []

    backend.clock.advance(timedelta(days=40))
    await engine.retention().run_once()

    assert await backend.evidence.list(eid) == []
