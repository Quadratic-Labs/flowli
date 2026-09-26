"""The SQLite projection of the control log, fed by the engine on filesystem CairnDB."""

from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.cairndb import CairnBackend
from flowli.adapters.cairndb_projection import PrefixRegistry, WorkflowProjection
from flowli.adapters.memory import ManualClock
from flowli.codec import unstructure
from flowli.domain import Actor, ExecutionStatus, FrameRef, NonRetryableError, Site
from flowli.runtime import Engine, EngineConfig, KnownExecution

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
    return Engine(
        backend.ports,
        Site(host="hosta", pid=1, worker_id="w-1"),
        clock=clock,
        config=EngineConfig(code_ref="git:abc"),
    )


@pytest.fixture
async def projection(backend, tmp_path) -> WorkflowProjection:
    p = backend.projection(db_path=str(tmp_path / "wf_view.sqlite"))
    yield p
    await p.stop()


async def drain(worker, limit=30):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit
    return n


def test_prefix_registry():
    reg = PrefixRegistry()

    async def exact(conn, entry): ...

    async def by_prefix(conn, entry): ...

    reg.register("execution.created", exact)
    reg.register_prefix("announce.", by_prefix)
    assert reg.has_handler("execution.created") and reg.get_handler("execution.created") is exact
    assert reg.has_handler("announce.review.requested")
    assert reg.get_handler("announce.anything") is by_prefix
    assert not reg.has_handler("frame.started")


def test_prefix_registry_get_handler_prefers_an_exact_match_over_an_overlapping_prefix():
    """An exact registration wins even when the same event type also matches a
    registered prefix -- `get_handler` checks the exact map first and must not
    fall through to the prefix scan regardless of what it finds there."""
    reg = PrefixRegistry()

    async def exact(conn, entry): ...

    async def by_prefix(conn, entry): ...

    reg.register("announce.special", exact)
    reg.register_prefix("announce.", by_prefix)
    assert reg.get_handler("announce.special") is exact


def test_prefix_registry_get_handler_names_the_event_type_when_nothing_matches():
    """The final fallback re-raises with the event type actually looked up, not
    a swallowed one -- a caller debugging `HandlerNotFoundError` needs to see
    what type was missing."""
    from cairndb.client.registry import HandlerNotFoundError

    reg = PrefixRegistry()
    with pytest.raises(HandlerNotFoundError, match="frame.started"):
        reg.get_handler("frame.started")


async def test_init_config_defaults_and_overrides(backend, tmp_path):
    """`WorkflowProjection.__init__` builds its `ClientConfig` from ITS OWN
    defaults (`version='1'`, `poll_interval=5.0`, per 08-projection.md section 4's
    `backend.projection(...)` signature) and must not silently fall back to
    ClientConfig's own, unrelated defaults -- and an explicit override of either
    must actually reach the config, not be dropped."""
    default_p = WorkflowProjection(backend.db, db_path=str(tmp_path / "d.sqlite"))
    assert default_p.config.schema_version == "1"
    assert default_p.config.poll_interval_seconds == 5.0

    custom_p = WorkflowProjection(
        backend.db, db_path=str(tmp_path / "c.sqlite"), version="7", poll_interval=9.0
    )
    assert custom_p.config.schema_version == "7"
    assert custom_p.config.poll_interval_seconds == 9.0


async def test_wait_for_defaults_to_a_30_second_timeout():
    """`wait_for`'s own default timeout (08-projection.md section 4) is 30
    seconds when a caller passes none -- distinct from any caller's own
    default (e.g. `flowli.api.reads.freshness`'s), which always passes an
    explicit value through and so never exercises this one."""
    calls = []

    class FakeUpdater:
        async def wait_for_sequence(self, target, timeout):
            calls.append(timeout)
            return True

    from flowli.adapters.cairndb import SEQ_BASE

    projection = object.__new__(WorkflowProjection)
    projection._updater = FakeUpdater()
    assert await projection.wait_for(SEQ_BASE + 5) is True
    assert calls == [30.0]


async def test_executions_table_follows_lifecycle(backend, engine, projection):
    @engine.workflow("ok", "1")
    async def ok(ctx, x):
        return await ctx.step(lambda: x + 1, name="inc")

    @engine.workflow("waits", "2")
    async def waits(ctx):
        return (await ctx.receive("go")).payload

    @engine.workflow("bad", "1")
    async def bad(ctx):
        raise NonRetryableError("nope")

    worker = engine.worker(queues=["default", "slow"])
    e_ok = await engine.start(ok, 41, key="ok:41", by=HUMAN)
    e_wait = await engine.start(waits, by=HUMAN, queue="slow")
    e_bad = await engine.start(bad, by=HUMAN)

    seq = await projection.refresh()
    assert seq is not None
    rows = {r.eid: r for r in projection.executions()}
    assert rows[e_ok].status == ExecutionStatus.PENDING
    assert rows[e_ok].dispatch_key == "ok:41" and rows[e_ok].workflow == "ok"
    assert rows[e_ok].created_by_kind == "human" and rows[e_ok].created_by_id == HUMAN.id
    assert rows[e_wait].queue == "slow" and rows[e_wait].version == "2"

    await drain(worker)
    await projection.refresh()
    ok_row = projection.execution(e_ok)
    assert ok_row.status == ExecutionStatus.COMPLETED and ok_row.result == 42
    assert ok_row.epoch == 1 and ok_row.worker_id == "w-1" and ok_row.host == "hosta"
    assert ok_row.last_type == "execution.completed"
    wait_row = projection.execution(e_wait)
    assert wait_row.status == ExecutionStatus.SUSPENDED
    assert wait_row.suspended_on == [f"channel:{e_wait}.go"]
    bad_row = projection.execution(e_bad)
    assert bad_row.status == ExecutionStatus.FAILED
    assert bad_row.error_type == "StepFailed" or bad_row.error_type == "NonRetryableError"
    assert "nope" in bad_row.error_message

    assert projection.counts_by_status() == {
        ExecutionStatus.COMPLETED: 1,
        ExecutionStatus.SUSPENDED: 1,
        ExecutionStatus.FAILED: 1,
    }
    assert [r.eid for r in projection.executions(status=ExecutionStatus.SUSPENDED)] == [e_wait]
    assert [r.eid for r in projection.executions(workflow="ok")] == [e_ok]

    # read-your-writes: signal, then wait for the announce sequence
    await engine.signal(e_wait, "go", "now", by=Actor.system("cron"))
    await drain(worker)
    last = (await backend.control.read())[-1].seq
    assert await projection.wait_for(last, timeout=5)
    row = projection.execution(e_wait)
    assert row.status == ExecutionStatus.COMPLETED and row.suspended_on is None and row.epoch == 2

    tasks = projection.connect().execute("SELECT kind, reason FROM tasks ORDER BY seq").fetchall()
    kinds = [(t["kind"], t["reason"]) for t in tasks]
    assert kinds.count(("start", "start")) == 3
    assert ("resume", "message:go") in kinds


async def test_executions_default_limit_and_combined_filters(backend, engine, projection):
    """08-projection.md section 4: `executions(status=None, workflow=None,
    parent_eid=None, limit=100, after=None)`. The default `limit` is 100, not
    some neighboring number, and `status`+`workflow` together must combine
    with AND (matching only rows that satisfy both), not garble the query."""
    import aiosqlite

    from flowli.domain import new_eid

    @engine.workflow("bulk", "1")
    async def bulk(ctx):
        return await ctx.receive("go")  # stays PENDING/SUSPENDED, never terminal

    @engine.workflow("other", "1")
    async def other(ctx):
        return await ctx.receive("go")

    await engine.start(bulk, by=HUMAN)
    await engine.start(other, by=HUMAN)
    await projection.refresh()  # the sqlite file must already exist

    # -- status + workflow together: only the row matching BOTH --
    assert [
        r.workflow for r in projection.executions(status=ExecutionStatus.PENDING, workflow="bulk")
    ] == ["bulk"]
    assert projection.executions(status=ExecutionStatus.PENDING, workflow="nope") == []

    # -- the default `limit` is 100, not some neighboring number --
    async with aiosqlite.connect(projection.path) as conn:
        await conn.executemany(
            "INSERT INTO executions (eid, workflow, version, status, queue, created_at, "
            "updated_at, last_type, last_seq) "
            "VALUES (?, 'bulk-wf', '1', 'pending', 'default', 'T', 'T', 'execution.created', ?)",
            [(str(new_eid()), str(i)) for i in range(110)],
        )
        await conn.commit()
    assert len(projection.executions(workflow="bulk-wf")) == 100


async def test_executions_after_cursor_breaks_updated_at_ties_by_eid(backend, engine, projection):
    """`executions(after=)` pages by (updated_at DESC, eid ASC) (08-projection.md
    section 4: 'after = (updated_at, eid) keyset'). Two rows sharing the SAME
    updated_at (created on the same clock tick) must be split by the `eid`
    tiebreaker, not silently dropped from the next page."""

    @engine.workflow("tie", "1")
    async def tie(ctx):
        return 1

    e1 = await engine.start(tie, by=HUMAN)
    e2 = await engine.start(tie, by=HUMAN)  # same clock tick: same updated_at
    await drain(engine.worker())
    await projection.refresh()

    rows = projection.executions()
    assert len(rows) == 2 and len({r.updated_at for r in rows}) == 1  # genuinely tied
    first_eid, second_eid = sorted((e1, e2))

    page1 = projection.executions(limit=1)
    assert [r.eid for r in page1] == [first_eid]

    page2 = projection.executions(after=(page1[0].updated_at, str(page1[0].eid)), limit=10)
    assert [r.eid for r in page2] == [second_eid]


async def test_lifecycle_entry_for_missing_row_creates_it(backend, engine, projection):
    """08-projection.md section 3: 'A lifecycle entry for an unknown eid creates the
    row. This happens when the control log was pruned before execution.created.'

    The engine always writes execution.created first, so exercise the fallback
    insert/update pair in _on_lifecycle directly, for an eid the projection has
    never seen."""
    import aiosqlite
    from cairndb import Event, EventType, SchemaVersion, SequencedEvent, SequenceNumber

    from flowli.domain import ROOT_FID, Code, EntryType, Provenance, new_eid

    @engine.workflow("seed", "1")
    async def seed(ctx):
        return 1

    worker = engine.worker()
    await engine.start(seed, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file/schema must already exist

    def make_prov(epoch, worker_id, host):
        return Provenance.now(
            actor=Actor.worker(worker_id),
            site=Site(host=host, pid=1, worker_id=worker_id, epoch=epoch),
            code=Code(workflow="orphan-wf", version="9", frame_kind="root", frame_name="root"),
        )

    def make_entry(etype, seq, payload, prov):
        event = Event(
            event_type=EventType(etype),
            timestamp=prov.at,
            payload=payload,
            schema_version=SchemaVersion("1.0.0"),
            metadata={"provenance": unstructure(prov), "fid": ROOT_FID},
        )
        return SequencedEvent(sequence=SequenceNumber(commit=seq, index=0), event=event)

    async def apply(seq_event):
        async with aiosqlite.connect(projection.path) as conn:
            await projection._on_lifecycle(conn, seq_event)
            await conn.commit()

    # Case A: the very first entry the projection ever sees for this eid is already
    # terminal (everything before it, execution.created included, was pruned). The
    # fallback-created row must carry the same fields a normal completion would set,
    # not just the bare workflow/version/status columns.
    eid_a = new_eid()
    seq_a = make_entry(
        EntryType.EXECUTION_COMPLETED,
        101,
        {"eid": str(eid_a), "value": {"n": 42}},
        make_prov(epoch=11, worker_id="w-a", host="host-a"),
    )
    await apply(seq_a)
    row_a = projection.execution(eid_a)
    assert row_a is not None
    assert row_a.workflow == "orphan-wf" and row_a.version == "9"
    assert row_a.status == ExecutionStatus.COMPLETED
    assert row_a.result == {"n": 42}
    assert row_a.epoch == 11 and row_a.worker_id == "w-a" and row_a.host == "host-a"
    raw_a = (
        projection.connect()
        .execute("SELECT last_seq FROM executions WHERE eid = ?", (str(eid_a),))
        .fetchone()
    )
    assert raw_a["last_seq"] == str(seq_a.sequence)

    # Case B: a non-terminal orphan row, then a later entry updates it in place --
    # the ordinary (non-fallback) path through the same function.
    eid_b = new_eid()
    seq_b1 = make_entry(
        EntryType.EXECUTION_STARTED,
        201,
        {"eid": str(eid_b), "args": []},
        make_prov(epoch=1, worker_id="w-b", host="host-b"),
    )
    await apply(seq_b1)
    row_b1 = projection.execution(eid_b)
    assert row_b1.status == ExecutionStatus.RUNNING
    assert row_b1.result is None

    seq_b2 = make_entry(
        EntryType.EXECUTION_SUSPENDED,
        202,
        {"eid": str(eid_b), "on": ["x"]},
        make_prov(epoch=1, worker_id="w-b", host="host-b"),
    )
    await apply(seq_b2)
    row_b2 = projection.execution(eid_b)
    assert row_b2.status == ExecutionStatus.SUSPENDED
    assert row_b2.suspended_on == ["x"]
    assert row_b2.result is None  # never completed: must stay NULL, not the text "null"
    raw_b2 = (
        projection.connect()
        .execute("SELECT last_seq, result FROM executions WHERE eid = ?", (str(eid_b),))
        .fetchone()
    )
    assert raw_b2["last_seq"] == str(seq_b2.sequence)
    assert raw_b2["result"] is None

    seq_b3 = make_entry(
        EntryType.EXECUTION_RESUMED,
        203,
        {"eid": str(eid_b), "epoch": 2, "reason": "signal"},
        make_prov(epoch=2, worker_id="w-b", host="host-b"),
    )
    await apply(seq_b3)
    raw_b3 = (
        projection.connect()
        .execute("SELECT suspended_on FROM executions WHERE eid = ?", (str(eid_b),))
        .fetchone()
    )
    assert raw_b3["suspended_on"] is None  # cleared on resume, not the text "null"


async def test_on_task_field_mapping_and_defensive_defaults(backend, engine, projection):
    """08-projection.md section 3: the `tasks` table is 'seq, task_id, queue, kind,
    eid, fid, reason, enqueued_at, actor_kind, actor_id', one row per task.enqueued
    entry. Exercise `_on_task` directly: once with a fully-populated entry (every
    column must carry its OWN field, not a neighbor's, and not None), and once with
    an entry missing target/reason/actor/at (the NOT NULL columns must fall back to
    "", never NULL)."""
    import aiosqlite
    from cairndb import Event, EventType, SchemaVersion, SequencedEvent, SequenceNumber

    from flowli.domain import ROOT_FID, Code, EntryType, Provenance, new_eid

    @engine.workflow("seed", "1")
    async def seed(ctx):
        return 1

    worker = engine.worker()
    await engine.start(seed, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file/schema must already exist

    def make_prov(actor):
        return Provenance.now(
            actor=actor,
            site=Site(host="host-t", pid=1, worker_id="w-t"),
            code=Code(workflow="orphan-wf", version="9", frame_kind="root", frame_name="root"),
        )

    def make_entry(seq, payload, metadata):
        event = Event(
            event_type=EventType(EntryType.TASK_ENQUEUED),
            timestamp=Timestamp.now(),
            payload=payload,
            schema_version=SchemaVersion("1.0.0"),
            metadata=metadata,
        )
        return SequencedEvent(sequence=SequenceNumber(commit=seq, index=0), event=event)

    async def apply(seq_event):
        async with aiosqlite.connect(projection.path) as conn:
            await projection._on_task(conn, seq_event)
            await conn.commit()

    # Case A: a fully-populated task.enqueued -- every tasks column must carry
    # its OWN field, not a neighboring one, and not None.
    eid = new_eid()
    prov_a = make_prov(Actor.worker("w-full"))
    seq_a = make_entry(
        301,
        {
            "task_id": "tid-full",
            "queue": "default",
            "kind": "start",
            "target": {"eid": str(eid), "fid": "root/child:k"},
            "reason": "start",
        },
        {"provenance": unstructure(prov_a), "fid": ROOT_FID},
    )
    await apply(seq_a)
    row = (
        projection.connect()
        .execute("SELECT * FROM tasks WHERE task_id = ?", ("tid-full",))
        .fetchone()
    )
    assert row["eid"] == str(eid)
    assert row["fid"] == "root/child:k"
    assert row["reason"] == "start"
    assert row["enqueued_at"] == prov_a.at.to_iso()
    assert row["actor_kind"] == "worker"
    assert row["actor_id"] == "w-full"

    # `TaskRow.from_row` must carry each column into its OWN field, not None
    # and not a neighbor's.
    task_rows = projection.tasks(eid=str(eid))
    assert len(task_rows) == 1
    tr = task_rows[0]
    assert tr.seq == row["seq"]
    assert tr.task_id == "tid-full"
    assert tr.queue == "default"
    assert tr.kind == "start"
    assert tr.eid == str(eid)
    assert tr.fid == "root/child:k"
    assert tr.reason == "start"
    assert tr.enqueued_at == prov_a.at.to_iso()
    assert tr.actor_kind == "worker"
    assert tr.actor_id == "w-full"

    # Case B: target/reason/actor/at all absent -- the NOT NULL columns must
    # default to "", never NULL (which would violate the schema).
    seq_b = make_entry(
        302,
        {"task_id": "tid-bare", "queue": "default", "kind": "start"},
        {"provenance": {}, "fid": ROOT_FID},
    )
    await apply(seq_b)
    row_b = (
        projection.connect()
        .execute("SELECT * FROM tasks WHERE task_id = ?", ("tid-bare",))
        .fetchone()
    )
    assert row_b["eid"] == ""
    assert row_b["fid"] == ""
    assert row_b["reason"] == ""
    assert row_b["enqueued_at"] == ""
    assert row_b["actor_kind"] is None
    assert row_b["actor_id"] is None


async def test_on_created_field_mapping_and_defensive_defaults(backend, engine, projection):
    """08-projection.md section 3: `eid, workflow, version, queue, dispatch_key,
    parent_eid, parent_fid` come from the `execution.created` payload, and
    `created_at`/`updated_at`/`created_by_kind`/`created_by_id` from its
    provenance. Exercise `_on_created` directly: once with a fully-populated
    entry (every column carries its OWN field, not a neighbor's), and once
    with an entry missing queue/parent/dispatch_key/actor/at (the defensive
    defaults: queue falls back to the schema's own 'default', created_at/
    updated_at to "", never None or the literal string "None")."""
    import aiosqlite
    from cairndb import Event, EventType, SchemaVersion, SequencedEvent, SequenceNumber

    from flowli.domain import Code, EntryType, Provenance, new_eid

    @engine.workflow("seed2", "1")
    async def seed2(ctx):
        return 1

    worker = engine.worker()
    await engine.start(seed2, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file/schema must already exist

    def make_prov(actor):
        return Provenance.now(
            actor=actor,
            site=Site(host="host-c", pid=1, worker_id="w-c"),
            code=Code(workflow="orphan-wf", version="9", frame_kind="root", frame_name="root"),
        )

    def make_entry(seq, payload, metadata):
        event = Event(
            event_type=EventType(EntryType.EXECUTION_CREATED),
            timestamp=Timestamp.now(),
            payload=payload,
            schema_version=SchemaVersion("1.0.0"),
            metadata=metadata,
        )
        return SequencedEvent(sequence=SequenceNumber(commit=seq, index=0), event=event)

    async def apply(seq_event):
        async with aiosqlite.connect(projection.path) as conn:
            await projection._on_created(conn, seq_event)
            await conn.commit()

    # Case A: a fully-populated execution.created -- every column must carry
    # its own field, not a neighbor's, and not a fallback default.
    eid = new_eid()
    parent_eid = new_eid()
    prov_a = make_prov(Actor.worker("w-full"))
    seq_a = make_entry(
        401,
        {
            "eid": str(eid),
            "workflow": "wf-full",
            "version": "3",
            "queue": "custom-q",
            "parent": {"eid": str(parent_eid), "fid": "root/child:k"},
            "dispatch_key": "dk-1",
        },
        {"provenance": unstructure(prov_a)},
    )
    await apply(seq_a)
    row = (
        projection.connect()
        .execute("SELECT * FROM executions WHERE eid = ?", (str(eid),))
        .fetchone()
    )
    assert row["workflow"] == "wf-full"
    assert row["version"] == "3"
    assert row["queue"] == "custom-q"
    assert row["parent_eid"] == str(parent_eid)
    assert row["parent_fid"] == "root/child:k"
    assert row["dispatch_key"] == "dk-1"
    assert row["created_at"] == prov_a.at.to_iso()
    assert row["updated_at"] == prov_a.at.to_iso()
    assert row["last_seq"] == str(seq_a.sequence)
    assert row["created_by_kind"] == "worker"
    assert row["created_by_id"] == "w-full"

    # Case B: queue/parent/dispatch_key/actor/at all absent -- queue falls
    # back to the schema's own default, created_at/updated_at to "" (never
    # None, and never the literal string "None"), and last_seq is always the
    # real sequence.
    eid_b = new_eid()
    seq_b = make_entry(
        402,
        {"eid": str(eid_b), "workflow": "wf-bare", "version": "1"},
        {"provenance": {}},
    )
    await apply(seq_b)
    row_b = (
        projection.connect()
        .execute("SELECT * FROM executions WHERE eid = ?", (str(eid_b),))
        .fetchone()
    )
    assert row_b["queue"] == "default"
    assert row_b["parent_eid"] is None
    assert row_b["parent_fid"] is None
    assert row_b["dispatch_key"] is None
    assert row_b["created_at"] == ""
    assert row_b["updated_at"] == ""
    assert row_b["last_seq"] == str(seq_b.sequence)
    assert row_b["created_by_kind"] is None
    assert row_b["created_by_id"] is None


async def test_on_migrated_field_mapping_and_defensive_default(backend, engine, projection):
    """08-projection.md: `version` | replaced by `execution.migrated`', and
    `updated_at` comes from ITS provenance. Exercise `_on_migrated` directly:
    once with a real provenance timestamp (both `version` and `updated_at` must
    come from THIS entry), and once with an entry whose metadata carries no
    provenance at all (`updated_at` must default to "", never NULL/None --
    the column is NOT NULL)."""
    import aiosqlite
    from cairndb import Event, EventType, SchemaVersion, SequencedEvent, SequenceNumber

    from flowli.domain import Code, EntryType, Provenance, new_eid

    @engine.workflow("mig", "1")
    async def mig(ctx):
        return 1

    worker = engine.worker()
    eid = await engine.start(mig, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file/schema must already exist

    def make_entry(seq, payload, metadata):
        event = Event(
            event_type=EventType(EntryType.EXECUTION_MIGRATED),
            timestamp=Timestamp.now(),
            payload=payload,
            schema_version=SchemaVersion("1.0.0"),
            metadata=metadata,
        )
        return SequencedEvent(sequence=SequenceNumber(commit=seq, index=0), event=event)

    async def apply(seq_event):
        async with aiosqlite.connect(projection.path) as conn:
            await projection._on_migrated(conn, seq_event)
            await conn.commit()

    prov = Provenance.now(
        actor=Actor.worker("w-mig"),
        site=Site(host="host-mig", pid=1, worker_id="w-mig"),
        code=Code(workflow="orphan-wf", version="9", frame_kind="root", frame_name="root"),
    )
    await apply(
        make_entry(501, {"eid": str(eid), "version": "7"}, {"provenance": unstructure(prov)})
    )
    row = projection.execution(eid)
    assert row.version == "7"
    assert row.updated_at == prov.at.to_iso()

    # no provenance at all -- updated_at must fall back to "" (never NULL/None).
    await apply(make_entry(502, {"eid": str(eid), "version": "8"}, {}))
    row2 = projection.execution(eid)
    assert row2.version == "8"
    assert row2.updated_at == ""


async def test_on_archived_field_mapping_and_defensive_default(backend, engine, projection):
    """08-projection.md: `archived_at` | provenance time of `execution.archived`'.
    Exercise `_on_archived` directly: once with a real provenance timestamp
    (`archived_at` must come from THIS entry, not just be non-NULL), and once
    with an entry whose metadata carries no provenance at all (`archived_at`
    must default to "", never the string "None" -- the column IS nullable, so
    a wrong default would slip through a plain `is not None` check)."""
    import aiosqlite
    from cairndb import Event, EventType, SchemaVersion, SequencedEvent, SequenceNumber

    from flowli.domain import Code, EntryType, Provenance, new_eid

    @engine.workflow("arch", "1")
    async def arch(ctx):
        return 1

    worker = engine.worker()
    eid = await engine.start(arch, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file/schema must already exist

    def make_entry(seq, payload, metadata):
        event = Event(
            event_type=EventType(EntryType.EXECUTION_ARCHIVED),
            timestamp=Timestamp.now(),
            payload=payload,
            schema_version=SchemaVersion("1.0.0"),
            metadata=metadata,
        )
        return SequencedEvent(sequence=SequenceNumber(commit=seq, index=0), event=event)

    async def apply(seq_event):
        async with aiosqlite.connect(projection.path) as conn:
            await projection._on_archived(conn, seq_event)
            await conn.commit()

    prov = Provenance.now(
        actor=Actor.worker("w-arch"),
        site=Site(host="host-arch", pid=1, worker_id="w-arch"),
        code=Code(workflow="orphan-wf", version="9", frame_kind="root", frame_name="root"),
    )
    await apply(make_entry(601, {"eid": str(eid)}, {"provenance": unstructure(prov)}))
    assert projection.execution(eid).archived_at == prov.at.to_iso()

    # a second, orphan eid archived with no provenance at all -- archived_at
    # must default to "" (a real ISO timestamp would never look like that).
    eid_b = new_eid()
    seq_b1 = Event(
        event_type=EventType(EntryType.EXECUTION_CREATED),
        timestamp=Timestamp.now(),
        payload={"eid": str(eid_b), "workflow": "orphan-wf", "version": "9"},
        schema_version=SchemaVersion("1.0.0"),
        metadata={"provenance": unstructure(prov)},
    )
    async with aiosqlite.connect(projection.path) as conn:
        await projection._on_created(
            conn, SequencedEvent(sequence=SequenceNumber(commit=602, index=0), event=seq_b1)
        )
        await conn.commit()
    await apply(make_entry(603, {"eid": str(eid_b)}, {}))
    assert projection.execution(eid_b).archived_at == ""


async def test_children_and_terminal_status_is_sticky(backend, engine, projection):
    @engine.workflow("child", "1")
    async def child(ctx, x):
        return x

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return await ctx.child(child, 7, key="k")

    worker = engine.worker()
    eid = await engine.start(parent, by=HUMAN)
    await drain(worker)
    await projection.refresh()
    kids = projection.children(eid)
    assert len(kids) == 1
    assert kids[0].eid == FrameRef(eid, "root/child:k").child_eid
    assert kids[0].parent_eid == str(eid)
    assert kids[0].parent_fid == "root/child:k" and kids[0].status == ExecutionStatus.COMPLETED
    assert kids[0].created_by_kind == "worker"
    assert projection.execution(eid).result == 7

    # a late, out-of-order suspended announcement must not regress a terminal row
    from flowli.domain import Entry

    prov = engine.provenance(HUMAN)
    await backend.control.announce(
        Entry("execution.suspended", "root", {"eid": str(eid), "on": ["x"]}, prov)
    )
    await projection.refresh()
    assert projection.execution(eid).status == ExecutionStatus.COMPLETED


def test_children_forwards_a_10000_limit_not_executions_own_default_of_100():
    """`children()` exists precisely to avoid `executions()`'s own 100-row
    default (08-projection.md section 4) -- a workflow with more than 100
    children must not be silently truncated to that default."""
    calls = []

    class Fake:
        def executions(self, **kw):
            calls.append(kw)
            return []

    WorkflowProjection.children(Fake(), "e-1")
    assert calls == [{"parent_eid": "e-1", "limit": 10_000}]


async def test_execution_of_an_unknown_eid_is_none_not_a_crash(backend, engine, projection):
    """`execution()` on a ready projection returns None for an eid with no
    matching row -- not an attempt to build an `ExecutionRow` out of no row."""
    from flowli.domain import new_eid

    @engine.workflow("seed_exec_none", "1")
    async def seed(ctx):
        return 1

    await engine.start(seed, by=HUMAN)
    await projection.refresh()  # the projection is ready; it just has no such row
    assert projection.execution(new_eid()) is None


async def test_reviews_and_announcements(backend, engine, projection):
    @engine.workflow("approval", "1")
    async def approval(ctx):
        rid = await ctx.uuid()
        deadline = (await ctx.now()) + timedelta(days=3)
        await ctx.announce(
            "review.requested",
            {
                "rid": rid,
                "eid": str(ctx.eid),
                "queue": "finance",
                "payload": {"amount": 100},
                "deadline": deadline.to_iso(),
            },
        )
        reply = await ctx.receive("decision")
        await ctx.announce(
            "review.decided",
            {
                "rid": rid,
                "verdict": reply.payload["verdict"],
                "by": unstructure(reply.sent_by.actor),
            },
        )
        return reply.payload["verdict"]

    worker = engine.worker()
    eid = await engine.start(approval, by=HUMAN)
    await drain(worker)
    await projection.refresh()
    expected_deadline = (T0 + timedelta(days=3)).to_iso()
    (pending,) = projection.pending_reviews("finance")
    assert pending.eid == eid and pending.payload == {"amount": 100} and pending.status == "pending"
    assert pending.deadline == expected_deadline
    assert pending.requested_at == T0.to_iso()
    assert projection.pending_reviews("legal") == []
    requested = projection.announcements(kind="review.requested")
    assert len(requested) == 1 and requested[0].eid == str(eid)
    assert requested[0].kind == "review.requested"
    assert requested[0].fid == "root/announce#0"
    assert requested[0].at == T0.to_iso()
    assert requested[0].actor_kind == "worker" and requested[0].actor_id == "w-1"
    assert requested[0].payload == {
        "rid": pending.rid,
        "eid": str(eid),
        "queue": "finance",
        "payload": {"amount": 100},
        "deadline": expected_deadline,
    }
    # `AnnouncementRow.from_row` must carry `seq` through untouched, not None.
    raw = (
        projection.connect()
        .execute("SELECT seq FROM announcements WHERE fid = ?", ("root/announce#0",))
        .fetchone()
    )
    assert requested[0].seq == raw["seq"]

    cfo = Actor.human("cfo@example.com")
    await engine.signal(eid, "decision", {"verdict": "approve"}, by=cfo)
    await drain(worker)
    await projection.refresh()
    assert projection.pending_reviews() == []
    review = projection.review(pending.rid)
    assert review.status == "decided" and review.verdict == "approve"
    assert review.decided_by == unstructure(cfo)
    assert review.decided_at == T0.to_iso()
    assert projection.execution(eid).result == "approve"
    # the decision's own announcement got a distinct seq (a shared literal seq would
    # collide with the review.requested row and be dropped by INSERT OR IGNORE)
    assert len(projection.announcements(kind="review.decided")) == 1


async def test_tasks_and_announcements_query_filters(backend, engine, projection):
    """08-projection.md section 4: `tasks(eid=, queue=, kind=, limit=)` and
    `announcements(kind=, eid=, limit=)` build a SQL WHERE clause from whichever
    filters are given. Each filter must actually narrow the result (not just
    parse), several filters together must combine with AND (not garble the
    query), no filter at all must still be valid SQL and return every row, and
    the default `limit` is 100."""
    import aiosqlite

    @engine.workflow("seed4", "1")
    async def seed4(ctx):
        return 1

    worker = engine.worker()
    await engine.start(seed4, by=HUMAN)
    await drain(worker)
    await projection.refresh()  # the sqlite file must already exist

    async def insert_task(seq, *, queue):
        async with aiosqlite.connect(projection.path) as conn:
            await conn.execute(
                "INSERT INTO tasks (seq, task_id, queue, kind, eid, fid, reason, "
                "enqueued_at, actor_kind, actor_id) "
                "VALUES (?, ?, ?, 'k', '', '', '', '', NULL, NULL)",
                (seq, seq, queue),
            )
            await conn.commit()

    async def insert_announcement(seq, *, kind, eid):
        async with aiosqlite.connect(projection.path) as conn:
            await conn.execute(
                "INSERT INTO announcements (seq, kind, eid, fid, at, actor_kind, "
                "actor_id, payload) VALUES (?, ?, ?, '', 'T', NULL, NULL, '{}')",
                (seq, kind, eid),
            )
            await conn.commit()

    # -- tasks(): a `queue` filter must narrow the result, not just decorate it --
    await insert_task("100", queue="alpha")
    await insert_task("101", queue="beta")
    assert {t.task_id for t in projection.tasks(queue="alpha")} == {"100"}
    assert {t.task_id for t in projection.tasks(queue="beta")} == {"101"}
    # -- no filter at all is still valid SQL, and returns every row (`seed4`'s
    # own start-task row is also there, hence a subset check, not equality) --
    assert {"100", "101"} <= {t.task_id for t in projection.tasks()}

    # -- announcements(): an `eid` filter, and `kind`+`eid` together, must each
    # narrow the result (not raise, and not silently drop the AND) --
    await insert_announcement("200", kind="review.requested", eid="e1")
    await insert_announcement("201", kind="review.requested", eid="e2")
    await insert_announcement("202", kind="review.decided", eid="e1")
    assert {a.seq for a in projection.announcements(eid="e1")} == {"200", "202"}
    assert {a.seq for a in projection.announcements(kind="review.requested", eid="e1")} == {"200"}
    # -- no filter at all is still valid SQL, and returns every row --
    assert {"200", "201", "202"} <= {a.seq for a in projection.announcements()}

    # -- the default `limit` is 100, not some neighboring number --
    async with aiosqlite.connect(projection.path) as conn:
        await conn.executemany(
            "INSERT INTO tasks (seq, task_id, queue, kind, eid, fid, reason, "
            "enqueued_at, actor_kind, actor_id) "
            "VALUES (?, 'bulk', 'q', 'k', '', '', '', '', NULL, NULL)",
            [(f"bulk-{i:04d}",) for i in range(110)],
        )
        await conn.executemany(
            "INSERT INTO announcements (seq, kind, eid, fid, at, actor_kind, "
            "actor_id, payload) VALUES (?, 'bulk', '', '', 'T', NULL, NULL, '{}')",
            [(f"bulk-{i:04d}",) for i in range(110)],
        )
        await conn.commit()
    assert len(projection.tasks()) == 100
    assert len(projection.announcements()) == 100


async def test_review_requested_defaults_when_fields_missing(backend, engine, projection):
    """A raw review.requested announce that skips 'eid'/'queue' still projects cleanly:
    eid is null rather than a literal sentinel, and queue falls back to 'default'
    (08-projection.md's `_on_announce` handles any announce.* payload, not only the
    ones the review() pattern helper produces)."""
    from flowli.domain import Entry

    prov = engine.provenance(HUMAN)
    await backend.control.announce(
        Entry("announce.review.requested", "root", {"rid": "r-defaults"}, prov)
    )
    await projection.refresh()

    review = projection.review("r-defaults")
    assert review.eid is None
    assert review.queue == "default"


async def test_review_of_an_unknown_rid_is_none_not_a_crash(backend, engine, projection):
    """`review()` on a ready projection returns None for an rid with no
    matching row -- not an attempt to build a `ReviewRow` out of no row."""

    @engine.workflow("seed_review_none", "1")
    async def seed(ctx):
        return 1

    await engine.start(seed, by=HUMAN)
    await projection.refresh()  # the projection is ready; it just has no such row
    assert projection.review("no-such-rid") is None


def test_review_row_from_row_handles_a_null_payload():
    """`reviews.payload` is a nullable column (SCHEMA in cairndb_projection.py); the
    single writer (`_on_announce`) always JSON-encodes it, so a real SQL NULL never
    happens in practice today, but `ReviewRow.from_row`'s `is None` guard exists for
    exactly that column and must not call `json.loads(None)` if it ever does."""
    import sqlite3

    from flowli.adapters.cairndb_projection import ReviewRow

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE reviews (rid TEXT, eid TEXT, queue TEXT, status TEXT, payload TEXT, "
        "deadline TEXT, requested_at TEXT, decided_at TEXT, verdict TEXT, decided_by TEXT)"
    )
    conn.execute(
        "INSERT INTO reviews VALUES ('r-null-payload', NULL, 'ops', 'pending', NULL, "
        "NULL, '2026-01-01T00:00:00Z', NULL, NULL, NULL)"
    )
    row = conn.execute("SELECT * FROM reviews WHERE rid = 'r-null-payload'").fetchone()
    review = ReviewRow.from_row(row)
    assert review.payload is None


def test_known_maps_every_field_and_derives_archived_flag():
    """`_known` (the sweeper/retention `ControlSource` adapter, 08-projection.md
    section 5) must carry `status`, `last_type`, `updated_at` and `queue` each
    into their OWN `KnownExecution` field, and must derive `archived` from
    whether `archived_at` is set -- not leave it at `KnownExecution`'s own
    default of `False` regardless (sweeper.py checks `not k.archived` to avoid
    re-archiving an already-archived row)."""
    from flowli.adapters.cairndb_projection import ExecutionRow, _known
    from flowli.domain import new_eid

    base = dict(
        eid=new_eid(),
        workflow="wf",
        version="1",
        status=ExecutionStatus.COMPLETED,
        queue="q-known",
        parent_eid=None,
        parent_fid=None,
        dispatch_key=None,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-02T00:00:00Z",
        last_type="execution.completed",
        epoch=None,
        suspended_on=None,
        result=None,
        error_type=None,
        error_message=None,
        created_by_kind=None,
        created_by_id=None,
        worker_id=None,
        host=None,
    )
    not_archived = _known(ExecutionRow(**base, archived_at=None))
    assert not_archived.status == ExecutionStatus.COMPLETED
    assert not_archived.last_type == "execution.completed"
    assert not_archived.last_at == Timestamp.from_iso("2026-01-02T00:00:00Z")
    assert not_archived.queue == "q-known"
    assert not_archived.archived is False

    archived = _known(ExecutionRow(**base, archived_at="2026-01-03T00:00:00Z"))
    assert archived.archived is True


async def test_review_expiry(backend, engine, projection, clock):
    """announce.review.expired folds a pending review to status='expired' with a real
    decided_at, and never regresses a review that is already decided (08-projection.md:
    'A decision or expiry for a review that is not pending is ignored.')."""
    from flowli.domain import Entry

    async def announce(kind, payload):
        prov = engine.provenance(HUMAN)
        await backend.control.announce(Entry(f"announce.{kind}", "root", payload, prov))

    await announce("review.requested", {"rid": "r-exp", "queue": "ops"})
    await announce("review.requested", {"rid": "r-decided", "queue": "ops"})
    await announce("review.decided", {"rid": "r-decided", "verdict": "approve", "by": None})

    clock.advance(timedelta(hours=1))
    expired_at = clock().to_iso()
    await announce("review.expired", {"rid": "r-exp"})
    await announce("review.expired", {"rid": "r-decided"})  # already decided: ignored

    await projection.refresh()

    expired = projection.review("r-exp")
    assert expired.status == "expired"
    assert expired.decided_at == expired_at

    decided = projection.review("r-decided")
    assert decided.status == "decided" and decided.verdict == "approve"


async def test_projection_as_sweeper_source(backend, engine, projection, clock):
    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(minutes=5))
        return 1

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    worker = engine.worker()
    sweeper = engine.sweeper(source=projection)
    assert sweeper.source is projection  # engine.sweeper must forward its kwargs
    e_nap = await engine.start(nap, by=HUMAN)
    e_lost = await engine.start(w, by=HUMAN)
    await drain(worker)  # nap suspended, w completed

    snap = await projection.snapshot()
    assert set(snap) == {e_nap}
    assert isinstance(snap[e_nap], KnownExecution)
    assert snap[e_nap].status == ExecutionStatus.SUSPENDED and snap[e_nap].queue == "default"

    clock.advance(timedelta(minutes=5))
    report = await sweeper.run_once()
    assert len(report.timers_fired) == 1 and report.recovered == [] and report.repaired == []
    await drain(worker)
    await projection.refresh()
    assert projection.execution(e_nap).status == ExecutionStatus.COMPLETED
    assert projection.execution(e_lost).status == ExecutionStatus.COMPLETED
    assert (await sweeper.run_once()).total == 0


async def test_background_updater_start_stop(backend, engine, projection):
    @engine.workflow("w", "1")
    async def w(ctx):
        return "v"

    await projection.start()
    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    last = (await backend.control.read())[-1].seq
    assert await projection.wait_for(last, timeout=10)
    assert projection.execution(eid).status == ExecutionStatus.COMPLETED
    await projection.stop()
