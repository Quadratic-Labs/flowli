"""The CairnDB adapter over filesystem storage, then the whole engine on top of it."""

import asyncio
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowlet.adapters.cairndb import SEQ_BASE, CairnBackend
from flowlet.adapters.memory import ManualClock
from flowlet.domain import (
    Actor,
    Entry,
    Execution,
    ExecutionStatus,
    FrameRef,
    LeaseLost,
    Message,
    Site,
    Task,
    TaskKind,
    Timer,
    execution_channel,
)
from flowlet.runtime import Engine, EngineConfig
from tests.ids import E_AAA, E_ABC, E_BBB, E_DEF, E_NOPE, E_ZZZ

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


def task(prov, key="", queue="default", not_before=None) -> Task:
    return Task(queue, TaskKind.START, FrameRef(E_ABC, "root"), "start", prov, key, not_before)


# --- serialization --------------------------------------------------------------------


def test_dumps_is_compact_key_sorted_and_falls_back_to_str_for_a_stray_uuid():
    """`_dumps` is the one place every stored object's bytes come from: compact
    (no incidental whitespace), key-sorted (so two writers of equal content
    always produce equal bytes), and its `default` hook must still cover a
    UUID that reaches it unconverted."""
    from flowlet.adapters.cairndb import _dumps

    assert _dumps({"b": 1, "a": 2}) == b'{"a":2,"b":1}'
    stray = uuid.uuid4()
    assert _dumps(stray) == f'"{stray}"'.encode()


def test_json_default_refuses_anything_that_is_not_a_uuid():
    """The `default` hook is a last resort for the one case it knows
    (`UUID`); anything else must raise, naming the offending type, not
    silently swallow or mis-stringify it."""
    from flowlet.adapters.cairndb import _json_default

    with pytest.raises(TypeError, match="not JSON-compatible: set"):
        _json_default({1, 2, 3})


# --- logs ---------------------------------------------------------------------------


def test_encode_seq_combines_commit_and_index_not_subtracts_them():
    """`encode_seq` must pack `(commit, index)` so that ordering by the
    resulting int matches CairnDB's own `(commit, index)` order: index must
    be added, not subtracted, or two events sharing a commit would sort
    backwards."""
    from cairndb import SequenceNumber

    from flowlet.adapters.cairndb import SEQ_BASE, encode_seq

    assert encode_seq(SequenceNumber(commit=2, index=3)) == 2 * SEQ_BASE + 3


async def test_journal_roundtrip_and_ordering(backend, prov):
    j = backend.journal
    s1 = await j.append(E_ABC, Entry.execution_started(prov, ["x"]))
    s2 = await j.append(E_ABC, Entry.frame_started(prov, "root/a#0", "step", "a", "d", 1))
    assert s1 < s2 and s1 >= SEQ_BASE
    entries = await j.read(E_ABC)
    assert [s.seq for s in entries] == [s1, s2]
    assert entries[1].item == Entry.frame_started(prov, "root/a#0", "step", "a", "d", 1)
    assert entries[1].item.provenance == prov
    assert [s.seq for s in await j.read(E_ABC, after=s1)] == [s2]
    assert await j.read(E_ABC, after=s2) == []
    assert await j.tail(E_ABC) == s2
    assert await j.tail(E_ZZZ) == 0 and await j.read(E_ZZZ) == []


async def test_tail_of_a_journal_with_exactly_one_commit(backend, prov):
    """A journal's very first commit must still report its own tail correctly:
    the "empty log" guard is keyed on `current_tail() == 0`, one whole commit
    away from `== 1`."""
    j = backend.journal
    s1 = await j.append(E_AAA, Entry.execution_started(prov, ["x"]))
    assert await j.tail(E_AAA) == s1


async def test_control_log(backend, prov):
    seq = await backend.control.announce(Entry.announce(prov, "root", "review.requested", {}))
    entries = await backend.control.read()
    assert entries[0].seq == seq and entries[0].item.type == "announce.review.requested"


async def test_journal_append_tags_the_event_with_schema_version(backend, prov):
    """`_entry_event`'s tags mirror `send`'s (see
    test_channel_send_tags_the_event_with_message_type_and_schema_version):
    every commit carries one real Event, not one with a blank schema tag."""
    from cairndb import SchemaVersion

    from flowlet.domain import SCHEMA_VERSION

    j = backend.journal
    await j.append(E_ABC, Entry.execution_started(prov, ["x"]))
    log = await backend.logs.get(j.name(E_ABC))
    [stored] = [s async for s in log.read(after=0)]
    assert stored.event.schema_version == SchemaVersion(SCHEMA_VERSION)


def test_event_entry_defaults_fid_to_root_for_metadata_without_it(prov):
    """Metadata that predates the `fid` field (or simply lacks it) must fall
    back to `ROOT_FID`, not `None` -- every `Entry` needs a real fid."""
    from cairndb import Event, EventType, SchemaVersion

    from flowlet.adapters.cairndb import _event_entry
    from flowlet.codec import unstructure
    from flowlet.domain import ROOT_FID, SCHEMA_VERSION

    event = Event(
        event_type=EventType("announce.review.requested"),
        timestamp=prov.at,
        payload={},
        schema_version=SchemaVersion(SCHEMA_VERSION),
        metadata={"provenance": unstructure(prov)},  # no "fid" key at all
    )
    assert _event_entry(event).fid == ROOT_FID


async def test_drop_forgets_the_log_so_a_later_get_reopens_it(backend):
    """`drop` is "close and forget": a `get` afterwards must build a fresh Log,
    never hand back the one that was just closed."""
    name = "wf.exec.reopen-check"
    first = await backend.logs.get(name)
    await backend.logs.drop(name)
    again = await backend.logs.get(name)
    assert again is not first


async def test_drop_of_a_never_opened_log_does_not_raise(backend):
    """A fresh process's log cache starts empty. Retention may `drop` a log
    this cache never `get` before it, and that miss must not crash the call."""
    await backend.logs.drop("wf.exec.never-opened")  # must not raise


async def test_drop_clears_commits_and_both_schema_snapshots_up_to_a_huge_bound(
    backend, monkeypatch
):
    """`drop`'s bound must cover any real log, under the current schema and
    the legacy one: too small, or the wrong schema name, would leave old
    commits or snapshots behind forever."""
    from cairndb.engine.logs import NamespacedStorage

    calls: list[tuple] = []

    async def record_commits(self, before):
        calls.append(("commits", before))

    async def record_snapshots(self, schema, before):
        calls.append(("snapshots", schema, before))

    monkeypatch.setattr(NamespacedStorage, "delete_commits_before", record_commits)
    monkeypatch.setattr(NamespacedStorage, "delete_snapshots_before", record_snapshots)

    await backend.logs.drop("wf.exec.bound-check")

    assert calls == [
        ("commits", 2**62),
        ("snapshots", "1", 2**62),
        ("snapshots", "1.0.0", 2**62),
    ]


async def test_log_cache_hits_return_the_same_log_and_evicts_the_least_recently_used(
    backend,
):
    """A `get` of a cached name must be a hit (the same `Log`, not a fresh one),
    and a hit counts as recently used: once the cache is over `maxsize`, the
    entry nobody touched again is the one that gets evicted and closed, not
    whichever happens to have been created first."""
    from flowlet.adapters.cairndb import _LogCache

    cache = _LogCache(backend.db, maxsize=2)
    a = await cache.get("cache-a")
    assert await cache.get("cache-a") is a  # a hit: same object, not a fresh Log

    b = await cache.get("cache-b")
    await cache.get("cache-a")  # touch "a" again: "b" is now the least recently used
    c = await cache.get("cache-c")  # over maxsize: evicts "b", not "a"

    assert await cache.get("cache-a") is a
    assert await cache.get("cache-c") is c
    assert await cache.get("cache-b") is not b  # "b" was evicted and reopened fresh

    await cache.close()


def test_log_cache_default_maxsize_is_128():
    """Pins the cache's own default so a silent change doesn't go unnoticed."""
    from flowlet.adapters.cairndb import _LogCache

    assert _LogCache(db=object())._maxsize == 128


def test_backend_threads_its_log_cache_size_through_to_the_cache():
    """`CairnBackend`'s `log_cache_size` must actually reach `_LogCache`, not
    be silently dropped in favor of the cache's own default -- and that
    default (128) must match `_LogCache`'s own."""
    assert CairnBackend(db=object()).logs._maxsize == 128
    assert CairnBackend(db=object(), log_cache_size=5).logs._maxsize == 5


# --- coordination --------------------------------------------------------------------


async def test_dispatch_claim(backend):
    assert await backend.dispatch.claim_start("k", E_AAA) == (True, E_AAA)
    assert await backend.dispatch.claim_start("k", E_BBB) == (False, E_AAA)


async def test_lease_exclusive_fenced_and_stealable(backend, prov):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=0.3)
    assert l1 is not None and l1.epoch == 1
    assert await o.acquire(E_ABC, "w-2", ttl=0.3) is None
    info = await o.inspect(E_ABC)
    assert info.holder == "w-1" and not info.released and not info.is_expired(Timestamp.now())

    await asyncio.sleep(0.4)
    assert (await o.inspect(E_ABC)).is_expired(Timestamp.now())
    l2 = await o.acquire(E_ABC, "w-2", ttl=5)
    assert l2 is not None and l2.epoch == 2
    # The message is the inner cairndb exception's, not a placeholder: it is
    # what tells an operator *why* the lease was lost.
    with pytest.raises(LeaseLost, match="fenced"):
        await l1.renew()
    with pytest.raises(LeaseLost, match="fenced"):
        await l1.update_state(lambda s: s)
    with pytest.raises(LeaseLost, match="fenced"):
        await l1.release({"x": 1})
    await l2.release({"status": "done"})
    info = await o.inspect(E_ABC)
    assert info.released and info.state == {"status": "done"} and info.epoch == 2


async def test_cooperative_cancel_before_and_during_lease(backend, prov):
    o = backend.ownership
    await o.request_cancel(E_ABC, prov)  # no lease document yet
    lease = await o.acquire(E_ABC, "w-1", ttl=5)
    assert lease.state["cancel_requested"]["actor"]["id"] == prov.actor.id
    await lease.release({})
    lease = await o.acquire(E_DEF, "w-1", ttl=5)
    await o.request_cancel(E_DEF, prov)
    assert lease.state == {} or lease.state is None
    await lease.renew()
    assert lease.state["cancel_requested"]
    assert (await lease.refresh_state())["cancel_requested"]
    await lease.release()


async def test_request_cancel_merges_the_flag_into_existing_state(backend, prov):
    """The flag joins whatever the holder already wrote; it must not replace it."""
    o = backend.ownership
    lease = await o.acquire(E_ABC, "w-1", ttl=5)
    await lease.update_state(lambda s: {"handle": "s-1"})
    await o.request_cancel(E_ABC, prov)
    await lease.renew()
    assert lease.state["handle"] == "s-1"
    assert lease.state["cancel_requested"]["actor"]["id"] == prov.actor.id


async def test_request_cancel_falls_back_to_cooperative_write_when_it_loses_the_race(
    backend, prov, monkeypatch
):
    """No lease document exists yet, so `request_cancel` tries to create one,
    released, with the flag. Another caller (typically the execution finally
    starting) may win that creation first: our own `lease` call then reports
    the key as already held, and the flag must still land, cooperatively, on
    the document the winner made.
    """
    o = backend.ownership
    key = o.key(E_ABC)
    real_lease = backend.db.lease

    async def contested_lease(k, **kwargs):
        if k == key:
            await real_lease(k, ttl=60, holder="w-1")  # someone else wins the race first
        return await real_lease(k, **kwargs)

    monkeypatch.setattr(backend.db, "lease", contested_lease)
    await o.request_cancel(E_ABC, prov)

    info = await o.inspect(E_ABC)
    assert info.holder == "w-1" and not info.released
    assert info.state["cancel_requested"]["actor"]["id"] == prov.actor.id


async def test_inspect_treats_a_legacy_doc_without_a_deadline_as_none(backend):
    """`deadline_at` is read with `.get`, not `[...]` (see `_event_entry`'s
    analogous `fid` fallback): a document from before the field existed must
    parse as `deadline_at=None`, not crash inside `Timestamp.from_iso`."""
    import json

    o = backend.ownership
    await backend.db.objects.put(
        o.key(E_ABC), json.dumps({"epoch": 1, "holder": None, "state": None}).encode()
    )
    info = await o.inspect(E_ABC)
    assert info is not None and info.deadline_at is None


# --- queue -------------------------------------------------------------------------------


def test_lease_key_replaces_only_the_leading_slash_t_slash_segment():
    """Per the key layout (module docstring): a task key's `/t/` segment becomes
    `/l/`, and only that one occurrence -- not any `/t/` that happens to appear
    later, inside the task id itself."""
    from flowlet.adapters.cairndb import CairnQueue

    key = "wf/queues/default/t/2026-09-07T09:00:00.000000Z-start:/t/echo"
    expected = "wf/queues/default/l/2026-09-07T09:00:00.000000Z-start:/t/echo"
    assert CairnQueue._lease_key(key) == expected


async def test_queue_enqueue_dequeue_ack(backend, prov):
    q = backend.queue
    assert await q.enqueue(task(prov)) is True
    assert await q.enqueue(task(prov)) is False
    c1 = await q.dequeue("default", "w-1", ttl=5)
    assert c1.task.task_id == f"start:{E_ABC}"
    assert await q.dequeue("default", "w-2", ttl=5) is None
    await q.ack(c1)
    assert await q.dequeue("default", "w-2", ttl=5) is None
    assert await q.pending("default") == []
    assert await q.enqueue(task(prov)) is True  # id free again after ack


async def test_ensure_repairs_a_marker_that_outlived_its_task(backend, prov):
    """A crash between the two writes of `enqueue` must not strand the task id.

    `enqueue` claims `ids/{task_id}` and then writes the task. The marker
    refuses every later `enqueue`, so only `ensure` can put the task back.
    """
    q = backend.queue
    t = task(prov)
    await q.enqueue(t)
    for key in await backend.db.objects.list("wf/queues/default/t/"):
        await backend.db.objects.delete(key)  # the task write never landed

    assert await q.enqueue(t) is False  # the marker refuses it: the hole
    assert await q.pending("default") == []

    assert await q.ensure(t) is True
    assert [p.task_id for p in await q.pending("default")] == [t.task_id]
    assert await q.ensure(t) is False  # idempotent once the task is back

    claimed = await q.dequeue("default", "w-1", ttl=5)
    assert claimed is not None and claimed.task.task_id == t.task_id
    await q.ack(claimed)
    assert await q.pending("default") == []


async def test_ensure_beside_a_half_written_enqueue_queues_one_task(backend, clock, prov):
    """The window `ensure` exists for is also the window it can race.

    A repair that ran while a live `enqueue` held the marker but had not yet
    written its task used to place a second object under a second key, because
    the two chose their key from their own clock. The marker names the key, so
    both aim at it and put-if-absent settles which of them creates it.
    """
    q = backend.queue
    t = task(prov)
    # `enqueue`, stopped between its two writes.
    key = q._fresh_key(t)
    assert await backend.db.objects.put(q._id_key(t), key.encode(), if_absent=True) is not None

    # Time moves on, so the repair would choose a different key of its own.
    clock.advance(timedelta(seconds=5))
    assert q._fresh_key(t) != key

    assert await q.ensure(t) is True  # repairs, at the key the marker names
    # The interrupted `enqueue`, resuming. It aims at the same key, so it adds
    # nothing and reports that it did not create the task.
    assert await q._write_task(t, key) is False

    assert [p.task_id for p in await q.pending("default")] == [t.task_id]
    assert await backend.db.objects.list("wf/queues/default/t/") == [key]

    claimed = await q.dequeue("default", "w-1", ttl=5)
    await q.ack(claimed)
    assert await q.dequeue("default", "w-2", ttl=5) is None  # no second copy behind it


async def test_two_ensures_of_one_stranded_id_agree_on_one_key(backend, prov):
    """Concurrent repairs, including of a marker from before markers named a key."""
    q = backend.queue
    t = task(prov)
    await backend.db.objects.put(q._id_key(t), b"", if_absent=True)  # a legacy marker

    first, second = await asyncio.gather(q.ensure(t), q.ensure(t))
    assert sorted([first, second]) == [False, True]
    assert len(await backend.db.objects.list("wf/queues/default/t/")) == 1
    assert (await backend.db.objects.get(q._id_key(t))).data.decode().endswith(t.task_id)


async def test_claim_key_retries_when_the_marker_vanishes_between_the_put_and_the_read(
    backend, prov, monkeypatch
):
    """A marker acked between the failed put-if-absent and the read means the
    id is free again: the call must retry from the top and claim it itself,
    not give up and report nothing.
    """
    q = backend.queue
    t = task(prov)
    id_key = q._id_key(t)
    await backend.db.objects.put(id_key, b"", if_absent=True)  # a marker, about to vanish
    real_get = backend.db.objects.get

    async def flaky_get(key):
        if key == id_key:
            await backend.db.objects.delete(id_key)  # acked, right before we read it
            return None
        return await real_get(key)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    assert await q._claim_key(t) == q._fresh_key(t)


async def test_claim_key_adopts_a_legacy_marker_under_cas_not_a_blind_overwrite(
    backend, prov, monkeypatch
):
    """Adopting a marker from before markers named their key must CAS on the
    marker's etag: a concurrent adoption between this call's read and its own
    write must make it lose and converge on THAT key, not stomp over it with
    its own.
    """
    q = backend.queue
    t = task(prov)
    id_key = q._id_key(t)
    await backend.db.objects.put(id_key, b"", if_absent=True)  # a legacy marker
    winner_key = "wf/queues/default/t/winner"
    real_get = backend.db.objects.get

    async def racing_get(key):
        obj = await real_get(key)
        if key == id_key and obj is not None and not obj.data:
            # Someone else adopts the marker for a different key, right
            # between this read and our own CAS write.
            await backend.db.objects.put(id_key, winner_key.encode(), if_match=obj.etag)
        return obj

    monkeypatch.setattr(backend.db.objects, "get", racing_get)
    assert await q._claim_key(t) == winner_key


async def test_nack_repoints_the_marker_at_the_new_key(backend, clock, prov):
    """An `ensure` between a nack and its delete must not revive the old key."""
    q = backend.queue
    t = task(prov)
    await q.enqueue(t)
    claimed = await q.dequeue("default", "w-1", ttl=5)
    await q.nack(claimed, timedelta(seconds=30))

    named = (await backend.db.objects.get(q._id_key(t))).data.decode()
    assert await backend.db.objects.get(named) is not None
    assert await q.ensure(t) is False  # the nacked copy is the one task
    assert len(await backend.db.objects.list("wf/queues/default/t/")) == 1

    assert await q.dequeue("default", "w-1", ttl=5) is None  # still delayed
    clock.advance(timedelta(seconds=31))
    assert (await q.dequeue("default", "w-1", ttl=5)) is not None


async def test_ensure_without_a_marker_is_a_plain_enqueue(backend, prov):
    q = backend.queue
    t = task(prov)
    assert await q.ensure(t) is True
    assert await q.enqueue(t) is False
    assert [p.task_id for p in await q.pending("default")] == [t.task_id]


async def test_attach_and_request_cancel(backend, prov):
    """The relay's two verbs: re-attach by holder, and ask the holder to stop."""
    q = backend.queue
    await q.enqueue(task(prov))
    claimed = await q.dequeue("default", "runner-a", ttl=60)

    same = await q.attach("default", claimed.task.task_id, "runner-a", 60)
    assert same is not None and same.lease.epoch == claimed.lease.epoch
    assert await q.attach("default", claimed.task.task_id, "runner-b", 60) is None

    # The handle a consumer leaves for its own restart, or for a thief.
    await same.lease.update_state(lambda s: {"handle": "s-1"})
    await q.request_cancel("default", claimed.task.task_id, prov)
    await claimed.lease.renew()
    assert claimed.lease.state["handle"] == "s-1"
    assert claimed.lease.state["cancel_requested"]["actor"]["id"] == prov.actor.id


async def test_take_finds_the_named_task_among_others_and_leases_it_to_the_worker(
    backend, clock, prov
):
    """`take` is `dequeue` for one known task_id: it must walk past every other
    task on the queue to find the one named, not just the first in line."""
    q = backend.queue
    decoy = task(prov, key="decoy")
    await q.enqueue(decoy)
    clock.advance(timedelta(seconds=1))  # so the target sorts strictly after the decoy
    target = task(prov, key="target")
    await q.enqueue(target)

    claimed = await q.take("default", target.task_id, "w-1", ttl=60)
    assert claimed is not None and claimed.task.task_id == target.task_id

    # Leased to the worker named, not to nobody: only that holder can attach.
    same = await q.attach("default", target.task_id, "w-1", 60)
    assert same is not None and same.lease.epoch == claimed.lease.epoch
    assert await q.attach("default", target.task_id, "w-2", 60) is None

    # The decoy is untouched: still there, and takeable in its own right.
    assert decoy.task_id in [p.task_id for p in await q.pending("default")]


async def test_take_sees_a_task_exactly_at_its_visible_time(backend, clock, prov):
    """A task becomes takeable *at* its `not_before`, not strictly after it."""
    q = backend.queue
    t = task(prov, not_before=T0)  # the clock reads exactly T0 right now
    await q.enqueue(t)
    claimed = await q.take("default", t.task_id, "w-1", ttl=5)
    assert claimed is not None


async def test_take_drops_the_stale_lease_of_a_task_gone_between_list_and_get(
    backend, prov, monkeypatch
):
    """`take` may list a task's key, then lose a race with an ack before it
    reads the object. It must still walk away clean: release the lease it
    just took, and delete the lease document, rather than leave one dangling.
    """
    q = backend.queue
    t = task(prov)
    await q.enqueue(t)
    key = q._fresh_key(t)
    real_get = backend.db.objects.get

    async def flaky_get(k):
        if k == key:
            return None  # acked between the list and the lease, from take's view
        return await real_get(k)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    assert await q.take("default", t.task_id, "w-1", ttl=5) is None
    assert await backend.db.objects.get(q._lease_key(key)) is None  # no dangling lease


async def test_drop_stale_lease_suppresses_release_of_an_already_released_lease(backend, prov):
    """The task is gone: `_drop_stale_lease`'s own `release()` may itself find
    the lease already released (the acker got to this exact document first).
    That `LeaseLost` must be swallowed, not left to escape -- and the lease
    document must still be removed either way."""
    from flowlet.adapters.cairndb import CairnLease

    q = backend.queue
    t = task(prov)
    await q.enqueue(t)
    key = q._fresh_key(t)
    inner = await backend.db.lease(q._lease_key(key), ttl=5, holder="w-1")
    await inner.release()  # already released before _drop_stale_lease gets it

    await q._drop_stale_lease(CairnLease(inner), key)  # must not raise
    assert await backend.db.objects.get(q._lease_key(key)) is None


async def test_dequeue_skips_a_task_gone_between_list_and_lease_and_finds_the_next_one(
    backend, clock, prov, monkeypatch
):
    """`dequeue` may list a task's key, then lose a race with an ack before it
    reads the object. It must drop the stale lease it just took (release it
    and delete the lease document, not leave one dangling) and move on to the
    next candidate, not give up on the whole queue.
    """
    q = backend.queue
    gone = task(prov, key="gone")
    await q.enqueue(gone)
    gone_key = q._fresh_key(gone)
    clock.advance(timedelta(seconds=1))
    survivor = task(prov, key="survivor")
    await q.enqueue(survivor)
    real_get = backend.db.objects.get

    async def flaky_get(k):
        if k == gone_key:
            return None  # acked between the list and the lease
        return await real_get(k)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    claimed = await q.dequeue("default", "w-1", ttl=5)
    assert claimed is not None and claimed.task.task_id == survivor.task_id
    assert await backend.db.objects.get(q._lease_key(gone_key)) is None  # no dangling lease


async def test_peek_reads_a_task_without_a_lease(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    claimed = await q.dequeue("default", "runner-a", ttl=60)
    seen = await q.peek("default", claimed.task.task_id)
    assert seen is not None and seen.task_id == claimed.task.task_id
    assert await q.peek("default", "start:nothing") is None


async def test_peek_returns_none_when_the_task_is_gone_between_key_of_and_get(
    backend, prov, monkeypatch
):
    """Mirrors `take`/`dequeue`'s analogous race (see
    test_take_drops_the_stale_lease_of_a_task_gone_between_list_and_get): the
    task may be acked between `_key_of`'s listing and `peek`'s own read."""
    q = backend.queue
    t = task(prov)
    await q.enqueue(t)
    key = q._fresh_key(t)
    real_get = backend.db.objects.get

    async def flaky_get(k):
        if k == key:
            return None  # acked between _key_of's listing and peek's own get
        return await real_get(k)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    assert await q.peek("default", t.task_id) is None


async def test_pending_stops_at_exactly_the_limit(backend, clock, prov):
    """`limit` is a hard cap on the returned list, not a hint: with more
    pending tasks than `limit`, the loop must stop at exactly `limit` items,
    neither one short nor one over."""
    q = backend.queue
    for i in range(5):
        clock.advance(timedelta(seconds=1))
        await q.enqueue(task(prov, key=f"k{i}"))
    result = await q.pending("default", limit=3)
    assert isinstance(result, list) and len(result) == 3


async def test_pending_default_limit_is_one_hundred(backend, clock, prov):
    """The default `limit` matches the `Queue` protocol's documented value
    (spec 03 section 7): calling `pending` with no `limit` on a queue deeper
    than 100 must still stop at 100, not 101."""
    q = backend.queue
    for i in range(101):
        clock.advance(timedelta(seconds=1))
        await q.enqueue(task(prov, key=f"k{i}"))
    assert len(await q.pending("default")) == 100


async def test_queue_visibility_timeout(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    c1 = await q.dequeue("default", "w-1", ttl=0.3)
    await asyncio.sleep(0.4)
    c2 = await q.dequeue("default", "w-2", ttl=5)
    assert c2 is not None and c2.lease.epoch == 2
    with pytest.raises(LeaseLost):
        await c1.lease.renew()
    await q.ack(c2)


async def test_queue_not_before_ordering_and_nack(backend, clock, prov):
    q = backend.queue
    await q.enqueue(task(prov, key="late", not_before=T0 + timedelta(minutes=5)))
    clock.advance(timedelta(seconds=1))
    await q.enqueue(task(prov, key="early"))
    c = await q.dequeue("default", "w-1", ttl=5)
    assert c.task.key == "early"
    await q.nack(c, timedelta(seconds=30))
    assert await q.dequeue("default", "w-1", ttl=5) is None
    clock.advance(timedelta(seconds=31))
    assert (await q.dequeue("default", "w-1", ttl=5)).task.key == "early"
    clock.advance(timedelta(minutes=5))
    assert (await q.dequeue("default", "w-1", ttl=5)).task.key == "late"


async def test_depth_counts_a_task_exactly_at_its_visible_time_as_visible(backend, prov):
    """Mirrors `test_take_sees_a_task_exactly_at_its_visible_time`: a task
    becomes visible *at* its `not_before`, not strictly after it, so `depth`
    must count it too."""
    q = backend.queue
    await q.enqueue(task(prov, not_before=T0))  # the clock reads exactly T0 right now
    depth = await q.depth("default")
    assert depth.total == 1 and depth.visible == 1


# --- channel, timers, executions -----------------------------------------------------------


async def test_channel_send_read_waits(backend, prov):
    ch = backend.channel
    m = Message(f"{E_ABC}.payments", 0, {"n": 1}, prov, correlation="c")
    s1 = await ch.send(m)
    s2 = await ch.send(m)
    got = await ch.read(f"{E_ABC}.payments")
    assert [x.seq for x in got] == [s1, s2]
    assert got[0].payload == {"n": 1} and got[0].correlation == "c" and got[0].sent_by == prov
    assert [x.seq for x in await ch.read(f"{E_ABC}.payments", after=s1)] == [s2]
    assert await ch.read("nothing.here") == []

    ref = FrameRef(E_ABC, "root/receive#0")
    await ch.register_wait("global.rates", ref)
    await ch.register_wait("global.rates", FrameRef(E_AAA, "root/x#0"))
    assert await ch.waiters("global.rates") == [FrameRef(E_AAA, "root/x#0"), ref]
    assert await ch.all_waits() == [
        ("global.rates", FrameRef(E_AAA, "root/x#0")),
        ("global.rates", ref),
    ]
    await ch.clear_wait("global.rates", ref)
    assert await ch.waiters("global.rates") == [FrameRef(E_AAA, "root/x#0")]


async def test_channel_send_tags_the_event_with_message_type_and_schema_version(
    backend, prov
):
    """`send`'s stored Event carries the same type and schema tags as every
    other write in this adapter (see `_entry_event`): not because `read` uses
    them today, but because the log holds one real Event per message."""
    from cairndb import EventType, SchemaVersion

    from flowlet.domain import SCHEMA_VERSION

    ch = backend.channel
    m = Message(f"{E_ABC}.tags", 0, {"n": 1}, prov, correlation="c")
    await ch.send(m)

    log = await backend.logs.get(ch.name(m.channel))
    [stored] = [s async for s in log.read(after=0)]
    assert stored.event.event_type == EventType("message")
    assert stored.event.schema_version == SchemaVersion(SCHEMA_VERSION)


async def test_timers(backend):
    t = backend.timers
    ref = FrameRef(E_ABC, "root/sleep#0")
    t1 = Timer(T0 + timedelta(minutes=1), ref)
    t2 = Timer(T0 + timedelta(minutes=2), ref)
    await t.schedule(t2)
    await t.schedule(t1)
    assert await t.due(T0) == []
    assert await t.due(T0 + timedelta(minutes=2)) == [t1, t2]
    await t.remove(t1)
    assert await t.due(T0 + timedelta(minutes=2)) == [t2]


async def test_execution_store(backend, prov):
    s = backend.executions
    ex = Execution(E_ABC, "w", "1", {"args": [1], "kwargs": {}}, prov, queue="slow")
    assert await s.create(ex) is True
    assert await s.create(replace(ex, workflow="z")) is False
    assert await s.read(E_ABC) == ex
    assert await s.read(E_NOPE) is None


async def test_replace_retries_under_cas_conflict_and_applies_fn_to_fresh_data(
    backend, prov, monkeypatch
):
    """A lost CAS on `put` must not clobber a concurrent write: `replace`
    reads again and applies `fn` to what is on disk NOW, not to the copy it
    first saw (spec 03 section 10: "On a lost CAS it reads again and
    retries")."""
    s = backend.executions
    ex = Execution(E_ABC, "w", "1", {"args": [], "kwargs": {}}, prov, queue="default")
    await s.create(ex)
    key = s.key(E_ABC)
    real_get = backend.db.objects.get
    raced = False

    async def racing_get(k):
        nonlocal raced
        obj = await real_get(k)
        if k == key and not raced:
            raced = True
            # A concurrent writer lands right after our own first read.
            await s.replace(E_ABC, lambda e: replace(e, queue="raced"))
        return obj

    monkeypatch.setattr(backend.db.objects, "get", racing_get)
    result = await s.replace(E_ABC, lambda e: replace(e, workflow="v2"))
    assert result.workflow == "v2" and result.queue == "raced"
    assert await s.read(E_ABC) == result


async def test_replace_gives_up_after_exactly_eight_cas_failures(backend, prov, monkeypatch):
    """The retry budget is exactly 8 attempts: once every one of them loses
    the CAS, `replace` must give up loudly -- naming the execution it could
    not settle -- rather than retry forever or return something silently
    wrong."""
    s = backend.executions
    ex = Execution(E_ABC, "w", "1", {"args": [], "kwargs": {}}, prov)
    await s.create(ex)
    real_put = backend.db.objects.put
    attempts = 0

    async def losing_put(key, data, **kwargs):
        nonlocal attempts
        if key == s.key(E_ABC):
            attempts += 1
            return None  # every CAS always loses
        return await real_put(key, data, **kwargs)

    monkeypatch.setattr(backend.db.objects, "put", losing_put)
    with pytest.raises(RuntimeError, match=str(E_ABC)):
        await s.replace(E_ABC, lambda e: replace(e, workflow="v2"))
    assert attempts == 8  # exactly 8 tried, no ninth


async def test_archive_write_read_round_trips_bytes_values(backend):
    """`write`'s `use_bin_type=True` must survive the round trip through
    `read`'s `raw=False`: a `bytes` value in the archived data comes back as
    `bytes`, not flattened into `str` like msgpack's legacy raw encoding."""
    a = backend.archive
    data = {"eid": str(E_ABC), "blob": b"\xff\x00binary", "note": "text"}
    assert await a.write(E_ABC, data) is True
    got = await a.read(E_ABC)
    assert got["blob"] == b"\xff\x00binary" and isinstance(got["blob"], bytes)
    assert got["note"] == "text" and isinstance(got["note"], str)


# --- the engine on cairndb -----------------------------------------------------------------


async def test_engine_end_to_end_on_cairndb(backend, clock):
    engine = Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=clock,
        config=EngineConfig(code_ref="git:abc"),
    )

    @engine.workflow("double", "1")
    async def double(ctx, x):
        return await ctx.step(lambda: x * 2, name="mul")

    @engine.workflow("invoice", "1")
    async def invoice(ctx, invoice_id):
        await ctx.step(lambda: {"id": invoice_id, "risk": 0.9}, name="fetch")
        decision = await ctx.receive("review", timeout=timedelta(days=3))
        if decision is None or decision.payload["verdict"] != "approve":
            return "rejected"
        await ctx.sleep(timedelta(minutes=1))
        total = await ctx.child(double, 21, key="c")
        await ctx.send("out", {"status": "approved", "total": total})
        return f"approved:{total}"

    worker, sweeper = engine.worker(), engine.sweeper()

    async def drain(limit=20):
        n = 0
        while await worker.run_once():
            n += 1
            assert n < limit
        return n

    eid = await engine.start(invoice, "inv-42", key="invoice:42", by=HUMAN)
    assert await engine.start(invoice, "inv-42", key="invoice:42", by=HUMAN) == eid
    await drain()
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    full = execution_channel(eid, "review")
    assert await backend.channel.waiters(full) == [FrameRef(eid, "root/receive#0")]

    await engine.signal(eid, "review", {"verdict": "approve"}, by=Actor.human("cfo@example.com"))
    await drain()
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED  # sleeping
    assert (await sweeper.run_once()).timers_fired == []
    clock.advance(timedelta(minutes=1))
    assert len((await sweeper.run_once()).timers_fired) == 1
    await drain()  # resume, start child, child runs, parent resumes and completes
    assert await engine.status(eid) == ExecutionStatus.COMPLETED

    journal = await engine.journal(eid)
    assert journal[-1].item.payload == {"value": "approved:42"}
    who = [s.item for s in journal if s.item.type == "execution.resumed"]
    assert [e.payload["epoch"] for e in who] == [2, 3, 4]
    review_msg = (await backend.channel.read(full))[0]
    assert review_msg.sent_by.actor == Actor.human("cfo@example.com")
    out = await backend.channel.read(execution_channel(eid, "out"))
    assert out[0].payload == {"status": "approved", "total": 42}
    assert await backend.channel.all_waits() == []
    assert await backend.queue.pending("default") == []
    control = [s.item.type for s in await backend.control.read()]
    assert control[0] == "execution.created" and control[-1] == "execution.completed"
    assert (await sweeper.run_once()).total == 0


def _engine(backend, clock) -> Engine:
    return Engine(backend.ports, Site(host="h", pid=1, worker_id="w-1"), clock=clock)


async def _drain(worker, limit=20):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit


async def test_sweeper_fires_a_timer_through_a_stranded_marker(backend, clock):
    """`enqueue` claims `ids/{task_id}` before it writes the task, so a crash
    between the two strands the id. The sweeper removes a fired timer whether
    or not its enqueue won, so without `ensure` the resume is lost for good."""
    engine = _engine(backend, clock)

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(minutes=1))
        return "woke"

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(nap, by=HUMAN)
    await _drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED

    clock.advance(timedelta(minutes=1))
    [timer] = await backend.timers.due(clock())
    task_id = Task.id_for(TaskKind.RESUME, eid, f"timer:{timer.timer_id}")
    await backend.db.objects.put(f"wf/queues/default/ids/{task_id}", b"", if_absent=True)

    assert len((await sweeper.run_once()).timers_fired) == 1
    assert await backend.timers.due(clock()) == []  # the timer is gone either way
    assert [t.task_id for t in await backend.queue.pending("default")] == [task_id]

    await _drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_sweeper_restarts_a_lost_start_through_a_stranded_marker(backend, clock):
    engine = _engine(backend, clock)

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    for key in await backend.db.objects.list("wf/queues/default/t/"):
        await backend.db.objects.delete(key)  # the task is lost, the marker stays

    clock.advance(timedelta(seconds=61))
    assert (await sweeper.run_once()).restarted == [eid]
    assert [t.task_id for t in await backend.queue.pending("default")] == [
        Task.id_for(TaskKind.START, eid)
    ]
    await _drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_sweeper_recovers_through_a_stranded_marker(backend, clock):
    """Recovery must also survive a stranded `ids/{task_id}` marker: without `repair=True`
    the resume enqueue would be silently refused by `Queue.ensure` and the execution would
    be stuck forever (module docstring: the sweeper is the last thing that puts these
    tasks back)."""
    engine = _engine(backend, clock)

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    lease = await backend.ownership.acquire(eid, "w-9", ttl=120)
    await backend.control.announce(
        Entry("execution.started", "root", {"eid": str(eid), "args": {}}, engine.provenance(HUMAN))
    )
    await lease.release()  # released is expired regardless of deadline (spec 03 section 4)

    task_id = Task.id_for(TaskKind.RESUME, eid, f"recovery:{lease.epoch}")
    await backend.db.objects.put(f"wf/queues/default/ids/{task_id}", b"", if_absent=True)

    report = await sweeper.run_once()
    assert report.recovered == [eid]
    assert task_id in [t.task_id for t in await backend.queue.pending("default")]

    await _drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_evidence_round_trip_and_retention(backend, prov):
    """Attempt logs and attachments as plain objects. Nothing here is authority."""
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    ref = EvidenceRef(E_ABC, "root/fetch#0", 1)

    await ev.append_log(ref, b'{"event": "one"}\n')
    await ev.append_log(ref, b'{"event": "two"}\n')
    assert await ev.read_log(ref) == b'{"event": "one"}\n{"event": "two"}\n'
    # `get` under the reserved log name is `read_log`, not a separate,
    # unimplemented path.
    assert await ev.get(ref, "log") == await ev.read_log(ref)
    never_logged = EvidenceRef(E_ABC, "root/other#0", 1)
    assert await ev.get(never_logged, "log") is None

    key = await ev.put(ref, "diff.patch", b"--- a\n", "text/plain")
    assert key.endswith("/a/diff.patch")
    assert await ev.get(ref, "diff.patch") == b"--- a\n"
    assert await ev.get(ref, "nothing") is None

    items = await ev.list(E_ABC)
    names = {(i.ref.fid, i.ref.attempt, i.name) for i in items}
    assert names == {("root/fetch#0", 1, "log"), ("root/fetch#0", 1, "diff.patch")}
    by_name = {i.name: i for i in items}
    assert by_name["log"].media_type == "application/x-ndjson"
    assert by_name["log"].size == len(b'{"event": "one"}\n{"event": "two"}\n')
    assert by_name["diff.patch"].media_type == "text/plain"
    assert by_name["diff.patch"].size == len(b"--- a\n")

    # Retention removes it with the execution: the archive does not hold it.
    assert await ev.delete_for(E_ABC) >= 3
    assert await ev.list(E_ABC) == []


async def test_evidence_list_falls_back_to_octet_stream_without_a_media_type_sidecar(
    backend, prov
):
    """An attachment written before the media-type sidecar existed (or by any writer
    that only ever puts the content key) still lists with a media type, not a crash."""
    from flowlet.adapters import evidence as keys
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    ref = EvidenceRef(E_ABC, "root/fetch#0", 1)

    await ev._ensure_meta(ref)
    await backend.db.objects.put(keys.attachment_key(ref, "legacy.bin"), b"old data")

    items = await ev.list(E_ABC)
    (item,) = [i for i in items if i.name == "legacy.bin"]
    assert item.media_type == "application/octet-stream"


async def test_append_log_never_overwrites_a_part_already_claimed_at_a_candidate(
    backend, prov, monkeypatch
):
    """The retry loop's whole point is put-if-absent: if the candidate index
    it tries is already taken -- a racing flush that lands after this call's
    own listing but before its write -- `append_log` must move on to the next
    candidate rather than silently stomp over what's there."""
    from flowlet.adapters import evidence as keys
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    ref = EvidenceRef(E_ABC, "root/fetch#0", 1)
    sentinel_key = keys.log_key(ref, 0)
    await ev._ensure_meta(ref)
    await backend.db.objects.put(sentinel_key, b"sentinel", if_absent=True)

    real_list = backend.db.objects.list
    calls = 0

    async def undercounting_list(prefix):
        nonlocal calls
        calls += 1
        result = await real_list(prefix)
        if calls == 1:
            # From append_log's point of view, the racing flush that claimed
            # candidate 0 hasn't landed yet -- so its own first try is 0.
            return [k for k in result if k != sentinel_key]
        return result

    monkeypatch.setattr(backend.db.objects, "list", undercounting_list)
    await ev.append_log(ref, b"mine")

    assert await ev.read_log(ref) == b"sentinelmine"


async def test_append_log_gives_up_after_exactly_eight_candidates(backend, prov, monkeypatch):
    """The retry budget is exactly 8 candidates (n..n+7): once every one of
    them reads as already taken, the call must give up loudly -- naming the
    frame it could not settle -- rather than keep trying forever."""
    from flowlet.adapters import evidence as keys
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    ref = EvidenceRef(E_ABC, "root/fetch#0", 1)
    log_prefix = f"{keys.base(ref)}/{keys.LOG_NAME}."
    real_put = backend.db.objects.put
    attempts: list[str] = []

    async def counting_put(key, data, **kwargs):
        if key.startswith(log_prefix):
            attempts.append(key)
            return None  # every candidate is always already taken
        return await real_put(key, data, **kwargs)

    monkeypatch.setattr(backend.db.objects, "put", counting_put)
    with pytest.raises(RuntimeError, match=str(E_ABC)):
        await ev.append_log(ref, b"x")
    assert len(attempts) == 8  # exactly n..n+7 tried, no ninth


async def test_list_sums_every_log_part_and_skips_one_gone_missing(backend, prov, monkeypatch):
    """The log's size is the sum of every flushed part, not just the last one
    written; a part that reads back as gone (a race with retention) must
    contribute nothing, rather than crash the whole listing."""
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    ref = EvidenceRef(E_ABC, "root/fetch#0", 1)
    await ev.append_log(ref, b"a" * 5)
    await ev.append_log(ref, b"b" * 7)  # this part will read back as gone
    await ev.append_log(ref, b"c" * 11)

    prefix = "wf/evidence/{}/".format(E_ABC)
    part_keys = sorted(
        k for k in await backend.db.objects.list(prefix) if "/log." in k
    )
    vanished = part_keys[1]
    real_get = backend.db.objects.get

    async def flaky_get(key):
        if key == vanished:
            return None
        return await real_get(key)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    (item,) = [i for i in await ev.list(E_ABC) if i.name == "log"]
    assert item.size == 5 + 11


async def test_list_continues_past_one_missing_meta_object(backend, prov, monkeypatch):
    """One frame's meta may read back as gone (e.g. a concurrent delete);
    `list` must still report every other frame's evidence, not abandon the
    whole execution at the first gap."""
    from flowlet.domain import EvidenceRef

    ev = backend.evidence
    refs = [EvidenceRef(E_ABC, f"root/f{i}#0", 1) for i in range(3)]
    for ref in refs:
        await ev.append_log(ref, b"x")

    prefix = "wf/evidence/{}/".format(E_ABC)
    all_keys = sorted(await backend.db.objects.list(prefix))
    first_meta = next(k for k in all_keys if k.endswith("/meta"))
    real_get = backend.db.objects.get

    async def flaky_get(key):
        if key == first_meta:
            return None
        return await real_get(key)

    monkeypatch.setattr(backend.db.objects, "get", flaky_get)
    items = await ev.list(E_ABC)
    assert len({i.ref.fid for i in items}) == 2  # every frame but the vanished one
