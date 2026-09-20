from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowlet.adapters.memory import ManualClock, MemoryBackend, _ts
from flowlet.domain import (
    ClaimedTask,
    Entry,
    FrameRef,
    Lease,
    LeaseLost,
    Message,
    Ports,
    Task,
    TaskKind,
    Timer,
)
from tests.ids import E_AAA, E_ABC, E_BBB, E_DEF, E_NOPE

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend(clock=ManualClock(T0))


def task(prov, key="", queue="default", not_before=None) -> Task:
    return Task(
        key=key,
        queue=queue,
        kind=TaskKind.START,
        target=FrameRef(E_ABC, "root"),
        reason="start",
        enqueued_by=prov,
        not_before=not_before,
    )


def test_backend_builds_ports(backend):
    assert isinstance(backend.ports, Ports)


# --- clock helpers -----------------------------------------------------------


def test_ts_wraps_a_bare_datetime_but_passes_through_a_timestamp():
    raw = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    wrapped = _ts(raw)
    assert isinstance(wrapped, Timestamp)
    assert wrapped == Timestamp(raw)
    assert _ts(T0) is T0  # already a Timestamp: passed through unchanged


# --- journal / control log ------------------------------------------------


async def test_journal_is_dense_and_per_execution(backend, prov):
    j = backend.journal
    assert await j.append(E_ABC, Entry.execution_started(prov, [])) == 1
    assert await j.append(E_ABC, Entry.frame_started(prov, "root/a#0", "step", "a", "d", 1)) == 2
    assert await j.append(E_DEF, Entry.execution_started(prov, [])) == 1
    assert [s.seq for s in await j.read(E_ABC)] == [1, 2]
    assert [s.seq for s in await j.read(E_ABC, after=1)] == [2]
    assert await j.tail(E_ABC) == 2
    assert await j.tail("zzz") == 0


async def test_control_log(backend, prov):
    c = backend.control
    assert await c.announce(Entry.announce(prov, "root", "review.requested", {"rid": "r1"})) == 1
    assert (await c.read())[0].item.type == "announce.review.requested"
    assert await c.announce(Entry.announce(prov, "root", "review.approved", {"rid": "r1"})) == 2
    assert [s.seq for s in await c.read(after=1)] == [2]  # strictly after, the seq itself excluded


# --- dispatch --------------------------------------------------------------


async def test_dispatch_exactly_one_winner(backend):
    won, eid = await backend.dispatch.claim_start("invoice:42", E_AAA)
    assert (won, eid) == (True, E_AAA)
    won, eid = await backend.dispatch.claim_start("invoice:42", E_BBB)
    assert (won, eid) == (False, E_AAA)
    won, _ = await backend.dispatch.claim_start("invoice:42", E_AAA)
    assert not won  # a second call never "wins", like db.claim


# --- ownership -------------------------------------------------------------


async def test_lease_is_exclusive_until_expiry_then_stealable(backend, prov):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=120)
    assert isinstance(l1, Lease) and l1.epoch == 1
    assert await o.acquire(E_ABC, "w-2", ttl=120) is None
    backend.clock.advance(timedelta(seconds=121))
    l2 = await o.acquire(E_ABC, "w-2", ttl=120)
    assert l2 is not None and l2.epoch == 2
    with pytest.raises(LeaseLost, match="fenced"):
        await l1.renew()
    with pytest.raises(LeaseLost):
        await l1.release({"status": "x"})
    await l2.renew()  # still valid


async def test_lease_expires_exactly_at_its_deadline_not_after(backend):
    """Spec 03 section 6: expired when deadline_at is not after now -- the boundary itself
    counts as expired, so a steal exactly at the deadline must succeed."""
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=100)
    backend.clock.advance(timedelta(seconds=100))  # now == deadline_at exactly
    l2 = await o.acquire(E_ABC, "w-2", ttl=100)
    assert l2 is not None
    with pytest.raises(LeaseLost):
        await l1.renew()


async def test_renew_extends_deadline(backend):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=100)
    backend.clock.advance(timedelta(seconds=90))
    await l1.renew()
    backend.clock.advance(timedelta(seconds=90))
    assert await o.acquire(E_ABC, "w-2", ttl=100) is None


async def test_release_then_reacquire_bumps_epoch(backend):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=100)
    await l1.release({"status": "suspended"})
    with pytest.raises(LeaseLost):
        await l1.renew()
    l2 = await o.acquire(E_ABC, "w-1", ttl=100)
    assert l2.epoch == 2
    assert l2.state == {"status": "suspended"}


async def test_cooperative_cancel_is_seen_on_renew(backend, prov):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=100)
    await o.request_cancel(E_ABC, prov)
    assert l1.state is None  # not absorbed yet
    await l1.renew()
    assert l1.state["cancel_requested"]["actor"]["id"] == "w-1"
    assert (await l1.refresh_state())["cancel_requested"]
    assert l1.state["cancel_requested"]  # refresh_state() must also update the cached .state


async def test_update_state_is_given_the_current_state_not_none(backend):
    o = backend.ownership
    lease = await o.acquire(E_ABC, "w-1", ttl=100)
    await lease.update_state(lambda _: {"count": 1})
    result = await lease.update_state(
        lambda s: {**(s or {}), "count": (s or {}).get("count", 0) + 1}
    )
    assert result == {"count": 2}
    assert lease.state == {"count": 2}


async def test_request_cancel_merges_into_existing_lease_state(backend, prov):
    """cooperative_write must fold onto the document's current state, not overwrite it."""
    o = backend.ownership
    lease = await o.acquire(E_ABC, "w-1", ttl=100)
    await lease.update_state(lambda _: {"other": True})
    await o.request_cancel(E_ABC, prov)
    await lease.renew()
    assert lease.state["other"] is True
    assert lease.state["cancel_requested"]["actor"]["id"] == "w-1"


async def test_inspect_reports_the_full_lease_document(backend):
    o = backend.ownership
    lease = await o.acquire(E_ABC, "w-1", ttl=120)
    await lease.update_state(lambda _: {"x": 1})

    held = await o.inspect(E_ABC)
    assert held.holder == "w-1"
    assert held.released is False
    assert held.state == {"x": 1}

    await lease.release({"y": 2})
    gone = await o.inspect(E_ABC)
    assert gone.holder is None
    assert gone.released is True
    assert gone.state == {"y": 2}


async def test_release_updates_the_lease_handles_own_state(backend):
    o = backend.ownership
    l1 = await o.acquire(E_ABC, "w-1", ttl=100)
    await l1.release({"phase": "done"})
    assert l1.state == {"phase": "done"}


# --- queue -----------------------------------------------------------------


async def test_enqueue_is_idempotent_on_task_id(backend, prov):
    q = backend.queue
    assert await q.enqueue(task(prov)) is True
    assert await q.enqueue(task(prov)) is False
    assert len(await q.pending("default")) == 1


async def test_pending_default_limit_is_100(backend, prov):
    q = backend.queue
    for i in range(101):
        await q.enqueue(task(prov, key=f"k{i:03d}"))
    assert len(await q.pending("default")) == 100
    assert len(await q.pending("default", limit=101)) == 101


async def test_queue_reads_tolerate_a_queue_that_was_never_enqueued_to(backend, prov):
    q = backend.queue
    assert await q.pending("ghost") == []
    assert await q.peek("ghost", "nope") is None
    assert await q.attach("ghost", "nope", "w-1", 60) is None
    await q.request_cancel("ghost", "nope", prov)  # must not raise


async def test_dequeue_one_worker_at_a_time_then_ack(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    c1 = await q.dequeue("default", "w-1", ttl=60)
    assert c1 is not None and c1.task.task_id == f"start:{E_ABC}"
    assert await q.dequeue("default", "w-2", ttl=60) is None
    await q.ack(c1)
    assert await q.dequeue("default", "w-2", ttl=60) is None
    assert await q.pending("default") == []


async def test_ack_deletes_the_lease_document(backend, prov):
    """Spec 03 section 7: ack releases, then deletes task, marker and lease key."""
    q = backend.queue
    await q.enqueue(task(prov))
    c = await q.dequeue("default", "w-1", ttl=60)
    lease_key = c.key + ".lease"
    assert lease_key in backend.leases.docs
    await q.ack(c)
    assert lease_key not in backend.leases.docs


async def test_ack_is_a_safe_no_op_for_a_queue_it_never_saw(backend, prov):
    class StubLease:
        async def release(self) -> None:
            return None

    claimed = ClaimedTask(task=task(prov, queue="ghost"), key="wf/queues/ghost/x", lease=StubLease())
    await backend.queue.ack(claimed)  # must not raise


async def test_expired_task_lease_makes_task_visible_again(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    c1 = await q.dequeue("default", "w-1", ttl=60)
    backend.clock.advance(timedelta(seconds=61))
    c2 = await q.dequeue("default", "w-2", ttl=60)
    assert c2 is not None and c2.lease.epoch == 2
    with pytest.raises(LeaseLost):
        await c1.lease.renew()


async def test_take_respects_visibility_and_an_unknown_queue(backend, prov):
    q = backend.queue
    assert await q.take("nosuchqueue", "nosuchtask", "w-1", 60) is None

    later = T0 + timedelta(minutes=5)
    await q.enqueue(task(prov, not_before=later))
    tid = f"start:{E_ABC}"
    assert await q.take("default", tid, "w-1", 60) is None  # not yet visible

    backend.clock.advance(timedelta(minutes=5))
    claimed = await q.take("default", tid, "w-1", 60)
    assert claimed is not None and claimed.task.task_id == tid


async def test_take_then_attach_recovers_the_same_lease(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    tid = f"start:{E_ABC}"
    claimed = await q.take("default", tid, "runner-a", 60)
    assert claimed is not None

    same = await q.attach("default", tid, "runner-a", 60)
    assert same is not None
    assert same.lease.epoch == claimed.lease.epoch


async def test_not_before_and_ordering(backend, prov):
    q = backend.queue
    later = T0 + timedelta(minutes=5)
    await q.enqueue(task(prov, key="b", not_before=later))
    backend.clock.advance(timedelta(seconds=1))
    await q.enqueue(task(prov, key="a"))
    c = await q.dequeue("default", "w-1", ttl=60)
    assert c.task.key == "a"
    await q.ack(c)
    assert await q.dequeue("default", "w-1", ttl=60) is None
    backend.clock.advance(timedelta(minutes=5))
    c = await q.dequeue("default", "w-1", ttl=60)
    assert c.task.key == "b"


async def test_nack_requeues_with_delay(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov))
    c = await q.dequeue("default", "w-1", ttl=60)
    await q.nack(c, timedelta(seconds=30))
    assert await q.dequeue("default", "w-1", ttl=60) is None
    backend.clock.advance(timedelta(seconds=31))
    c2 = await q.dequeue("default", "w-1", ttl=60)
    assert c2 is not None and c2.task.task_id == f"start:{E_ABC}"


async def test_queues_are_independent(backend, prov):
    q = backend.queue
    await q.enqueue(task(prov, queue="finance"))
    assert await q.dequeue("default", "w-1", ttl=60) is None
    assert await q.dequeue("finance", "w-1", ttl=60) is not None



# --- attach, state and cancel of a task ------------------------------------


async def test_attach_gives_the_holder_its_task_back(backend, prov):
    """Spec 10 section 4.3: a restarted runner recovers by its own holder."""
    q = backend.queue
    await q.enqueue(task(prov))
    claimed = await q.dequeue("default", "runner-a", ttl=60)

    same = await q.attach("default", claimed.task.task_id, "runner-a", 90)
    assert same is not None
    assert same.lease.epoch == claimed.lease.epoch  # the same period, not a new one
    assert same.task.task_id == claimed.task.task_id

    assert await q.attach("default", claimed.task.task_id, "runner-b", 60) is None
    assert await q.attach("default", "start:nothing", "runner-a", 60) is None

    await same.lease.renew()  # the attach call's own ttl governs renewal, not the original's
    assert same.lease.deadline_at == backend.clock() + timedelta(seconds=90)


async def test_a_steal_inherits_the_state_of_the_dead_holder(backend, prov):
    """Spec 10 section 4.4: the thief reads the handle the corpse left, so it
    can adopt or terminate the session instead of leaving it to run forever."""
    q = backend.queue
    await q.enqueue(task(prov))
    first = await q.dequeue("default", "runner-a", ttl=1)
    await first.lease.update_state(lambda s: {"handle": {"session_id": "s-1"}})

    backend.clock.advance(timedelta(seconds=2))
    thief = await q.dequeue("default", "runner-b", ttl=60)
    assert thief is not None
    assert thief.lease.epoch > first.lease.epoch
    assert thief.lease.state == {"handle": {"session_id": "s-1"}}


async def test_request_cancel_is_seen_on_the_next_renew(backend, prov):
    """It fences nobody: the holder keeps the task and reads the request."""
    q = backend.queue
    await q.enqueue(task(prov))
    claimed = await q.dequeue("default", "runner-a", ttl=60)

    await q.request_cancel("default", claimed.task.task_id, prov)
    await claimed.lease.renew()
    assert claimed.lease.state["cancel_requested"]["actor"]["id"] == prov.actor.id

# --- channel ---------------------------------------------------------------


async def test_channel_assigns_dense_seq_and_reads_after(backend, prov):
    ch = backend.channel
    m = Message(f"{E_ABC}.payments", 0, {"n": 1}, prov)
    assert await ch.send(m) == 1
    assert await ch.send(m) == 2
    assert [x.seq for x in await ch.read(f"{E_ABC}.payments")] == [1, 2]
    assert [x.seq for x in await ch.read(f"{E_ABC}.payments", after=1)] == [2]
    assert await ch.read("nothing") == []


async def test_channel_waits(backend):
    ch = backend.channel
    ref = FrameRef(E_ABC, "root/receive#0")
    await ch.register_wait("global.rates", ref)
    await ch.register_wait("global.rates", FrameRef(E_AAA, "root/x#0"))
    assert await ch.waiters("global.rates") == [FrameRef(E_AAA, "root/x#0"), ref]
    await ch.clear_wait("global.rates", ref)
    assert await ch.waiters("global.rates") == [FrameRef(E_AAA, "root/x#0")]


async def test_waiters_on_an_unregistered_channel_is_empty(backend):
    assert await backend.channel.waiters("nothing.here") == []


async def test_delete_channel_of_an_unknown_channel_is_a_safe_no_op(backend):
    await backend.channel.delete_channel("nothing.here")  # must not raise


async def test_all_waits_orders_by_eid_not_by_channel_insertion_or_fid(backend):
    ch = backend.channel
    # fid order deliberately disagrees with eid order, and insertion order
    # disagrees with both, so a mutant collapsing the sort key is caught.
    await ch.register_wait("c1", FrameRef(E_ABC, "aaa"))
    await ch.register_wait("c1", FrameRef(E_AAA, "zzz"))
    await ch.register_wait("c2", FrameRef(E_BBB, "m"))
    assert await ch.all_waits() == [
        ("c1", FrameRef(E_AAA, "zzz")),
        ("c1", FrameRef(E_ABC, "aaa")),
        ("c2", FrameRef(E_BBB, "m")),
    ]


# --- timers ----------------------------------------------------------------


async def test_timers_due_in_order(backend):
    t = backend.timers
    ref = FrameRef(E_ABC, "root/sleep#0")
    t2 = Timer(T0 + timedelta(minutes=2), ref)
    t1 = Timer(T0 + timedelta(minutes=1), ref)
    await t.schedule(t2)
    await t.schedule(t1)
    await t.schedule(t1)  # idempotent
    assert await t.due(T0) == []
    assert await t.due(T0 + timedelta(minutes=2)) == [t1, t2]
    await t.remove(t1)
    assert await t.due(T0 + timedelta(minutes=2)) == [t2]


async def test_timers_remove_of_an_unscheduled_timer_is_a_safe_no_op(backend):
    ghost = Timer(T0 + timedelta(minutes=1), FrameRef(E_ABC, "root/sleep#0"))
    await backend.timers.remove(ghost)  # must not raise


# --- execution store -------------------------------------------------------


async def test_execution_store_put_if_absent(backend, prov):
    from dataclasses import replace

    from flowlet.domain import Execution

    s = backend.executions
    ex = Execution(E_ABC, "w", "1", None, prov)
    assert await s.create(ex) is True
    assert await s.create(replace(ex, workflow="other")) is False
    assert (await s.read(E_ABC)).workflow == "w"
    assert await s.read(E_NOPE) is None


async def test_execution_store_delete_of_an_unknown_eid_is_a_safe_no_op(backend):
    await backend.executions.delete(E_NOPE)  # must not raise


# --- archive -----------------------------------------------------------------


async def test_archive_write_is_write_once_per_eid(backend):
    a = backend.archive
    assert await a.write(E_ABC, {"journal": [1]}) is True
    assert await a.write(E_ABC, {"journal": [2]}) is False  # already archived: not overwritten
    assert (await a.read(E_ABC))["journal"] == [1]
