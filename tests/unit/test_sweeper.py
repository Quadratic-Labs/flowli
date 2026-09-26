from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.domain import (
    ROOT_FID,
    Actor,
    ExecutionStatus,
    FrameRef,
    LeaseInfo,
    Site,
    execution_channel,
)
from flowli.runtime import Context, Engine, EngineConfig

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend(clock=ManualClock(T0))


@pytest.fixture
def engine(backend) -> Engine:
    return Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(exec_ttl=120, task_ttl=60),
    )


async def drain(worker, limit=20):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit
    return n


async def pending_ids(backend, queue="default"):
    return [t.task_id for t in await backend.queue.pending(queue)]


# --- LeaseInfo.is_expired ---------------------------------------------------------


def test_lease_info_is_expired_when_released_even_with_a_future_deadline():
    """A released lease is expired regardless of its deadline (spec 03 section 4)."""
    info = LeaseInfo(
        epoch=1, holder="w-1", deadline_at=T0 + timedelta(hours=1), released=True, state=None
    )
    assert info.is_expired(T0)


def test_lease_info_is_expired_exactly_at_its_deadline():
    """ "Not after now" includes the boundary: deadline_at == now is expired."""
    info = LeaseInfo(epoch=1, holder="w-1", deadline_at=T0, released=False, state=None)
    assert info.is_expired(T0)
    assert not info.is_expired(T0 - timedelta(seconds=1))


# --- Sweeper.__init__ -------------------------------------------------------------


def test_sweeper_init_defaults(engine):
    """Defaults per specs/05-protocols.md section 9: a 60-second repair window and
    the "sweeper" system actor. `_known` starts as an empty dict, not None -- nothing
    reads it before the first `run_once()` overwrites it, but it must still be a dict a
    caller can safely inspect (e.g. `.items()`) right after construction."""
    from flowli.runtime.sweeper import Sweeper

    sweeper = Sweeper(engine)
    assert sweeper.repair_window == timedelta(seconds=60)
    assert sweeper.actor == Actor.system("sweeper")
    assert sweeper._known == {}


def test_sweeper_init_threads_sweeper_id_into_the_actor(engine):
    """A custom `sweeper_id` must actually reach `Actor.system`, not be ignored in favor
    of a fixed name."""
    from flowli.runtime.sweeper import Sweeper

    sweeper = Sweeper(engine, sweeper_id="repair-bot")
    assert sweeper.actor == Actor.system("repair-bot")


# --- ControlView.apply -----------------------------------------------------------


async def test_control_view_apply_queue_carries_forward_and_defaults(engine):
    """Only an `execution.created` entry's payload carries `queue`; every later entry for
    the same eid must carry the queue already known forward. An eid this particular view
    has never seen before, whose first entry lacks `queue` (e.g. a fresh `ControlView` fed
    by `_repair_control_log` when the sweeper's `source` is an external projection), falls
    back to the same "default" `KnownExecution.queue` itself defaults to."""
    from flowli.domain import Entry, EntryType, parse_eid
    from flowli.runtime.sweeper import ControlView

    view = ControlView()
    prov = engine.provenance(HUMAN)
    eid = parse_eid("00000000-0000-0000-0000-0000000000e1")
    view.apply(
        1, Entry(EntryType.EXECUTION_CREATED, "root", {"eid": str(eid), "queue": "custom"}, prov)
    )
    assert view.executions[eid].queue == "custom"

    view.apply(2, Entry(EntryType.EXECUTION_STARTED, "root", {"eid": str(eid), "args": {}}, prov))
    assert view.executions[eid].queue == "custom"  # carried forward: this entry has no "queue"

    other = parse_eid("00000000-0000-0000-0000-0000000000e2")
    view.apply(
        3, Entry(EntryType.EXECUTION_SUSPENDED, "root", {"eid": str(other), "on": ["x"]}, prov)
    )
    assert view.executions[other].queue == "default"  # never seen before, no "queue" in payload


# --- ControlView.terminal_before ---------------------------------------------------


async def test_control_view_terminal_before_excludes_the_boundary_instant(engine):
    """ "Older than `before`", not "at or older than" (matches the cairndb projection's
    own `updated_at < ?`, and the same strict boundary the repair-window checks use): an
    entry exactly at `before` is not yet old enough to retain."""
    from flowli.domain import Entry, EntryType, parse_eid
    from flowli.runtime.sweeper import ControlView

    view = ControlView()
    prov = engine.provenance(HUMAN)
    eid = parse_eid("00000000-0000-0000-0000-0000000000e3")
    view.apply(1, Entry(EntryType.EXECUTION_COMPLETED, "root", {"eid": str(eid), "value": 1}, prov))

    before = view.executions[eid].last_at
    assert await view.terminal_before(before) == {}
    assert await view.terminal_before(before + timedelta(seconds=1)) == {eid: view.executions[eid]}


# --- timers ---------------------------------------------------------------------


async def test_sweeper_fires_due_timers_only(backend, engine):
    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return "woke"

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(nap, by=HUMAN)
    await drain(worker)
    assert (await sweeper.run_once()).total == 0
    assert len(backend.timers.timers) == 1

    backend.clock.advance(timedelta(hours=1))
    [timer] = list(backend.timers.timers.values())
    report = await sweeper.run_once()
    assert report.timers_fired == [timer.timer_id]
    assert backend.timers.timers == {}
    [task] = await backend.queue.pending("default")
    assert task.task_id.startswith(f"resume:{eid}:timer:")
    assert task.reason == "timer"
    assert task.enqueued_by.code.frame_name == "sweeper.timer"
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await sweeper.run_once()).total == 0


async def test_sweeper_fire_timers_swallows_unknown_execution(backend, engine):
    """`enqueue_resume` looks the execution up first and can raise `UnknownExecution` if
    the record vanished underneath a still-scheduled timer (e.g. a concurrent retention
    race); firing must swallow that quietly -- and still remove the stale timer -- rather
    than let it crash the sweep."""

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(nap, by=HUMAN)
    await drain(worker)
    backend.clock.advance(timedelta(hours=1))
    await backend.executions.delete(eid)  # the record vanished underneath the pending timer

    report = await sweeper.run_once()  # must not raise
    assert report.timers_fired == []
    assert backend.timers.timers == {}  # the stale timer is removed either way


# --- dead executions --------------------------------------------------------------


async def test_sweeper_recovers_execution_whose_worker_died(backend, engine, monkeypatch):
    calls = []

    @engine.workflow("w", "1")
    async def w(ctx):
        calls.append(1)
        return await ctx.step(lambda: "ok", name="s")

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)

    # crash the worker after it recorded execution.started
    original_run = Context.run

    class Crash(BaseException):
        """Not an Exception: nothing in the worker records it, like a real process death."""

    async def crash(self, *a, **k):
        raise Crash

    monkeypatch.setattr(Context, "run", crash)
    with pytest.raises(Crash):
        await worker.run_once()
    monkeypatch.setattr(Context, "run", original_run)

    assert await engine.status(eid) == ExecutionStatus.RUNNING
    info = await backend.ownership.inspect(eid)
    assert not info.is_expired(backend.clock())
    assert (await sweeper.run_once()).recovered == []  # lease still valid

    backend.clock.advance(timedelta(seconds=121))
    report = await sweeper.run_once()
    assert report.recovered == [eid]
    assert f"resume:{eid}:recovery:1" in await pending_ids(backend)

    # a fresh worker takes over; the old START task is visible again too
    fresh = engine.worker(worker_id="w-2")
    await drain(fresh)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert await pending_ids(backend) == []
    resumed = [s.item for s in await engine.journal(eid) if s.item.type == "execution.resumed"]
    assert resumed and resumed[0].payload["epoch"] == 2
    assert (await sweeper.run_once()).total == 0


async def test_sweeper_ignores_running_execution_with_live_lease(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    lease = await backend.ownership.acquire(eid, "w-9", ttl=120)
    prov = engine.provenance(HUMAN)
    from flowli.domain import Entry

    await backend.control.announce(
        Entry("execution.started", "root", {"eid": str(eid), "args": {}}, prov)
    )
    assert (await engine.sweeper().run_once()).recovered == []
    await lease.release()


async def test_sweeper_recovery_does_not_break_on_an_early_continue(backend, engine):
    """Each guard in the dead-execution scan must skip only its own execution: a `break`
    in place of either `continue` there would silently stop the sweeper from recovering
    every eid that sorts after the first one it decides to skip."""
    from flowli.domain import Entry

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return 1

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()

    eidA = await engine.start(nap, by=HUMAN)  # not RUNNING: hits the status guard
    await drain(worker)

    eidB = await engine.start(w, by=HUMAN)  # RUNNING, but its lease is live
    leaseB = await backend.ownership.acquire(eidB, "w-9", ttl=120)
    await backend.control.announce(
        Entry("execution.started", "root", {"eid": str(eidB), "args": {}}, engine.provenance(HUMAN))
    )

    eidC = await engine.start(w, by=HUMAN)  # RUNNING, and its lease will expire
    await backend.ownership.acquire(eidC, "w-8", ttl=1)
    await backend.control.announce(
        Entry("execution.started", "root", {"eid": str(eidC), "args": {}}, engine.provenance(HUMAN))
    )

    backend.clock.advance(timedelta(seconds=2))  # eidC's lease is now expired; eidB's is not
    report = await sweeper.run_once()
    assert report.recovered == [eidC]
    await leaseB.release()


async def test_sweeper_recovery_swallows_unknown_execution(backend, engine, monkeypatch):
    """`enqueue_resume` looks the execution up first and can raise `UnknownExecution` if
    the record vanished underneath it (e.g. a concurrent retention race); recovery must
    swallow that quietly rather than let it crash the sweep."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(lambda: "ok", name="s")

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)

    original_run = Context.run

    class Crash(BaseException):
        """Not an Exception: nothing in the worker records it, like a real process death."""

    async def crash(self, *a, **k):
        raise Crash

    monkeypatch.setattr(Context, "run", crash)
    with pytest.raises(Crash):
        await worker.run_once()
    monkeypatch.setattr(Context, "run", original_run)

    backend.clock.advance(timedelta(seconds=121))
    await backend.executions.delete(eid)  # the record vanished underneath the dead lease
    report = await sweeper.run_once()  # must not raise
    assert report.recovered == []


# --- lost starts ----------------------------------------------------------------------


async def test_sweeper_reenqueues_lost_start_after_window(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    sweeper = engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    backend.queue.queues["default"].clear()  # the START task vanished
    assert (await sweeper.run_once()).restarted == []  # inside the window
    backend.clock.advance(timedelta(seconds=61))
    assert (await sweeper.run_once()).restarted == [eid]
    [task] = await backend.queue.pending("default")
    assert task.task_id == f"start:{eid}"
    assert task.reason == "start"
    assert task.target.fid == ROOT_FID
    assert task.enqueued_by.code.frame_name == "sweeper.restart"
    assert (await sweeper.run_once()).restarted == []  # task exists now: no-op
    await drain(engine.worker())
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_sweeper_restarts_exactly_at_the_window_boundary(backend, engine):
    """ "Not after now" includes the boundary here too (as for recovery and repair): a
    PENDING execution whose `execution.created` entry is exactly `repair_window` old is
    old enough to restart, not one instant more."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    sweeper = engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    backend.queue.queues["default"].clear()  # the START task vanished

    backend.clock.advance(sweeper.repair_window)  # exactly at the boundary, not past it
    assert (await sweeper.run_once()).restarted == [eid]


async def test_sweeper_restart_does_not_break_on_an_early_continue(backend, engine):
    """Each guard in the lost-start scan must skip only its own execution: a `break` in
    place of either `continue` there would silently stop the sweeper from restarting
    every eid that sorts after the first one it decides to skip."""
    from flowli.domain import parse_eid
    from flowli.runtime.sweeper import KnownExecution, SweepReport

    sweeper = engine.sweeper()
    eid_a = parse_eid("00000000-0000-0000-0000-00000000000a")  # not PENDING: hits the status guard
    eid_b = parse_eid(
        "00000000-0000-0000-0000-00000000000b"
    )  # PENDING, but fresh: hits the window guard
    eid_c = parse_eid("00000000-0000-0000-0000-00000000000c")  # PENDING and stale: needs restarting

    sweeper._known = {
        eid_a: KnownExecution(ExecutionStatus.RUNNING, "execution.started", T0),
        eid_b: KnownExecution(ExecutionStatus.PENDING, "execution.created", T0),
        eid_c: KnownExecution(
            ExecutionStatus.PENDING, "execution.created", T0 - timedelta(seconds=61)
        ),
    }
    report = SweepReport()
    await sweeper._restart_lost(T0, report)
    assert report.restarted == [eid_c]


# --- control-log repair -------------------------------------------------------------------


async def test_sweeper_repairs_missing_terminal_announcement(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return "v"

    sweeper = engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    lost = backend.control.entries.pop()  # the worker died between append and announce
    assert lost.item.type == "execution.completed"

    assert (await sweeper.run_once()).repaired == []  # inside the window
    backend.clock.advance(timedelta(seconds=61))
    assert (await sweeper.run_once()).repaired == [eid]
    last = backend.control.entries[-1].item
    assert last.type == "execution.completed" and last.payload == {"eid": str(eid), "value": "v"}
    assert last.fid == "root"
    assert (await sweeper.run_once()).repaired == []


async def test_sweeper_repairs_exactly_at_the_window_boundary(backend, engine):
    """ "Not after now" includes the boundary here too: an entry exactly `repair_window`
    old is old enough to repair, whether the staleness is judged from the known control
    status or from the unannounced journal entry itself (both land on T0 in this test)."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return "v"

    sweeper = engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    backend.control.entries.pop()  # the worker died between append and announce

    backend.clock.advance(sweeper.repair_window)  # exactly at the boundary, not past it
    assert (await sweeper.run_once()).repaired == [eid]


async def test_sweeper_repair_does_not_break_on_an_early_continue(backend, engine):
    """Each guard in the repair loop must skip only its own execution: a `break` in place
    of any `continue` there would silently stop the sweeper from repairing every eid that
    sorts after the first one it decides to skip."""

    @engine.workflow("done", "1")
    async def done(ctx):
        return 1

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()

    eid1 = await engine.start(done, by=HUMAN)  # terminal: hits the is_terminal guard
    await drain(worker)

    eid2 = await engine.start(nap, by=HUMAN)  # non-terminal, but its log stays consistent
    await drain(worker)

    eid3 = await engine.start(nap, by=HUMAN)  # desynced below, but only once known is stale
    await drain(worker)

    eid4 = await engine.start(done, by=HUMAN)  # the one that genuinely needs repairing
    await drain(worker)
    backend.control.entries.pop()  # the worker died between append and announce

    backend.clock.advance(timedelta(minutes=5))  # known is now stale for eid1..eid4

    # a worker resumed eid3 and journaled its completion, but crashed before announcing
    # it -- fresh in the journal even though `known` still shows the old, stale status
    from flowli.domain import Entry, EntryType

    prov = engine.provenance(HUMAN)
    await backend.journal.append(
        eid3, Entry(EntryType.EXECUTION_COMPLETED, "root", {"value": 1}, prov)
    )

    report = await sweeper.run_once()
    assert report.repaired == [eid4]


async def test_sweeper_does_not_repair_a_fresh_entry_when_known_is_stale(backend, engine):
    """The repair window keeps the sweeper from racing a live worker between a journal
    append and its announcement (spec 05 section 9) -- even when the *known* control
    status is itself old, a journal entry that was *just* written must still be left
    alone."""

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return 1

    sweeper = engine.sweeper()
    eid = await engine.start(nap, by=HUMAN)
    await drain(engine.worker())  # suspended; control shows execution.suspended at T0
    backend.clock.advance(timedelta(minutes=5))  # known.last_at is now well past the window

    from flowli.domain import Entry, EntryType

    prov = engine.provenance(HUMAN)  # fresh: provenance.at == now
    await backend.journal.append(
        eid, Entry(EntryType.EXECUTION_COMPLETED, "root", {"value": 1}, prov)
    )

    report = await sweeper.run_once()
    assert report.repaired == []  # must not race the entry that was *just* written
    assert backend.control.entries[-1].item.type == "execution.suspended"


async def test_sweeper_repair_ignores_trailing_frame_only_entries(backend, engine):
    """Only `execution.*` entries belong in the control log: a frame-level entry that
    lands in the journal after the last known execution-level one (e.g. a worker died
    mid-step) must not be mistaken for a lifecycle transition worth repairing."""

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return 1

    sweeper = engine.sweeper()
    eid = await engine.start(nap, by=HUMAN)
    await drain(engine.worker())  # suspended; journal + control both show execution.suspended

    from flowli.domain import Entry, EntryType
    from flowli.runtime.sweeper import SweepReport

    prov = engine.provenance(HUMAN)
    await backend.journal.append(eid, Entry(EntryType.FRAME_COMPLETED, "some/frame", {}, prov))
    backend.clock.advance(timedelta(minutes=5))

    await sweeper._refresh_view()
    report = SweepReport()
    await sweeper._repair_control_log(backend.clock(), report)
    assert report.repaired == []
    assert backend.control.entries[-1].item.type == "execution.suspended"


async def test_sweeper_does_not_repair_consistent_log(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    backend.clock.advance(timedelta(minutes=5))
    report = await engine.sweeper().run_once()
    assert report.repaired == [] and report.recovered == []
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED


# --- orphan waits ----------------------------------------------------------------------------


async def test_sweeper_clears_orphan_waits_and_keeps_live_ones(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    a = await engine.start(w, by=HUMAN)
    b = await engine.start(w, by=HUMAN)
    await drain(worker)
    live = execution_channel(a, "x")
    # a stale marker for b on a channel it does not wait on, and one on a finished execution
    await backend.channel.register_wait("stale.channel", FrameRef(b, "root/receive#0"))
    await engine.signal(b, "x", None, by=HUMAN)
    await drain(worker)
    assert await engine.status(b) == ExecutionStatus.COMPLETED

    report = await sweeper.run_once()
    assert sorted(report.waits_cleared) == [("stale.channel", b)]
    assert await backend.channel.all_waits() == [(live, FrameRef(a, "root/receive#0"))]


async def test_sweeper_clears_the_orphan_wait_of_a_cancelled_execution(backend, engine):
    """Cancelling a suspended execution never runs a `frame.fulfilled` to pop its frame
    out of `suspended` -- `worker._cancel` only appends `execution.cancelled` -- so a
    cancelled execution's memo is terminal *and* `suspended[fid]` still matches its wait,
    both at once. The marker must still be cleared: `is_terminal` alone must decide this,
    not `is_terminal` weakened by an `or` with the still-matching condition."""

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED

    await engine.cancel(eid, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.CANCELLED
    live = execution_channel(eid, "x")
    assert await backend.channel.all_waits() == [(live, FrameRef(eid, "root/receive#0"))]

    report = await sweeper.run_once()
    assert report.waits_cleared == [(live, eid)]
    assert await backend.channel.all_waits() == []


async def test_view_survives_across_runs_and_run_forever_stops(backend, engine):
    import asyncio

    sweeper = engine.sweeper()

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    await engine.start(w, by=HUMAN)
    await sweeper.run_once()
    cursor = sweeper.view.cursor
    assert cursor == len(backend.control.entries)
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.02)
        stop.set()

    await asyncio.gather(sweeper.run_forever(stop, interval=0.005), stopper())
    assert sweeper.view.cursor == cursor


async def test_run_forever_sweeps_repeatedly_until_stop_is_set(backend, engine):
    """The loop condition is everything here: it must run *while* `stop` is not set, and
    return once it is. Inverting it would make `run_forever` return before ever sweeping
    even once; a `timeout=None` wait would make it sweep exactly once and then block
    until `stop` is set instead of sweeping again every `interval`."""
    import asyncio

    sweeper = engine.sweeper()
    calls = 0
    original_run_once = sweeper.run_once

    async def counting():
        nonlocal calls
        calls += 1
        return await original_run_once()

    sweeper.run_once = counting
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.02)
        stop.set()

    await asyncio.gather(sweeper.run_forever(stop, interval=0.005), stopper())
    assert calls >= 2  # several 5ms sweeps must fit in the 20ms before stop is set
    assert stop.is_set()


async def test_run_forever_uses_a_sixty_second_default_interval(engine, monkeypatch):
    """Docs/specs/05-protocols.md section 9: "It runs every minute" -- the sweeper must
    default `interval` to 60 seconds when the caller does not pass one. Caught live, by
    capturing the timeout `run_forever` actually hands to `asyncio.wait_for` on its first
    iteration, rather than by waiting a minute."""
    import asyncio

    sweeper = engine.sweeper()
    stop = asyncio.Event()
    seen_timeout = None

    async def capturing_wait_for(aw, timeout):
        nonlocal seen_timeout
        seen_timeout = timeout
        stop.set()  # let the loop exit after this one iteration
        return await aw  # stop is set now, so the real .wait() resolves at once

    monkeypatch.setattr(asyncio, "wait_for", capturing_wait_for)
    await sweeper.run_forever(stop)  # no explicit interval: exercises the default
    assert seen_timeout == 60.0
