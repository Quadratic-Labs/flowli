"""Structured events: names, fields, and bound context, captured with structlog.testing."""

import json
import logging
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from cairndb import Timestamp
from structlog.testing import LogCapture

from flowlet.adapters.memory import ManualClock, MemoryBackend
from flowlet.domain import Actor, FrameRef, LeaseLost, NonRetryableError, Site, Task, TaskKind
from flowlet.log import bound, configure_logging, get_logger
from flowlet.runtime import Context, Engine, EngineConfig, WorkflowRef
from tests.ids import E_ABC

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")


@pytest.fixture
def engine():
    backend = MemoryBackend(clock=ManualClock(T0))
    return Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(),
    ), backend


async def drain(worker, limit=30):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit


@contextmanager
def capture_logs():
    """Like structlog.testing.capture_logs, but keeps the contextvars merge."""
    cap = LogCapture()
    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, cap],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        cache_logger_on_first_use=False,
    )
    try:
        yield cap.entries
    finally:
        structlog.reset_defaults()


def events(captured, name):
    return [e for e in captured if e["event"] == name]


def test_bound_fields_reach_events():
    log = get_logger("t")
    with capture_logs() as captured:
        with bound(eid=E_ABC, epoch=3):
            log.info("inner", x=1)
        log.info("outer")
    inner, outer = captured
    assert inner["eid"] == E_ABC and inner["epoch"] == 3 and inner["x"] == 1
    assert "eid" not in outer


async def test_worker_lifecycle_events_carry_context(engine):
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx, x):
        await ctx.step(lambda: x, name="s")
        m = await ctx.receive("go")
        return m.payload

    with capture_logs() as captured:
        eid = await engine.start(w, 7, by=HUMAN)
        worker = engine.worker()
        await drain(worker)
        seq = await engine.signal(eid, "go", "ok", by=Actor.system("bank"))
        await drain(worker)

    created = events(captured, "execution_created")[0]
    assert created["eid"] == str(eid) and created["by"] == "human:thomas@example.com"
    assert created["workflow"] == "w" and created["log_level"] == "info"
    assert created["version"] == "1" and created["queue"] == "default"
    assert created["parent"] is None  # a root execution has no parent

    started = events(captured, "execution_started")[0]
    assert started["eid"] == str(eid) and started["epoch"] == 1 and started["worker_id"] == "w-1"
    assert started["task_id"] == f"start:{eid}" and started["task_kind"] == "start"
    assert started["workflow"] == "w" and started["version"] == "1"
    assert started["queue"] == "default"  # bound() in _process, not just task_id/task_kind

    suspended = events(captured, "execution_suspended")[0]
    assert suspended["on"] == [f"channel:{eid}.go"] and suspended["eid"] == str(eid)

    sent = events(captured, "message_sent")[0]
    assert sent["by"] == "system:bank" and sent["channel"] == f"{eid}.go"
    assert sent["eid"] == str(eid) and sent["seq"] == seq

    resumed = events(captured, "execution_resumed")[0]
    assert resumed["epoch"] == 2 and resumed["reason"] == "message:go"
    assert resumed["workflow"] == "w" and resumed["version"] == "1"

    completed = events(captured, "execution_completed")[0]
    assert (
        completed["eid"] == str(eid) and completed["epoch"] == 2 and completed["worker_id"] == "w-1"
    )

    frame = events(captured, "frame_started")[0]
    assert frame["fid"] == "root/s#0" and frame["attempt"] == 1 and frame["eid"] == str(eid)
    assert frame["log_level"] == "debug"

    completed_frame = events(captured, "frame_completed")[0]
    assert completed_frame["fid"] == "root/s#0" and completed_frame["log_level"] == "debug"

    # each successful run_once (start, then resume-to-completion) acks its own task
    acked = events(captured, "task_acked")
    assert len(acked) == 2 and all(e["log_level"] == "debug" for e in acked)


async def test_execution_created_event_names_the_real_parent_for_a_child(engine):
    """A child's own "execution_created" log entry must carry its actual parent eid,
    not a hardcoded placeholder -- distinct from a root execution, whose parent really
    is None (see test_worker_lifecycle_events_carry_context)."""
    engine, backend = engine

    @engine.workflow("child", "1")
    async def child(ctx):
        return 1

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return await ctx.child(child)

    eid = await engine.start(parent, by=HUMAN)
    worker = engine.worker()
    with capture_logs() as captured:
        await drain(worker)

    child_created = next(e for e in events(captured, "execution_created") if e["eid"] != str(eid))
    assert child_created["parent"] == str(eid)


async def test_task_dequeued_event_fields(engine):
    """The first thing _process logs names exactly which task it claimed and which
    execution it targets -- nothing else asserts these fields."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    worker = engine.worker()
    with capture_logs() as captured:
        await drain(worker)

    dequeued = events(captured, "task_dequeued")[0]
    assert dequeued["reason"] == "start" and dequeued["eid"] == str(eid)
    assert dequeued["log_level"] == "debug"


async def test_task_lease_lost_mid_dispatch_logs_bare_event(engine):
    """The worker's OWN task queue lease (not the execution's ownership lease, already
    covered by test_execution_lease_lost_event_fields) can be lost mid-dispatch too --
    05-protocols.md #1 step 6: the renew loop wired directly onto _process cancels the
    in-flight dispatch and must log task_lease_lost with just the eid (no `detail`,
    unlike the execution-lease-lost variant) and drop the task without acking or
    nacking it."""
    _, backend = engine
    import asyncio

    engine = Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(task_ttl=0.05, poll_interval=0.01),
    )
    worker = engine.worker()

    original_dequeue = backend.queue.dequeue

    async def fencing_dequeue(*a, **k):
        claimed = await original_dequeue(*a, **k)
        if claimed is not None:

            async def renew():
                raise LeaseLost("task lease fenced")

            claimed.lease.renew = renew
        return claimed

    backend.queue.dequeue = fencing_dequeue

    @engine.workflow("w", "1")
    async def w(ctx):
        async def slow():
            await asyncio.sleep(0.3)
            return 1

        return await ctx.step(slow, name="s1")

    eid = await engine.start(w, by=HUMAN)
    with capture_logs() as captured:
        assert await worker.run_once()  # dropped: task lease lost, not crashed
    backend.queue.dequeue = original_dequeue

    lost = events(captured, "task_lease_lost")[0]
    assert lost["eid"] == str(eid) and lost["log_level"] == "warning"
    assert "detail" not in lost
    assert not events(captured, "task_acked") and not events(captured, "task_nacked")


def _detached_add(a, b):
    return a + b


async def test_detached_step_done_log_fields(engine):
    """`detached_step_done` names exactly which execution/frame ran and how it ended --
    nothing else asserts these fields."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    fid = "root/add:1"
    task = Task(
        queue="default",
        kind=TaskKind.RUN_STEP,
        target=FrameRef(eid, fid),
        reason="delegate",
        enqueued_by=engine.provenance(HUMAN),
        key=fid,
        payload={"fn": f"{__name__}:_detached_add", "args": [2, 3], "name": "add"},
    )
    with capture_logs() as captured:
        await engine.enqueue(task)
        await drain(worker)

    done = events(captured, "detached_step_done")[0]
    assert done["eid"] == str(eid) and done["fid"] == fid and done["status"] == "completed"


async def test_failure_and_frame_failed_events(engine):
    engine, backend = engine

    @engine.workflow("bad", "1")
    async def bad(ctx):
        return await ctx.step(lambda: (_ for _ in ()).throw(NonRetryableError("nope")), name="boom")

    with capture_logs() as captured:
        await engine.start(bad, by=HUMAN)
        await drain(engine.worker())

    failed_frame = events(captured, "frame_failed")[0]
    assert (
        failed_frame["fid"] == "root/boom#0" and failed_frame["error_type"] == "NonRetryableError"
    )
    assert failed_frame["attempt"] == 1
    assert failed_frame["retryable"] is False
    failed = events(captured, "execution_failed")[0]
    assert failed["error_type"] == "StepFailed" and failed["log_level"] == "warning"
    assert failed["message"] == "step root/boom#0 failed: NonRetryableError: nope"


async def test_cancellation_event_carries_by(engine):
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("never")

    eid = await engine.start(w, by=HUMAN)
    worker = engine.worker()
    await drain(worker)

    with capture_logs() as captured:
        await engine.cancel(eid, by=HUMAN)
        await drain(worker)

    cancelled = events(captured, "execution_cancelled")[0]
    assert cancelled["log_level"] == "info"
    assert cancelled["by"]["actor"]["id"] == "thomas@example.com"


async def test_migrate_and_cancel_events(engine):
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w1(ctx):
        return await ctx.receive("go")

    @engine.workflow("w", "2")
    async def w2(ctx):
        return await ctx.receive("go")

    worker = engine.worker()
    eid = await engine.start(w1, by=HUMAN)
    await drain(worker)

    with capture_logs() as captured:
        await engine.migrate(eid, "2", by=HUMAN)
    migrated = events(captured, "execution_migrated")[0]
    assert migrated["eid"] == str(eid)
    assert migrated["from_version"] == "1"
    assert migrated["version"] == "2"
    assert migrated["by"] == "human:thomas@example.com"

    with capture_logs() as captured:
        await engine.cancel(eid, by=HUMAN)
    cancel_requested = events(captured, "cancel_requested")[0]
    assert cancel_requested["eid"] == str(eid)
    assert cancel_requested["by"] == "human:thomas@example.com"


async def test_execution_unknown_event_has_hint(engine):
    """A worker that dequeues a task for an archived (or otherwise gone) execution logs
    why it dropped it -- nothing else asserts this event's fields."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    worker = engine.worker()
    await drain(worker)
    await engine.enqueue_resume(eid, "late", "late", engine.provenance(HUMAN))
    backend.clock.advance(timedelta(days=31))
    await engine.retention(delay=timedelta(days=30)).run_once()

    with capture_logs() as captured:
        assert await worker.run_once()  # the late task is dropped, not crashed
    unknown = events(captured, "execution_unknown")[0]
    assert unknown["hint"] == "archived?"


async def test_workflow_not_registered_event_fields(engine):
    """A worker that doesn't know a workflow logs which one, so an operator can tell
    a missing deploy from any other nack -- nothing else asserts these fields."""
    engine, backend = engine

    async def w(ctx):
        return 1

    engine.registry.register(WorkflowRef("w", "1", w))
    await engine.start(w, by=HUMAN)
    bare = Engine(
        backend.ports, Site("h", 2, "w-2"), clock=backend.clock, config=EngineConfig(nack_delay=5)
    )
    with capture_logs() as captured:
        assert await bare.worker().run_once()
    warned = events(captured, "workflow_not_registered")[0]
    assert warned["workflow"] == "w" and warned["version"] == "1"

    nacked = events(captured, "task_nacked")[0]
    assert nacked["delay_s"] == 5.0 and nacked["log_level"] == "info"


async def test_execution_blocked_event_fields(engine):
    """A worker that blocks an execution on a nondeterminism mismatch logs the frame
    that disagreed -- nothing else asserts these fields."""
    engine, backend = engine
    arg = {"v": 1}

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.step(lambda v: v, arg["v"], name="s")
        await ctx.receive("x")

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    arg["v"] = 2
    await engine.signal(eid, "x", None, by=HUMAN)

    with capture_logs() as captured:
        await drain(worker)
    blocked = events(captured, "execution_blocked")[0]
    assert blocked["reason"] == "nondeterminism" and blocked["fid"] == "root/s#0"
    assert blocked["log_level"] == "error"


async def test_execution_lease_lost_event_fields(engine):
    """A worker fenced out mid-run (renewal failed) logs that it discarded the run,
    with the exact reason -- nothing else asserts these events' fields."""
    _, backend = engine
    import asyncio

    engine = Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(exec_ttl=0.05, poll_interval=0.01),
    )
    worker = engine.worker()

    original_acquire = backend.ownership.acquire

    async def failing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)

        async def renew():
            raise LeaseLost("fenced mid-run")

        lease.renew = renew
        return lease

    backend.ownership.acquire = failing_acquire

    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(lambda: asyncio.sleep(0.3), name="s")

    eid = await engine.start(w, by=HUMAN)
    with capture_logs() as captured:
        assert await worker.run_once()  # dropped: lease lost mid-run, not crashed
    backend.ownership.acquire = original_acquire

    lost = events(captured, "execution_lease_lost")[0]
    assert lost["outcome"] == "discarded" and lost["log_level"] == "warning"
    task_lost = events(captured, "task_lease_lost")[0]
    assert task_lost["detail"] == f"execution {eid} lease lost during run"
    assert task_lost["eid"] == str(eid)


async def test_execution_owned_elsewhere_event_fields(engine):
    """A worker that loses the ownership race for an execution logs which one it
    backed off from -- nothing else asserts this event's fields."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    other = await backend.ownership.acquire(eid, "w-9", ttl=120)
    worker = engine.worker()

    with capture_logs() as captured:
        assert await worker.run_once()  # acked, dropped: owned elsewhere
    owned = events(captured, "execution_owned_elsewhere")[0]
    assert owned["eid"] == str(eid) and owned["log_level"] == "info"
    await other.release()


async def test_worker_iteration_failed_event_fields(engine):
    """run_forever swallows any unexpected exception from one iteration and logs it
    with the worker's own id -- nothing else asserts this event's fields."""
    engine, backend = engine
    import asyncio

    worker = engine.worker()

    async def raising_run_once():
        raise RuntimeError("boom")

    worker.run_once = raising_run_once

    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.05)
        stop.set()

    with capture_logs() as captured:
        await asyncio.gather(worker.run_forever(stop), stopper())

    failed = events(captured, "worker_iteration_failed")[0]
    assert failed["worker_id"] == worker.worker_id and failed["log_level"] == "error"


async def test_delegate_task_on_worker_queue_event(engine):
    """A DELEGATE task should never reach a worker queue (01-domain-model.md section 6)
    -- if one lands there anyway, the worker logs it and nacks with a delay instead of
    running it or crashing."""
    engine, backend = engine
    worker = engine.worker()

    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.DELEGATE,
            target=FrameRef(E_ABC, "root/x#0"),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key="k1",
        )
    )

    with capture_logs() as captured:
        assert await worker.run_once()  # nacked, not run
    warned = events(captured, "delegate_task_on_worker_queue")[0]
    assert warned["log_level"] == "warning"
    nacked = events(captured, "task_nacked")[0]
    assert nacked["delay_s"] == 5.0 and nacked["log_level"] == "info"


async def test_sweeper_and_retention_events(engine):
    engine, backend = engine

    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(minutes=1))
        return 1

    worker = engine.worker()
    eid = await engine.start(nap, by=HUMAN)
    await drain(worker)
    backend.clock.advance(timedelta(minutes=1))
    [timer] = list(backend.timers.timers.values())
    with capture_logs() as captured:
        await engine.sweeper().run_once()
    assert events(captured, "timer_fired")[0]["eid"] == str(eid)
    assert events(captured, "timer_fired")[0]["timer_id"] == timer.timer_id
    assert events(captured, "sweep_done")[0]["timers_fired"] == 1

    await drain(worker)
    backend.clock.advance(timedelta(days=31))
    with capture_logs() as captured:
        await engine.retention(delay=timedelta(days=30)).run_once()
    assert events(captured, "execution_archived")[0]["eid"] == str(eid)
    assert events(captured, "retention_done")[0]["archived"] == [eid]
    assert events(captured, "retention_done")[0]["cleaned"] == []


async def test_execution_recovered_log_fields(engine, monkeypatch):
    """`execution_recovered` names exactly which execution and dead epoch triggered the
    recovery -- nothing else asserts these fields."""
    engine, backend = engine

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
    with capture_logs() as captured:
        report = await sweeper.run_once()
    recovered = events(captured, "execution_recovered")[0]
    assert recovered["eid"] == str(eid)
    assert recovered["dead_epoch"] == 1
    # sweep_done must carry the report's own `recovered` list, not just the other counts
    assert report.recovered == [eid]
    assert events(captured, "sweep_done")[0]["recovered"] == [eid]


async def test_control_log_repaired_log_fields(engine):
    """`control_log_repaired` names exactly which execution and entry type the sweeper
    re-announced -- nothing else asserts these fields."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return "v"

    worker, sweeper = engine.worker(), engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    lost = backend.control.entries.pop()  # the worker died between append and announce
    assert lost.item.type == "execution.completed"

    backend.clock.advance(timedelta(seconds=61))
    with capture_logs() as captured:
        report = await sweeper.run_once()
    repaired = events(captured, "control_log_repaired")[0]
    assert repaired["eid"] == str(eid)
    assert repaired["entry"] == "execution.completed"
    # sweep_done must carry the report's own `repaired` list, not just the other counts
    assert report.repaired == [eid]
    assert events(captured, "sweep_done")[0]["repaired"] == [eid]


async def test_start_reenqueued_log_fields(engine):
    """`start_reenqueued` names exactly which execution the sweeper put the START task
    back for -- nothing else asserts this field."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    sweeper = engine.sweeper()
    eid = await engine.start(w, by=HUMAN)
    backend.queue.queues["default"].clear()  # the START task vanished

    backend.clock.advance(timedelta(seconds=61))
    with capture_logs() as captured:
        report = await sweeper.run_once()
    reenqueued = events(captured, "start_reenqueued")[0]
    assert reenqueued["eid"] == str(eid)
    # sweep_done must carry the report's own `restarted` list, not just the other counts
    assert report.restarted == [eid]
    assert events(captured, "sweep_done")[0]["restarted"] == [eid]


async def test_sweep_done_reports_the_waits_cleared_count(engine):
    """`sweep_done` must carry `waits_cleared` as the report's own count -- nothing else
    asserts this field."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")
        return 1

    worker, sweeper = engine.worker(), engine.sweeper()
    a = await engine.start(w, by=HUMAN)
    b = await engine.start(w, by=HUMAN)
    await drain(worker)
    # a stale marker for b on a channel it does not wait on
    await backend.channel.register_wait("stale.channel", FrameRef(b, "root/receive#0"))
    await engine.signal(b, "x", None, by=HUMAN)
    await drain(worker)

    with capture_logs() as captured:
        report = await sweeper.run_once()
    assert report.waits_cleared == [("stale.channel", b)]
    assert events(captured, "sweep_done")[0]["waits_cleared"] == 1


async def test_retention_warns_when_archive_exists_with_journal_still_present(engine):
    """Crash recovery: the archive was written but the journal delete never ran. A rerun
    of fold() must warn about the leftover journal, not silently retry the archive write."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    backend.clock.advance(timedelta(days=31))

    # simulate a crash right after the archive write, before the journal delete
    await backend.archive.write(eid, {"eid": str(eid), "journal": []})

    with capture_logs() as captured:
        result = await engine.retention(delay=timedelta(days=30)).fold(eid)
    assert result is False
    warned = events(captured, "archive_exists_with_journal")
    assert warned and warned[0]["eid"] == str(eid)


async def test_retention_does_not_warn_once_the_journal_is_already_gone(engine):
    """After a crash that deletes everything but the record, a rerun finds an empty
    journal alongside the existing archive: it must not raise a false alarm, and it must
    log the fold as cleaned, not archived."""
    engine, backend = engine

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    await drain(engine.worker())
    backend.clock.advance(timedelta(days=31))
    retention = engine.retention(delay=timedelta(days=30))

    original_delete = backend.executions.delete

    async def crash(eid_):
        raise RuntimeError("disk on fire")

    backend.executions.delete = crash
    with pytest.raises(RuntimeError):
        await retention.run_once()
    backend.executions.delete = original_delete

    with capture_logs() as captured:
        await retention.run_once()
    assert events(captured, "archive_exists_with_journal") == []
    cleaned = events(captured, "execution_cleaned")
    assert cleaned and cleaned[0]["eid"] == str(eid)
    assert events(captured, "execution_archived") == []


def test_configure_logging_json_renders_one_object_per_line(capsys):
    configure_logging("INFO", "json")
    try:
        get_logger("t").info("hello", zeta=1, eid=str(E_ABC))
        get_logger("t").debug("hidden")  # below INFO
        err = capsys.readouterr().err.strip().splitlines()
        assert len(err) == 1
        obj = json.loads(err[0])
        assert obj["event"] == "hello" and obj["eid"] == str(E_ABC) and obj["level"] == "info"
        assert obj["timestamp"].endswith("Z")
        # JSONRenderer(sort_keys=True): keys come out alphabetically, not insertion order.
        assert list(obj.keys()) == sorted(obj.keys())
    finally:
        structlog.reset_defaults()


def test_configure_logging_console(capsys):
    configure_logging("WARNING", "console")
    try:
        get_logger("t").warning("careful", eid=str(E_ABC))
        get_logger("t").info("quiet")
        err = capsys.readouterr().err
        assert "careful" in err and f"eid={E_ABC}" in err and "quiet" not in err
    finally:
        structlog.reset_defaults()


def test_configure_logging_unknown_level_falls_back_to_info(capsys):
    """`level.upper()` not found on `logging` -> INFO, not a crash (getattr's fallback)."""
    configure_logging("not-a-real-level", "json")
    try:
        get_logger("t").info("visible")
        get_logger("t").debug("hidden")  # still filtered, as under INFO
        err = capsys.readouterr().err.strip().splitlines()
        assert len(err) == 1
        assert json.loads(err[0])["event"] == "visible"
    finally:
        structlog.reset_defaults()


def test_configure_logging_console_colors_when_stderr_is_a_tty(monkeypatch):
    """colors=sys.stderr.isatty(): a real terminal gets ANSI styling."""

    class FakeTTYStream:
        def __init__(self):
            self.chunks = []

        def write(self, s):
            self.chunks.append(s)

        def flush(self):
            pass

        def isatty(self):
            return True

    fake = FakeTTYStream()
    monkeypatch.setattr(sys, "stderr", fake)
    configure_logging("INFO", "console")
    try:
        get_logger("t").info("hello")
    finally:
        structlog.reset_defaults()
    assert "\x1b[" in "".join(fake.chunks)


def test_configure_logging_disables_logger_caching(capsys):
    """cache_logger_on_first_use=False: a later reconfigure reaches loggers already handed out."""
    # Simulate a stale global structlog config (e.g. left by other code in this process)
    # that had caching turned on, to prove configure_logging always turns it back off.
    structlog.configure(cache_logger_on_first_use=True)
    configure_logging("INFO", "console")
    try:
        log = get_logger("t")
        log.info("first")  # would get cached here if caching were left on
        configure_logging("WARNING", "console")
        log.info("second")  # must be filtered out under the new, stricter level
        log.warning("third")
        err = capsys.readouterr().err
        assert "first" in err
        assert "second" not in err
        assert "third" in err
    finally:
        structlog.reset_defaults()


def test_configure_logging_configures_stdlib_root_logger():
    """The trailing logging.basicConfig call applies `level` and `format` to stdlib loggers."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers[:] = []  # so basicConfig actually (re)configures regardless of `force`
    try:
        configure_logging("DEBUG", "console")
        assert root.level == logging.DEBUG
        record = logging.LogRecord(
            name="asyncio",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        assert root.handlers[0].format(record) == "INFO asyncio: hello"
    finally:
        structlog.reset_defaults()
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)


def test_configure_logging_reconfigures_stdlib_root_logger_despite_existing_handler():
    """force=True: reconfiguring stdlib logging takes effect even if a handler is already set."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        root.handlers[:] = [logging.NullHandler()]
        root.setLevel(logging.CRITICAL)
        configure_logging("DEBUG", "console")
        assert root.level == logging.DEBUG
        assert isinstance(root.handlers[0], logging.StreamHandler)
    finally:
        structlog.reset_defaults()
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)


def test_stderr_logger_flushes_immediately(monkeypatch):
    """_StderrLogger.msg passes flush=True to print, so output is not buffered."""

    class RecordingStream:
        def __init__(self):
            self.written = []
            self.flushed = False

        def write(self, s):
            self.written.append(s)

        def flush(self):
            self.flushed = True

        def isatty(self):
            return False

    stream = RecordingStream()
    monkeypatch.setattr(sys, "stderr", stream)
    configure_logging("INFO", "json")
    try:
        get_logger("t").info("hello")
    finally:
        structlog.reset_defaults()
    assert stream.flushed is True
    assert "hello" in "".join(stream.written)
