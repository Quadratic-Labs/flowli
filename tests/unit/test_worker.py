import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.codec import digest
from flowli.domain import (
    ROOT_FID,
    Actor,
    Code,
    Condition,
    EntryType,
    ExecutionStatus,
    Failed,
    FrameKind,
    FrameRef,
    LeaseLost,
    Message,
    NonRetryableError,
    RetryPolicy,
    Site,
    Task,
    TaskKind,
    execution_channel,
)
from flowli.runtime import Done, Engine, EngineConfig, Wait, Worker

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
        config=EngineConfig(code_ref="git:abc", poll_interval=0.01),
    )


@pytest.fixture
def worker(engine) -> Worker:
    return engine.worker()


async def drain(worker: Worker, limit: int = 20) -> int:
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit, "worker did not settle"
    return n


def control_types(backend):
    return [s.item.type for s in backend.control.entries]


def _fence_ownership_after(backend, calls_before_fencing: int = 0) -> None:
    """Patch backend.ownership.acquire so the NEXT acquired lease's refresh_state
    lets `calls_before_fencing` calls through normally, then raises LeaseLost --
    simulating a worker fenced out by a newer epoch partway through a run."""
    original_acquire = backend.ownership.acquire

    async def fencing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)
        original_refresh = lease.refresh_state
        remaining = {"n": calls_before_fencing}

        async def refresh_state():
            if remaining["n"] <= 0:
                raise LeaseLost("fenced")
            remaining["n"] -= 1
            return await original_refresh()

        lease.refresh_state = refresh_state
        backend.ownership.acquire = original_acquire
        return lease

    backend.ownership.acquire = fencing_acquire


# --- provenance -------------------------------------------------------------------------


def test_provenance_defaults_to_the_no_workflow_sentinel(engine):
    """Called with no overrides (as cancel/signal/broadcast do), `provenance` must stamp
    the documented sentinel values -- not just build *some* Provenance."""
    prov = engine.provenance(HUMAN)
    assert prov.code == Code("-", "-", "root", "engine", "git:abc")
    assert prov.attempt == 1
    assert prov.actor == HUMAN
    assert prov.site == engine.site


def test_provenance_honors_every_override(engine):
    other_site = Site(host="h2", pid=9, worker_id="w-2")
    prov = engine.provenance(
        HUMAN,
        workflow="wf",
        version="3",
        frame_kind="step",
        frame_name="fetch",
        site=other_site,
        attempt=2,
    )
    assert prov.code == Code("wf", "3", "step", "fetch", "git:abc")
    assert prov.attempt == 2
    assert prov.site == other_site


# --- tasks ------------------------------------------------------------------------------


def _spy_queue(engine):
    """Point engine.ports.queue at a thin proxy that records whether `enqueue` or
    `ensure` was called, without touching the real queue object -- MemoryQueue.ensure
    delegates to `self.enqueue`, so patching attributes directly on the real queue
    would double-count that internal call."""
    calls: list[str] = []
    real = engine.ports.queue

    class _Spy:
        async def enqueue(self, task):
            calls.append("enqueue")
            return await real.enqueue(task)

        async def ensure(self, task):
            calls.append("ensure")
            return await real.ensure(task)

        def __getattr__(self, name):
            return getattr(real, name)

    engine.ports = replace(engine.ports, queue=_Spy())
    return calls


async def test_enqueue_defaults_to_the_plain_path_not_repair(backend, engine):
    """`repair` must default to False: the cheap `Queue.enqueue`, not the
    listing-costing `Queue.ensure` repair path (`create_execution` and every other
    caller besides the sweeper rely on this default)."""
    calls = _spy_queue(engine)
    prov = engine.provenance(HUMAN)

    task_a = Task(
        queue="default",
        kind=TaskKind.RESUME,
        target=FrameRef(uuid.uuid4(), ROOT_FID),
        reason="test",
        enqueued_by=prov,
        key="a",
    )
    await engine.enqueue(task_a)
    assert calls == ["enqueue"]

    calls.clear()
    task_b = Task(
        queue="default",
        kind=TaskKind.RESUME,
        target=FrameRef(uuid.uuid4(), ROOT_FID),
        reason="test",
        enqueued_by=prov,
        key="b",
    )
    await engine.enqueue(task_b, repair=True)
    assert calls == ["ensure"]


async def test_enqueue_resume_defaults_to_the_plain_path_not_repair(backend, engine):
    """Same default as `enqueue` above: every caller of `enqueue_resume` besides the
    sweeper's explicit repair paths relies on `repair` defaulting to False."""

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)
    calls = _spy_queue(engine)
    prov = engine.provenance(HUMAN)

    await engine.enqueue_resume(eid, "cause-a", "reason-a", prov)
    assert calls == ["enqueue"]

    calls.clear()
    await engine.enqueue_resume(eid, "cause-b", "reason-b", prov, repair=True)
    assert calls == ["ensure"]


async def test_signal_forwards_correlation_onto_the_message(backend, engine):
    """`signal` (like `deliver`) must forward `correlation` onto the sent message --
    not silently drop it on its way to `deliver`."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    full = execution_channel(eid, "go")
    await engine.signal(eid, "go", "ok", by=HUMAN, correlation="corr-9")
    sent = (await backend.channel.read(full))[0]
    assert sent.correlation == "corr-9"


# --- happy path -----------------------------------------------------------------


async def test_start_runs_to_completion(backend, engine, worker):
    calls = []

    @engine.workflow("hello", "1")
    async def hello(ctx, who):
        calls.append(who)
        return await ctx.step(lambda: f"hi {who}", name="greet")

    eid = await engine.start(hello, "bob", by=HUMAN)
    assert await engine.status(eid) == ExecutionStatus.PENDING
    assert (await backend.queue.pending("default"))[0].task_id == f"start:{eid}"
    assert control_types(backend) == ["execution.created", "task.enqueued"]

    assert await drain(worker) == 1
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert await backend.queue.pending("default") == []
    journal = [s.item.type for s in await engine.journal(eid)]
    assert journal == [
        EntryType.EXECUTION_STARTED,
        EntryType.FRAME_STARTED,
        EntryType.FRAME_COMPLETED,
        EntryType.EXECUTION_COMPLETED,
    ]
    assert control_types(backend)[-2:] == ["execution.started", "execution.completed"]
    assert all(s.item.fid == ROOT_FID for s in backend.control.entries[-2:])
    entries = await engine.journal(eid)
    completed = entries[-1].item
    assert completed.payload == {"value": "hi bob"}
    assert completed.provenance.site.epoch == 1
    assert completed.provenance.actor == Actor.worker("w-1")
    assert completed.provenance.code.workflow == "hello"
    assert completed.provenance.code.version == "1"
    assert completed.provenance.code.frame_name == "worker"
    started = next(s.item for s in entries if s.item.type == EntryType.EXECUTION_STARTED)
    assert started.payload == {"args": {"args": ["bob"], "kwargs": {}}}
    frame_started = next(s.item for s in entries if s.item.type == EntryType.FRAME_STARTED)
    assert frame_started.provenance.code.code_ref == "git:abc"
    doc = backend.ownership.lease_doc(eid)
    assert doc.released and doc.state == {"status": "completed"}
    created = (await engine.execution(eid)).created_by
    assert created.actor == HUMAN


async def test_start_with_key_is_idempotent(engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    a = await engine.start(w, key="k", by=HUMAN)
    b = await engine.start(w, key="k", by=HUMAN)
    assert a == b


async def test_start_records_the_workflow_and_version_on_created_by(engine):
    """`created_by.code.workflow/version` must name the workflow actually started, not
    the engine.provenance() sentinel "-" a caller gets by omitting them."""

    @engine.workflow("billing", "3")
    async def billing(ctx):
        return 1

    eid = await engine.start(billing, by=HUMAN)
    execution = await engine.execution(eid)
    assert execution.created_by.code.workflow == "billing"
    assert execution.created_by.code.version == "3"


async def test_failed_step_fails_execution(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(lambda: (_ for _ in ()).throw(ValueError("bad")), name="boom")

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.FAILED
    failed = (await engine.journal(eid))[-1].item
    assert failed.payload["error_type"] == "StepFailed"
    assert control_types(backend)[-1] == "execution.failed"
    assert backend.ownership.lease_doc(eid).state == {"status": "failed"}


# --- suspension and messages -----------------------------------------------------------


async def test_receive_suspends_then_signal_resumes(backend, engine, worker):
    @engine.workflow("pay", "1")
    async def pay(ctx):
        m = await ctx.receive("payments")
        return m.payload["amount"]

    eid = await engine.start(pay, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    full = execution_channel(eid, "payments")
    assert await backend.channel.waiters(full) == [FrameRef(eid, "root/receive#0")]
    assert backend.ownership.lease_doc(eid).state == {"status": "suspended"}
    assert control_types(backend)[-1] == "execution.suspended"

    seq = await engine.signal(eid, "payments", {"amount": 10}, by=Actor.system("bank"))
    assert seq == 1
    pending = await backend.queue.pending("default")
    assert [t.task_id for t in pending] == [f"resume:{eid}:message:{full}:1"]

    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": 10}
    assert await backend.channel.waiters(full) == []
    resumed = [s.item for s in await engine.journal(eid) if s.item.type == "execution.resumed"]
    assert resumed[0].payload == {"epoch": 2, "reason": "message:payments"}


async def test_deliver_stamps_signal_provenance_and_correlation(backend, engine):
    """`deliver` (unlike `signal`) takes the full channel name directly, and it must
    forward `correlation` onto the message rather than dropping it -- and always tag
    its own provenance frame_name "signal", the same as `signal` uses internally."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    seq = await engine.deliver(
        eid, "custom.reply", {"verdict": "ok"}, by=HUMAN, correlation="corr-1"
    )
    sent = (await backend.channel.read("custom.reply"))[0]
    assert sent.seq == seq
    assert sent.payload == {"verdict": "ok"}
    assert sent.correlation == "corr-1"
    assert sent.sent_by.actor == HUMAN
    assert sent.sent_by.code.frame_name == "signal"


async def test_message_sent_during_suspension_is_not_lost(backend, engine, worker):
    """A sender races the release: the worker's re-check enqueues the resume itself."""

    @engine.workflow("pay", "1")
    async def pay(ctx):
        return (await ctx.receive("payments")).payload

    eid = await engine.start(pay, by=HUMAN)
    full = execution_channel(eid, "payments")

    original_acquire = backend.ownership.acquire

    async def racing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)
        inner_release = lease.release

        async def release(state=None):
            # the sender lands after the wait is registered and before the release
            await backend.channel.send(Message(full, 0, "late", HUMAN_PROV))
            await inner_release(state)

        lease.release = release
        return lease

    HUMAN_PROV = engine.provenance(HUMAN)
    backend.ownership.acquire = racing_acquire
    assert await worker.run_once()
    assert [t.task_id for t in await backend.queue.pending("default")] == [
        f"resume:{eid}:message:{full}:1"
    ]
    backend.ownership.acquire = original_acquire
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_second_message_sent_during_suspension_is_not_lost(backend, engine, worker):
    """The post-release recheck must look for messages strictly after what this frame has
    already consumed (05-protocols.md #3 step 6), not from the channel's start -- otherwise
    a late second message would be masked by re-discovering the already-consumed first one,
    and the resume would be keyed by the wrong (stale) seq."""

    @engine.workflow("pay", "1")
    async def pay(ctx):
        first = await ctx.receive("topic")
        second = await ctx.receive("topic")
        return [first.payload, second.payload]

    eid = await engine.start(pay, by=HUMAN)
    full = execution_channel(eid, "topic")
    await drain(worker)  # suspended on the first receive

    original_acquire = backend.ownership.acquire

    async def racing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)
        inner_release = lease.release

        async def release(state=None):
            # the sender lands after the second wait is registered and before the release
            await backend.channel.send(Message(full, 0, "two", HUMAN_PROV))
            await inner_release(state)

        lease.release = release
        return lease

    HUMAN_PROV = engine.provenance(HUMAN)
    await engine.signal(eid, "topic", "one", by=HUMAN)  # fulfils the first receive, seq=1
    backend.ownership.acquire = racing_acquire
    # processes the resume: fulfils the first receive, suspends on the second -- "two"
    # races in right at that suspend's release, before this call returns
    assert await worker.run_once()
    backend.ownership.acquire = original_acquire
    pending = await backend.queue.pending("default")
    assert [t.task_id for t in pending] == [f"resume:{eid}:message:{full}:2"]
    assert pending[0].reason == f"message:{full}"
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["task_id"] == f"resume:{eid}:message:{full}:2"
    ]
    assert enqueued and enqueued[-1].provenance.actor == Actor.worker("w-1")

    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": ["one", "two"]}


async def test_suspend_only_treats_child_conditions_as_children(backend, engine, worker):
    """Only a CHILD condition should be tracked for the post-release child recheck (step 7)
    -- a non-child condition whose parsed name happens to be non-None too (e.g. a timer
    id) must not be mistaken for a child and spuriously enqueue a resume for it."""

    @engine.workflow("done_already", "1")
    async def done_already(ctx):
        return 1

    other_eid = await engine.start(done_already, by=HUMAN)
    await drain(engine.worker())
    assert await engine.status(other_eid) == ExecutionStatus.COMPLETED

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return 0

    eid = await engine.start(parent, by=HUMAN)
    start_task = await backend.queue.dequeue("default", "w-1", engine.config.task_ttl)
    await backend.queue.ack(start_task)  # the parent's own start task is not what we're testing
    execution = await engine.execution(eid)
    memo = await engine.memo(eid)
    lease = await backend.ownership.acquire(eid, "w-1", engine.config.exec_ttl)
    prov = engine.provenance(worker.actor, frame_name="worker")

    # A non-child condition (a timer) whose parsed "name" happens to be non-None too --
    # here, another (already-terminal) execution's id, to make a wrongly-tracked "child"
    # observable if the kind check is ever weakened.
    waits = (Wait("root/t#0", f"timer:{other_eid}", None),)
    await worker._suspend(execution, memo, lease, prov, waits)

    assert await backend.queue.pending("default") == []


async def test_suspend_recheck_handles_terminal_and_missing_children(backend, engine, worker):
    """Step 7 of "suspend an execution" (05-protocols.md #3): after releasing the lease,
    re-check each child condition -- if the child already reached a terminal status (it
    raced ahead of us), enqueue its resume ourselves. A child eid that no longer resolves
    (suppress(Exception)) must not stop the recheck from still handling every OTHER
    child condition in the same wait set."""

    @engine.workflow("done_already", "1")
    async def done_already(ctx):
        return 1

    real_child = await engine.start(done_already, by=HUMAN)
    await drain(engine.worker())
    assert await engine.status(real_child) == ExecutionStatus.COMPLETED

    missing_child = uuid.uuid4()

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return 0

    eid = await engine.start(parent, by=HUMAN)
    start_task = await backend.queue.dequeue("default", "w-1", engine.config.task_ttl)
    await backend.queue.ack(start_task)  # the parent's own start task is not what we're testing
    execution = await engine.execution(eid)
    memo = await engine.memo(eid)
    lease = await backend.ownership.acquire(eid, "w-1", engine.config.exec_ttl)
    prov = engine.provenance(worker.actor, frame_name="worker")

    waits = (
        Wait("root/a#0", Condition.child(str(missing_child)), None),
        Wait("root/b#0", Condition.child(str(real_child)), None),
    )
    await worker._suspend(execution, memo, lease, prov, waits)

    pending = await backend.queue.pending("default")
    assert [t.task_id for t in pending] == [f"resume:{eid}:child:{real_child}"]
    assert pending[0].reason == f"child:{real_child}"
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["task_id"] == f"resume:{eid}:child:{real_child}"
    ]
    assert enqueued and enqueued[-1].provenance.actor == Actor.worker("w-1")


async def test_lease_fencing_blocks_recording_execution_suspended(backend, engine, worker):
    """_suspend's write also goes through the guarded _record: a worker fenced out right
    as it suspends must not publish execution.suspended."""

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)
    _fence_ownership_after(backend, calls_before_fencing=1)  # let "started" through, fence "suspended"
    assert await worker.run_once()  # dropped: fenced before the suspended entry could be written

    entries = await engine.journal(eid)
    assert any(s.item.type == EntryType.EXECUTION_STARTED for s in entries)
    assert not any(s.item.type == EntryType.EXECUTION_SUSPENDED for s in entries)
    assert await engine.status(eid) == ExecutionStatus.RUNNING


async def test_broadcast_wakes_every_waiter(backend, engine, worker):
    @engine.workflow("rates", "1")
    async def rates(ctx):
        return (await ctx.receive("rates.eur", scope="global")).payload

    a = await engine.start(rates, by=HUMAN)
    b = await engine.start(rates, by=HUMAN)
    await drain(worker)
    await engine.broadcast("rates.eur", 1.1, by=Actor.system("ecb"))
    assert len(await backend.queue.pending("default")) == 2
    await drain(worker)
    assert await engine.status(a) == ExecutionStatus.COMPLETED
    assert await engine.status(b) == ExecutionStatus.COMPLETED


async def test_broadcast_records_provenance_and_keys_the_resume_by_message(backend, engine, worker):
    """The broadcast message itself must carry the real payload and the caller's
    provenance (tagged frame_name "broadcast"), and the RESUME it fans out must be
    keyed by `message:{channel}:{seq}` -- that's what makes a second broadcast enqueue
    its own resume instead of colliding with (and being dropped as a duplicate of) the
    first one's task id."""

    @engine.workflow("rates", "1")
    async def rates(ctx):
        return (await ctx.receive("rates.eur", scope="global")).payload

    eid = await engine.start(rates, by=HUMAN)
    await drain(worker)  # suspended, registered as a waiter

    by_actor = Actor.system("ecb")
    seq = await engine.broadcast("rates.eur", 1.1, by=by_actor)

    sent = (await backend.channel.read("rates.eur"))[-1]
    assert sent.payload == 1.1
    assert sent.sent_by.actor == by_actor
    assert sent.sent_by.code.frame_name == "broadcast"

    pending = await backend.queue.pending("default")
    assert [t.task_id for t in pending] == [f"resume:{eid}:message:rates.eur:{seq}"]
    assert pending[0].reason == "message:rates.eur"
    assert pending[0].enqueued_by.code.frame_name == "broadcast"


async def test_sleep_schedules_timer_and_resume_after_due(backend, engine, worker):
    @engine.workflow("nap", "1")
    async def nap(ctx):
        await ctx.sleep(timedelta(hours=1))
        return "woke"

    eid = await engine.start(nap, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    timers = list(backend.timers.timers.values())
    assert len(timers) == 1 and timers[0].due_at == T0 + timedelta(hours=1)
    # the sweeper will do this: fire the timer by enqueueing a resume
    backend.clock.advance(timedelta(hours=1, seconds=1))
    await engine.enqueue_resume(
        eid, f"timer:{timers[0].timer_id}", "timer", engine.provenance(HUMAN)
    )
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_retry_with_backoff_suspends_with_timer(backend, engine, worker):
    n = {"c": 0}

    def flaky():
        n["c"] += 1
        if n["c"] == 1:
            raise OSError("x")
        return "ok"

    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(
            flaky, retry=RetryPolicy(max_attempts=2, backoff=timedelta(seconds=30))
        )

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    assert len(backend.timers.timers) == 1
    backend.clock.advance(timedelta(seconds=31))
    await engine.enqueue_resume(eid, "timer:x", "timer", engine.provenance(HUMAN))
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


# --- children --------------------------------------------------------------------------


async def test_child_execution_end_to_end(backend, engine, worker):
    @engine.workflow("double", "1")
    async def double(ctx, x):
        return await ctx.step(lambda: x * 2, name="mul")

    @engine.workflow("parent", "1")
    async def parent(ctx):
        a = await ctx.child(double, 2, key="a")
        b = await ctx.child(double, 5, key="b")
        return a + b

    eid = await engine.start(parent, by=HUMAN)
    assert await worker.run_once()  # parent runs, starts child a, suspends
    child_a = FrameRef(eid, "root/double:a").child_eid
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    assert await engine.status(child_a) == ExecutionStatus.PENDING
    child_exec = await engine.execution(child_a)
    assert child_exec.parent == FrameRef(eid, "root/double:a")
    assert child_exec.created_by.actor == Actor.worker("w-1")

    await drain(worker)  # child a, parent resume, child b, parent resume
    assert await engine.status(child_a) == ExecutionStatus.COMPLETED
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": 14}


async def test_child_failure_propagates_as_child_failed(backend, engine, worker):
    @engine.workflow("bad", "1")
    async def bad(ctx):
        raise NonRetryableError("nope")

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return await ctx.child(bad)

    eid = await engine.start(parent, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.FAILED
    failed = (await engine.journal(eid))[-1].item
    assert failed.payload["error_type"] == "ChildFailed"
    child_eid = FrameRef(eid, "root/bad#0").child_eid
    assert failed.payload["message"] == f"child {child_eid} failed: nope"
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["reason"] == f"child:{child_eid}"
    ]
    assert enqueued and enqueued[-1].provenance.actor == Actor.worker("w-1")


# --- cancel ---------------------------------------------------------------------------


async def test_cancel_suspended_execution(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("never")

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    await engine.cancel(eid, by=HUMAN)
    pending = await backend.queue.pending("default")
    assert [t.task_id for t in pending] == [f"resume:{eid}:cancel"]
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["task_id"] == f"resume:{eid}:cancel"
    ]
    assert enqueued[-1].payload["reason"] == "cancel"
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.CANCELLED
    cancelled = (await engine.journal(eid))[-1].item
    assert cancelled.payload["by"]["actor"]["id"] == "thomas@example.com"
    assert cancelled.payload["by"]["code"]["frame_name"] == "cancel"
    assert control_types(backend)[-1] == "execution.cancelled"


async def test_cancel_before_first_run(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(lambda: 1, name="s")

    eid = await engine.start(w, by=HUMAN)
    await engine.cancel(eid, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.CANCELLED
    assert not any(s.item.type == "frame.started" for s in await engine.journal(eid))


async def test_cancel_raised_mid_execution_is_handled(backend, engine, worker):
    """Cancel can also surface as a `Cancelled` raised out of ctx.run() itself (a
    live frame boundary noticing cancel_requested), not only via the pre-run check.
    _run_owned must handle that outcome the same way: record execution.cancelled,
    notify the parent, and release the lease -- not fall through to a generic fail."""
    captured_leases = []
    original_acquire = backend.ownership.acquire

    async def capturing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)
        captured_leases.append(lease)
        return lease

    backend.ownership.acquire = capturing_acquire

    second_step_ran = {"v": False}

    @engine.workflow("w", "1")
    async def w(ctx):
        async def request_cancel_mid_run():
            await engine.cancel(eid, by=HUMAN)
            # simulate the cooperative write being picked up: refresh this run's
            # own cached lease state, as a real renewal would.
            await captured_leases[0].refresh_state()
            return 1

        await ctx.step(request_cancel_mid_run, name="s1")

        def second():
            second_step_ran["v"] = True
            return 2

        return await ctx.step(second, name="s2")

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    backend.ownership.acquire = original_acquire

    assert second_step_ran["v"] is False  # aborted before the second step
    assert await engine.status(eid) == ExecutionStatus.CANCELLED
    cancelled = (await engine.journal(eid))[-1].item
    assert cancelled.type == EntryType.EXECUTION_CANCELLED
    assert cancelled.payload["by"]["actor"]["id"] == "thomas@example.com"
    assert cancelled.provenance.actor == Actor.worker("w-1")
    assert control_types(backend)[-1] == "execution.cancelled"
    assert backend.ownership.lease_doc(eid).state == {"status": "cancelled"}


async def test_cancel_by_falls_back_to_actor_without_cancel_requested_flag(backend, engine, worker):
    """`by` in _cancel prefers the cancel_requested provenance recorded on the lease
    state, but must fall back to the calling Provenance's own actor when the lease
    carries no such flag -- not silently record a missing `by`."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    execution = await engine.execution(eid)
    memo = await engine.memo(eid)
    lease = await backend.ownership.acquire(eid, "w-1", engine.config.exec_ttl)
    prov = engine.provenance(HUMAN, frame_name="cancel")

    await worker._cancel(execution, memo, lease, prov)

    cancelled = (await engine.journal(eid))[-1].item
    assert cancelled.payload["by"] is not None
    assert cancelled.payload["by"]["id"] == "thomas@example.com"


async def test_lease_fencing_blocks_recording_execution_cancelled(backend, engine, worker):
    """_cancel's write goes through the same guarded _record as every other lifecycle
    entry: a worker fenced out right as it processes a pre-run cancellation must not
    publish execution.cancelled."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    await engine.cancel(eid, by=HUMAN)

    _fence_ownership_after(backend, calls_before_fencing=0)
    assert await worker.run_once()  # dropped: fenced before the cancelled entry could be written

    assert await engine.journal(eid) == []
    assert await engine.status(eid) == ExecutionStatus.PENDING


async def test_child_cancellation_propagates_as_child_failed(backend, engine, worker):
    """Cancelling a suspended child must notify the parent the same way a child failure
    does: notify_parent's body must carry status="cancelled" and error="cancelled",
    stamped with the cancelling worker's own provenance -- so the parent's ctx.child()
    raises ChildFailed with exactly those values."""

    @engine.workflow("stuck", "1")
    async def stuck(ctx):
        await ctx.receive("never")

    @engine.workflow("parent", "1")
    async def parent(ctx):
        return await ctx.child(stuck)

    eid = await engine.start(parent, by=HUMAN)
    await drain(worker)  # parent starts the child and suspends waiting on it
    child_eid = FrameRef(eid, "root/stuck#0").child_eid
    assert await engine.status(child_eid) == ExecutionStatus.SUSPENDED

    await engine.cancel(child_eid, by=HUMAN)
    await drain(worker)

    assert await engine.status(eid) == ExecutionStatus.FAILED
    failed = (await engine.journal(eid))[-1].item
    assert failed.payload["error_type"] == "ChildFailed"
    assert failed.payload["message"] == f"child {child_eid} cancelled: cancelled"
    enqueued = [
        s.item
        for s in backend.control.entries
        if s.item.type == "task.enqueued" and s.item.payload["reason"] == f"child:{child_eid}"
    ]
    assert enqueued and enqueued[-1].provenance.actor == Actor.worker("w-1")


# --- nondeterminism ----------------------------------------------------------------------


async def test_nondeterminism_blocks_execution_for_operator(backend, engine, worker):
    arg = {"v": 1}

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.step(lambda v: v, arg["v"], name="s")
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    arg["v"] = 2
    await engine.signal(eid, "x", None, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED
    last = (await engine.journal(eid))[-1].item
    assert last.payload == {"on": ["operator"]}
    assert last.provenance.actor == Actor.worker("w-1")
    assert backend.ownership.lease_doc(eid).state == {
        "status": "suspended",
        "blocked": "nondeterminism",
    }


async def test_lease_fencing_blocks_recording_nondeterminism_block(backend, engine, worker):
    """Like every lifecycle entry, the nondeterminism-blocked suspend is only written
    after a guarded refresh of the lease: a fenced worker must not publish it."""
    arg = {"v": 1}

    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.step(lambda v: v, arg["v"], name="s")
        await ctx.receive("x")

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    arg["v"] = 2
    await engine.signal(eid, "x", None, by=HUMAN)
    before = len(await engine.journal(eid))

    _fence_ownership_after(backend, calls_before_fencing=1)  # let "resumed" through, not "blocked"
    assert await worker.run_once()  # dropped: fenced before the blocked entry could be written

    # "resumed" is legitimately recorded (that record wasn't fenced); the nondeterminism
    # block itself must not be -- so exactly one new entry, and no second "on": [...] block.
    entries = await engine.journal(eid)
    assert len(entries) == before + 1
    assert entries[-1].item.type == EntryType.EXECUTION_RESUMED
    assert await engine.status(eid) == ExecutionStatus.RUNNING


# --- ownership and unknown workflows ----------------------------------------------------------


async def test_lease_fencing_blocks_recording_execution_started(backend, engine, worker):
    """_record refreshes the lease before writing (see its docstring): a worker fenced
    out right after acquiring must not publish execution.started at all."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    _fence_ownership_after(backend, calls_before_fencing=0)
    assert await worker.run_once()  # dropped: fenced before the started entry could be written
    assert await engine.status(eid) == ExecutionStatus.PENDING
    assert await engine.journal(eid) == []


async def test_lease_fencing_blocks_recording_execution_resumed(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        await ctx.receive("go")
        return 1

    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    before = len(await engine.journal(eid))
    await engine.signal(eid, "go", None, by=HUMAN)

    _fence_ownership_after(backend, calls_before_fencing=0)
    assert await worker.run_once()  # dropped: fenced before the resumed entry could be written

    assert len(await engine.journal(eid)) == before
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED


async def test_lease_fencing_blocks_recording_execution_failed(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return await ctx.step(lambda: (_ for _ in ()).throw(ValueError("bad")), name="boom")

    eid = await engine.start(w, by=HUMAN)
    _fence_ownership_after(backend, calls_before_fencing=1)  # let "started" through, fence "failed"
    assert await worker.run_once()  # dropped: fenced before the failed entry could be written

    entries = await engine.journal(eid)
    assert any(s.item.type == EntryType.EXECUTION_STARTED for s in entries)
    assert not any(s.item.type == EntryType.EXECUTION_FAILED for s in entries)
    assert await engine.status(eid) == ExecutionStatus.RUNNING


def test_renew_loop_init_sets_interval_and_callback():
    """03-ports.md, 05-protocols.md and 10-agent-runner.md all pin the renew cadence at
    exactly ttl/3 -- and on_lost must be the exact callback passed in, since _run calls
    it by identity on LeaseLost."""
    from flowli.runtime.worker import _RenewLoop

    def on_lost() -> None:
        pass

    loop = _RenewLoop(lease=None, ttl=9.0, on_lost=on_lost)
    assert loop._interval == 3.0
    assert loop._on_lost is on_lost
    assert loop.lost is False


async def test_lease_renewal_failure_aborts_the_run(backend):
    """If the exec lease can't be renewed mid-run, _run_owned must abort the run
    (raise LeaseLost) rather than let ctx.run() continue to completion under an
    expired lease -- which would risk another worker double-processing the same
    execution. This exercises the _RenewLoop(..., self._cancel_current) wiring."""
    engine = Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(exec_ttl=0.05, poll_interval=0.01),
    )
    worker = engine.worker()

    renew_calls = {"n": 0}
    original_acquire = backend.ownership.acquire

    async def failing_acquire(*a, **k):
        lease = await original_acquire(*a, **k)

        async def renew():
            renew_calls["n"] += 1
            raise LeaseLost("fenced mid-run")

        lease.renew = renew
        return lease

    backend.ownership.acquire = failing_acquire

    second_step_ran = {"v": False}

    import asyncio

    @engine.workflow("w", "1")
    async def w(ctx):
        async def first():
            await asyncio.sleep(0.3)  # give the renew loop time to fire
            return 1

        await ctx.step(first, name="s1")

        def second():
            second_step_ran["v"] = True
            return 2

        return await ctx.step(second, name="s2")

    eid = await engine.start(w, by=HUMAN)
    assert await worker.run_once()  # dropped: lease lost mid-run, not completed
    backend.ownership.acquire = original_acquire

    assert renew_calls["n"] >= 1
    assert second_step_ran["v"] is False
    assert await engine.status(eid) == ExecutionStatus.RUNNING


async def test_task_dropped_when_another_worker_owns_execution(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    other = await backend.ownership.acquire(eid, "w-9", ttl=120)
    assert await worker.run_once()
    assert await backend.queue.pending("default") == []  # acked, dropped
    assert await engine.status(eid) == ExecutionStatus.PENDING
    await other.release()


async def test_unknown_workflow_is_nacked(backend, engine, worker):
    async def w(ctx):
        return 1

    engine.registry.register(
        __import__("flowli.runtime", fromlist=["WorkflowRef"]).WorkflowRef("w", "1", w)
    )
    eid = await engine.start(w, by=HUMAN)
    bare = Engine(
        backend.ports, Site("h", 2, "w-2"), clock=backend.clock, config=EngineConfig(nack_delay=5)
    )
    assert await bare.worker().run_once()
    assert (await backend.queue.pending("default"))[0].task_id == f"start:{eid}"
    assert await backend.queue.dequeue("default", "w-2", 60) is None  # delayed
    backend.clock.advance(timedelta(seconds=6))
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED


async def test_run_forever_stops(engine, worker):
    import asyncio

    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.05)
        stop.set()

    await asyncio.gather(worker.run_forever(stop), stopper())


async def test_run_forever_backs_off_after_a_failed_iteration(worker):
    """A failed iteration is treated like an idle one (busy=False): run_forever must
    wait on `stop` before trying again, not skip straight past it. Deterministic, not
    timing-based: `stop` is already set by the time the wait would happen, so whether
    the wait call itself happens at all is the signal, independent of poll_interval."""
    import asyncio

    stop = asyncio.Event()
    original_wait = stop.wait
    wait_calls = {"n": 0}

    async def counting_wait():
        wait_calls["n"] += 1
        return await original_wait()

    stop.wait = counting_wait

    async def raising_run_once():
        stop.set()
        raise RuntimeError("boom")

    worker.run_once = raising_run_once

    await asyncio.wait_for(worker.run_forever(stop), timeout=5)
    assert wait_calls["n"] == 1


async def test_run_forever_waits_only_when_idle_not_when_busy(worker):
    """The wait belongs to the idle branch (`if not busy`) -- a task actually processed
    must loop straight back to dequeue the next one, not wait first. Deterministic:
    `stop` is already set by the time either branch would run, so whether the wait
    call itself happens tells us which branch was taken."""
    import asyncio

    stop = asyncio.Event()
    original_wait = stop.wait
    wait_calls = {"n": 0}

    async def counting_wait():
        wait_calls["n"] += 1
        return await original_wait()

    stop.wait = counting_wait

    async def idle_run_once():
        stop.set()
        return False  # idle: not busy

    worker.run_once = idle_run_once

    await asyncio.wait_for(worker.run_forever(stop), timeout=5)
    assert wait_calls["n"] == 1


async def test_run_forever_idle_wait_times_out_on_its_own(worker):
    """The idle wait must time out on its own (poll_interval) and loop back to poll
    again -- not block on `stop` forever, or the worker would stop noticing new tasks
    once idle with nothing external ever signalling `stop`. Bounded by an outer
    timeout so a regression here fails fast instead of hanging."""
    import asyncio

    calls = {"n": 0}
    stop = asyncio.Event()

    async def counting_run_once():
        calls["n"] += 1
        if calls["n"] >= 3:
            stop.set()
        return False  # idle every time

    worker.run_once = counting_run_once

    await asyncio.wait_for(worker.run_forever(stop), timeout=2)
    assert calls["n"] >= 3


# --- detached step -------------------------------------------------------------------------


def detached_add(a, b):
    return a + b


async def test_run_step_task(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/add:1"
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_add", "args": [2, 3], "name": "add"},
        )
    )
    await drain(worker)
    memo = await engine.memo(eid)
    assert memo.memos[fid].value == 5
    msgs = await backend.channel.read(FrameRef(eid, fid).step_channel)
    assert msgs[0].payload == {"status": "completed", "value": 5}


class DetachedNamespace:
    """A step function reached through a dotted qualname (module:Class.method), not
    just a bare top-level name -- _resolve must walk every "." segment of qualname."""

    @staticmethod
    def add(a, b):
        return a + b


async def test_run_step_task_resolves_a_dotted_qualname(backend, engine, worker):
    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/add:1"
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:DetachedNamespace.add", "args": [2, 3], "name": "add"},
        )
    )
    await drain(worker)
    memo = await engine.memo(eid)
    assert memo.memos[fid].value == 5


def detached_boom(a, b):
    raise ValueError(f"boom:{a}:{b}")


async def detached_greet(*, greeting="hi"):
    return greeting


async def _run_until_step_reports(backend, worker, step_channel):
    """Advance the worker one task at a time until the detached step's own outcome is
    on its channel, and stop there -- leaving any task the step just enqueued (its
    RESUME) untouched, for the caller to inspect."""
    while not await backend.channel.read(step_channel):
        assert await worker.run_once(), "worker queue emptied before the step ran"


async def test_run_step_task_records_frame_provenance_digest_and_resume(backend, engine, worker):
    """frame.started/frame.completed, the sent message, and the RESUME task the step
    enqueues all carry the worker's own provenance and the step's real fid/args --
    nothing but the memoized value and the raw message body was asserted before."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/add:1"
    step_channel = FrameRef(eid, fid).step_channel
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_add", "args": [2, 3], "name": "add"},
        )
    )
    await _run_until_step_reports(backend, worker, step_channel)

    entries = await backend.journal.read(eid)
    started = next(s.item for s in entries if s.item.type == EntryType.FRAME_STARTED)
    completed = next(s.item for s in entries if s.item.type == EntryType.FRAME_COMPLETED)
    assert started.fid == fid and completed.fid == fid
    assert started.payload == {
        "kind": FrameKind.STEP.value,
        "name": "add",
        "args_digest": digest({"args": [2, 3], "kwargs": {}}),
        "attempt": 1,
    }
    assert completed.payload == {"attempt": 1, "value": 5}
    for entry in (started, completed):
        assert entry.provenance.actor == worker.actor
        assert entry.provenance.code.workflow == "w" and entry.provenance.code.version == "1"
        assert entry.provenance.code.frame_kind == FrameKind.STEP.value
        assert entry.provenance.code.frame_name == "add"
        assert entry.provenance.attempt == 1

    msgs = await backend.channel.read(step_channel)
    assert msgs[0].sent_by.actor == worker.actor

    resumed = [t for t in await backend.queue.pending("default") if t.kind is TaskKind.RESUME]
    assert len(resumed) == 1
    assert resumed[0].reason == f"step:{fid}"
    assert resumed[0].key == f"step:{fid}"
    assert resumed[0].enqueued_by.actor == worker.actor


async def test_run_step_task_defaults_name_and_args_and_awaits_the_result(backend, engine, worker):
    """A payload without `name` or `args` falls back to `fn.__name__` and `[]`, an
    awaitable result is awaited, and `kwargs` reaches the call and the recorded digest."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/greet:1"
    step_channel = FrameRef(eid, fid).step_channel
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_greet", "kwargs": {"greeting": "hola"}},
        )
    )
    await _run_until_step_reports(backend, worker, step_channel)

    memo = await engine.memo(eid)
    assert memo.memos[fid].value == "hola"  # kwargs reached the fn and the coroutine was awaited

    started = next(
        s.item for s in await backend.journal.read(eid) if s.item.type == EntryType.FRAME_STARTED
    )
    assert started.payload["name"] == "detached_greet"  # defaulted from fn.__name__
    assert started.payload["args_digest"] == digest({"args": [], "kwargs": {"greeting": "hola"}})

    msgs = await backend.channel.read(step_channel)
    assert msgs[0].payload == {"status": "completed", "value": "hola"}


async def test_run_step_task_records_failure(backend, engine, worker):
    """A raising fn records frame.failed (not frame.completed) with the real error, and
    still reports and resumes -- only the success path was exercised before."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/boom:1"
    step_channel = FrameRef(eid, fid).step_channel
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_boom", "args": [2, 3], "name": "boom"},
        )
    )
    await _run_until_step_reports(backend, worker, step_channel)

    memo = await engine.memo(eid)
    assert fid not in memo.memos
    assert memo.failures[fid][-1] == Failed("ValueError", "boom:2:3", True)

    failed_entry = next(
        s.item for s in await backend.journal.read(eid) if s.item.type == EntryType.FRAME_FAILED
    )
    assert failed_entry.fid == fid
    assert failed_entry.payload["attempt"] == 1
    assert failed_entry.provenance.actor == worker.actor

    msgs = await backend.channel.read(step_channel)
    assert msgs[0].payload == {
        "status": "failed",
        "error": {"error_type": "ValueError", "message": "boom:2:3", "retryable": True},
    }

    resumed = [t for t in await backend.queue.pending("default") if t.kind is TaskKind.RESUME]
    assert len(resumed) == 1


async def test_run_step_task_second_attempt_after_failure_increments_attempt(
    backend, engine, worker
):
    """After a frame.failed, a later RUN_STEP task for the same fid runs as attempt 2 --
    `memo.next_attempt` and the provenance attempt it feeds were never checked before."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/flaky:1"
    step_channel = FrameRef(eid, fid).step_channel
    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_boom", "args": [1, 1], "name": "flaky"},
        )
    )
    await _run_until_step_reports(backend, worker, step_channel)

    await engine.enqueue(
        Task(
            queue="default",
            kind=TaskKind.RUN_STEP,
            target=FrameRef(eid, fid),
            reason="delegate",
            enqueued_by=engine.provenance(HUMAN),
            key=fid,
            payload={"fn": f"{__name__}:detached_add", "args": [4, 5], "name": "flaky"},
        )
    )
    while fid not in (await engine.memo(eid)).memos:
        assert await worker.run_once(), "worker queue emptied before the retry ran"

    memo = await engine.memo(eid)
    assert memo.memos[fid].value == 9

    started = [
        s.item
        for s in await backend.journal.read(eid)
        if s.item.type == EntryType.FRAME_STARTED and s.item.fid == fid
    ]
    assert len(started) == 2
    assert started[1].payload["attempt"] == 2
    assert started[1].provenance.attempt == 2


async def test_run_step_task_skips_a_redelivered_task_once_memoized(backend, engine, worker):
    """`run_detached_step` is called again with the very same task on an at-least-once
    redelivery; once the frame is memoized it must return without re-running `fn` or
    appending another frame.started/frame.completed pair."""

    @engine.workflow("w", "1")
    async def w(ctx):
        return 1

    eid = await engine.start(w, by=HUMAN)
    fid = "root/add:1"
    step_channel = FrameRef(eid, fid).step_channel
    task = Task(
        queue="default",
        kind=TaskKind.RUN_STEP,
        target=FrameRef(eid, fid),
        reason="delegate",
        enqueued_by=engine.provenance(HUMAN),
        key=fid,
        payload={"fn": f"{__name__}:detached_add", "args": [2, 3], "name": "add"},
    )
    await engine.enqueue(task)
    await _run_until_step_reports(backend, worker, step_channel)
    entries_after_first_run = len(await backend.journal.read(eid))

    result = await worker.run_detached_step(task)

    assert result == Done()
    assert len(await backend.journal.read(eid)) == entries_after_first_run
