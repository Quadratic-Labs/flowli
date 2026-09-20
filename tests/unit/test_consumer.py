"""The consumer of a delegate task. Spec 10 sections 2 to 5."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from cairndb import Timestamp

from flowlet.adapters.memory import ManualClock, MemoryBackend
from flowlet.domain import Actor, ExecutionStatus, Site
from flowlet.patterns import delegate
from flowlet.runtime import Consumer, ConsumerConfig, Engine, Held, Refused
from flowlet.runtime import consumer as consumer_module
from flowlet.runtime.consumer import request_cancel

T0 = Timestamp(datetime(2026, 9, 11, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


@pytest.fixture
def backend(clock) -> MemoryBackend:
    return MemoryBackend(clock=clock)


@pytest.fixture
def engine(backend) -> Engine:
    engine = Engine(backend.ports, Site.local("w-1"), clock=backend.clock)

    @engine.workflow("delegating", "1")
    async def delegating(ctx, intent):
        reply = await delegate(ctx, "agents", {"intent": intent}, timeout=timedelta(hours=1))
        return "none" if reply is None else reply.payload["outcome"]

    return engine


async def drain(engine):
    worker = engine.worker(queues=["default"])
    n = 0
    while await worker.run_once():
        n += 1
        assert n < 30
    return n


class FakeAgent:
    """A handler whose work finishes when the test says so."""

    def __init__(self, *, finish_at=1, payload=None):
        self.finish_at = finish_at
        self.payload = payload or {"outcome": "done"}
        self.polls = 0
        self.started: list[str] = []
        self.reattached: list[dict] = []
        self.terminated: list[dict] = []
        self.released: list[str] = []
        self.interrupted = False
        self.refuse = False

    async def prepare(self, held: Held) -> None:
        if self.refuse:
            raise Refused("scopes are held elsewhere", timedelta(seconds=1))

    async def start(self, held: Held):
        self.started.append(held.task_id)
        return {"kind": "fake", "pid": 4242}

    async def reattach(self, held: Held, handle):
        self.reattached.append(handle)
        return handle

    async def poll(self, held: Held, handle):
        self.polls += 1
        return self.payload if self.polls >= self.finish_at else None

    async def interrupt(self, held: Held, handle, *, grace):
        self.interrupted = True
        return {"outcome": "interrupted", "grace": grace}

    async def terminate(self, held: Held, handle) -> None:
        self.terminated.append(handle)

    async def release(self, held: Held) -> None:
        self.released.append(held.task_id)


class RecordingLog:
    """Stands in for the module's `log`, so a test can assert on one call
    without configuring structlog's rendering pipeline."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def info(self, event, **kwargs):
        self.calls.append((event, kwargs))

    def warning(self, event, **kwargs):
        self.calls.append((event, kwargs))

    def exception(self, event, **kwargs):
        self.calls.append((event, kwargs))


def consumer(engine, handler, **kwargs) -> Consumer:
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=0.3, **kwargs)
    return Consumer(engine, handler, config)


async def start_and_delegate(engine) -> str:
    eid = await engine.start(engine.registry.get("delegating", "1").fn, "fix it", by=HUMAN)
    await drain(engine)
    return eid


# --- construction ------------------------------------------------------------


async def test_a_consumer_defaults_its_actor_to_the_configured_holder(engine):
    """No explicit `actor=` must still identify replies as this holder, or
    provenance for a delivered reply names nobody in particular."""
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=0.3)
    c = Consumer(engine, FakeAgent(), config)
    assert c.actor == Actor.worker("runner-a")


# --- the loop --------------------------------------------------------------


async def test_a_consumer_answers_the_frame_that_waits(engine, backend):
    eid = await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    agent = FakeAgent(payload={"outcome": "merged"})

    c = consumer(engine, agent)
    assert await c.run_once() is True

    await drain(engine)
    assert await engine.status(eid) is ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": "merged"}
    # Delivered, then acked: the queue is empty and the work was released.
    assert await backend.queue.pending("agents") == []
    assert agent.released
    assert c.report.delivered == [task.task_id]


async def test_run_once_binds_the_task_queue_and_eid_for_everything_it_logs(engine, backend):
    """`run_once`'s own `bound(...)` (distinct from `recover()`'s) must
    correlate every log with this task's id, its queue, and the eid of the
    execution that delegated it."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    captured = {}

    class Capturing(FakeAgent):
        async def poll(self, held, handle):
            captured["eid"] = str(held.task.eid)
            captured["ctx"] = structlog.contextvars.get_contextvars()
            return await super().poll(held, handle)

    await consumer(engine, Capturing()).run_once()

    assert captured["ctx"].get("task_id") == task.task_id
    assert captured["ctx"].get("queue") == "agents"
    assert captured["ctx"].get("eid") == captured["eid"]


async def test_an_empty_queue_is_not_busy(engine):
    assert await consumer(engine, FakeAgent()).run_once() is False


async def test_a_task_of_the_engine_goes_back_on_its_queue(engine, backend):
    """A start, a resume or a step belongs to a worker, not to a consumer."""
    await engine.start(engine.registry.get("delegating", "1").fn, "x", by=HUMAN)
    config = ConsumerConfig(queues=("default",), holder="runner-a", ttl=1.0)
    assert await Consumer(engine, FakeAgent(), config).run_once() is False
    assert len(await backend.queue.pending("default")) == 1


async def test_dequeue_uses_the_configured_holder(engine, backend, monkeypatch):
    """The holder recorded on the acquired lease must be this consumer's own
    (spec 10 section 4.3): a restart attaches by that same holder name."""
    await start_and_delegate(engine)
    seen_holders = []
    original_dequeue = backend.queue.dequeue

    async def spy_dequeue(queue, holder, ttl):
        seen_holders.append(holder)
        return await original_dequeue(queue, holder, ttl)

    monkeypatch.setattr(backend.queue, "dequeue", spy_dequeue)

    await consumer(engine, FakeAgent()).run_once()

    assert seen_holders == ["runner-a"]  # the `consumer()` fixture's ConsumerConfig.holder


async def test_dequeue_tries_the_next_queue_when_one_is_empty(engine, backend):
    """A configured queue with nothing on it must not stop the search: the
    next queue in `config.queues` still gets a look (spec 10 section 11)."""
    await start_and_delegate(engine)
    agent = FakeAgent()
    config = ConsumerConfig(queues=("empty", "agents"), holder="runner-a", ttl=0.3)

    assert await Consumer(engine, agent, config).run_once() is True

    assert agent.started


async def test_dequeue_tries_the_next_queue_after_nacking_an_engine_task(engine, backend):
    """A non-delegate task on one configured queue is nacked and the search
    for a delegate task continues into the next queue within the same call,
    not abandoned (spec 10 section 11)."""
    await start_and_delegate(engine)  # a delegate task now sits on "agents"
    # A second execution's own START task, left undrained, sits on "default".
    await engine.start(engine.registry.get("delegating", "1").fn, "y", by=HUMAN)
    (start_task,) = await backend.queue.pending("default")
    agent = FakeAgent()
    config = ConsumerConfig(queues=("default", "agents"), holder="runner-a", ttl=0.3)

    assert await Consumer(engine, agent, config).run_once() is True

    assert agent.started  # the delegate task on "agents" was reached and served
    # The start task was nacked back onto "default", not lost or served here.
    default_ids = [t.task_id for t in await backend.queue.pending("default")]
    assert start_task.task_id in default_ids


async def test_a_refusal_returns_the_task(engine, backend):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    agent = FakeAgent()
    agent.refuse = True

    c = consumer(engine, agent)
    await c.run_once()

    assert not agent.started
    (pending,) = await backend.queue.pending("agents")  # still there, for another pass
    # The refusal's own delay (1s) wins over the config's default conflict
    # delay (10s): `Refused.delay or config.conflict_delay`, not `and`.
    assert pending.not_before == T0 + timedelta(seconds=1)
    assert c.report.refused == [task.task_id]


async def test_a_refusal_whose_nack_is_fenced_is_suppressed(engine, backend, clock):
    """A refusal must not blow up even when the lease was stolen out from
    under it before the nack could land -- the thief owns the task now, so
    giving it back is not this consumer's job any more."""
    await start_and_delegate(engine)

    class Stealing(FakeAgent):
        async def prepare(self, held):
            clock.advance(timedelta(seconds=1))  # this holder's ttl (0.3s) expires
            await backend.queue.dequeue("agents", "runner-b", 60)  # a thief steals it
            raise Refused("scopes are held elsewhere")

    c = consumer(engine, Stealing())

    await c.run_once()  # must not raise

    assert c.report.refused  # gave up on it, one way or another


async def test_task_refused_is_logged_with_task_id_and_reason(engine, backend, monkeypatch):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    agent = FakeAgent()
    agent.refuse = True

    await consumer(engine, agent).run_once()

    assert (
        "task_refused",
        {"task_id": task.task_id, "reason": "scopes are held elsewhere"},
    ) in log.calls


# --- ownership of a long attempt -------------------------------------------


async def test_the_handle_is_written_before_the_work_can_outlive_us(engine, backend):
    """Spec 10 section 4.2: the address of the work goes into the lease state."""
    await start_and_delegate(engine)
    seen = {}

    class Watcher(FakeAgent):
        async def poll(self, held, handle):
            seen["handle"] = held.handle  # what a restart or a thief would find
            seen["poll_handle"] = handle  # what the watch loop itself was given
            return await super().poll(held, handle)

    await consumer(engine, Watcher()).run_once()
    assert seen["handle"] == {"kind": "fake", "pid": 4242}
    assert seen["poll_handle"] == {"kind": "fake", "pid": 4242}


async def test_a_restart_attaches_to_what_it_still_holds(engine, backend):
    """Spec 10 section 4.3: same holder, same epoch, its own state intact."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    claimed = await backend.queue.dequeue("agents", "runner-a", 60)
    await claimed.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 7}})
    captured = {}

    class Capturing(FakeAgent):
        async def reattach(self, held, handle):
            captured["held"] = held
            return await super().reattach(held, handle)

    agent = Capturing()
    taken = await consumer(engine, agent).recover()

    assert taken == [task.task_id]
    assert agent.reattached == [{"kind": "fake", "pid": 7}]
    assert not agent.started  # it re-attached, it did not start a second agent
    # Recovery is the same period of ownership continuing, never a steal.
    assert captured["held"].adopted is False


async def test_recovery_ignores_a_task_held_by_someone_else(engine, backend):
    await start_and_delegate(engine)
    await backend.queue.dequeue("agents", "runner-b", 60)

    assert await consumer(engine, FakeAgent()).recover() == []


async def test_recovery_continues_past_a_task_it_cannot_attach_to(engine, backend):
    """One task held by someone else must not stop recovery of the rest of
    the queue (spec 10 section 4.3 recovers every task this holder still
    owns, not just the first one it looks at)."""
    await start_and_delegate(engine)
    await start_and_delegate(engine)
    tasks = await backend.queue.pending("agents")
    assert len(tasks) == 2
    await backend.queue.dequeue("agents", "runner-x", 60)  # takes the oldest
    await backend.queue.dequeue("agents", "runner-a", 60)  # takes the other one

    taken = await consumer(engine, FakeAgent()).recover()

    assert taken == [tasks[1].task_id]


async def test_recover_reports_the_tasks_it_recovers(engine, backend):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    await backend.queue.dequeue("agents", "runner-a", 60)

    c = consumer(engine, FakeAgent())
    taken = await c.recover()

    assert taken == [task.task_id]
    assert c.report.recovered == [task.task_id]


async def test_recover_uses_the_configured_ttl_to_attach(engine, backend, monkeypatch):
    """The re-acquired lease must be extended by this consumer's own ttl."""
    await start_and_delegate(engine)
    await backend.queue.dequeue("agents", "runner-a", 60)
    seen_ttls = []
    original_attach = backend.queue.attach

    async def spy_attach(queue, task_id, holder, ttl):
        seen_ttls.append(ttl)
        return await original_attach(queue, task_id, holder, ttl)

    monkeypatch.setattr(backend.queue, "attach", spy_attach)

    await consumer(engine, FakeAgent()).recover()

    assert seen_ttls == [0.3]  # the `consumer()` fixture's ConsumerConfig.ttl


async def test_task_recovered_is_logged_with_task_id_and_epoch(engine, backend, monkeypatch):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    claimed = await backend.queue.dequeue("agents", "runner-a", 60)
    epoch = claimed.lease.epoch  # attach does not bump it (section 4.3)

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    await consumer(engine, FakeAgent()).recover()

    assert ("task_recovered", {"task_id": task.task_id, "epoch": epoch}) in log.calls


async def test_recovery_binds_the_task_and_queue_for_everything_it_logs(engine, backend):
    """Spec 10 section 4.3: a log line from deep inside recovery must still
    be correlated with its own task and queue."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    claimed = await backend.queue.dequeue("agents", "runner-a", 60)
    await claimed.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 7}})
    captured = {}

    class Capturing(FakeAgent):
        async def reattach(self, held, handle):
            captured["ctx"] = structlog.contextvars.get_contextvars()
            return await super().reattach(held, handle)

    await consumer(engine, Capturing()).recover()

    assert captured["ctx"]["task_id"] == task.task_id
    assert captured["ctx"]["queue"] == "agents"


async def test_a_thief_inherits_the_handle_and_adopts_the_session(engine, backend, clock):
    """Spec 10 section 4.4: an acquisition preserves the state of the document."""
    await start_and_delegate(engine)
    dead = await backend.queue.dequeue("agents", "runner-a", 1)
    await dead.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 99}})
    clock.advance(timedelta(seconds=2))  # the lease of runner-a expires
    captured = {}

    class Capturing(FakeAgent):
        async def reattach(self, held, handle):
            captured["held"] = held
            return await super().reattach(held, handle)

    agent = Capturing()
    config = ConsumerConfig(queues=("agents",), holder="runner-b", ttl=0.3)
    await Consumer(engine, agent, config).run_once()

    assert agent.reattached == [{"kind": "fake", "pid": 99}]
    assert not agent.started
    assert captured["held"].adopted is True  # a steal, not a restart of its own attempt


async def test_no_handle_means_no_adoption(engine, backend):
    """Adoption is a property of a steal (section 4.4), never of a fresh
    start where nothing was inherited."""
    await start_and_delegate(engine)
    captured = {}

    class Capturing(FakeAgent):
        async def start(self, held):
            captured["held"] = held
            return await super().start(held)

    await consumer(engine, Capturing()).run_once()
    assert captured["held"].adopted is False


async def test_prepare_is_given_the_lease_this_consumer_holds(engine, backend):
    """`prepare` acts on this attempt's own `Held`, never a stand-in."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    captured = {}

    class Capturing(FakeAgent):
        async def prepare(self, held):
            captured["held"] = held
            await super().prepare(held)

    await consumer(engine, Capturing()).run_once()
    assert captured["held"].task_id == task.task_id


async def test_a_thief_that_cannot_adopt_must_terminate_what_it_inherited(engine, backend, clock):
    """Otherwise a hosted session runs and bills forever, with nobody to read it."""
    await start_and_delegate(engine)
    dead = await backend.queue.dequeue("agents", "runner-a", 1)
    await dead.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 99}})
    clock.advance(timedelta(seconds=2))

    class Orphaned(FakeAgent):
        async def reattach(self, held, handle):
            return None  # the session is gone, or is not ours to take

    agent = Orphaned()
    config = ConsumerConfig(queues=("agents",), holder="runner-b", ttl=0.3)
    await Consumer(engine, agent, config).run_once()

    assert agent.terminated == [{"kind": "fake", "pid": 99}]
    assert agent.started  # and only then a new attempt


async def test_terminate_receives_the_held_task_it_belongs_to(engine, backend, clock):
    """Spec 10 section 4.4: the handler's own `terminate` must see the exact
    `Held` this attempt was given, not a stand-in."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    dead = await backend.queue.dequeue("agents", "runner-a", 1)
    await dead.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 99}})
    clock.advance(timedelta(seconds=2))
    captured = {}

    class Orphaned(FakeAgent):
        async def reattach(self, held, handle):
            return None  # the session is gone, or is not ours to take

        async def terminate(self, held, handle) -> None:
            captured["held"] = held
            await super().terminate(held, handle)

    config = ConsumerConfig(queues=("agents",), holder="runner-b", ttl=0.3)
    await Consumer(engine, Orphaned(), config).run_once()

    assert captured["held"].task_id == task.task_id


async def test_terminate_suppresses_a_handler_that_raises(engine, backend):
    """`terminate` promises never to raise (the `Handler` protocol says so),
    but `_terminate` must not depend on that promise being kept."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    class Explosive(FakeAgent):
        async def poll(self, held, handle):
            raise RuntimeError("the agent host went away")

        async def terminate(self, held, handle) -> None:
            self.terminated.append(handle)
            raise RuntimeError("terminate blew up too")

    agent = Explosive()
    c = consumer(engine, agent)

    await c.run_once()  # must not raise, even though terminate did

    assert agent.terminated == [{"kind": "fake", "pid": 4242}]
    assert c.report.terminated == [task.task_id]


async def test_work_terminated_is_logged_with_the_task_id(engine, backend, clock, monkeypatch):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    dead = await backend.queue.dequeue("agents", "runner-a", 1)
    await dead.lease.update_state(lambda s: {**(s or {}), "handle": {"kind": "fake", "pid": 99}})
    clock.advance(timedelta(seconds=2))

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    class Orphaned(FakeAgent):
        async def reattach(self, held, handle):
            return None  # the session is gone, or is not ours to take

    config = ConsumerConfig(queues=("agents",), holder="runner-b", ttl=0.3)
    await Consumer(engine, Orphaned(), config).run_once()

    assert ("work_terminated", {"task_id": task.task_id}) in log.calls


# --- the heartbeat ----------------------------------------------------------


async def test_the_heartbeat_interval_is_ttl_over_three_with_a_floor(engine, backend, monkeypatch):
    """The `consumer()` fixture's ttl (0.3s) gives ttl/3 = 0.1s, above the
    0.05s floor -- so the sleep between polls is exactly that."""
    await start_and_delegate(engine)
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 5:  # a loop that never sees a non-None poll must not spin forever
            raise AssertionError("the watch loop slept far more than the one expected heartbeat")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    agent = FakeAgent(finish_at=2)  # None on the first poll, so the loop sleeps once
    await consumer(engine, agent).run_once()

    assert sleeps == [pytest.approx(0.1)]


async def test_the_heartbeat_sleep_is_capped_at_one_second(engine, backend, monkeypatch):
    """A ttl large enough to put ttl/3 above 1s must still sleep 1s at a time."""
    await start_and_delegate(engine)
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 5:  # a loop that never sees a non-None poll must not spin forever
            raise AssertionError("the watch loop slept far more than the one expected heartbeat")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    agent = FakeAgent(finish_at=2)
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=4.5)  # ttl/3 = 1.5s
    await Consumer(engine, agent, config).run_once()

    assert sleeps == [pytest.approx(1.0)]


async def test_the_watch_loop_waits_a_full_interval_before_its_first_heartbeat(
    engine, backend, monkeypatch
):
    """Elapsed time starts at zero and accumulates in 1s steps: with a 3s
    interval, a cancel that arrives after the first cycle is only seen once
    a second full cycle has accumulated -- three 1s sleeps each time, not
    fewer."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    sleeps = 0

    async def fake_sleep(seconds):
        nonlocal sleeps
        sleeps += 1
        # A loop whose poll is never reached (or never seen) must not spin forever.
        if sleeps > 12:
            raise AssertionError("the watch loop never reached a second heartbeat cycle")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    class DelayedCancel(FakeAgent):
        async def poll(self, held, handle):
            self.polls += 1
            if self.polls > 3:
                # The cancel lands only after the first cycle would already
                # have completed, so a second cycle must run to see it.
                await request_cancel(engine, "agents", task.task_id, HUMAN)
            return None

    agent = DelayedCancel()
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=9.0)  # interval = 3s
    await Consumer(engine, agent, config).run_once()

    assert agent.interrupted
    assert agent.polls == 6  # two full 3-second cycles, heartbeating every 1s


async def test_cancel_seen_is_logged_with_the_task_id(engine, backend, monkeypatch):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    class Cancellable(FakeAgent):
        async def poll(self, held, handle):
            await request_cancel(engine, "agents", task.task_id, HUMAN)
            return None

    await consumer(engine, Cancellable()).run_once()

    assert ("cancel_seen", {"task_id": task.task_id}) in log.calls


# --- cancel ----------------------------------------------------------------


async def test_a_cancel_reaches_the_consumer_through_its_task_lease(engine, backend):
    """Spec 10 section 5: the consumer reads it on its heartbeat, unfenced."""
    eid = await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    class Cancellable(FakeAgent):
        async def poll(self, held, handle):
            # Someone asks for a stop while the work runs.
            await request_cancel(engine, "agents", task.task_id, HUMAN)
            return None

    agent = Cancellable()
    await consumer(engine, agent).run_once()

    assert agent.interrupted
    await drain(engine)
    # The runner answers even when it is cancelled, so the frame gets a real
    # outcome instead of a timeout.
    assert (await engine.journal(eid))[-1].item.payload == {"value": "interrupted"}


async def test_interrupt_receives_the_held_task_its_handle_and_the_grace_budget(engine, backend):
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")
    captured = {}

    class Cancellable(FakeAgent):
        async def poll(self, held, handle):
            await request_cancel(engine, "agents", task.task_id, HUMAN)
            return None

        async def interrupt(self, held, handle, *, grace):
            captured["held"] = held
            captured["handle"] = handle
            captured["grace"] = grace
            return await super().interrupt(held, handle, grace=grace)

    agent = Cancellable()
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=0.3, grace=42.0)
    await Consumer(engine, agent, config).run_once()

    assert captured["held"].task_id == task.task_id
    assert captured["handle"] == {"kind": "fake", "pid": 4242}
    assert captured["grace"] == 42.0


async def test_the_work_is_released_even_when_it_fails(engine, monkeypatch):
    await start_and_delegate(engine)
    delivered = {}
    original_deliver = engine.deliver

    async def spy_deliver(eid, channel, payload, **kwargs):
        delivered["payload"] = payload
        return await original_deliver(eid, channel, payload, **kwargs)

    monkeypatch.setattr(engine, "deliver", spy_deliver)

    class Broken(FakeAgent):
        async def poll(self, held, handle):
            raise RuntimeError("the agent host went away")

    agent = Broken()
    await consumer(engine, agent).run_once()

    assert agent.released
    assert agent.terminated == [{"kind": "fake", "pid": 4242}]  # the handle it left behind
    assert delivered["payload"] == {
        "outcome": "failed",
        "error": "RuntimeError: the agent host went away",
    }


async def test_reply_delivered_is_logged_with_task_id_and_eid(engine, backend, monkeypatch):
    eid = await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    await consumer(engine, FakeAgent()).run_once()

    assert ("reply_delivered", {"task_id": task.task_id, "eid": str(eid)}) in log.calls


async def test_task_lease_lost_before_ack_is_logged_with_the_task_id(
    engine, backend, clock, monkeypatch
):
    """A thief can steal the lease between delivery and ack: the reply still
    goes out (delivery does not check this lease), but the ack that follows
    finds itself fenced. Nothing crashes; the near-miss is only worth a log
    line naming the task."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    log = RecordingLog()
    monkeypatch.setattr(consumer_module, "log", log)

    class Fenced(FakeAgent):
        async def poll(self, held, handle):
            self.polls += 1
            if self.polls == 1:
                clock.advance(timedelta(seconds=1))  # this holder's ttl (0.3s) expires
                await backend.queue.dequeue("agents", "runner-b", 60)  # a thief steals it
            return self.payload

    await consumer(engine, Fenced()).run_once()

    assert ("task_lease_lost_before_ack", {"task_id": task.task_id}) in log.calls


async def test_a_thief_fences_the_watcher_which_gives_back_cleanly(engine, backend, clock):
    """Spec 10 section 4.4: once another runner has the lease, `renew`
    propagates `LeaseLost`. The fenced consumer delivers nothing, acks
    nothing, and only releases -- it does not know whether the thief adopted
    the handle, so it must not touch it."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    class Fenced(FakeAgent):
        stolen = False

        async def poll(self, held, handle):
            if not self.stolen:
                self.stolen = True
                clock.advance(timedelta(seconds=1))  # this holder's ttl (0.3s) expires
                await backend.queue.dequeue("agents", "runner-b", 60)  # a thief steals it
                return None
            return await super().poll(held, handle)

    agent = Fenced()
    await consumer(engine, agent).run_once()

    assert agent.released == [task.task_id]
    assert agent.terminated == []


async def test_request_cancel_names_its_own_frame_in_the_provenance(engine, backend):
    """The provenance recorded with the cancel names its own frame, the same
    way every other engine-initiated write does (docs/specs/05, provenance) --
    not the engine's own default of "engine"."""
    await start_and_delegate(engine)
    (task,) = await backend.queue.pending("agents")

    await request_cancel(engine, "agents", task.task_id, HUMAN)

    claimed = await backend.queue.dequeue("agents", "runner-a", 60)
    prov = claimed.lease.state["cancel_requested"]
    assert prov["code"]["frame_name"] == "cancel"
