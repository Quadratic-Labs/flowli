import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.codec import digest
from flowli.domain import (
    Actor,
    Cancelled,
    ChildFailed,
    Condition,
    DuplicateFrameError,
    EntryType,
    Execution,
    Failed,
    FrameRef,
    MemoTable,
    Message,
    NondeterminismError,
    NonRetryableError,
    Provenance,
    RetryPolicy,
    Site,
    StepFailed,
    TaskKind,
    Timer,
    execution_channel,
)
from flowli.runtime import Context, Raised, Returned, Suspended, Wait, WorkflowRef
from tests.ids import E_ABC

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
EID = E_ABC


class StubStarter:
    def __init__(self) -> None:
        self.started: list[tuple[Execution, str]] = []

    async def start_child(self, execution: Execution, queue: str) -> None:
        self.started.append((execution, queue))


class Harness:
    """Builds a fresh Context over the same backend for each run, like a worker does."""

    def __init__(self, backend: MemoryBackend, fn, *, registry: dict | None = None):
        self.backend = backend
        self.wf = WorkflowRef("wf", "1", fn)
        self.registry = registry or {}
        self.starter = StubStarter()
        self.cancel = False
        self.last: Context | None = None

    async def run(self, *args, **kwargs):
        memo = MemoTable.build(await self.backend.journal.read(EID))
        ctx = Context(
            eid=EID,
            workflow=self.wf,
            memo=memo,
            ports=self.backend.ports,
            actor=Actor.worker("w-1"),
            site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
            clock=self.backend.clock,
            code_ref="git:abc",
            starter=self.starter,
            workflow_of=lambda fn: self.registry[fn],
            cancel_requested=lambda: self.cancel,
        )
        self.last = ctx
        return await ctx.run(*args, **kwargs)

    async def types(self):
        return [(s.item.type, s.item.fid) for s in await self.backend.journal.read(EID)]


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend(clock=ManualClock(T0))


def prov() -> Provenance:
    from flowli.domain import Code

    return Provenance(Actor.system("test"), Site("h", 1, "ext"), Code("x", "1", "step", "x"), 1, T0)


# --- steps and memo ---------------------------------------------------------------


async def test_steps_run_once_then_replay_from_memo(backend):
    calls = []

    def fetch(x):
        calls.append(x)
        return {"id": x}

    async def score(inv):
        calls.append("score")
        return 0.9

    async def wf(ctx, x):
        inv = await ctx.step(fetch, x)
        s = await ctx.step(score, inv)
        return [inv, s]

    h = Harness(backend, wf)
    assert await h.run("inv-42") == Returned([{"id": "inv-42"}, 0.9])
    assert calls == ["inv-42", "score"]
    assert await h.run("inv-42") == Returned([{"id": "inv-42"}, 0.9])
    assert calls == ["inv-42", "score"]  # nothing ran again
    assert await h.types() == [
        (EntryType.FRAME_STARTED, "root/fetch#0"),
        (EntryType.FRAME_COMPLETED, "root/fetch#0"),
        (EntryType.FRAME_STARTED, "root/score#0"),
        (EntryType.FRAME_COMPLETED, "root/score#0"),
    ]


async def test_frame_entries_carry_provenance_and_digest(backend):
    async def wf(ctx):
        return await ctx.step(lambda a, b: a + b, 1, 2, name="add")

    await Harness(backend, wf).run()
    started = (await backend.journal.read(EID))[0].item
    assert started.payload["kind"] == "step" and started.payload["name"] == "add"
    assert len(started.payload["args_digest"]) == 64
    assert started.provenance.code.workflow == "wf"
    assert started.provenance.code.version == "1"
    assert started.provenance.code.code_ref == "git:abc"
    assert started.provenance.code.frame_name == "add"
    assert started.provenance.site.epoch == 1


async def test_step_passes_through_positional_and_keyword_arguments(backend):
    calls = []

    def fetch(x, *, scope):
        calls.append((x, scope))
        return scope

    async def wf(ctx):
        return await ctx.step(fetch, "inv-1", scope="global")

    h = Harness(backend, wf)
    assert await h.run() == Returned("global")
    assert calls == [("inv-1", "global")]


async def test_step_failure_without_retry_raises_step_failed_on_every_replay(backend):
    calls = []

    def boom():
        calls.append(1)
        raise ValueError("bad")

    async def wf(ctx):
        return await ctx.step(boom)

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert out.error.failed.error_type == "ValueError"
    assert out.error.fid == "root/boom#0"
    assert str(out.error) == (
        f"step {out.error.fid} failed: "
        f"{out.error.failed.error_type}: {out.error.failed.message}"
    )
    out2 = await h.run()
    assert isinstance(out2, Raised) and isinstance(out2.error, StepFailed)
    assert out2.error.fid == "root/boom#0"  # the exhausted-attempts raise still names the frame
    assert calls == [1]


async def test_immediate_retries_then_success(backend):
    n = {"calls": 0}

    def flaky():
        n["calls"] += 1
        if n["calls"] < 3:
            raise OSError("flaky")
        return "ok"

    async def wf(ctx):
        return await ctx.step(flaky, retry=RetryPolicy(max_attempts=3))

    assert await Harness(backend, wf).run() == Returned("ok")
    types = [t for t, _ in await Harness(backend, wf).types()]
    assert types.count(EntryType.FRAME_FAILED) == 2
    assert types.count(EntryType.FRAME_STARTED) == 3
    started = [s.item for s in await backend.journal.read(EID) if s.item.type == "frame.started"]
    assert [e.payload["attempt"] for e in started] == [1, 2, 3]
    completed = [s.item for s in await backend.journal.read(EID) if s.item.type == "frame.completed"]
    assert [e.payload["attempt"] for e in completed] == [3]  # recorded against the winning attempt


async def test_non_retryable_error_stops_retries(backend):
    async def wf(ctx):
        return await ctx.step(
            lambda: (_ for _ in ()).throw(NonRetryableError("no")),
            name="x",
            retry=RetryPolicy(max_attempts=5, backoff=timedelta(seconds=10)),
        )

    out = await Harness(backend, wf).run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert not out.error.failed.retryable

    failed = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_FAILED
    )
    assert failed.payload["attempt"] == 1
    # not retryable: no next attempt is scheduled, even though the policy has backoff
    assert "retry_at" not in failed.payload
    assert failed.provenance.code.frame_name == "x"


async def test_retry_with_backoff_suspends_on_timer_then_resumes(backend):
    n = {"calls": 0}

    def flaky():
        n["calls"] += 1
        if n["calls"] == 1:
            raise OSError("flaky")
        return "ok"

    async def wf(ctx):
        return await ctx.step(
            flaky, retry=RetryPolicy(max_attempts=2, backoff=timedelta(seconds=30))
        )

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended)
    (w,) = out.waits
    assert w.on.startswith("timer:") and w.deadline == T0 + timedelta(seconds=30)
    suspended = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_SUSPENDED
    )
    assert suspended.payload["attempt"] == 1
    assert suspended.provenance.code.frame_name == "flaky"
    # too early: suspends again without new entries
    backend.clock.advance(timedelta(seconds=10))
    before = await backend.journal.tail(EID)
    assert isinstance(await h.run(), Suspended)
    assert await backend.journal.tail(EID) == before
    backend.clock.advance(timedelta(seconds=25))
    assert await h.run() == Returned("ok")
    assert n["calls"] == 2
    types = [t for t, _ in await h.types()]
    assert EntryType.FRAME_FULFILLED in types


async def test_retry_resume_exactly_at_due_time_runs_next_attempt_without_waiting(backend):
    """At the exact due instant the frame is due now, not still waiting (`<`, not `<=`)."""
    n = {"calls": 0}

    def flaky():
        n["calls"] += 1
        if n["calls"] == 1:
            raise OSError("flaky")
        return "ok"

    async def wf(ctx):
        return await ctx.step(
            flaky, retry=RetryPolicy(max_attempts=2, backoff=timedelta(seconds=30))
        )

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    backend.clock.advance(timedelta(seconds=30))  # exactly the due instant, not past it
    assert await h.run() == Returned("ok")
    assert n["calls"] == 2

    fulfilled = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_FULFILLED
    )
    assert fulfilled.fid == "root/flaky#0"
    assert fulfilled.payload["attempt"] == 2
    assert fulfilled.provenance.code.frame_name == "flaky"
    assert fulfilled.provenance.attempt == 2


async def test_retry_backoff_resume_before_suspend_entry_records_attempt_and_provenance(backend):
    """A worker can crash after journalling frame.failed but before frame.suspended -- each

    journal write is its own step (05-protocols.md). Resuming into the retry-at branch with
    no prior suspend entry must still journal the wait with the right attempt and provenance,
    not garbage silently swallowed because the state matched on every other replay.
    """

    async def wf(ctx):
        return await ctx.step(
            lambda: 1 / 0,
            name="s",
            retry=RetryPolicy(max_attempts=2, backoff=timedelta(seconds=30)),
        )

    fid = "root/s#0"
    args_digest = digest({"args": [], "kwargs": {}})
    memo = MemoTable(
        digests={fid: args_digest},
        failures={fid: [Failed("ZeroDivisionError", "division by zero", True)]},
        retry_at={fid: (T0 + timedelta(seconds=30)).to_iso()},
    )
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=backend.ports,
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
        code_ref="git:abc",
    )
    out = await ctx.run()
    assert isinstance(out, Suspended)
    (w,) = out.waits
    assert w.deadline == T0 + timedelta(seconds=30)  # not lost, even though nothing recorded it yet

    suspended = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_SUSPENDED
    )
    assert suspended.payload["attempt"] == 2
    assert suspended.provenance.code.frame_name == "s"
    assert suspended.provenance.attempt == 2


# --- receive / sleep --------------------------------------------------------------


async def test_receive_suspends_then_returns_message_and_consumes_in_order(backend):
    async def wf(ctx):
        a = await ctx.receive("payments")
        b = await ctx.receive("payments")
        return [a.payload, b.payload]

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended)
    (w,) = out.waits
    ch = execution_channel(EID, "payments")
    assert w.on == Condition.channel(ch) and w.deadline is None
    await backend.channel.send(Message(ch, 0, {"n": 1}, prov()))
    out = await h.run()
    assert isinstance(out, Suspended)  # second receive waits
    await backend.channel.send(Message(ch, 0, {"n": 2}, prov()))
    assert await h.run() == Returned([{"n": 1}, {"n": 2}])
    assert await h.run() == Returned([{"n": 1}, {"n": 2}])  # stable on replay


async def test_receive_timeout_returns_none(backend):
    async def wf(ctx):
        m = await ctx.receive("never", timeout=timedelta(minutes=5))
        return m

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended) and out.waits[0].deadline == T0 + timedelta(minutes=5)
    backend.clock.advance(timedelta(minutes=6))
    assert await h.run() == Returned(None)
    assert await h.run() == Returned(None)


async def test_receive_message_available_immediately_records_frame_details(backend):
    async def wf(ctx):
        return await ctx.receive("payments", name="wait-msg")

    full = execution_channel(EID, "payments")
    await backend.channel.send(Message(full, 0, {"n": 1}, prov()))
    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned) and out.value.payload == {"n": 1}

    entries = [s.item for s in await backend.journal.read(EID)]
    started = next(e for e in entries if e.type == EntryType.FRAME_STARTED)
    assert started.payload["kind"] == "receive"
    assert started.payload["name"] == "wait-msg"
    assert started.payload["attempt"] == 1
    assert len(started.payload["args_digest"]) == 64
    assert started.provenance.code.frame_name == "wait-msg"
    assert started.provenance.attempt == 1

    fulfilled = next(e for e in entries if e.type == EntryType.FRAME_FULFILLED)
    assert fulfilled.payload == {"attempt": 1, "on": Condition.channel(full), "message_seq": 1}


async def test_receive_timeout_at_exact_deadline_clears_wait_and_records_details(backend):
    async def wf(ctx):
        return await ctx.receive("never", name="wait-msg", timeout=timedelta(minutes=5))

    full = execution_channel(EID, "never")
    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended)
    fid = out.waits[0].fid
    # mirror what the worker does when a frame suspends: register the channel wait
    await backend.channel.register_wait(full, FrameRef(EID, fid))

    backend.clock.advance(timedelta(minutes=5))  # exactly at the deadline: already due
    assert await h.run() == Returned(None)
    assert await backend.channel.waiters(full) == []  # the registered wait was cleared

    entries = [s.item for s in await backend.journal.read(EID)]
    suspended = next(e for e in entries if e.type == EntryType.FRAME_SUSPENDED)
    assert suspended.payload["attempt"] == 1
    assert suspended.provenance.code.frame_name == "wait-msg"

    fulfilled = next(e for e in entries if e.type == EntryType.FRAME_FULFILLED)
    assert fulfilled.fid == fid
    assert fulfilled.payload == {"attempt": 1, "on": Condition.channel(full), "message_seq": None}
    assert fulfilled.provenance.code.frame_name == "wait-msg"


async def test_receive_changed_timeout_on_replay_raises_nondeterminism(backend):
    state = {"timeout": timedelta(minutes=5)}

    async def wf(ctx):
        return await ctx.receive("never", timeout=state["timeout"])

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    state["timeout"] = timedelta(minutes=10)
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)
    assert out.error.fid == "root/receive#0"


async def test_receive_records_channel_and_timeout_in_digest(backend):
    async def wf(ctx):
        return await ctx.receive("never", timeout=timedelta(minutes=5))

    full = execution_channel(EID, "never")
    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    started = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED
    )
    assert started.payload["args_digest"] == digest({"channel": full, "timeout_s": 300.0})


async def test_receive_replay_raises_when_consumed_message_is_gone(backend):
    """A message the journal says this frame consumed must still be on the channel

    (03-ports.md section 8: `delete_channel` is retention-only, after archiving a
    terminal execution, so a live execution should never see its own messages
    vanish). If it isn't, that's corruption worth failing loudly on.
    """

    async def wf(ctx):
        return (await ctx.receive("payments")).payload

    full = execution_channel(EID, "payments")
    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    await backend.channel.send(Message(full, 0, {"n": 1}, prov()))
    assert await h.run() == Returned({"n": 1})

    await backend.channel.delete_channel(full)  # simulate the consumed message getting lost
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, RuntimeError)
    assert str(out.error) == f"channel {full} lost message 1 consumed by root/receive#0"


async def test_global_scope_receive(backend):
    async def wf(ctx):
        return (await ctx.receive("rates.eur", scope="global")).payload

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    await backend.channel.send(Message("rates.eur", 0, 1.1, prov()))
    assert await h.run() == Returned(1.1)


async def test_sleep_computes_due_once(backend):
    fid = "root/sleep#0"

    async def wf(ctx):
        await ctx.sleep(timedelta(hours=1))
        return "woke"

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended) and out.waits[0].deadline == T0 + timedelta(hours=1)
    expected_on = Condition.timer(Timer(T0 + timedelta(hours=1), FrameRef(EID, fid)).timer_id)
    assert out.waits[0].on == expected_on

    entries = [s.item for s in await backend.journal.read(EID)]
    started = [e for e in entries if e.type == EntryType.FRAME_STARTED]
    assert len(started) == 1
    assert started[0].payload["kind"] == "sleep"
    assert started[0].payload["name"] == "sleep"
    assert started[0].payload["attempt"] == 1
    expected_digest_input = {"duration_s": timedelta(hours=1).total_seconds()}
    assert started[0].payload["args_digest"] == digest(expected_digest_input)
    assert started[0].provenance.code.frame_name == "sleep"
    assert started[0].provenance.attempt == 1
    suspended = next(e for e in entries if e.type == EntryType.FRAME_SUSPENDED)
    assert suspended.payload["attempt"] == 1

    backend.clock.advance(timedelta(minutes=30))
    out = await h.run()
    assert out.waits[0].deadline == T0 + timedelta(hours=1)  # not recomputed
    backend.clock.advance(timedelta(minutes=31))
    assert await h.run() == Returned("woke")


async def test_sleep_with_elapsed_duration_returns_immediately(backend):
    async def wf(ctx):
        await ctx.sleep(timedelta(0))
        return "woke"

    h = Harness(backend, wf)
    assert await h.run() == Returned("woke")

    fulfilled = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_FULFILLED
    )
    assert fulfilled.fid == "root/sleep#0"
    assert fulfilled.payload["attempt"] == 1
    assert fulfilled.provenance.code.frame_name == "sleep"


async def test_sleep_until_computes_due_from_instant(backend):
    target = T0 + timedelta(hours=2)

    async def wf(ctx):
        await ctx.sleep_until(target)
        return "woke"

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended) and out.waits[0].deadline == target
    backend.clock.advance(timedelta(hours=1))
    assert isinstance(await h.run(), Suspended)
    backend.clock.advance(timedelta(hours=1))
    assert await h.run() == Returned("woke")


async def test_sleep_key_scopes_the_frame_id(backend):
    async def wf(ctx):
        await ctx.sleep(timedelta(0), key="k1")
        return "woke"

    h = Harness(backend, wf)
    assert await h.run() == Returned("woke")
    fids = {fid for _, fid in await h.types()}
    assert fids == {"root/sleep:k1"}


async def test_sleep_changed_duration_on_replay_raises_nondeterminism(backend):
    state = {"seconds": 3600}

    async def wf(ctx):
        await ctx.sleep(timedelta(seconds=state["seconds"]))
        return "woke"

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Suspended)
    state["seconds"] = 7200
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)


# --- gather ---------------------------------------------------------------------


async def test_gather_memoizes_finished_steps_while_receive_waits(backend):
    calls = []

    def a():
        calls.append("a")
        return "A"

    def b():
        calls.append("b")
        return "B"

    async def wf(ctx):
        return await ctx.gather(ctx.step(a), ctx.receive("x"), ctx.step(b))

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended) and len(out.waits) == 1
    assert calls == ["a", "b"]
    await backend.channel.send(Message(execution_channel(EID, "x"), 0, "X", prov()))
    out = await h.run()
    assert isinstance(out, Returned)
    assert out.value[0] == "A" and out.value[1].payload == "X" and out.value[2] == "B"
    assert calls == ["a", "b"]


async def test_gather_frame_ids_are_argument_ordered(backend):
    async def wf(ctx):
        await ctx.gather(ctx.step(lambda: 1, name="s"), ctx.step(lambda: 2, name="s"))
        return ctx.fid

    h = Harness(backend, wf)
    assert await h.run() == Returned("root")
    fids = {fid for _, fid in await h.types()}
    assert fids == {"root/s#0", "root/s#1"}


# --- driver internals: idle detection and cancellation -----------------------------


async def test_maybe_idle_requires_no_live_frame_and_a_real_wait(backend):
    """_maybe_idle must only signal idle when all three hold at once: a real Event,
    nothing currently executing, and something actually waiting -- any one being false
    (even though the other two are true) must not signal it."""

    async def wf(ctx):
        return None

    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=backend.ports,
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
    )
    ctx._idle = asyncio.Event()

    ctx._active = 1
    ctx._waiting = {"root/x#0": Wait("root/x#0", "channel:x", None)}
    ctx._maybe_idle()
    assert not ctx._idle.is_set()  # a live frame is still executing

    ctx._active = 0
    ctx._waiting = {}
    ctx._maybe_idle()
    assert not ctx._idle.is_set()  # nothing is actually waiting

    ctx._waiting = {"root/x#0": Wait("root/x#0", "channel:x", None)}
    ctx._maybe_idle()
    assert ctx._idle.is_set()  # both hold: genuinely idle


async def test_run_does_not_suspend_a_gather_branch_still_executing_when_another_waits(backend):
    """The suspend check (`_active == 0 and self._waiting`) requires BOTH: a step still
    mid-flight inside a gather must not be cancelled just because a sibling branch is
    already parked waiting -- `or` would cancel (and lose) it prematurely."""
    calls = []

    async def slow():
        await asyncio.sleep(0.05)
        calls.append("slow-done")
        return "ok"

    async def wf(ctx):
        # receive() first: it registers its wait before the still-running step below
        # gets its own first turn, so any "active==0" reached at that instant is a
        # false idle signal that the real check must not act on until slow() finishes.
        return await ctx.gather(ctx.receive("x"), ctx.step(slow, name="slow"))

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Suspended)
    assert calls == ["slow-done"]  # the still-running step must finish, not be cancelled
    types = [t for t, _ in await h.types()]
    assert EntryType.FRAME_COMPLETED in types  # its completion must be durably journaled


class _SlowCompletionJournal:
    """A journal whose append() genuinely suspends on a frame's own completion write,
    like a real (disk/network) backend would -- MemoryJournal.append never actually
    yields, so it can never exercise this timing on its own."""

    def __init__(self, inner) -> None:
        self._inner = inner

    async def append(self, eid, entry):
        if entry.type == EntryType.FRAME_COMPLETED:
            await asyncio.sleep(0.05)
        return await self._inner.append(eid, entry)

    async def read(self, eid, after=0):
        return await self._inner.read(eid, after=after)

    async def tail(self, eid):
        return await self._inner.tail(eid)


async def test_run_suppresses_cancellederror_from_a_still_completing_frame(backend):
    """Once a frame's own thunk returns, `_active` drops to 0 even though the frame's
    task is still suspended writing frame.completed -- if the driver declares Suspended
    at that instant (because a sibling is already parked waiting) and cancels `main`,
    that in-flight append is cancelled directly (not through `_wait`'s translation to
    `_Never`), so `asyncio.CancelledError` -- not just `_Never` -- must be suppressed."""

    async def wf(ctx):
        return await ctx.gather(ctx.step(lambda: 1, name="s"), ctx.receive("x"))

    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=replace(backend.ports, journal=_SlowCompletionJournal(backend.journal)),
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
    )
    out = await ctx.run()
    assert isinstance(out, Suspended)  # not a raised, uncaught CancelledError


async def test_run_cancels_the_still_running_workflow_task_when_run_itself_is_cancelled(backend):
    """A worker can cancel its own `ctx.run()` call from outside mid-run (e.g. a lease
    renewal failing, worker.py's `_RenewLoop`). The still-executing workflow task must be
    cancelled too in that case, not left running detached in the background."""
    proceed = asyncio.Event()
    finished = asyncio.Event()

    async def slow():
        await proceed.wait()
        finished.set()
        return 1

    async def wf(ctx):
        return await ctx.step(slow, name="slow")

    h = Harness(backend, wf)
    run_task = asyncio.ensure_future(h.run())
    for _ in range(5):
        await asyncio.sleep(0)  # let it reach the step and start waiting on `proceed`
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    proceed.set()  # if the inner workflow task were still alive, this would let it finish
    for _ in range(5):
        await asyncio.sleep(0)
    assert not finished.is_set()


# --- send / announce / helpers ------------------------------------------------------


async def test_send_and_announce_are_memoized_steps(backend):
    async def wf(ctx):
        seq = await ctx.send("out", {"hello": 1})
        cseq = await ctx.announce("review.requested", {"rid": "r1"})
        return [seq, cseq]

    h = Harness(backend, wf)
    assert await h.run() == Returned([1, 1])
    assert await h.run() == Returned([1, 1])
    assert len(await backend.channel.read(execution_channel(EID, "out"))) == 1
    assert len(await backend.control.read()) == 1
    assert (await backend.control.read())[0].item.type == "announce.review.requested"


async def test_announce_records_name_digest_and_provenance(backend):
    async def wf(ctx):
        return await ctx.announce("review.requested", {"rid": "r1"})

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Returned)
    started = next(
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED
    )
    assert started.payload["name"] == "announce"
    assert started.payload["args_digest"] == digest(
        {"kind": "review.requested", "payload": {"rid": "r1"}}
    )
    assert started.provenance.code.frame_name == "announce"
    control_entry = (await backend.control.read())[0].item
    assert control_entry.provenance.code.frame_name == "announce"


async def test_announce_key_scopes_the_frame_id(backend):
    async def wf(ctx):
        return await ctx.announce("review.requested", {"rid": "r1"}, key="k1")

    h = Harness(backend, wf)
    await h.run()
    fids = {fid for _, fid in await h.types()}
    assert fids == {"root/announce:k1"}


async def test_send_records_name_correlation_and_provenance(backend):
    async def wf(ctx):
        return await ctx.send("out", {"hello": 1}, correlation="corr-1")

    h = Harness(backend, wf)
    assert await h.run() == Returned(1)
    started = (await backend.journal.read(EID))[0].item
    assert started.payload["name"] == "send"
    assert started.provenance.code.frame_name == "send"
    (message,) = await backend.channel.read(execution_channel(EID, "out"))
    assert message.correlation == "corr-1"
    assert message.sent_by.code.frame_name == "send"


async def test_send_multiple_calls_are_independent_frames(backend):
    async def wf(ctx):
        s1 = await ctx.send("a", 1)
        s2 = await ctx.send("b", 2)
        return [s1, s2]

    h = Harness(backend, wf)
    assert await h.run() == Returned([1, 1])
    assert len(await backend.channel.read(execution_channel(EID, "a"))) == 1
    assert len(await backend.channel.read(execution_channel(EID, "b"))) == 1


async def test_send_key_scopes_the_frame_id(backend):
    async def wf(ctx):
        return await ctx.send("out", 1, key="k1")

    h = Harness(backend, wf)
    await h.run()
    fids = {fid for _, fid in await h.types()}
    assert fids == {"root/send:k1"}


async def test_send_changed_payload_on_replay_raises_nondeterminism(backend):
    payload = {"v": 1}

    async def wf(ctx):
        return await ctx.send("out", payload["v"])

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Returned)
    payload["v"] = 2
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)


# --- enqueue -----------------------------------------------------------------------


async def test_enqueue_creates_task_with_defaults_and_reason(backend):
    async def wf(ctx):
        return await ctx.enqueue("orders", {"x": 1}, task_key="k1")

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned)
    assert await h.run() == out  # replayed from memo, no second task
    (task,) = await backend.queue.pending("orders")
    assert task.task_id == out.value
    assert task.reason == "delegate"
    assert task.kind == TaskKind.DELEGATE
    assert task.enqueued_by.code.frame_name == "enqueue"
    started = (await backend.journal.read(EID))[0].item
    assert started.payload["name"] == "enqueue"
    fids = {fid for _, fid in await h.types()}
    assert "root/enqueue#0" in fids
    announced = [
        s.item.payload for s in await backend.control.read() if s.item.type == "task.enqueued"
    ]
    assert announced[0]["reason"] == "delegate"


async def test_enqueue_step_allows_exactly_three_attempts(backend):
    class FlakyQueue:
        def __init__(self) -> None:
            self.calls = 0

        async def enqueue(self, task) -> bool:
            self.calls += 1
            raise OSError("boom")

    async def wf(ctx):
        return await ctx.enqueue("orders", {"x": 1}, task_key="k1")

    flaky = FlakyQueue()
    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=replace(backend.ports, queue=flaky),
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
    )
    out = await ctx.run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert flaky.calls == 3


async def test_enqueue_changed_queue_on_replay_raises_nondeterminism(backend):
    queue_name = {"v": "orders"}

    async def wf(ctx):
        return await ctx.enqueue(queue_name["v"], {"x": 1}, task_key="k1")

    h = Harness(backend, wf)
    assert isinstance(await h.run(), Returned)
    queue_name["v"] = "priority"
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)


# --- withdraw ----------------------------------------------------------------------


async def test_withdraw_claims_and_acks_a_pending_task(backend):
    async def wf(ctx):
        tid = await ctx.enqueue("orders", {"x": 1}, task_key="k1")
        removed = await ctx.withdraw("orders", tid, key="w1")
        return [tid, removed]

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned)
    tid, removed = out.value
    assert removed is True
    assert await backend.queue.pending("orders") == []
    assert await h.run() == out  # replayed from memo: no second take/ack
    started = {
        s.item.fid: s.item
        for s in await backend.journal.read(EID)
        if s.item.type == EntryType.FRAME_STARTED
    }
    assert started["root/withdraw:w1"].payload["name"] == "withdraw"
    assert started["root/withdraw:w1"].payload["args_digest"] == digest(
        {"queue": "orders", "task_id": tid}
    )


async def test_withdraw_missing_task_returns_false_with_default_name(backend):
    async def wf(ctx):
        return await ctx.withdraw("orders", "no-such-task")

    h = Harness(backend, wf)
    assert await h.run() == Returned(False)
    fids = {fid for _, fid in await h.types()}
    assert "root/withdraw#0" in fids  # default name, no key


async def test_withdraw_take_uses_the_context_site_worker_id_and_a_30s_lease(backend):
    calls = []

    class RecordingQueue:
        async def take(self, queue, task_id, worker, ttl):
            calls.append((queue, task_id, worker, ttl))
            return None

    async def wf(ctx):
        return await ctx.withdraw("orders", "t1")

    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=replace(backend.ports, queue=RecordingQueue()),
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-9", epoch=1),
        clock=backend.clock,
    )
    out = await ctx.run()
    assert out == Returned(False)
    assert calls == [("orders", "t1", "w-9", 30.0)]


async def test_withdraw_step_allows_exactly_three_attempts(backend):
    class FlakyQueue:
        def __init__(self) -> None:
            self.calls = 0

        async def take(self, queue, task_id, worker, ttl):
            self.calls += 1
            raise OSError("boom")

    async def wf(ctx):
        return await ctx.withdraw("orders", "t1")

    flaky = FlakyQueue()
    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=replace(backend.ports, queue=flaky),
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
    )
    out = await ctx.run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert flaky.calls == 3


async def test_now_uuid_random_are_stable_across_replays(backend):
    async def wf(ctx):
        t = await ctx.now()
        u = await ctx.uuid()
        r = await ctx.random()
        return [t.to_iso(), u, r]

    h = Harness(backend, wf)
    first = await h.run()
    backend.clock.advance(timedelta(days=1))
    assert await h.run() == first
    assert first.value[0].startswith("2026-09-07T09:00")


async def test_now_records_name_and_digest_and_calls_are_independent(backend):
    async def wf(ctx):
        t1 = await ctx.now()
        backend.clock.advance(timedelta(hours=1))
        t2 = await ctx.now()
        return [t1.to_iso(), t2.to_iso()]

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned)
    assert out.value[0] != out.value[1]  # each call gets its own frame id, not a shared one
    started = [
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED
    ]
    assert len(started) == 2
    assert all(s.payload["name"] == "now" for s in started)
    assert all(s.payload["args_digest"] == digest({}) for s in started)


async def test_random_records_name_and_digest_and_calls_are_independent(backend):
    async def wf(ctx):
        a = await ctx.random()
        b = await ctx.random()
        return [a, b]

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned)
    assert out.value[0] != out.value[1]  # each call gets its own frame id, not a shared one
    started = [
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED
    ]
    assert len(started) == 2
    assert all(s.payload["name"] == "random" for s in started)
    assert all(s.payload["args_digest"] == digest({}) for s in started)


async def test_uuid_records_name_and_digest_and_calls_are_independent(backend):
    async def wf(ctx):
        a = await ctx.uuid()
        b = await ctx.uuid()
        return [a, b]

    h = Harness(backend, wf)
    out = await h.run()
    assert isinstance(out, Returned)
    assert out.value[0] != out.value[1]  # each call gets its own frame id, not a shared one
    started = [
        s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED
    ]
    assert len(started) == 2
    assert all(s.payload["name"] == "uuid" for s in started)
    assert all(s.payload["args_digest"] == digest({}) for s in started)


async def test_fid_inside_step_and_duplicate_key(backend):
    seen = []

    async def wf(ctx):
        await ctx.step(lambda: seen.append(ctx.fid), name="probe", key="k")
        await ctx.step(lambda: None, name="probe", key="k")

    out = await Harness(backend, wf).run()
    assert seen == ["root/probe:k"]
    assert isinstance(out, Raised) and isinstance(out.error, DuplicateFrameError)


# --- nondeterminism and cancel ------------------------------------------------------


async def test_changed_args_on_replay_raise_nondeterminism(backend):
    arg = {"v": 1}

    async def wf(ctx):
        return await ctx.step(lambda v: v, arg["v"], name="s")

    h = Harness(backend, wf)
    assert await h.run() == Returned(1)
    arg["v"] = 2
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)
    assert out.error.fid == "root/s#0"
    assert out.error.recorded is not None and out.error.computed is not None
    assert out.error.recorded != out.error.computed
    assert str(out.error) == (
        f"frame {out.error.fid}: args digest {out.error.computed} "
        f"differs from journal {out.error.recorded}"
    )


async def test_cancel_requested_raises_before_live_frame_but_not_on_memo(backend):
    async def wf(ctx):
        await ctx.step(lambda: 1, name="a")
        await ctx.step(lambda: 2, name="b")
        return "done"

    h = Harness(backend, wf)
    assert await h.run() == Returned("done")
    h.cancel = True
    assert await h.run() == Returned("done")  # all memoized: no live frame, no cancel point

    backend2 = MemoryBackend(clock=ManualClock(T0))
    h2 = Harness(backend2, wf)
    h2.cancel = True
    out = await h2.run()
    assert isinstance(out, Raised) and isinstance(out.error, Cancelled)
    assert str(out.error) == f"execution {EID} cancelled"


# --- child -------------------------------------------------------------------------


async def test_child_starts_once_then_waits_then_returns_value(backend):
    async def child_wf(ctx, x):
        return x * 2

    async def wf(ctx):
        return await ctx.child(child_wf, 21, key="c")

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    out = await h.run()
    assert isinstance(out, Suspended)
    CHILD_FID = "root/child_wf:c"
    child_eid = FrameRef(EID, CHILD_FID).child_eid
    assert out.waits[0].on == Condition.child(child_eid)
    assert len(h.starter.started) == 1
    ex, queue = h.starter.started[0]
    assert ex.eid == child_eid and ex.parent.fid == "root/child_wf:c" and queue == "default"
    assert ex.args == {"args": [21], "kwargs": {}}
    # The execution records which frame created it: the child's own "start" step.
    assert ex.created_by.code.frame_kind == "step"
    assert ex.created_by.code.frame_name == "start"

    started = {s.item.fid: s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED}
    assert started["root/child_wf:c/start#0"].payload["name"] == "start"
    assert started["root/child_wf:c"].payload["kind"] == "child"
    assert started["root/child_wf:c"].payload["name"] == "child_wf"

    assert isinstance(await h.run(), Suspended)
    assert len(h.starter.started) == 1  # start step memoized

    await backend.channel.send(
        Message(
            FrameRef(EID, CHILD_FID).child_channel, 0, {"status": "completed", "value": 42}, prov()
        )
    )
    assert await h.run() == Returned(42)
    fids = {fid for _, fid in await h.types()}
    assert "root/child_wf:c/start#0" in fids and "root/child_wf:c" in fids


async def test_child_failure_raises_child_failed(backend):
    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    assert isinstance(await h.run(), Suspended)
    CHILD_FID = "root/child_wf#0"
    child_eid = FrameRef(EID, CHILD_FID).child_eid
    await backend.channel.send(
        Message(
            FrameRef(EID, CHILD_FID).child_channel, 0, {"status": "failed", "error": "x"}, prov()
        )
    )
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, ChildFailed)
    assert out.error.child_eid == child_eid
    assert out.error.status == "failed"
    assert out.error.error == "x"
    assert str(out.error) == f"child {child_eid} failed: x"


async def test_child_failure_without_error_omits_it_from_message(backend):
    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    assert isinstance(await h.run(), Suspended)
    CHILD_FID = "root/child_wf#0"
    child_eid = FrameRef(EID, CHILD_FID).child_eid
    await backend.channel.send(
        Message(FrameRef(EID, CHILD_FID).child_channel, 0, {"status": "cancelled"}, prov())
    )
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, ChildFailed)
    assert out.error.status == "cancelled"
    assert out.error.error is None
    # No error text: the trailing ": " is stripped rather than left dangling.
    assert str(out.error) == f"child {child_eid} cancelled:"


async def test_child_failure_status_defaults_to_unknown_when_missing(backend):
    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    assert isinstance(await h.run(), Suspended)
    CHILD_FID = "root/child_wf#0"
    # A malformed completion message with no "status" key at all: still not "completed",
    # so it is treated as a failure, and the missing status falls back to "unknown".
    await backend.channel.send(
        Message(FrameRef(EID, CHILD_FID).child_channel, 0, {"error": "boom"}, prov())
    )
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, ChildFailed)
    assert out.error.status == "unknown"
    assert out.error.error == "boom"


async def test_child_start_without_starter_raises_non_retryable(backend):
    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    h.starter = None
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert out.error.failed.error_type == "NonRetryableError"
    assert out.error.failed.message == "Context has no ChildStarter: child() is unavailable"
    assert not out.error.failed.retryable


async def test_child_start_step_allows_exactly_three_attempts(backend):
    class FlakyStarter:
        def __init__(self) -> None:
            self.calls = 0

        async def start_child(self, execution, queue) -> None:
            self.calls += 1
            raise OSError("boom")

    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    starter = FlakyStarter()
    h.starter = starter
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, StepFailed)
    assert starter.calls == 3


async def test_child_changed_args_on_replay_raise_nondeterminism(backend):
    arg = {"v": 1}

    async def child_wf(ctx, x):
        return x

    async def wf(ctx):
        return await ctx.child(child_wf, arg["v"])

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    assert isinstance(await h.run(), Suspended)
    arg["v"] = 2
    out = await h.run()
    assert isinstance(out, Raised) and isinstance(out.error, NondeterminismError)
    assert out.error.fid == "root/child_wf#0/start#0"


async def test_child_receive_frame_digest_reflects_args(backend):
    async def child_wf(ctx, x):
        return x

    async def wf(ctx):
        return await ctx.gather(ctx.child(child_wf, 1), ctx.child(child_wf, 2))

    h = Harness(backend, wf, registry={child_wf: WorkflowRef("child_wf", "1", child_wf)})
    out = await h.run()
    assert isinstance(out, Suspended) and len(out.waits) == 2
    started = [s.item for s in await backend.journal.read(EID) if s.item.type == EntryType.FRAME_STARTED]
    digests = {e.fid: e.payload["args_digest"] for e in started if e.payload["kind"] == "child"}
    assert digests.keys() == {"root/child_wf#0", "root/child_wf#1"}
    assert digests["root/child_wf#0"] != digests["root/child_wf#1"]


async def test_child_without_workflow_registry_raises_the_exact_message(backend):
    async def child_wf(ctx):
        return None

    async def wf(ctx):
        return await ctx.child(child_wf)

    memo = MemoTable.build(await backend.journal.read(EID))
    ctx = Context(
        eid=EID,
        workflow=WorkflowRef("wf", "1", wf),
        memo=memo,
        ports=backend.ports,
        actor=Actor.worker("w-1"),
        site=Site(host="h", pid=1, worker_id="w-1", epoch=1),
        clock=backend.clock,
        starter=StubStarter(),
        workflow_of=None,
    )
    out = await ctx.run()
    assert isinstance(out, Raised) and isinstance(out.error, RuntimeError)
    assert str(out.error) == "Context has no workflow registry: child() is unavailable"


async def test_child_default_name_uses_the_function_name_not_the_workflow_name(backend):
    """01/07: a workflow name may hold `.`, a frame name may not, so the default frame
    name comes from the function's `__name__`, which is always a plain identifier --
    not from the (possibly dotted) registered workflow name."""

    async def actual_fn(ctx):
        return 1

    async def wf(ctx):
        return await ctx.child(actual_fn)

    h = Harness(
        backend, wf, registry={actual_fn: WorkflowRef("registered.name", "1", actual_fn)}
    )
    out = await h.run()
    assert isinstance(out, Suspended)
    fids = {fid for _, fid in await h.types()}
    assert "root/actual_fn#0" in fids
    assert "root/registered.name#0" not in fids


async def test_child_default_name_falls_back_to_workflow_name_when_fn_has_no_dunder_name(backend):
    class CallableWorkflow:
        async def __call__(self, ctx):
            return 1

    fn = CallableWorkflow()
    assert not hasattr(fn, "__name__")

    async def wf(ctx):
        return await ctx.child(fn)

    h = Harness(backend, wf, registry={fn: WorkflowRef("callable-wf", "1", fn)})
    out = await h.run()
    assert isinstance(out, Suspended)
    fids = {fid for _, fid in await h.types()}
    assert "root/callable-wf#0" in fids
