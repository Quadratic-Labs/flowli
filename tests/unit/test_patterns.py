from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp

from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.codec import unstructure
from flowli.domain import (
    Actor,
    ExecutionStatus,
    FrameRef,
    NonRetryableError,
    Site,
    Task,
    TaskKind,
)
from flowli.patterns import DelegateTask, delegate, fan_out, on_tick, review, saga
from flowli.patterns.review import Decision, review_task_key
from flowli.runtime import Engine, EngineConfig

T0 = Timestamp(datetime(2026, 9, 7, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")
CFO = Actor.human("cfo@example.com")


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend(clock=ManualClock(T0))


@pytest.fixture
def engine(backend) -> Engine:
    return Engine(
        backend.ports,
        Site(host="h", pid=1, worker_id="w-1"),
        clock=backend.clock,
        config=EngineConfig(nack_delay=5),
    )


async def drain(worker, limit=30):
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit
    return n


def announced(backend, kind):
    return [s.item.payload for s in backend.control.entries if s.item.type == f"announce.{kind}"]


# --- delegate ---------------------------------------------------------------------


async def test_delegate_enqueues_once_and_returns_reply(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        reply = await delegate(ctx, "scoring", {"invoice": 42}, key="score")
        return reply.payload

    worker = engine.worker()  # listens on "default" only
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.SUSPENDED

    (task,) = await backend.queue.pending("scoring")
    assert task.kind is TaskKind.DELEGATE
    assert task.task_id == f"delegate:{eid}:delegate:score"
    dt = DelegateTask.from_task_payload(task.payload)
    assert dt.eid == eid and dt.fid == "root" and dt.payload == {"invoice": 42}
    assert dt.reply_channel.startswith(f"{eid}.reply.")
    assert dt.reply_fid == "root/delegate-receive:score"
    assert dt.waiting_fid == "root/delegate-receive:score"
    assert any(s.item.type == "task.enqueued" for s in backend.control.entries)

    # an external consumer takes the task, answers, acks
    claimed = await backend.queue.take("scoring", task.task_id, "scorer-1", 30)
    await engine.deliver(dt.eid, dt.reply_channel, {"risk": 0.2}, by=Actor.system("scorer"))
    await backend.queue.ack(claimed)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": {"risk": 0.2}}
    assert await backend.queue.pending("scoring") == []
    fids = {s.item.fid for s in await engine.journal(eid)}
    assert {"root/delegate-enqueue:score", "root/delegate-receive:score"} <= fids


async def test_delegate_timeout_withdraws_task(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        reply = await delegate(ctx, "humans", "q", timeout=timedelta(hours=1))
        return "expired" if reply is None else "answered"

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    (task,) = await backend.queue.pending("humans")
    assert task.task_id == f"delegate:{eid}:delegate"
    dt = DelegateTask.from_task_payload(task.payload)
    assert dt.reply_fid == "root/delegate-receive#0"  # no key: "#0", not ":None"
    backend.clock.advance(timedelta(hours=1, seconds=1))
    await engine.sweeper().run_once()
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "expired"}
    assert await backend.queue.pending("humans") == []


async def test_delegate_timeout_withdraws_task_with_key(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        reply = await delegate(ctx, "humans", "q", timeout=timedelta(hours=1), key="case-9")
        return "expired" if reply is None else "answered"

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    assert len(await backend.queue.pending("humans")) == 1
    backend.clock.advance(timedelta(hours=1, seconds=1))
    await engine.sweeper().run_once()
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "expired"}
    fids = {s.item.fid for s in await engine.journal(eid)}
    # the withdraw frame keeps this delegate's name and key, like enqueue and receive do
    assert "root/delegate-withdraw:case-9" in fids


async def test_worker_leaves_delegate_tasks_alone(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        await delegate(ctx, "default", "x")  # a human queue that a worker also polls: misconfig

    worker = engine.worker()
    await engine.start(w, by=HUMAN)
    await drain(worker)
    # the delegate task was nacked with a delay, not consumed
    assert [t.kind for t in await backend.queue.pending("default")] == [TaskKind.DELEGATE]
    assert await backend.queue.dequeue("default", "x", 1) is None
    backend.clock.advance(timedelta(seconds=6))
    assert (await backend.queue.dequeue("default", "x", 1)).task.kind is TaskKind.DELEGATE


# --- review ------------------------------------------------------------------------


async def test_review_approve_end_to_end(backend, engine):
    @engine.workflow("invoice", "1")
    async def invoice(ctx, amount):
        decision = await review(ctx, "finance", {"amount": amount}, timeout=timedelta(days=3))
        if decision is None:
            return "expired"
        return f"{decision.verdict} by {decision.by.id}"

    worker = engine.worker()
    eid = await engine.start(invoice, 100, by=HUMAN)
    await drain(worker)
    (req,) = announced(backend, "review.requested")
    rid = req["rid"]
    assert req["eid"] == str(eid) and req["queue"] == "finance"
    assert req["payload"] == {"amount": 100}
    assert req["deadline"] == "2026-09-10T09:00:00.000000Z"
    (task,) = await backend.queue.pending("finance")
    assert task.task_id == Task.id_for(TaskKind.DELEGATE, eid, review_task_key(rid))
    # the delegate payload keeps rid, payload and deadline exactly as given
    # (06-patterns.md #2 step 3)
    assert task.payload["payload"] == {
        "rid": rid,
        "payload": {"amount": 100},
        "deadline": req["deadline"],
    }

    backend.clock.advance(timedelta(days=1))
    decision = await engine.reviews.decide(rid, eid=eid, queue="finance", verdict="approve", by=CFO)
    assert decision.by == CFO and decision.at == backend.clock()
    assert await backend.queue.pending("finance") == []
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "approve by cfo@example.com"}
    (dec,) = announced(backend, "review.decided")
    assert dec == {"rid": rid, "verdict": "approve", "by": unstructure(CFO)}
    assert announced(backend, "review.expired") == []
    # the default `name="review"` and `key=rid` frame the delegate call (06-patterns.md #2 step 3)
    fids = {s.item.fid for s in await engine.journal(eid)}
    assert {f"root/review-enqueue:{rid}", f"root/review-receive:{rid}"} <= fids


async def test_two_reviewers_one_decision(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        d = await review(ctx, "finance", {})
        return d.verdict

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    rid = announced(backend, "review.requested")[0]["rid"]
    first = await engine.reviews.decide(rid, eid=eid, queue="finance", verdict="approve", by=CFO)
    second = await engine.reviews.decide(
        rid, eid=eid, queue="finance", verdict="reject", by=Actor.human("cto@example.com")
    )
    assert second == first  # the loser learns the winner's decision
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "approve"}


async def test_decide_completes_delivery_after_crash_between_claim_and_deliver(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        return (await review(ctx, "finance", {})).verdict

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    rid = announced(backend, "review.requested")[0]["rid"]
    # simulate: the claim landed, then the process died
    mine = Decision("approve", None, CFO, backend.clock())
    won, _ = await backend.dispatch.claim(f"reviews/{rid}/decision", unstructure(mine))
    assert won
    assert len(await backend.queue.pending("finance")) == 1
    again = await engine.reviews.decide(
        rid, eid=eid, queue="finance", verdict="reject", by=Actor.human("other@example.com")
    )
    assert again.verdict == "approve" and again.by == CFO
    assert await backend.queue.pending("finance") == []
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "approve"}


async def test_review_timeout_expires_and_withdraws(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        d = await review(ctx, "finance", {}, timeout=timedelta(hours=2))
        return "expired" if d is None else d.verdict

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    rid = announced(backend, "review.requested")[0]["rid"]
    backend.clock.advance(timedelta(hours=3))
    await engine.sweeper().run_once()
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "expired"}
    (exp,) = announced(backend, "review.expired")
    assert exp == {"rid": rid}  # 06-patterns.md #2 step 4
    assert await backend.queue.pending("finance") == []


async def test_review_task_key_stays_rid_based_with_a_custom_key(backend, engine):
    """`task_key` is always `review:{rid}`, even when the caller's `key` differs.

    `key` only frames the delegate call (so two reviews under one frame need
    two names or two keys); `decide` looks the task up by rid alone
    (06-patterns.md #2 step 3, and `decide`'s own step 2). If `review` forgot
    to pass `task_key` explicitly, the task would be filed under the frame's
    key/name instead, and `decide` would never find it.
    """

    @engine.workflow("w", "1")
    async def w(ctx):
        d = await review(ctx, "finance", {}, name="approval", key="custom-key")
        return d.verdict

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    rid = announced(backend, "review.requested")[0]["rid"]
    fids = {s.item.fid for s in await engine.journal(eid)}
    assert {"root/approval-enqueue:custom-key", "root/approval-receive:custom-key"} <= fids
    (task,) = await backend.queue.pending("finance")
    assert task.task_id == Task.id_for(TaskKind.DELEGATE, eid, review_task_key(rid))

    decision = await engine.reviews.decide(rid, eid=eid, queue="finance", verdict="approve", by=CFO)
    assert decision.verdict == "approve"
    assert await backend.queue.pending("finance") == []
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": "approve"}


async def test_decide_records_the_optional_data_field(backend, engine):
    @engine.workflow("w", "1")
    async def w(ctx):
        d = await review(ctx, "finance", {})
        return {"verdict": d.verdict, "data": d.data}

    worker = engine.worker()
    eid = await engine.start(w, by=HUMAN)
    await drain(worker)
    rid = announced(backend, "review.requested")[0]["rid"]
    decision = await engine.reviews.decide(
        rid, eid=eid, queue="finance", verdict="reject", by=CFO, data={"reason": "over budget"}
    )
    assert decision.data == {"reason": "over budget"}
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {
        "value": {"verdict": "reject", "data": {"reason": "over budget"}}
    }


def test_reviews_default_take_ttl_is_30_seconds(engine):
    assert engine.reviews.take_ttl == 30.0


# --- scheduled start, saga, fan-out ----------------------------------------------------


async def test_on_tick_is_exactly_once_per_tick(backend, engine):
    @engine.workflow("etl", "1")
    async def etl(ctx):
        return 1

    tick = Timestamp(datetime(2026, 9, 7, 10, 0, tzinfo=UTC))
    a = await on_tick(engine, etl, "nightly", tick)
    b = await on_tick(engine, etl, "nightly", tick)
    c = await on_tick(engine, etl, "nightly", tick + timedelta(days=1))
    assert a == b != c
    assert (await engine.execution(a)).created_by.actor == Actor.schedule("nightly")
    assert (await engine.execution(a)).dispatch_key == "nightly:2026-09-07T10:00:00.000000Z"


async def test_on_tick_forwards_args_kwargs_and_queue(backend, engine):
    @engine.workflow("etl_args", "1")
    async def etl_args(ctx, x, *, y):
        return x + y

    worker = engine.worker(["reports"])
    tick = Timestamp(datetime(2026, 9, 7, 10, 0, tzinfo=UTC))
    eid = await on_tick(engine, etl_args, "reports", tick, 3, queue="reports", y=4)
    assert (await engine.execution(eid)).queue == "reports"
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": 7}


async def test_saga_compensates_in_reverse_once(backend, engine):
    log = []

    def book_flight():
        log.append("flight")
        return "F1"

    def cancel_flight(ref):
        log.append(f"cancel {ref}")

    def book_hotel():
        log.append("hotel")
        return "H1"

    def cancel_hotel(ref):
        log.append(f"cancel {ref}")

    def charge():
        log.append("charge")
        raise NonRetryableError("card declined")

    @engine.workflow("trip", "1")
    async def trip(ctx):
        return await saga(
            ctx,
            [(book_flight, cancel_flight), (book_hotel, cancel_hotel), (charge, lambda v: None)],
        )

    worker = engine.worker()
    eid = await engine.start(trip, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.FAILED
    assert log == ["flight", "hotel", "charge", "cancel H1", "cancel F1"]
    fids = {s.item.fid for s in await engine.journal(eid)}
    assert {
        "root/saga-act:0",
        "root/saga-act:1",
        "root/saga-act:2",
        "root/saga-undo:1",
        "root/saga-undo:0",
    } <= fids


async def test_saga_returns_action_results_on_success(backend, engine):
    def book_flight():
        return "F1"

    def cancel_flight(ref):
        pass

    def book_hotel():
        return "H1"

    def cancel_hotel(ref):
        pass

    @engine.workflow("trip_ok", "1")
    async def trip_ok(ctx):
        return await saga(ctx, [(book_flight, cancel_flight), (book_hotel, cancel_hotel)])

    worker = engine.worker()
    eid = await engine.start(trip_ok, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": ["F1", "H1"]}


async def test_saga_undo_default_retry_allows_exactly_five_attempts(backend, engine):
    """The default undo_retry gives compensations up to 5 attempts (docstring: `saga`).

    A compensation that keeps failing must be retried, but only up to the default
    policy's limit -- and a failure that exhausts it must abort the remaining
    compensations rather than silently swallow the error.
    """
    hotel_calls: list[str] = []
    flight_calls: list[str] = []

    def book_flight():
        return "F1"

    def cancel_flight(ref):
        flight_calls.append(ref)

    def book_hotel():
        return "H1"

    def cancel_hotel_flaky(ref):
        hotel_calls.append(ref)
        if len(hotel_calls) < 6:
            raise Exception("hotel cancellation temporarily unavailable")

    def charge():
        raise NonRetryableError("card declined")

    @engine.workflow("trip_flaky_undo", "1")
    async def trip_flaky_undo(ctx):
        return await saga(
            ctx,
            [
                (book_flight, cancel_flight),
                (book_hotel, cancel_hotel_flaky),
                (charge, lambda v: None),
            ],
        )

    worker = engine.worker()
    eid = await engine.start(trip_flaky_undo, by=HUMAN)
    await drain(worker)
    assert await engine.status(eid) == ExecutionStatus.FAILED
    # Exactly 5 attempts are made (all fail); a 6th, would-be-successful attempt
    # never happens, and the flight compensation -- which runs after the hotel one
    # in reverse order -- never gets a chance to run either.
    assert len(hotel_calls) == 5
    assert flight_calls == []


async def test_fan_out_children_in_item_order(backend, engine):
    @engine.workflow("sq", "1")
    async def sq(ctx, x):
        return x * x

    @engine.workflow("all", "1")
    async def all_(ctx):
        return await fan_out(ctx, sq, [3, 1, 2])

    worker = engine.worker()
    eid = await engine.start(all_, by=HUMAN)
    await drain(worker)
    assert (await engine.journal(eid))[-1].item.payload == {"value": [9, 1, 4]}


async def test_fan_out_keys_children_by_position_under_given_name(backend, engine):
    """`fan_out`'s own `name` overrides the child's default, and `key` is the item index.

    The child function is deliberately named `sq` so a dropped `name=` override
    would fall back to it, and a dropped `key=` would fall back to an ordinal
    (`#`) instead of a positional key (`:`) -- either shows up in the fid.
    """

    @engine.workflow("sq", "1")
    async def sq(ctx, x):
        return x * x

    @engine.workflow("all", "1")
    async def all_(ctx):
        return await fan_out(ctx, sq, [3, 1, 2], name="squared")

    worker = engine.worker()
    eid = await engine.start(all_, by=HUMAN)
    await drain(worker)
    fids = {s.item.fid for s in await engine.journal(eid)}
    assert {"root/squared:0", "root/squared:1", "root/squared:2"} <= fids


async def test_fan_out_starts_children_on_given_queue(backend, engine):
    @engine.workflow("sq", "1")
    async def sq(ctx, x):
        return x * x

    @engine.workflow("all", "1")
    async def all_(ctx):
        return await fan_out(ctx, sq, [3, 1, 2], queue="workers")

    worker = engine.worker()
    eid = await engine.start(all_, by=HUMAN)
    await drain(worker)
    child_eid = FrameRef(eid, "root/sq:0").child_eid
    assert (await engine.execution(child_eid)).queue == "workers"


async def test_decision_is_delivered_even_when_the_task_cannot_be_taken(backend, engine):
    """Someone else holds the DELEGATE task when the decision is made.

    A worker wrongly subscribed to a human queue does exactly this: it
    dequeues the task and nacks it into the future. The decision is claimed
    by then, so the delivery must not depend on holding the task, or the
    verdict would be recorded and never seen.
    """

    @engine.workflow("held_review", "1")
    async def held_review(ctx, amount):
        decision = await review(ctx, "finance", {"amount": amount})
        return "none" if decision is None else decision.verdict

    worker = engine.worker()
    eid = await engine.start(held_review, 100, by=HUMAN)
    await drain(worker)
    (task,) = await backend.queue.pending("finance")
    rid = task.payload["payload"]["rid"]

    held = await backend.queue.take("finance", task.task_id, "someone-else", 60)
    assert held is not None

    await engine.reviews.decide(rid, eid=eid, queue="finance", verdict="approve", by=CFO)
    await drain(worker)

    assert await engine.status(eid) is ExecutionStatus.COMPLETED
    assert (await engine.journal(eid))[-1].item.payload == {"value": "approve"}
