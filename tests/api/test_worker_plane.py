"""The relay of the queue and the channel. Spec 09 section 9.

The scenario throughout is spec 10's runner: dequeue a delegate task, keep
the address of the work in the lease state, answer on the reply channel.
"""

import asyncio
import uuid
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from flowlet.api import ApiConfig
from flowlet.api.auth import Principal
from flowlet.api.dto import delegate_task
from flowlet.api.routes import worker
from flowlet.api.routes.executions import _cancel_delegates
from flowlet.api.routes.worker import _lease_body
from flowlet.domain import Actor, FrameRef, Task, TaskKind

from .conftest import OPERATOR, READER, RUNNER, STRAY, auth, drain
from tests.ids import E_ABC, E_BBB

DELEGATING = {"workflow": "delegates", "version": "1", "args": {"amount": 100}}

# Captured before any test monkeypatches asyncio.sleep, so the fakes below can
# still yield to the loop for real.
_REAL_SLEEP = asyncio.sleep


async def start_and_delegate(client, engine) -> str:
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]
    await drain(engine)
    return eid


async def take_one(client, wait=0.0) -> dict:
    r = await client.post(
        "/queues/agents/dequeue", json={"ttl_seconds": 60, "wait_seconds": wait},
        headers=auth(RUNNER),
    )
    assert r.status_code == 200, r.text
    return r.json()


async def test_dequeue_gives_the_task_and_a_holder(client, engine):
    eid = await start_and_delegate(client, engine)
    body = await take_one(client)

    assert body["task"]["kind"] == "delegate"
    assert body["task"]["eid"] == eid
    assert body["task"]["payload"] == {"amount": 100}
    assert body["task"]["reply_channel"].startswith(f"{eid}.reply.")
    assert body["epoch"] == 1
    assert body["deadline_at"]
    # Whoever knows the holder can act on the task, so it must not be guessable.
    assert body["holder"].startswith("http:runner-a:")
    # secrets.token_urlsafe(16) always renders to exactly 22 base64url chars.
    assert len(body["holder"]) == len("http:runner-a:") + 22


async def test_an_empty_queue_is_204(client):
    r = await client.post("/queues/agents/dequeue", json={}, headers=auth(RUNNER))
    assert r.status_code == 204


async def test_a_second_consumer_gets_nothing(client, engine):
    await start_and_delegate(client, engine)
    await take_one(client)
    r = await client.post("/queues/agents/dequeue", json={}, headers=auth(RUNNER))
    assert r.status_code == 204  # one task, one holder


async def test_consuming_needs_the_queue_capability(client, engine):
    await start_and_delegate(client, engine)
    r = await client.post("/queues/agents/dequeue", json={}, headers=auth(READER))
    assert r.status_code == 403
    # The runner may consume `agents` and nothing else.
    r = await client.post("/queues/default/dequeue", json={}, headers=auth(RUNNER))
    assert r.status_code == 403


async def test_the_relay_refuses_a_task_of_the_engine(client, engine):
    """A start, a resume or a step belongs to a worker, not to a consumer."""
    eid = (await client.post("/executions", json=DELEGATING, headers=auth(OPERATOR))).json()["eid"]

    r = await client.post("/queues/default/dequeue", json={}, headers=auth(STRAY))
    assert r.status_code == 409
    assert r.json()["code"] == "not_a_delegate_task"

    # The task went back on the queue rather than being lost. The relay's
    # nack delays it, so the engine's own worker sees it a few seconds later.
    depth = await engine.ports.queue.depth("default")
    assert depth.total == 1 and depth.claimed == 0
    assert (await engine.status(eid)).value == "pending"


async def test_renew_extends_and_shows_the_state(client, engine):
    await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id, holder = body["task"]["task_id"], body["holder"]

    r = await client.post(
        f"/queues/agents/tasks/{task_id}/renew", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert r.status_code == 200
    assert r.json()["epoch"] == body["epoch"]  # a renew is not a new period
    assert r.json()["deadline_at"] >= body["deadline_at"]


async def test_the_handle_survives_in_the_lease_state(client, engine):
    """Spec 10 section 4.2: the address of the work lives in the lease state,
    so a restart or a steal can find it."""
    await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id, holder = body["task"]["task_id"], body["holder"]

    handle = {"kind": "service", "session_id": "s-1", "url": "https://agents/s-1"}
    written = await client.post(
        f"/queues/agents/tasks/{task_id}/state",
        json={"holder": holder, "state": {"handle": handle}},
        headers=auth(RUNNER),
    )
    assert written.json()["state"]["handle"] == handle

    # Any instance of the service serves the next call, because the lease is
    # a document: this renew re-attaches from nothing but the holder.
    seen = await client.post(
        f"/queues/agents/tasks/{task_id}/renew", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert seen.json()["state"]["handle"] == handle


async def test_a_wrong_holder_is_409(client, engine):
    await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id = body["task"]["task_id"]

    r = await client.post(
        f"/queues/agents/tasks/{task_id}/renew",
        json={"holder": "http:runner-a:guessed"},
        headers=auth(RUNNER),
    )
    assert r.status_code == 409
    problem = r.json()
    assert problem["code"] == "not_holder"
    assert problem["title"] == "Not the holder"
    assert problem["detail"] == (
        f"http:runner-a:guessed does not hold {task_id} on agents, or the lease expired"
    )


async def test_cancel_reaches_the_holder_through_the_lease(client, engine):
    """Spec 10 section 5: the consumer reads `cancel_requested` on its next
    renew. It fences nobody."""
    await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id, holder = body["task"]["task_id"], body["holder"]

    asked = await client.post(f"/queues/agents/tasks/{task_id}/cancel", headers=auth(RUNNER))
    assert asked.status_code == 200

    seen = await client.post(
        f"/queues/agents/tasks/{task_id}/renew", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert seen.status_code == 200  # not fenced
    assert seen.json()["state"]["cancel_requested"]["actor"]["id"] == "runner-a"


async def test_deliver_answers_the_frame_then_ack(client, engine, projection):
    eid = await start_and_delegate(client, engine)
    body = await take_one(client)
    task = body["task"]

    sent = await client.post(
        f"/executions/{eid}/deliver",
        json={
            "queue": "agents",
            "task_id": task["task_id"],
            "holder": body["holder"],
            "channel": task["reply_channel"],
            "payload": {"verdict": "done"},
        },
        headers=auth(RUNNER),
    )
    assert sent.status_code == 200

    acked = await client.post(
        f"/queues/agents/tasks/{task['task_id']}/ack",
        json={"holder": body["holder"]},
        headers=auth(RUNNER),
    )
    assert acked.status_code == 200

    await drain(engine)
    await projection.refresh()
    row = (await client.get(f"/executions/{eid}", headers=auth(OPERATOR))).json()
    assert row["status"] == "completed" and row["result"] == "done"


async def test_deliver_refuses_a_channel_the_holder_does_not_own(client, engine):
    """Without this check one consumer could answer the frame of another."""
    eid = await start_and_delegate(client, engine)
    body = await take_one(client)

    r = await client.post(
        f"/executions/{eid}/deliver",
        json={
            "queue": "agents",
            "task_id": body["task"]["task_id"],
            "holder": body["holder"],
            "channel": f"{eid}.reply.somethingelse",
            "payload": {"verdict": "done"},
        },
        headers=auth(RUNNER),
    )
    assert r.status_code == 403
    assert r.json()["code"] == "wrong_channel"


async def test_nack_returns_the_task_to_the_queue(client, engine):
    await start_and_delegate(client, engine)
    body = await take_one(client)

    r = await client.post(
        f"/queues/agents/tasks/{body['task']['task_id']}/nack",
        json={"holder": body["holder"], "delay_seconds": 0},
        headers=auth(RUNNER),
    )
    assert r.status_code == 200

    again = await take_one(client)
    assert again["task"]["task_id"] == body["task"]["task_id"]
    assert again["holder"] != body["holder"]  # a new period, a new holder


async def test_ack_after_ack_is_409(client, engine):
    await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id, holder = body["task"]["task_id"], body["holder"]

    first = await client.post(
        f"/queues/agents/tasks/{task_id}/ack", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert first.status_code == 200
    second = await client.post(
        f"/queues/agents/tasks/{task_id}/ack", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert second.status_code == 409  # the task is gone


@pytest.mark.parametrize("wait", [0.0, 0.3])
async def test_long_poll_returns_when_the_queue_is_empty(client, wait):
    r = await client.post(
        "/queues/agents/dequeue", json={"wait_seconds": wait}, headers=auth(RUNNER)
    )
    assert r.status_code == 204


def test_delegate_task_prefers_the_payloads_own_target(prov):
    """The consumer's view is built from the payload's `target`, which can
    name a different frame than the task's own -- `reply_fid` (the frame
    that waits) is not necessarily the one that enqueued (`06-patterns.md`,
    section 1)."""
    task = Task(
        queue="agents",
        kind=TaskKind.DELEGATE,
        target=FrameRef(E_ABC, "root/x#0"),
        reason="delegate",
        enqueued_by=prov,
        payload={
            "target": {"eid": str(E_BBB), "fid": "root/y#0"},
            "reply_channel": f"{E_ABC}.reply.delegate",
            "reply_fid": "root/x/delegate-receive#0",
            "payload": {"amount": 7},
        },
    )
    assert delegate_task(task) == {
        "task_id": task.task_id,
        "queue": "agents",
        "kind": "delegate",
        "eid": str(E_BBB),
        "fid": "root/y#0",
        "reason": "delegate",
        "reply_channel": f"{E_ABC}.reply.delegate",
        "payload": {"amount": 7},
        "enqueued_at": prov.at.to_iso(),
    }


def test_delegate_task_falls_back_to_the_tasks_own_target_without_a_payload(prov):
    """A task with no payload -- or a payload with no `target` -- still gives
    a usable row: the eid and fid come from the task itself."""
    task = Task(
        queue="agents",
        kind=TaskKind.DELEGATE,
        target=FrameRef(E_ABC, "root/x#0"),
        reason="delegate",
        enqueued_by=prov,
    )
    assert delegate_task(task) == {
        "task_id": task.task_id,
        "queue": "agents",
        "kind": "delegate",
        "eid": str(E_ABC),
        "fid": "root/x#0",
        "reason": "delegate",
        "reply_channel": None,
        "payload": None,
        "enqueued_at": prov.at.to_iso(),
    }


class _StubLease:
    epoch = 3
    state = {"k": "v"}
    deadline_at = None


def test_lease_body_reports_no_deadline_when_the_lease_has_none():
    """`Lease.deadline_at` is typed `Timestamp | None` (03-ports.md section 7):
    `_lease_body` must not crash calling `.to_iso()` on a missing deadline."""
    claimed = SimpleNamespace(lease=_StubLease())
    assert _lease_body(claimed, "http:runner-a:tok") == {
        "holder": "http:runner-a:tok",
        "epoch": 3,
        "deadline_at": None,
        "state": {"k": "v"},
    }


async def test_poll_returns_the_moment_a_task_shows_up_within_the_wait(monkeypatch):
    """`_poll` must return as soon as a dequeue succeeds, not only once `wait`
    has fully elapsed -- otherwise a long poll would always be as slow as
    `wait`, defeating the point of long-polling."""
    ticks = {"n": 0}

    def fake_monotonic() -> float:
        ticks["n"] += 1
        return float(ticks["n"])

    async def fake_sleep(seconds: float) -> None:
        await _REAL_SLEEP(0)

    calls = []

    async def fake_dequeue(queue, holder, ttl):
        calls.append((queue, holder, ttl))
        return None if len(calls) == 1 else "the-task"

    monkeypatch.setattr(worker, "monotonic", fake_monotonic)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    engine = SimpleNamespace(ports=SimpleNamespace(queue=SimpleNamespace(dequeue=fake_dequeue)))
    config = ApiConfig(poll_interval=0.5, max_wait_seconds=20.0)

    result = await asyncio.wait_for(
        worker._poll(engine, "agents", "holder-x", 60.0, 2.0, config), timeout=5.0
    )

    assert result == "the-task"
    assert len(calls) == 2  # found on the second attempt, well before the 2s wait


async def test_poll_stops_exactly_at_its_deadline(monkeypatch):
    """The deadline check is `>=`: once `wait` is up, `_poll` must give up on
    that very check rather than run one more dequeue attempt."""
    ticks = {"n": 0}

    def fake_monotonic() -> float:
        ticks["n"] += 1
        return float(ticks["n"])

    async def fake_sleep(seconds: float) -> None:
        await _REAL_SLEEP(0)

    calls = []

    async def fake_dequeue(queue, holder, ttl):
        calls.append((queue, holder, ttl))
        return None if len(calls) == 1 else "too-late"

    monkeypatch.setattr(worker, "monotonic", fake_monotonic)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    engine = SimpleNamespace(ports=SimpleNamespace(queue=SimpleNamespace(dequeue=fake_dequeue)))
    config = ApiConfig(poll_interval=0.5, max_wait_seconds=20.0)

    # The pre-loop `monotonic()` call sets the deadline to 2.0; the loop's own
    # first check then lands on exactly 2.0 too.
    result = await asyncio.wait_for(
        worker._poll(engine, "agents", "holder-x", 60.0, 1.0, config), timeout=5.0
    )

    assert result is None
    assert len(calls) == 1  # gave up at the deadline instead of trying again


async def test_poll_never_widens_its_sleep_past_what_remains(monkeypatch):
    """The sleep is clamped to `max(0.0, deadline - monotonic())`: once the
    deadline is all but reached, the poll must sleep for zero, not overshoot
    it by sleeping longer (or on the wrong sign of the remaining time)."""
    ticks = {"n": 0}

    def fake_monotonic() -> float:
        ticks["n"] += 1
        return float(ticks["n"])

    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await _REAL_SLEEP(0)

    calls = []

    async def fake_dequeue(queue, holder, ttl):
        calls.append((queue, holder, ttl))
        return None if len(calls) < 3 else "eventually"

    monkeypatch.setattr(worker, "monotonic", fake_monotonic)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    engine = SimpleNamespace(ports=SimpleNamespace(queue=SimpleNamespace(dequeue=fake_dequeue)))
    config = ApiConfig(poll_interval=5.0, max_wait_seconds=20.0)

    result = await asyncio.wait_for(
        worker._poll(engine, "agents", "holder-x", 60.0, 2.0, config), timeout=5.0
    )

    assert result is None
    assert sleeps == [0.0]


async def test_cancelling_an_execution_reaches_its_delegate_consumer(
    client, engine, projection
):
    """Spec 05 section 8: the service propagates, because the journal records
    the task id of an enqueue and not its queue."""
    eid = await start_and_delegate(client, engine)
    body = await take_one(client)
    task_id, holder = body["task"]["task_id"], body["holder"]

    cancelled = await client.post(f"/executions/{eid}/cancel", headers=auth(OPERATOR))
    assert cancelled.status_code == 200
    assert cancelled.json()["tasks_cancelled"] == [task_id]

    seen = await client.post(
        f"/queues/agents/tasks/{task_id}/renew", json={"holder": holder}, headers=auth(RUNNER)
    )
    assert seen.status_code == 200  # the consumer keeps the task
    prov = seen.json()["state"]["cancel_requested"]
    assert prov["actor"]["id"] == "ops@example.com"
    assert prov["code"]["frame_name"] == "cancel"


async def test_cancelling_an_execution_does_not_touch_another_executions_delegate(
    client, engine, projection
):
    """The `tasks` lookup must be scoped to this execution's eid: without that
    filter, cancelling one execution would ask every delegate consumer in the
    system to stop (05-protocols.md section 8)."""
    eid_a = await start_and_delegate(client, engine)
    body_a = await take_one(client)
    task_a = body_a["task"]["task_id"]

    eid_b = await start_and_delegate(client, engine)
    body_b = await take_one(client)
    task_b, holder_b = body_b["task"]["task_id"], body_b["holder"]

    cancelled = await client.post(f"/executions/{eid_a}/cancel", headers=auth(OPERATOR))
    assert cancelled.status_code == 200
    assert cancelled.json()["tasks_cancelled"] == [task_a]

    seen_b = await client.post(
        f"/queues/agents/tasks/{task_b}/renew", json={"holder": holder_b}, headers=auth(RUNNER)
    )
    assert seen_b.status_code == 200
    assert not (seen_b.json()["state"] or {}).get("cancel_requested")


async def test_cancel_delegates_asks_projection_for_this_eids_delegates_capped(
    engine, monkeypatch
):
    """`_cancel_delegates` (05-protocols.md section 8) reads `projection.tasks`
    with the execution's own eid, delegate tasks only, and a bounded page --
    not the whole system's backlog."""

    @dataclass
    class _Row:
        queue: str
        task_id: str

    calls = []

    class FakeProjection:
        def tasks(self, *, eid=None, queue=None, kind=None, limit=100):
            calls.append({"eid": eid, "queue": queue, "kind": kind, "limit": limit})
            return [_Row("agents", "t-1")]

    cancelled = []

    async def fake_request_cancel(queue, task_id, by):
        cancelled.append((queue, task_id, by))

    monkeypatch.setattr(engine.ports.queue, "request_cancel", fake_request_cancel)

    eid = uuid.uuid4()
    who = Principal(Actor.human("ops@example.com"), frozenset())

    asked = await _cancel_delegates(engine, FakeProjection(), eid, who)

    assert asked == ["t-1"]
    assert calls == [{"eid": eid, "queue": None, "kind": TaskKind.DELEGATE.value, "limit": 100}]
    assert cancelled[0][:2] == ("agents", "t-1")
    assert cancelled[0][2].code.frame_name == "cancel"
