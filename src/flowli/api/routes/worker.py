"""The worker plane. See specs/09-http-api.md section 9.

This is a relay of the `Queue` and `Channel` ports for a consumer that
cannot reach the bucket. A consumer that can reach the bucket must use the
ports: the relay is one more hop and one more trust boundary.

The service keeps nothing between two calls. A holder addresses an ownership
period in the bucket, and any instance of the service can serve the next call
of that period, because `Queue.attach` rebuilds the handle from the lease
document.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import timedelta
from time import monotonic
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse

from flowli.domain import ClaimedTask, TaskKind, parse_eid

from ..auth import TASKS_CONSUME, Principal
from ..deps import Config, Engine, Projection
from ..deps import principal as principal_dep
from ..dto import (
    AckRequest,
    DeliverRequest,
    DequeueRequest,
    HolderRequest,
    NackRequest,
    StateRequest,
    delegate_task,
    task_row,
)
from ..problems import Problem
from ..reads import SEQ_HEADER

router = APIRouter(tags=["worker"])

Consumer = Annotated[Principal, Depends(principal_dep)]


def _mint(who: Principal) -> str:
    """`http:{client_id}:{secret}`.

    Whoever presents the holder can renew, ack and nack the task, because
    `attach` accepts it (`03-ports.md`, section 7). So the string must be
    unguessable: a UUIDv7 would not do, its high bits are a timestamp.
    """
    return f"http:{who.actor.id}:{secrets.token_urlsafe(16)}"


async def _held(engine: Any, queue: str, task_id: str, holder: str, ttl: float) -> ClaimedTask:
    claimed: ClaimedTask | None = await engine.ports.queue.attach(queue, task_id, holder, ttl)
    if claimed is None:
        raise Problem(
            409,
            "not_holder",
            "Not the holder",
            f"{holder} does not hold {task_id} on {queue}, or the lease expired",
        )
    return claimed


def _lease_body(claimed: ClaimedTask, holder: str) -> dict[str, Any]:
    deadline = claimed.lease.deadline_at
    return {
        "holder": holder,
        "epoch": claimed.lease.epoch,
        "deadline_at": None if deadline is None else deadline.to_iso(),
        "state": claimed.lease.state,
    }


async def _poll(
    engine: Any, queue: str, holder: str, ttl: float, wait: float, config: Config
) -> ClaimedTask | None:
    """One dequeue, then a poll until `wait` runs out. A queue gives a few
    dequeues per second (`03-ports.md`, section 7), so a long poll is cheap
    for the client and not free for the bucket."""
    deadline = monotonic() + min(wait, config.max_wait_seconds)
    while True:
        claimed: ClaimedTask | None = await engine.ports.queue.dequeue(queue, holder, ttl)
        if claimed is not None or monotonic() >= deadline:
            return claimed
        await asyncio.sleep(min(config.poll_interval, max(0.0, deadline - monotonic())))


@router.post("/queues/{queue}/dequeue")
async def dequeue(
    queue: str, body: DequeueRequest, engine: Engine, config: Config, who: Consumer
) -> Response:
    who.require(TASKS_CONSUME, queue)
    holder = _mint(who)
    ttl = min(body.ttl_seconds or config.task_ttl, config.max_task_ttl)
    claimed = await _poll(engine, queue, holder, ttl, body.wait_seconds, config)
    if claimed is None:
        return Response(status_code=204)
    if claimed.task.kind is not TaskKind.DELEGATE:
        # A start, a resume or a step belongs to a worker of the engine.
        await engine.ports.queue.nack(claimed, timedelta(seconds=config.nack_delay))
        raise Problem(
            409,
            "not_a_delegate_task",
            "Not a delegate task",
            f"{claimed.task.task_id} is a {claimed.task.kind.value} task",
        )
    return JSONResponse(
        {"task": delegate_task(claimed.task), **_lease_body(claimed, holder)}
    )


@router.post("/queues/{queue}/tasks/{task_id}/renew")
async def renew(
    queue: str, task_id: str, body: HolderRequest, engine: Engine, config: Config, who: Consumer
) -> Response:
    who.require(TASKS_CONSUME, queue)
    ttl = body.ttl_seconds or config.task_ttl
    claimed = await _held(engine, queue, task_id, body.holder, ttl)
    await claimed.lease.renew()
    # The state is how a consumer sees `cancel_requested`, so a relayed
    # consumer reads the body of every renew (spec 10, section 5).
    return JSONResponse(_lease_body(claimed, body.holder))


@router.post("/queues/{queue}/tasks/{task_id}/state")
async def write_state(
    queue: str, task_id: str, body: StateRequest, engine: Engine, config: Config, who: Consumer
) -> Response:
    who.require(TASKS_CONSUME, queue)
    claimed = await _held(engine, queue, task_id, body.holder, config.task_ttl)
    state = await claimed.lease.update_state(lambda s: {**(s or {}), **body.state})
    return JSONResponse({"holder": body.holder, "epoch": claimed.lease.epoch, "state": state})


@router.post("/queues/{queue}/tasks/{task_id}/ack")
async def ack(
    queue: str, task_id: str, body: AckRequest, engine: Engine, config: Config, who: Consumer
) -> Response:
    who.require(TASKS_CONSUME, queue)
    claimed = await _held(engine, queue, task_id, body.holder, config.task_ttl)
    await engine.ports.queue.ack(claimed)
    return JSONResponse({"task_id": task_id, "acked": True})


@router.post("/queues/{queue}/tasks/{task_id}/nack")
async def nack(
    queue: str, task_id: str, body: NackRequest, engine: Engine, config: Config, who: Consumer
) -> Response:
    who.require(TASKS_CONSUME, queue)
    claimed = await _held(engine, queue, task_id, body.holder, config.task_ttl)
    await engine.ports.queue.nack(claimed, timedelta(seconds=body.delay_seconds))
    return JSONResponse({"task_id": task_id, "nacked": True, "delay_seconds": body.delay_seconds})


@router.post("/queues/{queue}/tasks/{task_id}/cancel")
async def cancel_task(
    queue: str, task_id: str, engine: Engine, who: Consumer
) -> Response:
    """Ask the holder of a task to stop. It fences nobody (spec 10, section 5)."""
    who.require(TASKS_CONSUME, queue)
    prov = engine.provenance(who.actor, frame_name="cancel")
    await engine.ports.queue.request_cancel(queue, task_id, prov)
    return JSONResponse({"task_id": task_id, "cancel_requested": True})


@router.post("/executions/{eid}/deliver")
async def deliver(
    eid: str,
    body: DeliverRequest,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Consumer,
) -> Response:
    """Answer the frame that waits. The caller must hold the task it answers."""
    who.require(TASKS_CONSUME, body.queue)
    parsed = parse_eid(eid)
    claimed = await _held(engine, body.queue, body.task_id, body.holder, config.task_ttl)
    task = delegate_task(claimed.task)
    # Without this check one consumer could answer the frame of another.
    if body.channel != task["reply_channel"] or str(parsed) != task["eid"]:
        raise Problem(
            403,
            "wrong_channel",
            "Not the reply channel of this task",
            f"{body.task_id} answers on {task['reply_channel']}",
        )
    seq = await engine.deliver(parsed, body.channel, body.payload, by=who.actor)
    headers = {}
    if config.refresh_after_write:
        control = await projection.refresh()
        if control is not None:
            headers[SEQ_HEADER] = str(control)
    return JSONResponse({"seq": seq}, headers=headers)


__all__ = ["router", "task_row"]
