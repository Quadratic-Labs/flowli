"""Review: a delegate to a human queue, plus announcements for the inbox projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from cairndb import Timestamp

from flowlet.codec import structure, unstructure
from flowlet.domain import Actor, Eid, Task, TaskKind
from flowlet.runtime import Context, Engine

from .delegate import DelegateTask, delegate


@dataclass(frozen=True, slots=True)
class Decision:
    verdict: str
    data: Any
    by: Actor
    at: Timestamp


def review_task_key(rid: str) -> str:
    """The dedup key of a review's DELEGATE task: `review:{rid}`."""
    return f"review:{rid}"


async def review(
    ctx: Context,
    queue: str,
    payload: Any,
    *,
    timeout: timedelta | None = None,
    name: str = "review",
    key: str | None = None,
) -> Decision | None:
    """Ask the humans on `queue`. Return their Decision, or None when `timeout` expires.

    `name` names this review's frames. Two reviews under one frame with one
    key need two names, or their frames collide.
    """
    rid = await ctx.uuid()
    deadline = None if timeout is None else (await ctx.now() + timeout).to_iso()
    await ctx.announce(
        "review.requested",
        {"rid": rid, "eid": str(ctx.eid), "queue": queue, "payload": payload, "deadline": deadline},
    )
    reply = await delegate(
        ctx,
        queue,
        {"rid": rid, "payload": payload, "deadline": deadline},
        timeout=timeout,
        name=name,
        key=key or rid,
        task_key=review_task_key(rid),
    )
    if reply is None:
        await ctx.announce("review.expired", {"rid": rid})
        return None
    decision = structure(reply.payload, Decision)
    await ctx.announce(
        "review.decided", {"rid": rid, "verdict": decision.verdict, "by": unstructure(decision.by)}
    )
    return decision


class Reviews:
    """Operator side. `engine.reviews.decide(...)`."""

    def __init__(self, engine: Engine, *, take_ttl: float = 30.0) -> None:
        self.engine = engine
        self.take_ttl = take_ttl

    async def decide(
        self, rid: str, *, eid: Eid, queue: str, verdict: str, by: Actor, data: Any = None
    ) -> Decision:
        """Record exactly one decision for `rid`, deliver it to the workflow, remove the task.

        `eid` and `queue` come from the inbox row (`review.requested` carries both).

        Idempotent: a second call returns the first decision. A call after a crash between
        the claim and the delivery completes the delivery.

        The delivery does not wait for the task. A task that another consumer holds, or that
        a worker pushed into the future with a nack, would otherwise swallow a decision that
        is already recorded. A repeat sends the same payload again, and the receive frame
        consumes the first message only.
        """
        mine = Decision(verdict, data, by, self.engine.clock())
        won, value = await self.engine.ports.dispatch.claim(
            f"reviews/{rid}/decision", unstructure(mine)
        )
        decision = mine if won else structure(value, Decision)  # pragma: no mutate

        task_id = Task.id_for(TaskKind.DELEGATE, eid, review_task_key(rid))
        task = await self.engine.ports.queue.peek(queue, task_id)
        if task is None:
            return decision  # already delivered and acked, or withdrawn on a timeout

        target = DelegateTask.from_task_payload(task.payload)
        await self.engine.deliver(
            target.eid, target.reply_channel, unstructure(decision), by=decision.by
        )
        claimed = await self.engine.ports.queue.take(
            queue, task_id, f"decide:{by.id}", self.take_ttl
        )  # pragma: no mutate
        if claimed is not None:
            await self.engine.ports.queue.ack(claimed)
        return decision
