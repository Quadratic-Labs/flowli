"""Delegate: put a task on a queue, wait for the answer on a channel.

The consumer of the queue can be a worker pool, a group of humans, or an external
system. It answers with `engine.deliver(eid, reply_channel, result, by=...)`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from flowli.codec import unstructure
from flowli.domain import DelegateTask, FrameRef, Message, Task, TaskKind
from flowli.runtime import Context
from flowli.runtime.context import Scope


def delegate_frame(name: str, key: str | None) -> str:
    return f"{name}:{key}" if key else name


async def delegate(
    ctx: Context,
    queue: str,
    payload: Any,
    *,
    timeout: timedelta | None = None,
    name: str = "delegate",
    key: str | None = None,
    task_key: str | None = None,
) -> Message | None:
    """Enqueue once, then receive the reply. None when `timeout` expires first.

    Frames: `{name}-enqueue`, `{name}-receive`, and `{name}-withdraw` on timeout,
    all under the caller's frame with `key`.
    """
    frame = delegate_frame(name, key)
    ref = FrameRef(ctx.eid, ctx.fid)
    channel = ref.reply_channel(frame)
    task_key = task_key or frame
    tid = Task.id_for(TaskKind.DELEGATE, ctx.eid, task_key)
    # The frame that will wait. Its id follows the same rule as the reply
    # channel, which already assumes one delegate per (name, key) under a
    # parent (`06-patterns.md`, section 1).
    reply_fid = f"{ctx.fid}/{name}-receive" + (f":{key}" if key else "#0")
    await ctx.enqueue(
        queue,
        {
            "target": unstructure(ref),
            "reply_channel": channel,
            "reply_fid": reply_fid,
            "payload": payload,
        },
        task_key=task_key,
        name=f"{name}-enqueue",
        key=key,
    )
    # "global": any consumer may answer, not only this execution. `_channel_name`
    # only special-cases "execution", so any other value behaves the same -- the
    # literal itself is not observable and is pinned here, not spread over the
    # multi-line call below where a per-argument pragma cannot reach it.
    scope: Scope = "global"  # pragma: no mutate
    reply = await ctx.receive(
        channel, scope=scope, timeout=timeout, name=f"{name}-receive", key=key
    )
    if reply is None:
        await ctx.withdraw(queue, tid, name=f"{name}-withdraw", key=key)
    return reply


# Re-exported: `06-patterns.md` names it here, the domain owns it.
__all__ = ["DelegateTask", "delegate", "delegate_frame"]
