"""Workflows for the concurrency tests. Importable by CLI subprocesses too."""

import asyncio
from datetime import timedelta

from flowli.runtime import Registry

registry = Registry()

STEP_SLEEP = 0.02  # seconds of real work per step, so tasks overlap across workers


async def work(tag: str, x: int) -> str:
    await asyncio.sleep(STEP_SLEEP)
    return f"{tag}:{x}"


@registry.workflow("square", "1")
async def square(ctx, x):
    await ctx.step(work, "child", x, name="child_work")
    return x * x


@registry.workflow("pipeline", "1")
async def pipeline(ctx, x):
    """Steps, a timer, a child, a human-like approval: every kind of suspension."""
    a = await ctx.step(work, "a", x, name="a")
    b = await ctx.step(work, "b", x, name="b")
    await ctx.sleep(timedelta(seconds=0.2))
    sq = await ctx.child(square, x, key="sq")
    approval = await ctx.receive("approve")
    return {"a": a, "b": b, "sq": sq, "approved_by": approval.sent_by.actor.id}


@registry.workflow("simple", "1")
async def simple(ctx, x):
    """No receive: completes with workers and a sweeper only (used across processes)."""
    a = await ctx.step(work, "a", x, name="a")
    await ctx.sleep(timedelta(seconds=0.2))
    b = await ctx.step(work, "b", x, name="b")
    sq = await ctx.child(square, x, key="sq")
    return [a, b, sq]


@registry.workflow("slow_step", "1")
async def slow_step(ctx, seconds):
    """One long step: lets a lease expire under a worker that is still running it."""
    v = await ctx.step(asyncio.sleep, seconds, name="long")
    return "done" if v is None else v
