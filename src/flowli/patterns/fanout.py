"""Fan-out: one child execution per item, gathered in argument order."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from flowli.runtime import Context


async def fan_out(
    ctx: Context,
    child_fn: Callable[..., Any],
    items: list[Any],
    *,
    name: str | None = None,
    queue: str = "default",
) -> list[Any]:
    """Start a child per item, keyed by position. Return the values in item order."""
    return await ctx.gather(
        *[
            ctx.child(child_fn, item, name=name, key=str(i), queue=queue)
            for i, item in enumerate(items)
        ]
    )
