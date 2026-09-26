"""Saga: run actions in order, undo the completed ones in reverse on failure."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from flowli.domain import RetryPolicy
from flowli.runtime import Context

Action = Callable[..., Any]


async def saga(
    ctx: Context,
    actions: list[tuple[Action, Action]],
    *,
    name: str = "saga",
    undo_retry: RetryPolicy | None = None,
) -> list[Any]:
    """Each (act, undo) pair runs as steps `{name}-act:{i}` and `{name}-undo:{i}`.

    `undo` receives the value `act` returned. On failure the compensations run in
    reverse order, then the error is raised again. A replay never runs an action or
    a compensation twice.
    """
    undo_retry = undo_retry or RetryPolicy(max_attempts=5)
    done: list[tuple[int, Action, Any]] = []
    results: list[Any] = []
    try:
        for i, (act, undo) in enumerate(actions):
            value = await ctx.step(act, name=f"{name}-act", key=str(i))
            done.append((i, undo, value))
            results.append(value)
    except Exception:
        for i, undo, value in reversed(done):
            await ctx.step(undo, value, name=f"{name}-undo", key=str(i), retry=undo_retry)
        raise
    return results
