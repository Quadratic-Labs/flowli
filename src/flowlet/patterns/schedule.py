"""Scheduled start: one execution per tick, exactly once, via the dispatch key."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cairndb import Timestamp

from flowlet.domain import Actor, Eid
from flowlet.runtime import Engine


async def on_tick(
    engine: Engine,
    workflow_fn: Callable[..., Any],
    schedule_name: str,
    tick: Timestamp,
    /,
    *args: Any,
    queue: str | None = None,
    **kwargs: Any,
) -> Eid:
    """Start `workflow_fn` for `tick`. Two schedulers that fire the same tick get the same eid."""
    return await engine.start(
        workflow_fn,
        *args,
        key=f"{schedule_name}:{tick.to_iso()}",
        queue=queue,
        by=Actor.schedule(schedule_name),
        **kwargs,
    )
