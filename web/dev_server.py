"""A backend for `npm run dev`: an engine, a worker, and the HTTP service.

It is a development fixture, not a deployment. It uses a filesystem bucket
under `.local/`, a static token map in place of an identity provider, and it
runs a worker in the same process so that started executions actually move.

    uv run --extra api python web/dev_server.py        # or: python web/dev_server.py
    npm run dev                                        # in web/

The tokens below are the two principals the interface shows: `dev-operator`
carries every capability, `dev-viewer` only the reads.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from pathlib import Path
from typing import Any

import uvicorn

from flowli.adapters.cairndb import CairnBackend
from flowli.api import ApiConfig, StaticAuthenticator, create_app
from flowli.api.auth import (
    EVIDENCE_READ,
    EXECUTIONS_CANCEL,
    EXECUTIONS_MIGRATE,
    EXECUTIONS_READ,
    EXECUTIONS_SIGNAL,
    EXECUTIONS_START,
    QUEUES_READ,
    REVIEWS_READ,
    WORKFLOWS_READ,
    Principal,
)
from flowli.domain import Actor, Site
from flowli.log import configure_logging, get_logger
from flowli.patterns import delegate, review
from flowli.runtime import Engine, Registry

HERE = Path(__file__).resolve().parent
BUCKET = HERE / ".local" / "bucket"
PROJECTION = HERE / ".local" / "wf_view.sqlite"
QUEUES = ("default", "finance", "agents")

registry = Registry()
log = get_logger("demo")


@registry.workflow("invoice_approval", "3")
async def invoice_approval(ctx: Any, invoice_id: str, amount: int = 100) -> str:
    """Check an invoice, then ask a person when it is large.

    A small invoice settles on its own. Anything over the threshold waits on
    the `finance` queue until somebody decides.
    """
    checked = await ctx.step(_check, invoice_id, amount, name="check")
    log.info("invoice_checked", invoice=invoice_id, amount=amount)
    if amount < 500:
        return f"{checked}:auto"
    decision = await review(ctx, "finance", {"invoice_id": invoice_id, "amount": amount})
    if decision is None:
        return f"{checked}:expired"
    return f"{checked}:{decision.verdict}"


def _check(invoice_id: str, amount: int) -> str:
    log.info("looked_up_invoice", invoice=invoice_id, rows=3)
    return f"{invoice_id}/{amount}"


@registry.workflow("nightly_report", "1")
async def nightly_report(ctx: Any, day: str) -> dict[str, Any]:
    """Gather three numbers, then wait a moment before it publishes."""
    rows = await ctx.step(lambda: 42, name="rows")
    await ctx.sleep(timedelta(seconds=5))
    log.info("publishing", day=day, rows=rows)
    return {"day": day, "rows": rows}


@registry.workflow("flaky_import", "1")
async def flaky_import(ctx: Any, source: str) -> str:
    """Fail twice, then succeed: a frame with three attempts and two failures."""
    state = {"n": 0}

    def attempt() -> str:
        state["n"] += 1
        log.info("importing", source=source, attempt=state["n"])
        if state["n"] < 3:
            raise RuntimeError(f"{source} refused the connection")
        return f"{source}:imported"

    from flowli.domain import RetryPolicy

    return await ctx.step(attempt, name="import", retry=RetryPolicy(max_attempts=3))


@registry.workflow("agent_task", "1")
async def agent_task(ctx: Any, intent: str) -> str:
    """Delegate to whoever consumes the `agents` queue, then wait for the answer."""
    reply = await delegate(ctx, "agents", {"intent": intent}, timeout=timedelta(hours=1))
    return "no answer" if reply is None else str(reply.payload.get("outcome"))


def principals() -> dict[str, Principal]:
    everything = frozenset({
        WORKFLOWS_READ, EXECUTIONS_READ, EXECUTIONS_START, EXECUTIONS_SIGNAL,
        EXECUTIONS_CANCEL, EXECUTIONS_MIGRATE, REVIEWS_READ, QUEUES_READ, EVIDENCE_READ,
        "reviews:decide:*", "tasks:consume:*",
    })
    return {
        "dev-operator": Principal(Actor.human("operator@example.com"), everything),
        "dev-viewer": Principal(
            Actor.human("viewer@example.com"),
            frozenset({WORKFLOWS_READ, EXECUTIONS_READ, REVIEWS_READ}),
        ),
    }


async def main() -> None:
    configure_logging("INFO", "console")
    BUCKET.mkdir(parents=True, exist_ok=True)
    backend = CairnBackend.configure({"storage": {"type": "filesystem", "path": str(BUCKET)}})
    engine = Engine(backend.ports, Site.local("dev"), registry=registry)
    projection = backend.projection(db_path=str(PROJECTION), poll_interval=1.0)

    app = create_app(
        engine,
        authenticator=StaticAuthenticator(principals()),
        projection=projection,
        config=ApiConfig(queues=QUEUES, title="flowli (dev)"),
    )

    stop = asyncio.Event()
    worker = engine.worker(queues=["default"])
    sweeper = engine.sweeper()
    jobs = asyncio.gather(worker.run_forever(stop), _sweep(sweeper, stop))

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8000, log_level="warning"))
    print("api on http://127.0.0.1:8000 — tokens: dev-operator, dev-viewer")
    try:
        await server.serve()
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await jobs
        await backend.close()


async def _sweep(sweeper: Any, stop: asyncio.Event) -> None:
    """Timers fire from here: nothing in the bucket fires at a time by itself."""
    while not stop.is_set():
        with contextlib.suppress(Exception):
            await sweeper.run_once()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=2.0)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
