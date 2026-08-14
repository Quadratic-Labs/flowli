"""
Flowlet command-line entrypoints.

One image, three roles — matching the serverless deployment shape:

- ``flowlet work``   — worker loop (Container Apps job, KEDA queue scaler).
- ``flowlet sweep``  — one sweep pass (Container Apps cron job, 1–5 min).
- ``flowlet api``    — FastAPI app via uvicorn (scale-to-zero container).

All three commands import the user's application module (``--app``, e.g.
``myproject.flows:flowlet``) to obtain the configured :class:`~flowlet.app.Flowlet`
instance, so flows, storage, and queue wiring live in exactly one place.
"""
import importlib
import logging
import sys
import time

import click

logger = logging.getLogger(__name__)


# region @cli
# ---
# role: orchestration
# intent: expose work / sweep / api entrypoints over a user-configured Flowlet app
# description: >
#   The CLI resolves a "module:attribute" reference to the user's configured
#   Flowlet instance and runs one of the three deployment roles against it.
#   work runs the lease-based worker loop; a --once flag processes a single
#   message and exits with the job's exit code (Container Apps job semantics).
#   sweep runs one crash-recovery/hygiene pass and exits.  api serves the
#   FastAPI router with uvicorn.
# rules:
#   - Commands MUST exit non-zero on configuration errors (missing queue/storage).
#   - sweep MUST perform exactly one pass; scheduling belongs to the platform.
# dependencies:
#   - app
#   - worker.execute
#   - sweeper
# aliases:
#   - cli
#   - entrypoints
# triggers:
#   - how do I run a worker
#   - how do I run the sweeper
#   - how do I serve the api
# ---


def _load_flowlet(app_ref: str):
    """Resolve a ``module:attribute`` reference to a Flowlet instance.

    Args:
        app_ref: Import string such as ``myproject.flows:flowlet``.

    Returns:
        The configured Flowlet instance.
    """
    module_name, _, attr = app_ref.partition(":")
    if not attr:
        raise click.UsageError("--app must be of the form 'package.module:attribute'")
    module = importlib.import_module(module_name)
    return getattr(module, attr)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(verbose: bool) -> None:
    """Flowlet — lightweight serverless flow orchestration."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)


@main.command()
@click.option("--app", "app_ref", required=True, help="Flowlet instance, e.g. 'myproject.flows:flowlet'.")
@click.option("--worker-id", default=None, help="Worker identity; defaults to hostname+pid.")
@click.option("--once", is_flag=True, help="Process a single message and exit with its code.")
@click.option("--poll-interval", default=2.0, show_default=True, help="Idle sleep between polls (seconds).")
def work(app_ref: str, worker_id: str | None, once: bool, poll_interval: float) -> None:
    """Run the worker: dequeue jobs and execute them under the lease model."""
    from .worker import execute_job

    flowlet = _load_flowlet(app_ref)
    if flowlet.queue is None or flowlet.state_repo is None:
        raise click.ClickException("Worker requires both queue and storage to be configured")

    if worker_id is None:
        import os
        import socket

        worker_id = f"{socket.gethostname()}-{os.getpid()}"

    logger.info("worker_started", extra={"worker_id": worker_id})
    if once:
        sys.exit(
            execute_job(
                flowlet.queue, flowlet.registry, flowlet.state_repo,
                flowlet.signals, worker_id,
                events=flowlet.events,
            )
        )

    try:
        while True:
            rc = execute_job(
                flowlet.queue, flowlet.registry, flowlet.state_repo,
                flowlet.signals, worker_id,
                events=flowlet.events,
            )
            if rc == 2:  # nothing to do — idle politely
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("worker_stopped", extra={"worker_id": worker_id})


@main.command()
@click.option("--app", "app_ref", required=True, help="Flowlet instance, e.g. 'myproject.flows:flowlet'.")
@click.option("--pending-grace", default=None, type=int, help="Seconds before a pending run is re-enqueued.")
@click.option("--archive-grace", default=None, type=int, help="Seconds a closed run stays in state/.")
def sweep(app_ref: str, pending_grace: int | None, archive_grace: int | None) -> None:
    """Run one sweep pass: recover expired leases, archive closed runs."""
    from . import sweeper

    flowlet = _load_flowlet(app_ref)
    if flowlet.queue is None or flowlet.state_repo is None:
        raise click.ClickException("Sweeper requires both queue and storage to be configured")

    kwargs = {}
    if pending_grace is not None:
        kwargs["pending_grace"] = pending_grace
    if archive_grace is not None:
        kwargs["archive_grace"] = archive_grace
    stats = sweeper.sweep(
        flowlet.state_repo, flowlet.queue, signals=flowlet.signals,
        history=flowlet.history, events=flowlet.events, **kwargs
    )
    click.echo(
        f"scanned={stats.scanned} requeued={stats.requeued} "
        f"failed={stats.failed} archived={stats.archived} errors={stats.errors}"
    )
    sys.exit(1 if stats.errors else 0)


@main.command()
@click.option("--app", "app_ref", required=True, help="Flowlet instance, e.g. 'myproject.flows:flowlet'.")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
@click.option("--prefix", default="/flowlet", show_default=True, help="Router mount prefix.")
def api(app_ref: str, host: str, port: int, prefix: str) -> None:
    """Serve the Flowlet HTTP API with uvicorn."""
    import uvicorn
    from fastapi import FastAPI

    flowlet = _load_flowlet(app_ref)
    fastapi_app = FastAPI(lifespan=flowlet.lifespan)
    fastapi_app.include_router(flowlet.router, prefix=prefix)
    uvicorn.run(fastapi_app, host=host, port=port)

# ---
# endregion
