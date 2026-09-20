"""`flowlet-codeflow`: the two processes of the controller.

    flowlet-codeflow merge --app myapp.flows:registry --repo /srv/repo
    flowlet-codeflow board --app myapp.flows:registry --projection ./wf_view.sqlite

Everything else is a workflow, and a worker runs it (`flowlet worker`).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Callable
from typing import Annotated, Any

import typer
from flowlet import cli_render as render
from flowlet.log import configure_logging, get_logger

from .board import MemoryBoard, Reconciler
from .merge import MergeConfig
from .merge import build_consumer as merge_consumer

app = typer.Typer(add_completion=False, help="The CodeFlow controller's processes.")

log = get_logger("flowlet.codeflow.cli")

LogFormatOpt = Annotated[
    str,
    typer.Option("--log-format", help="console (human) or json (one object per line)."),
]


def _load(app_ref: str, storage_path: str | None, worker_id: str) -> Any:
    from flowlet.cli import build_app

    return build_app(app_ref, storage_path=storage_path, worker_id=worker_id, code_ref=None)


@app.command()
def merge(
    app_ref: Annotated[str, typer.Option("--app")],
    repo: Annotated[str, typer.Option("--repo", help="the integration repository")],
    branch: Annotated[str, typer.Option("--branch")] = "main",
    queue: Annotated[str, typer.Option("--queue")] = "merge",
    holder: Annotated[str, typer.Option("--holder")] = "merge-queue",
    gate: Annotated[list[str], typer.Option("--gate", help="run before publishing")] = None,  # type: ignore[assignment]
    post: Annotated[list[str], typer.Option("--post", help="run after publishing")] = None,  # type: ignore[assignment]
    storage_path: Annotated[str | None, typer.Option("--storage-path")] = None,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
    log_format: LogFormatOpt = "console",
    once: Annotated[bool, typer.Option("--once")] = False,
) -> None:
    """The merge queue: one consumer, so one writer to the integration branch."""
    configure_logging(log_level, log_format)  # type: ignore[arg-type]
    loaded = _load(app_ref, storage_path, holder)
    consumer = merge_consumer(
        loaded.engine,
        MergeConfig(
            repo_path=repo,
            integration_branch=branch,
            gates=tuple(gate or ()),
            post_merge=tuple(post or ()),
        ),
        holder=holder,
        queue=queue,
    )
    log.info("merge_started", holder=holder, queue=queue, branch=branch, repo=repo, once=once)
    _run(
        _serve(
            consumer.run_once if once else consumer.run_forever,
            loaded,
            once=once,
            report=merge_report,
        )
    )


@app.command()
def board(
    app_ref: Annotated[str, typer.Option("--app")],
    projection: Annotated[str, typer.Option("--projection", help="the SQLite view")],
    interval: Annotated[float, typer.Option("--interval")] = 10.0,
    storage_path: Annotated[str | None, typer.Option("--storage-path")] = None,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
    log_format: LogFormatOpt = "console",
    once: Annotated[bool, typer.Option("--once")] = False,
) -> None:
    """The board reconciler. It ships with a board in memory: a deployment
    passes its own tracker instead."""
    configure_logging(log_level, log_format)  # type: ignore[arg-type]
    loaded = _load(app_ref, storage_path, "board")
    if loaded.backend is None:
        raise typer.BadParameter("--app must build a CairnDB backend for the projection")
    view = loaded.backend.projection(db_path=projection)
    reconciler = Reconciler(
        engine=loaded.engine, projection=view, board=MemoryBoard(), interval=interval
    )
    log.info("board_started", projection=projection, interval=interval, once=once)
    _run(
        _serve(
            reconciler.run_once if once else reconciler.run_forever,
            loaded,
            once=once,
            report=board_report,
        )
    )


def merge_report(merged: bool) -> str:
    """The `--once` summary: whether the queue published anything this pass."""
    return render.counters([("merged", int(merged))])


def board_report(reconciled: int) -> str:
    """The `--once` summary: how many executions the board was brought in line with."""
    return render.counters([("reconciled", reconciled)])


def _run(coro: Any) -> None:
    """Stop quietly on Ctrl-C, as every other flowlet job does."""
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:  # pragma: no cover - a signal, not a branch
        pass


async def _serve(
    run: Any, loaded: Any, *, once: bool, report: Callable[[Any], str] | None = None
) -> None:
    try:
        if once:
            result = await run()
            if report is not None:
                typer.echo(report(result))
            return
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
        await run(stop)
    finally:
        await loaded.close()


if __name__ == "__main__":  # pragma: no cover
    app()
