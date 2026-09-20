"""`flowlet-runner`: one process that consumes agent tasks.

    flowlet-runner --app myapp.flows:registry --queue agents \
        --repo /srv/repo --runner-id runner-a --command 'claude -p {intent}'

`--runner-id` is the holder, and it must survive a restart: a runner that
forgets it cannot attach to what it still holds (spec 10, section 4.3).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import Annotated, Any

import typer
from flowlet import cli_render as render
from flowlet.log import configure_logging, get_logger
from flowlet.runtime import ConsumerConfig

from .adapters import ServiceAgent, SubprocessAgent
from .runner import RunnerConfig, build_consumer

app = typer.Typer(add_completion=False, help="Run coding agents as delegate consumers.")

log = get_logger("flowlet.runner.cli")


def _load(app_ref: str, storage_path: str | None, runner_id: str) -> Any:
    """`module:attr` naming a Registry, an Engine, or a callable returning one.

    The engine CLI already knows how to build one, so this borrows it.
    """
    from flowlet.cli import build_app

    return build_app(app_ref, storage_path=storage_path, worker_id=runner_id, code_ref=None)


@app.command()
def run(
    app_ref: Annotated[str, typer.Option("--app", help="module:attr of a Registry or Engine")],
    repo: Annotated[str, typer.Option("--repo", help="the integration repository")],
    runner_id: Annotated[str, typer.Option("--runner-id", help="the stable holder")],
    queue: Annotated[list[str], typer.Option("--queue")] = None,  # type: ignore[assignment]
    command: Annotated[str | None, typer.Option("--command", help="the agent command")] = None,
    service_url: Annotated[str | None, typer.Option("--service-url")] = None,
    service_token: Annotated[str | None, typer.Option("--service-token")] = None,
    base_ref: Annotated[str, typer.Option("--base-ref")] = "HEAD",
    storage_path: Annotated[str | None, typer.Option("--storage-path")] = None,
    ttl: Annotated[float, typer.Option("--ttl", help="task lease seconds")] = 300.0,
    grace: Annotated[float, typer.Option("--grace", help="interrupt budget seconds")] = 120.0,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
    log_format: Annotated[str, typer.Option("--log-format")] = "console",
    once: Annotated[bool, typer.Option("--once", help="one task, then exit")] = False,
) -> None:
    configure_logging(log_level, log_format)  # type: ignore[arg-type]
    loaded = _load(app_ref, storage_path, runner_id)
    engine = loaded.engine
    adapter = (
        ServiceAgent(base_url=service_url, token=service_token)
        if service_url
        else SubprocessAgent()
    )
    consumer = build_consumer(
        engine,
        adapter,
        RunnerConfig(repo_path=repo, base_ref=base_ref),
        ConsumerConfig(
            queues=tuple(queue or ["agents"]),
            holder=runner_id,
            ttl=ttl,
            grace=grace,
        ),
        db=getattr(loaded.backend, "db", None),
    )
    if command:
        # A command given here is the default when an envelope names none.
        consumer.handler.default_command = command  # type: ignore[attr-defined]

    queues = list(queue or ["agents"])
    log.info("runner_started", runner_id=runner_id, queues=queues, repo=repo, once=once)
    _run(_serve(consumer, loaded, once=once))


def once_report(recovered: list[str], processed: bool) -> str:
    """The `--once` summary: what the runner reattached to, and whether it ran a task."""
    return render.counters([("recovered", len(recovered)), ("processed", int(processed))])


def _run(coro: Any) -> None:
    """Stop quietly on Ctrl-C, as every other flowlet job does."""
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:  # pragma: no cover - a signal, not a branch
        pass


async def _serve(consumer: Any, loaded: Any, *, once: bool) -> None:
    try:
        if once:
            recovered = await consumer.recover()
            processed = await consumer.run_once()
            typer.echo(once_report(recovered, processed))
            for task_id in recovered:
                label = render.paint("attached", dim=True)
                typer.echo(f"  {label} {render.paint(str(task_id), bold=True)}")
            return
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
        await consumer.run_forever(stop)
        log.info("runner_stopped", runner_id=loaded.engine.site.worker_id)
    finally:
        await loaded.close()


if __name__ == "__main__":  # pragma: no cover
    app()
