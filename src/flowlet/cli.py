"""Command line: run a worker, the sweeper, or the retention job.

    flowlet worker    --app myapp.flows:registry --queue default --queue slow
    flowlet sweeper   --app myapp.flows:registry --interval 60
    flowlet retention --app myapp.flows:registry --delay-days 30 --once

    flowlet status  EID --app ...  [--journal]
    flowlet signal  EID CHANNEL PAYLOAD --app ... --by thomas@example.com
    flowlet cancel  EID --app ... --by ...
    flowlet migrate EID VERSION --app ... --by ...

`--app module:attr` names a `Registry`, an `Engine`, or a zero-argument callable
that returns one. With a `Registry`, the CLI builds a CairnDB backend from the
`CAIRNDB_*` environment variables (see cairndb.storage.config) or `--storage-path`,
and an `Engine` on top of it. With an `Engine`, the CLI uses it as is.

Every job stops cleanly on SIGINT or SIGTERM. `--once` runs one pass and exits.
"""

# region ----- Imports -----

from __future__ import annotations

import asyncio
import contextlib
import getpass
import importlib
import json
import os
import signal
import socket
from collections.abc import Callable
from datetime import timedelta
from typing import Annotated, Any

import typer

from flowlet import cli_render as render
from flowlet.domain import Actor, Eid, InvalidName, parse_eid
from flowlet.log import LogFormat, configure_logging, get_logger
from flowlet.runtime import Engine, EngineConfig, Registry, UnknownExecution
from flowlet.runtime.sweeper import ControlSource

# endregion

# region ----- CLI definition -----

app = typer.Typer(
    name="flowlet",
    help="Run flowlet jobs: worker, sweeper, retention.",
    no_args_is_help=True,
    add_completion=False,
)

log = get_logger("flowlet.cli")

AppOpt = Annotated[
    str,
    typer.Option(
        "--app",
        "-a",
        envvar="FLOWLET2_APP",
        help="module:attr naming a Registry, an Engine, or a callable returning one.",
    ),
]
StoragePathOpt = Annotated[
    str | None,
    typer.Option(
        "--storage-path",
        envvar="CAIRNDB_STORAGE_PATH",
        help="Filesystem bucket path. Other backends: set CAIRNDB_* variables.",
    ),
]
LogLevelOpt = Annotated[str, typer.Option("--log-level", envvar="FLOWLET2_LOG_LEVEL")]
LogFormatOpt = Annotated[
    str,
    typer.Option(
        "--log-format",
        envvar="FLOWLET2_LOG_FORMAT",
        help="console (human) or json (one object per line).",
    ),
]
OnceOpt = Annotated[bool, typer.Option("--once", help="Run one pass, then exit.")]
CodeRefOpt = Annotated[
    str | None,
    typer.Option(
        "--code-ref", envvar="FLOWLET2_CODE_REF", help="Git sha or image digest for provenance."
    ),
]
ByOpt = Annotated[
    str | None,
    typer.Option(
        "--by",
        envvar="FLOWLET2_BY",
        help="Actor of the operation: an email, or kind:id (human, system, schedule).",
    ),
]
ProjectionOpt = Annotated[
    str | None,
    typer.Option(
        "--projection",
        help="Path of the SQLite projection to read statuses from. Default: fold the control log.",
    ),
]

# endregion

# region ----- app loading -----

def load_ref(ref: str) -> Any:
    """Import 'module.path:attribute'."""
    module_path, sep, attribute = ref.partition(":")
    if not sep or not module_path or not attribute:
        raise typer.BadParameter(f"expected 'module:attribute', got {ref!r}")
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise typer.BadParameter(f"cannot import {module_path!r}: {exc}") from exc
    obj: Any = module
    for part in attribute.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            raise typer.BadParameter(f"{module_path!r} has no attribute {attribute!r}") from None
    return obj


class LoadedApp:
    """The Engine to run, plus what must be closed on exit."""

    def __init__(self, engine: Engine, closers: list[Callable[[], Any]]) -> None:
        self.engine = engine
        self._closers = closers
        self.backend: Any = None

    async def close(self) -> None:
        for closer in reversed(self._closers):
            result = closer()
            if asyncio.iscoroutine(result):
                await result


def build_app(
    ref: str,
    *,
    storage_path: str | None,
    worker_id: str | None,
    code_ref: str | None,
    config: EngineConfig | None = None,
) -> LoadedApp:
    from flowlet.adapters.cairndb import CairnBackend
    from flowlet.domain import Site

    obj = load_ref(ref)
    if callable(obj) and not isinstance(obj, Engine | Registry):
        obj = obj()
    if isinstance(obj, Engine):
        return LoadedApp(obj, [])
    if not isinstance(obj, Registry):
        raise typer.BadParameter(
            f"{ref!r} is a {type(obj).__name__}, expected a Registry, an Engine, or a callable"
        )

    from cairndb import CairnDB
    from cairndb.storage.config import FilesystemStorageConfig, StorageConfig

    storage: StorageConfig
    if storage_path is not None:
        storage = FilesystemStorageConfig(path=storage_path)
    else:
        storage = StorageConfig.from_env()
    backend = CairnBackend(CairnDB(storage.create_storage()))
    site = Site.local(worker_id or default_worker_id(), instance=os.environ.get("HOSTNAME"))
    engine = Engine(
        backend.ports,
        site,
        registry=obj,
        config=config or EngineConfig(code_ref=code_ref),
    )
    loaded = LoadedApp(engine, [backend.close])
    loaded.backend = backend
    return loaded


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def source_for(loaded: LoadedApp, projection_path: str | None) -> ControlSource | None:
    if projection_path is None:
        return None
    if loaded.backend is None:
        raise typer.BadParameter("--projection needs a Registry app (the CLI owns the backend)")
    from flowlet.adapters.cairndb_projection import WorkflowProjection

    projection: WorkflowProjection = loaded.backend.projection(db_path=projection_path)
    return projection


def setup_logging(level: str, fmt: str = "console") -> None:
    if fmt not in ("console", "json"):
        raise typer.BadParameter(f"--log-format must be console or json, got {fmt!r}")
    log_format: LogFormat = "json" if fmt == "json" else "console"
    configure_logging(level, log_format)


def stop_event() -> asyncio.Event:
    """An Event set on SIGINT or SIGTERM."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
    return stop


def run(coro: Any) -> None:
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        pass


def parse_actor(text: str | None) -> Actor:
    """'thomas@x.io' -> human. 'system:cron' -> system. Default: the local user."""
    if not text:
        return Actor.human(f"{getpass.getuser()}@{socket.gethostname()}")
    kind, sep, ident = text.partition(":")
    if not sep:
        return Actor.human(text)
    match kind:
        case "human":
            return Actor.human(ident)
        case "system":
            return Actor.system(ident)
        case "schedule":
            return Actor.schedule(ident)
        case "worker":
            return Actor.worker(ident)
    raise typer.BadParameter(f"unknown actor kind {kind!r} in {text!r}")


def eid_arg(text: str) -> Eid:
    try:
        return parse_eid(text)
    except InvalidName as exc:
        raise typer.BadParameter(str(exc)) from None


def parse_payload(text: str) -> Any:
    """JSON when it parses, else the raw string."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def operator_main(app_ref: str, storage_path: str | None, fn: Callable[[Engine], Any]) -> None:
    """Load the app, run fn(engine), close. Exit 1 on an unknown execution or a refused action."""

    async def main() -> None:
        loaded = build_app(app_ref, storage_path=storage_path, worker_id=None, code_ref=None)
        try:
            await fn(loaded.engine)
        except (UnknownExecution, ValueError) as exc:
            render.error(str(exc))
            raise typer.Exit(code=1) from None
        finally:
            await loaded.close()

    run(main())

# endregion

# region ----- commands -----

@app.command()
def worker(
    app_ref: AppOpt,
    queue: Annotated[
        list[str] | None, typer.Option("--queue", "-q", help="Queue to poll. Repeatable.")
    ] = None,
    worker_id: Annotated[
        str | None, typer.Option("--worker-id", envvar="FLOWLET2_WORKER_ID")
    ] = None,
    exec_ttl: Annotated[float, typer.Option(help="Execution lease TTL, seconds.")] = 120.0,
    task_ttl: Annotated[float, typer.Option(help="Task lease TTL, seconds.")] = 60.0,
    poll_interval: Annotated[float, typer.Option(help="Idle sleep between polls, seconds.")] = 1.0,
    once: OnceOpt = False,
    storage_path: StoragePathOpt = None,
    code_ref: CodeRefOpt = None,
    log_level: LogLevelOpt = "INFO",
    log_format: LogFormatOpt = "console",
) -> None:
    """Dequeue tasks and run executions until stopped."""
    setup_logging(log_level, log_format)
    config = EngineConfig(
        exec_ttl=exec_ttl, task_ttl=task_ttl, poll_interval=poll_interval, code_ref=code_ref
    )

    async def main() -> None:
        loaded = build_app(
            app_ref,
            storage_path=storage_path,
            worker_id=worker_id,
            code_ref=code_ref,
            config=config,
        )
        try:
            queues = queue or [loaded.engine.config.default_queue]
            w = loaded.engine.worker(queues=queues, worker_id=worker_id)
            log.info("worker_started", worker_id=w.worker_id, queues=queues, once=once)
            if once:
                processed = await w.run_once()
                log.info("worker_pass_done", worker_id=w.worker_id, processed=processed)
            else:
                await w.run_forever(stop_event())
                log.info("worker_stopped", worker_id=w.worker_id)
        finally:
            await loaded.close()

    run(main())


@app.command()
def sweeper(
    app_ref: AppOpt,
    interval: Annotated[float, typer.Option(help="Seconds between sweeps.")] = 60.0,
    repair_window: Annotated[
        float, typer.Option(help="Seconds before a repair is attempted.")
    ] = 60.0,
    projection: ProjectionOpt = None,
    once: OnceOpt = False,
    storage_path: StoragePathOpt = None,
    code_ref: CodeRefOpt = None,
    log_level: LogLevelOpt = "INFO",
    log_format: LogFormatOpt = "console",
) -> None:
    """Fire timers, recover dead executions, repair the control log, clear orphan waits."""
    setup_logging(log_level, log_format)

    async def main() -> None:
        loaded = build_app(app_ref, storage_path=storage_path, worker_id=None, code_ref=code_ref)
        try:
            s = loaded.engine.sweeper(
                repair_window=timedelta(seconds=repair_window),
                source=source_for(loaded, projection),
            )
            if once:
                report = await s.run_once()
                typer.echo(
                    render.counters(
                        [
                            ("timers_fired", len(report.timers_fired)),
                            ("recovered", len(report.recovered)),
                            ("restarted", len(report.restarted)),
                            ("repaired", len(report.repaired)),
                            ("waits_cleared", len(report.waits_cleared)),
                        ]
                    )
                )
            else:
                await s.run_forever(stop_event(), interval=interval)
        finally:
            await loaded.close()

    run(main())


@app.command()
def retention(
    app_ref: AppOpt,
    delay_days: Annotated[
        float, typer.Option(help="Archive terminal executions older than this.")
    ] = 30.0,
    interval: Annotated[float, typer.Option(help="Seconds between runs.")] = 3600.0,
    projection: ProjectionOpt = None,
    once: OnceOpt = False,
    storage_path: StoragePathOpt = None,
    code_ref: CodeRefOpt = None,
    log_level: LogLevelOpt = "INFO",
    log_format: LogFormatOpt = "console",
) -> None:
    """Archive finished executions and delete their live state."""
    setup_logging(log_level, log_format)

    async def main() -> None:
        loaded = build_app(app_ref, storage_path=storage_path, worker_id=None, code_ref=code_ref)
        try:
            job = loaded.engine.retention(
                delay=timedelta(days=delay_days), source=source_for(loaded, projection)
            )
            if once:
                report = await job.run_once()
                typer.echo(
                    render.counters(
                        [("archived", len(report.archived)), ("cleaned", len(report.cleaned))]
                    )
                )
                for eid in report.archived:
                    label = render.paint("archived", dim=True)
                    typer.echo(f"  {label} {render.paint(str(eid), bold=True)}")
            else:
                await job.run_forever(stop_event(), interval=interval)
        finally:
            await loaded.close()

    run(main())

# endregion

# region ----- operator commands -----

@app.command()
def status(
    eid: Annotated[str, typer.Argument(help="Execution id.")],
    app_ref: AppOpt,
    journal: Annotated[
        bool, typer.Option("--journal", "-j", help="Also list the journal.")
    ] = False,
    storage_path: StoragePathOpt = None,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Show an execution: status, definition, creator, and optionally its journal."""
    setup_logging(log_level)
    eid_ = eid_arg(eid)

    async def show(engine: Engine) -> None:
        entries = await engine.journal(eid_)
        try:
            execution = await engine.execution(eid_)
            state = (await engine.status(eid_)).value
        except UnknownExecution:
            archive = await engine.archive(eid_)
            if archive is None:
                raise
            execution = None
            state = f"archived ({archive.get('archived_at')})"
        typer.echo(render.line("eid", render.paint(eid, bold=True)))
        typer.echo(render.line("status", render.status_value(state)))
        if execution is not None:
            typer.echo(
                render.line(
                    "workflow", render.workflow_value(execution.workflow, execution.version)
                )
            )
            typer.echo(render.field("queue", execution.queue))
            created = execution.created_by
            typer.echo(
                render.line("created", render.paint(created.at.to_iso(), dim=True))
                + " by "
                + render.actor_value(created.actor.kind, created.actor.id)
            )
            if execution.parent is not None:
                typer.echo(render.field("parent", f"{execution.parent.eid} {execution.parent.fid}"))
            if execution.dispatch_key:
                typer.echo(render.field("key", execution.dispatch_key, bold=True))
        if entries:
            last = entries[-1].item
            site, at = last.provenance.site, last.provenance.at
            typer.echo(
                render.line("last", render.event_value(last.type))
                + render.paint(f" at {at.to_iso()}", dim=True)
                + " by "
                + render.actor_value(last.provenance.actor.kind, last.provenance.actor.id)
                + render.paint(f" on {site.host} epoch {site.epoch}", dim=True)
            )
            if last.type == "execution.suspended":
                typer.echo(
                    render.field("waiting", ", ".join(last.payload.get("on", [])), bold=True)
                )
            elif last.payload:
                typer.echo(render.paint("payload", dim=True))
                typer.echo(render.json_block(last.payload))
        if journal:
            typer.echo("")
            typer.echo(render.journal_header())
            for s in entries:
                e = s.item
                typer.echo(
                    render.journal_row(
                        s.seq,
                        e.type,
                        e.fid,
                        e.provenance.at.to_iso(),
                        e.provenance.actor.kind,
                        e.provenance.actor.id,
                    )
                )

    operator_main(app_ref, storage_path, show)


@app.command("signal")
def signal_command(
    eid: Annotated[str, typer.Argument(help="Execution id.")],
    channel: Annotated[str, typer.Argument(help="Channel name, scoped to the execution.")],
    payload: Annotated[str, typer.Argument(help="JSON payload, or a plain string.")],
    app_ref: AppOpt,
    by: ByOpt = None,
    correlation: Annotated[str | None, typer.Option("--correlation")] = None,
    storage_path: StoragePathOpt = None,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Send a message on an execution's channel and enqueue its resume."""
    setup_logging(log_level)
    actor = parse_actor(by)
    value = parse_payload(payload)
    eid_ = eid_arg(eid)

    async def send(engine: Engine) -> None:
        await engine.execution(eid_)  # UnknownExecution -> exit 1
        seq = await engine.signal(eid_, channel, value, by=actor, correlation=correlation)
        typer.echo(
            "sent "
            + render.paint(f"seq={seq}", bold=True)
            + f" on {eid}.{channel} by "
            + render.actor_value(actor.kind, actor.id)
        )
        typer.echo(render.paint("payload", dim=True))
        typer.echo(render.json_block(value))

    operator_main(app_ref, storage_path, send)


@app.command()
def cancel(
    eid: Annotated[str, typer.Argument(help="Execution id.")],
    app_ref: AppOpt,
    by: ByOpt = None,
    storage_path: StoragePathOpt = None,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Request the cancellation of an execution. A worker applies it at the next frame."""
    setup_logging(log_level)
    actor = parse_actor(by)
    eid_ = eid_arg(eid)

    async def do(engine: Engine) -> None:
        current = await engine.status(eid_)
        if current.is_terminal:
            raise ValueError(f"execution {eid} is already {current.value}")
        await engine.cancel(eid_, by=actor)
        typer.echo(
            render.paint("cancel requested", fg="yellow", bold=True)
            + f" for {eid} by "
            + render.actor_value(actor.kind, actor.id)
        )

    operator_main(app_ref, storage_path, do)


@app.command()
def migrate(
    eid: Annotated[str, typer.Argument(help="Execution id.")],
    version: Annotated[str, typer.Argument(help="Target workflow version.")],
    app_ref: AppOpt,
    by: ByOpt = None,
    storage_path: StoragePathOpt = None,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Point a live execution at another workflow version, then enqueue its resume."""
    setup_logging(log_level)
    actor = parse_actor(by)
    eid_ = eid_arg(eid)

    async def do(engine: Engine) -> None:
        before = await engine.execution(eid_)
        try:
            engine.registry.get(before.workflow, version)
        except Exception:
            render.warning(f"{before.workflow} v{version} is not registered in this app")
        after = await engine.migrate(eid_, version, by=actor)
        typer.echo(
            render.paint("migrated", fg="green", bold=True)
            + f" {eid}: "
            + render.paint(before.workflow, fg="cyan")
            + " "
            + render.paint(f"v{before.version}", dim=True)
            + " -> "
            + render.paint(f"v{after.version}", bold=True)
            + " by "
            + render.actor_value(actor.kind, actor.id)
        )

    operator_main(app_ref, storage_path, do)

# endregion

if __name__ == "__main__":
    app()
