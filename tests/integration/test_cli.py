"""The flowlet CLI over a filesystem bucket, driven through typer's CliRunner."""

import asyncio
import json
import os
import time
from datetime import timedelta

import pytest
import typer
from typer.testing import CliRunner

from flowlet.adapters.cairndb import CairnBackend
from flowlet.cli import app, build_app, load_ref
from flowlet.domain import Actor, ExecutionStatus, Site
from flowlet.runtime import Engine
from tests.ids import E_NOPE

from . import sample_app

APP = "tests.integration.sample_app:registry"
HUMAN = Actor.human("thomas@example.com")
runner = CliRunner()


@pytest.fixture
def bucket(tmp_path) -> str:
    return str(tmp_path / "bucket")


def cli(*args: str, bucket: str) -> tuple[int, str]:
    result = runner.invoke(app, [*map(str, args), "--app", APP, "--storage-path", bucket])
    return result.exit_code, result.output + (str(result.exception) if result.exception else "")


async def with_backend(bucket: str, fn):
    """Run fn(engine, backend) over a fresh backend on the bucket, then close it."""
    backend = CairnBackend.configure({"storage": {"type": "filesystem", "path": bucket}})
    engine = Engine(backend.ports, Site.local("test"), registry=sample_app.registry)
    try:
        return await fn(engine, backend)
    finally:
        await backend.close()


# --- app loading ---------------------------------------------------------------------


def test_load_ref_variants(bucket):
    assert load_ref(APP) is sample_app.registry
    assert load_ref("os:path.sep") == os.path.sep  # dotted attribute path
    for bad in ["nocolon", ":attr", "mod:"]:
        with pytest.raises(typer.BadParameter) as exc_info:
            load_ref(bad)
        assert str(exc_info.value) == f"expected 'module:attribute', got {bad!r}"

    with pytest.raises(typer.BadParameter) as exc_info:
        load_ref("no.such.module:x")
    assert str(exc_info.value).startswith("cannot import 'no.such.module':")

    with pytest.raises(typer.BadParameter) as exc_info:
        load_ref("tests.integration.sample_app:missing")
    assert str(exc_info.value) == "'tests.integration.sample_app' has no attribute 'missing'"

    # only the FIRST ":" separates module from attribute -- not the last.
    with pytest.raises(typer.BadParameter) as exc_info:
        load_ref("os:path:sep")
    assert str(exc_info.value) == "'os' has no attribute 'path:sep'"


def test_build_app_from_registry_engine_and_callable(bucket, monkeypatch):
    monkeypatch.setenv("HOSTNAME", "test-host-42")
    loaded = build_app(APP, storage_path=bucket, worker_id="w-x", code_ref="git:1")
    assert loaded.engine.registry is sample_app.registry
    assert loaded.engine.site.worker_id == "w-x" and loaded.engine.config.code_ref == "git:1"
    assert loaded.engine.site.instance == "test-host-42"
    assert loaded.backend is not None
    asyncio.run(loaded.close())

    loaded2 = build_app(
        "tests.integration.sample_app:make_engine", storage_path=None, worker_id=None, code_ref=None
    )
    assert isinstance(loaded2.engine, Engine) and loaded2.backend is None
    asyncio.run(loaded2.close())  # must not crash: an Engine app still has (empty) closers

    with pytest.raises(typer.BadParameter) as exc_info:
        build_app(
            "tests.integration.sample_app:not_an_app",
            storage_path=bucket,
            worker_id=None,
            code_ref=None,
        )
    assert str(exc_info.value) == (
        "'tests.integration.sample_app:not_an_app' is a int, "
        "expected a Registry, an Engine, or a callable"
    )


def test_build_app_uses_env_storage_config_when_no_storage_path(bucket, monkeypatch):
    monkeypatch.setenv("CAIRNDB_STORAGE_PATH", bucket)
    loaded = build_app(APP, storage_path=None, worker_id=None, code_ref=None)
    assert loaded.backend is not None
    asyncio.run(loaded.close())


def test_source_for_variants(bucket, tmp_path):
    from flowlet.adapters.cairndb_projection import WorkflowProjection
    from flowlet.cli import source_for

    loaded = build_app(APP, storage_path=bucket, worker_id=None, code_ref=None)
    assert source_for(loaded, None) is None
    projection = source_for(loaded, str(tmp_path / "v.sqlite"))
    assert isinstance(projection, WorkflowProjection)
    asyncio.run(loaded.close())

    loaded2 = build_app(
        "tests.integration.sample_app:make_engine", storage_path=None, worker_id=None, code_ref=None
    )
    with pytest.raises(typer.BadParameter) as exc_info:
        source_for(loaded2, str(tmp_path / "v2.sqlite"))
    assert str(exc_info.value) == "--projection needs a Registry app (the CLI owns the backend)"
    asyncio.run(loaded2.close())


def test_setup_logging_maps_fmt_to_log_format(monkeypatch):
    from flowlet.cli import setup_logging

    calls = []
    monkeypatch.setattr(
        "flowlet.cli.configure_logging", lambda level, fmt: calls.append((level, fmt))
    )
    setup_logging("INFO", "console")
    setup_logging("DEBUG", "json")
    assert calls == [("INFO", "console"), ("DEBUG", "json")]

    with pytest.raises(typer.BadParameter) as exc_info:
        setup_logging("INFO", "xml")
    assert str(exc_info.value) == "--log-format must be console or json, got 'xml'"


def test_loaded_app_close_calls_closers_in_reverse_and_awaits_coroutines():
    from flowlet.cli import LoadedApp

    calls = []

    def closer_a():
        calls.append("a")

    async def closer_b():
        calls.append("b")

    loaded = LoadedApp(engine=None, closers=[closer_a, closer_b])
    asyncio.run(loaded.close())
    assert calls == ["b", "a"]


# --- commands ---------------------------------------------------------------------------


def test_worker_once_runs_a_task(bucket):
    async def start(engine, backend):
        return await engine.start(sample_app.greet, "bob", by=HUMAN)

    eid = asyncio.run(with_backend(bucket, start))

    code, out = cli("worker", "--once", "--worker-id", "w-cli", bucket=bucket)
    assert code == 0, out

    async def check(engine, backend):
        status = await engine.status(eid)
        last = (await engine.journal(eid))[-1].item
        return status, last

    status, last = asyncio.run(with_backend(bucket, check))
    assert status == ExecutionStatus.COMPLETED
    assert last.payload == {"value": "hi bob"}
    assert last.provenance.site.worker_id == "w-cli"

    code, out = cli("worker", "--once", bucket=bucket)
    assert code == 0 and "no task" in out or code == 0


def test_worker_polls_named_queues_only(bucket):
    async def start(engine, backend):
        return await engine.start(sample_app.greet, "amy", by=HUMAN, queue="slow")

    eid = asyncio.run(with_backend(bucket, start))
    code, _ = cli("worker", "--once", bucket=bucket)  # default queue: nothing there
    assert code == 0
    assert asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.PENDING
    code, _ = cli("worker", "--once", "-q", "slow", bucket=bucket)
    assert code == 0
    assert (
        asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.COMPLETED
    )


def test_sweeper_once_fires_due_timer(bucket):
    async def start_and_run(engine, backend):
        eid = await engine.start(sample_app.nap, by=HUMAN)
        await engine.worker().run_once()
        return eid

    eid = asyncio.run(with_backend(bucket, start_and_run))
    assert (
        asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.SUSPENDED
    )
    time.sleep(sample_app.NAP.total_seconds() + 0.2)  # the CLI uses the wall clock
    code, out = cli("sweeper", "--once", bucket=bucket)
    assert code == 0 and "timers_fired=1" in out, out
    code, _ = cli("worker", "--once", bucket=bucket)
    assert code == 0
    assert (
        asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.COMPLETED
    )
    code, out = cli("sweeper", "--once", bucket=bucket)
    assert code == 0 and "timers_fired=0" in out


def test_retention_once_archives(bucket, tmp_path):
    async def finish(engine, backend):
        eid = await engine.start(sample_app.greet, "old", by=HUMAN)
        await engine.worker().run_once()
        return eid

    eid = asyncio.run(with_backend(bucket, finish))
    code, out = cli("retention", "--once", "--delay-days", "30", bucket=bucket)
    assert code == 0 and "archived=0" in out
    code, out = cli(
        "retention",
        "--once",
        "--delay-days",
        "0",
        "--projection",
        str(tmp_path / "v.sqlite"),
        bucket=bucket,
    )
    assert code == 0 and "archived=1" in out and str(eid) in out, out

    async def check(engine, backend):
        archived = await engine.archive(eid)
        return archived is not None, await backend.executions.read(eid)

    has_archive, record = asyncio.run(with_backend(bucket, check))
    assert has_archive and record is None


def test_no_args_shows_help():
    result = runner.invoke(app, [])
    assert "worker" in result.output and "sweeper" in result.output and "retention" in result.output


# --- operator commands ------------------------------------------------------------------


def test_parse_actor_and_payload():
    from flowlet.cli import parse_actor, parse_payload

    assert parse_actor("thomas@example.com") == Actor.human("thomas@example.com")
    assert parse_actor("human:alice") == Actor.human("alice")
    assert parse_actor("system:cron") == Actor.system("cron")
    assert parse_actor("schedule:nightly") == Actor.schedule("nightly")
    assert parse_actor("worker:w-1") == Actor.worker("w-1")
    # only the FIRST ":" splits kind from ident -- not the last.
    assert parse_actor("system:cron:nightly") == Actor.system("cron:nightly")
    assert parse_actor(None).kind == "human"
    with pytest.raises(typer.BadParameter) as exc_info:
        parse_actor("alien:zork")
    assert str(exc_info.value) == "unknown actor kind 'alien' in 'alien:zork'"
    assert parse_payload('{"a": 1}') == {"a": 1}
    assert parse_payload("42") == 42
    assert parse_payload("plain text") == "plain text"


def test_eid_arg_rejects_invalid_uuid():
    from flowlet.cli import eid_arg

    with pytest.raises(typer.BadParameter) as exc_info:
        eid_arg("not-a-uuid")
    assert str(exc_info.value) == "invalid eid 'not-a-uuid': expected a UUID"


def test_status_pending_completed_journal_and_unknown(bucket):
    async def start(engine, backend):
        return await engine.start(sample_app.greet, "zoe", key="greet:zoe", by=HUMAN)

    eid = asyncio.run(with_backend(bucket, start))
    code, out = cli("status", eid, bucket=bucket)
    assert code == 0, out
    assert f"eid        {eid}" in out and "status     pending" in out
    assert "workflow   greet v1" in out and "key        greet:zoe" in out
    assert "by human:thomas@example.com" in out

    cli("worker", "--once", bucket=bucket)
    code, out = cli("status", eid, "--journal", bucket=bucket)
    assert code == 0 and "status     completed" in out
    assert "last       execution.completed" in out and "epoch 1" in out
    assert "frame.started" in out and "root/greet#0" in out

    code, out = cli("status", str(E_NOPE), bucket=bucket)
    assert code == 1 and "unknown execution" in out


def test_status_of_archived_execution_reads_the_archive(bucket):
    async def finish(engine, backend):
        eid = await engine.start(sample_app.greet, "old", by=HUMAN)
        await engine.worker().run_once()
        await engine.retention(delay=timedelta(0)).fold(eid)
        return eid

    eid = asyncio.run(with_backend(bucket, finish))
    code, out = cli("status", eid, "--journal", bucket=bucket)
    assert code == 0, out
    assert "status     archived (" in out and "execution.completed" in out


def test_signal_resumes_a_waiting_execution(bucket):
    async def start_and_run(engine, backend):
        eid = await engine.start(sample_app.wait_for_go, by=HUMAN)
        await engine.worker().run_once()
        return eid

    eid = asyncio.run(with_backend(bucket, start_and_run))
    code, out = cli("signal", eid, "go", '{"amount": 7}', "--by", "system:bank", bucket=bucket)
    assert code == 0, out
    assert "sent seq=" in out and f"on {eid}.go by system:bank" in out
    cli("worker", "--once", bucket=bucket)

    async def check(engine, backend):
        last = (await engine.journal(eid))[-1].item
        msg = (await backend.channel.read(f"{eid}.go"))[0]
        return last.payload, msg.sent_by.actor

    payload, actor = asyncio.run(with_backend(bucket, check))
    assert payload == {"value": {"amount": 7}} and actor == Actor.system("bank")

    code, out = cli("signal", str(E_NOPE), "go", "x", bucket=bucket)
    assert code == 1


def test_cancel_then_worker_cancels(bucket):
    async def start_and_run(engine, backend):
        eid = await engine.start(sample_app.wait_for_go, by=HUMAN)
        await engine.worker().run_once()
        return eid

    eid = asyncio.run(with_backend(bucket, start_and_run))
    code, out = cli("cancel", eid, "--by", "ops@example.com", bucket=bucket)
    assert code == 0 and "cancel requested" in out
    cli("worker", "--once", bucket=bucket)
    assert (
        asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.CANCELLED
    )
    code, out = cli("cancel", eid, bucket=bucket)
    assert code == 1 and "already cancelled" in out


def test_migrate_changes_version_and_refuses_terminal(bucket):
    async def start_and_run(engine, backend):
        eid = await engine.start(sample_app.wait_for_go, by=HUMAN)
        await engine.worker().run_once()
        return eid

    eid = asyncio.run(with_backend(bucket, start_and_run))
    code, out = cli("migrate", eid, "2", "--by", "ops@example.com", bucket=bucket)
    assert code == 0, out
    assert "v1 -> v2" in out and "by human:ops@example.com" in out
    assert asyncio.run(with_backend(bucket, lambda e, b: e.execution(eid))).version == "2"
    code, out = cli("status", eid, bucket=bucket)
    assert "workflow   wait_for_go v2" in out and "status     suspended" in out

    code, out = cli("migrate", eid, "9", bucket=bucket)  # unregistered: warned, still applied
    assert code == 0 and "not registered" in out

    cli("signal", eid, "go", "1", bucket=bucket)
    cli("worker", "--once", "-q", "default", bucket=bucket)  # v9 unknown to the worker: nacked
    asyncio.run(with_backend(bucket, lambda e, b: e.migrate(eid, "2", by=HUMAN)))
    cli("worker", "--once", bucket=bucket)
    cli("worker", "--once", bucket=bucket)
    assert (
        asyncio.run(with_backend(bucket, lambda e, b: e.status(eid))) == ExecutionStatus.COMPLETED
    )
    code, out = cli("migrate", eid, "3", bucket=bucket)
    assert code == 1 and "terminal" in out


def test_worker_json_logs(bucket):
    async def start(engine, backend):
        return await engine.start(sample_app.greet, "log", by=HUMAN)

    eid = asyncio.run(with_backend(bucket, start))
    result = runner.invoke(
        app,
        [
            "worker",
            "--once",
            "--worker-id",
            "w-json",
            "--log-format",
            "json",
            "--log-level",
            "INFO",
            "--app",
            APP,
            "--storage-path",
            bucket,
        ],
    )
    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
    names = [line["event"] for line in lines]
    assert "worker_started" in names and "execution_completed" in names
    completed = next(line for line in lines if line["event"] == "execution_completed")
    assert (
        completed["eid"] == str(eid)
        and completed["worker_id"] == "w-json"
        and completed["epoch"] == 1
    )

    result = runner.invoke(
        app, ["worker", "--once", "--log-format", "xml", "--app", APP, "--storage-path", bucket]
    )
    assert result.exit_code != 0
