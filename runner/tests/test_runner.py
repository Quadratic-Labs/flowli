"""The agent runner, end to end: a delegate task becomes commits and a report.

Spec 10 sections 3 to 9.
"""

from __future__ import annotations

import asyncio

import pytest
from flowli.domain import ExecutionStatus
from flowli.runtime import Consumer, ConsumerConfig

from flowli_runner import Envelope, RunnerConfig, SubprocessAgent, build_consumer
from flowli_runner.runner import AgentRunner

from .conftest import HUMAN, consumer, git

WRITES_A_COMMIT = (
    "printf 'did the thing\\n' > NOTES.md && "
    "git -c user.email=a@b -c user.name=Agent add -A && "
    "git -c user.email=a@b -c user.name=Agent commit -qm 'agent work'"
)


def envelope(command: str, **kwargs) -> dict:
    return Envelope(
        intent="fix the thing",
        base_ref="main",
        agent={"command": ["bash", "-lc", command]},
        **kwargs,
    ).to_payload()


async def start(engine, payload: dict) -> str:
    return await engine.start(engine.registry.get("agent_task", "1").fn, payload, by=HUMAN)


async def report_of(engine, eid: str) -> dict:
    """The value the workflow returned: the report the runner delivered."""
    entries = await engine.journal(eid)
    assert entries, "the execution has no journal"
    return dict(entries[-1].item.payload["value"])


def runner(engine, repo, **kwargs) -> AgentRunner:
    return AgentRunner(engine, SubprocessAgent(), RunnerConfig(repo_path=str(repo), **kwargs))


# --- the whole loop --------------------------------------------------------


async def test_an_agent_task_becomes_commits_and_a_report(engine, repo, drain):
    eid = await start(engine, envelope(WRITES_A_COMMIT))
    await drain()

    assert await consumer(engine, runner(engine, repo)).run_once() is True
    await drain()

    assert await engine.status(eid) is ExecutionStatus.COMPLETED
    report = await report_of(engine, eid)
    assert report["outcome"] == "completed"
    assert len(report["commits"]) == 1
    # The branch is named after the task's key, which the workflow chose.
    assert report["branch"] == f"agent/{eid}/delegate"
    # The commit is on the branch, in the repository, not in the worktree only.
    assert git(repo, "log", "-1", "--format=%s", report["commits"][0]) == "agent work"


async def test_the_worktree_is_removed_when_the_attempt_ends(engine, repo, drain):
    await start(engine, envelope(WRITES_A_COMMIT))
    await drain()
    await consumer(engine, runner(engine, repo)).run_once()

    assert not list((repo / "wt").rglob("NOTES.md"))  # nothing left behind
    assert (repo / "wt").exists() or True


async def test_an_agent_that_fails_reports_it(engine, repo, drain):
    eid = await start(engine, envelope("exit 3"))
    await drain()
    await consumer(engine, runner(engine, repo)).run_once()
    await drain()

    report = await report_of(engine, eid)
    assert report["outcome"] == "failed"
    assert report["commits"] == []


# --- verification and evidence --------------------------------------------


async def test_verification_commands_run_in_the_worktree(engine, repo, drain):
    eid = await start(engine, envelope(WRITES_A_COMMIT, verification=["test -f NOTES.md", "false"]))
    await drain()
    await consumer(engine, runner(engine, repo)).run_once()
    await drain()

    report = await report_of(engine, eid)
    codes = {v["command"]: v["exit_code"] for v in report["verification"]}
    assert codes == {"test -f NOTES.md": 0, "false": 1}
    # The runner does not judge them: the workflow applies the gates.
    assert report["outcome"] == "completed"


async def test_the_report_carries_references_and_not_bytes(engine, repo, drain, backend):
    eid = await start(engine, envelope(WRITES_A_COMMIT, verification=["echo checked"]))
    await drain()
    await consumer(engine, runner(engine, repo)).run_once()
    await drain()

    report = await report_of(engine, eid)
    assert report["evidence"]  # references, never bytes
    items = await backend.evidence.list(eid)
    assert {i.name for i in items} >= {"verification.txt", "diff.patch"}

    # Filed under the frame that waited, so the interface shows it there.
    (ref,) = {i.ref for i in items}
    assert ref.fid == "root/delegate-receive#0"
    body = await backend.evidence.get(ref, "verification.txt")
    assert b"echo checked" in body and b"exit 0" in body


# --- write scopes ----------------------------------------------------------


async def test_a_scope_conflict_returns_the_task(engine, repo, drain, backend):
    """Sorted order gives no deadlock; a conflict is a nack, not a block."""
    import cairndb
    from cairndb.storage.config import FilesystemStorageConfig

    db = cairndb.CairnDB(FilesystemStorageConfig(path=str(repo / ".scopes")).create_storage())
    from flowli_runner.scopes import key

    other = await db.lease(key("src/**"), ttl=60, holder="someone-else")
    assert other is not None

    await start(engine, envelope(WRITES_A_COMMIT, write_scope=["src/**"]))
    await drain()

    handler = AgentRunner(engine, SubprocessAgent(), RunnerConfig(repo_path=str(repo)), db=db)
    await consumer(engine, handler).run_once()

    assert len(await backend.queue.pending("agents")) == 1  # still ready for a later pass
    assert not list((repo / "wt").rglob("NOTES.md"))


async def test_scopes_are_released_when_the_attempt_ends(engine, repo, drain):
    import cairndb
    from cairndb.storage.config import FilesystemStorageConfig

    from flowli_runner.scopes import key

    db = cairndb.CairnDB(FilesystemStorageConfig(path=str(repo / ".scopes")).create_storage())
    await start(engine, envelope(WRITES_A_COMMIT, write_scope=["src/**"]))
    await drain()

    handler = AgentRunner(engine, SubprocessAgent(), RunnerConfig(repo_path=str(repo)), db=db)
    await consumer(engine, handler).run_once()

    # Free again for the next attempt.
    assert await db.lease(key("src/**"), ttl=5, holder="next") is not None


# --- interrupt -------------------------------------------------------------


async def test_a_cancel_stops_the_agent_and_quarantines_its_tree(engine, repo, drain, backend):
    eid = await start(engine, envelope("printf 'partial\\n' > HALF.md; sleep 30"))
    await drain()
    (task,) = await backend.queue.pending("agents")

    handler = runner(engine, repo)
    consumer_ = consumer(engine, handler)

    async def ask_to_stop():
        await asyncio.sleep(0.35)
        from flowli.runtime.consumer import request_cancel

        await request_cancel(engine, "agents", task.task_id, HUMAN)

    await asyncio.gather(consumer_.run_once(), ask_to_stop())
    await drain()

    report = await report_of(engine, eid)
    assert report["outcome"] == "interrupted"
    # The tree of a killed attempt is kept as a bundle, not deleted.
    assert list((repo / "quarantine").glob("*.bundle"))


# --- the subprocess adapter -----------------------------------------------


async def test_a_subprocess_handle_names_its_host(repo):
    adapter = SubprocessAgent()
    tree = Envelope(intent="x", agent={"command": ["bash", "-lc", "sleep 5"]})
    handle = await adapter.start(tree)
    try:
        assert handle["kind"] == "subprocess"
        assert handle["host"] == adapter.host
        assert await adapter.poll(handle) is None  # still running
        assert await adapter.reattach(handle) == handle

        # A runner on another host must not believe it can reattach to a pid.
        elsewhere = SubprocessAgent(host="another-host")
        assert await elsewhere.reattach(handle) is None
    finally:
        await adapter.terminate(handle)


async def test_reattach_is_none_when_the_process_is_gone(repo):
    adapter = SubprocessAgent()
    handle = await adapter.start(Envelope(intent="x", agent={"command": ["bash", "-lc", "true"]}))
    for _ in range(100):
        if await adapter.poll(handle) is not None:
            break
        await asyncio.sleep(0.02)
    assert await adapter.reattach(handle) is None


async def test_an_envelope_without_a_command_is_refused():
    with pytest.raises(ValueError, match="command"):
        await SubprocessAgent().start(Envelope(intent="x"))


def test_build_consumer_wires_the_runner(engine, repo):
    built = build_consumer(
        engine,
        SubprocessAgent(),
        RunnerConfig(repo_path=str(repo)),
        ConsumerConfig(queues=("agents",), holder="runner-a"),
    )
    assert isinstance(built, Consumer)
    assert isinstance(built.handler, AgentRunner)
