"""A git repository, an engine with the three workflows, and fake consumers."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from cairndb import Timestamp
from flowlet.adapters.memory import ManualClock, MemoryBackend
from flowlet.domain import Actor, DelegateTask, Site
from flowlet.runtime import Engine, Registry

from flowlet_codeflow import Policy, register
from flowlet_codeflow.gotchas import Registry as Gotchas

T0 = Timestamp(datetime(2026, 9, 11, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")
REVIEWER = Actor.human("lead@example.com")
AGENT = Actor.worker("agent-runner")


def git(repo, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    path = tmp_path / "repo"
    path.mkdir()
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.email", "test@example.com")
    git(path, "config", "user.name", "Test")
    (path / "README.md").write_text("hello\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "first")
    return path


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


@pytest.fixture
def backend(clock) -> MemoryBackend:
    return MemoryBackend(clock=clock)


@pytest.fixture
def gotchas() -> Gotchas:
    return Gotchas()


@pytest.fixture
def policy() -> Policy:
    return Policy(max_attempts=3, review_timeout=timedelta(days=2))


@pytest.fixture
def engine(backend, policy, gotchas) -> Engine:
    registry = Registry()
    engine = Engine(backend.ports, Site.local("w-1"), registry=registry, clock=backend.clock)
    engine.workflows = register(registry, policy=policy, gotchas=gotchas)  # type: ignore[attr-defined]
    return engine


class Agents:
    """A stand-in for the agent runner: it answers with a report."""

    def __init__(self, reports: list[dict[str, Any]] | None = None) -> None:
        self.reports = reports or []
        self.envelopes: list[dict[str, Any]] = []

    def report_for(self, envelope: dict[str, Any]) -> dict[str, Any]:
        if self.reports:
            return self.reports.pop(0)
        return {
            "outcome": "completed",
            "branch": f"agent/{envelope['intent']}",
            "base_commit": "0" * 40,
            "commits": ["c0ffee"],
            # Inside the write scope: a path outside it is a gate failure.
            "changed_paths": [
                glob.replace("**", "x.py") for glob in envelope.get("write_scope") or ["x.py"]
            ],
            "verification": [
                {"command": c, "exit_code": 0} for c in envelope.get("verification", [])
            ],
            "claims": [{"criteria_satisfied": envelope.get("acceptance_criteria", [])}],
        }


async def drive(
    engine,
    backend,
    *,
    agents: Agents,
    verdicts: dict[str, str] | None = None,
    merges: list[dict[str, Any]] | None = None,
    limit: int = 500,
) -> None:
    """Run the worker and every consumer of every queue, until nothing moves.

    Each queue has its own consumer here, exactly as it has its own process in
    a deployment. None of them plans anything.
    """
    verdicts = verdicts or {}
    worker = engine.worker(queues=["default"])
    for _ in range(limit):
        if await worker.run_once():
            continue
        if await _answer(engine, backend, "agents", lambda t: agents_reply(agents, t)):
            continue
        if await _answer(engine, backend, "merge", lambda t: _merge_reply(merges)):
            continue
        if await _decide(engine, backend, verdicts):
            continue
        return
    raise AssertionError("the controller did not settle")


def agents_reply(agents: Agents, task: DelegateTask) -> dict[str, Any]:
    envelope = dict(task.payload)
    agents.envelopes.append(envelope)
    return agents.report_for(envelope)


def _merge_reply(merges: list[dict[str, Any]] | None) -> dict[str, Any]:
    if merges:
        return merges.pop(0)
    return {"merged": True, "commit": "deadbee"}


async def _answer(engine, backend, queue: str, reply) -> bool:
    claimed = await backend.queue.dequeue(queue, f"{queue}-consumer", 60)
    if claimed is None:
        return False
    target = DelegateTask.from_task_payload(claimed.task.payload)
    await engine.deliver(target.eid, target.reply_channel, reply(target), by=AGENT)
    await backend.queue.ack(claimed)
    return True


async def _decide(engine, backend, verdicts: dict[str, str]) -> bool:
    for queue in ("code-review", "escalation", "plan"):
        pending = await backend.queue.pending(queue)
        if not pending:
            continue
        task = pending[0]
        target = DelegateTask.from_task_payload(task.payload)
        rid = task.payload["payload"]["rid"]
        await engine.reviews.decide(
            rid, eid=target.eid, queue=queue,
            verdict=verdicts.get(queue, "accept"), by=REVIEWER,
        )
        return True
    return False
