"""A git repository, an engine, and a workflow that delegates to an agent."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp
from flowli.adapters.memory import ManualClock, MemoryBackend
from flowli.domain import Actor, Site
from flowli.patterns import delegate
from flowli.runtime import Consumer, ConsumerConfig, Engine

T0 = Timestamp(datetime(2026, 9, 11, 9, 0, tzinfo=UTC))
HUMAN = Actor.human("thomas@example.com")


def git(repo, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    """A repository with one commit, and an integration branch to carve from."""
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
def engine(backend) -> Engine:
    engine = Engine(backend.ports, Site.local("w-1"), clock=backend.clock)

    @engine.workflow("agent_task", "1")
    async def agent_task(ctx, envelope):
        reply = await delegate(ctx, "agents", envelope, timeout=timedelta(hours=1))
        return "none" if reply is None else reply.payload

    return engine


@pytest.fixture
def drain(engine):
    async def _drain() -> int:
        worker = engine.worker(queues=["default"])
        n = 0
        while await worker.run_once():
            n += 1
            assert n < 30
        return n

    return _drain


def consumer(engine, handler, **kwargs) -> Consumer:
    config = ConsumerConfig(queues=("agents",), holder="runner-a", ttl=0.3, **kwargs)
    return Consumer(engine, handler, config)
