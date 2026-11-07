"""The queue a workflow is registered with: where a start that names none goes."""

import pytest

from flowli.adapters.memory import MemoryBackend
from flowli.domain import Actor, InvalidName, Site
from flowli.runtime import Engine, EngineConfig, Registry

BY = Actor.system("test")


@pytest.fixture
def registry() -> Registry:
    reg = Registry()

    @reg.workflow("nightly", "1", queue="batch")
    async def nightly(ctx) -> str:
        return "ok"

    @reg.workflow("anywhere", "1")
    async def anywhere(ctx) -> str:
        return "ok"

    return reg


@pytest.fixture
def engine(registry: Registry) -> Engine:
    config = EngineConfig(default_queue="main")
    return Engine(MemoryBackend().ports, Site.local("test"), registry=registry, config=config)


def fn(registry: Registry, name: str):
    return registry.get(name, "1").fn


async def test_a_start_without_a_queue_goes_to_the_workflow_queue(engine, registry):
    eid = await engine.start(fn(registry, "nightly"), by=BY)
    assert (await engine.execution(eid)).queue == "batch"
    assert [t.target.eid for t in await engine.ports.queue.pending("batch")] == [eid]


async def test_the_queue_of_a_start_wins(engine, registry):
    eid = await engine.start(fn(registry, "nightly"), queue="replay", by=BY)
    assert (await engine.execution(eid)).queue == "replay"


async def test_a_workflow_without_a_queue_goes_to_the_engine_default(engine, registry):
    eid = await engine.start(fn(registry, "anywhere"), by=BY)
    assert (await engine.execution(eid)).queue == "main"


async def test_a_child_goes_to_its_workflow_queue(engine, registry):
    @registry.workflow("parent", "1")
    async def parent(ctx) -> str:
        return await ctx.child(fn(registry, "nightly"), key="n")

    eid = await engine.start(parent, by=BY)
    await engine.worker(["main"]).run_once()
    (task,) = await engine.ports.queue.pending("batch")
    child = await engine.execution(task.target.eid)
    assert child.parent is not None and child.parent.eid == eid
    assert child.queue == "batch"


def test_engine_workflow_registers_the_queue(engine):
    @engine.workflow("routed", "1", queue="batch")
    async def routed(ctx) -> str:
        return "ok"

    assert engine.registry.of(routed).queue == "batch"


def test_an_invalid_queue_name_is_refused_at_registration():
    with pytest.raises(InvalidName):
        Registry().workflow("bad", "1", queue="no/slash")
