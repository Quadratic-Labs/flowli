"""A tiny app for the CLI tests: a Registry, and an Engine factory."""

from datetime import timedelta

from flowlet.runtime import Registry

registry = Registry()
NAP = timedelta(seconds=0.5)


@registry.workflow("greet", "1")
async def greet(ctx, who):
    return await ctx.step(lambda: f"hi {who}", name="greet")


@registry.workflow("nap", "1")
async def nap(ctx):
    await ctx.sleep(NAP)
    return "woke"


@registry.workflow("wait_for_go", "1")
async def wait_for_go(ctx):
    return (await ctx.receive("go")).payload


@registry.workflow("wait_for_go", "2")
async def wait_for_go_v2(ctx):
    return (await ctx.receive("go")).payload


def make_engine():
    """A callable app: builds an Engine over an in-memory backend."""
    from flowlet.adapters.memory import MemoryBackend
    from flowlet.domain import Site
    from flowlet.runtime import Engine

    return Engine(MemoryBackend().ports, Site.local("cli-test"), registry=registry)


not_an_app = 42
