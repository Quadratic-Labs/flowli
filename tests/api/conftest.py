"""An app over an in-memory engine, with a static token map."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from flowli.adapters.cairndb import CairnBackend
from flowli.api import ApiConfig, StaticAuthenticator, create_app
from flowli.api.auth import (
    EVIDENCE_READ,
    EXECUTIONS_CANCEL,
    EXECUTIONS_MIGRATE,
    EXECUTIONS_READ,
    EXECUTIONS_SIGNAL,
    EXECUTIONS_START,
    QUEUES_READ,
    REVIEWS_READ,
    WORKFLOWS_READ,
    Principal,
)
from flowli.domain import Actor, Site
from flowli.patterns import delegate
from flowli.patterns.review import review
from flowli.runtime import Engine, Registry

OPERATOR = "token-operator"
READER = "token-reader"
FINANCE = "token-finance"
RUNNER = "token-runner"
STRAY = "token-stray"  # a consumer wrongly given the engine's own queue


@pytest.fixture
def registry() -> Registry:
    reg = Registry()

    @reg.workflow("invoice_approval", "3")
    async def invoice_approval(ctx, invoice_id: str, amount: int = 0) -> str:
        """Approve one invoice.

        The long form of the docstring, which the catalog shows as the
        description.
        """
        return await ctx.step(lambda: f"{invoice_id}:{amount}", name="settle")

    @reg.workflow("untyped", "1")
    async def untyped(ctx, whatever) -> str:  # no annotation: no schema
        return "ok"

    @reg.workflow("waits", "1")
    async def waits(ctx) -> str:
        return (await ctx.receive("go")).payload

    @reg.workflow("delegates", "1")
    async def delegates(ctx, amount: int) -> str:
        reply = await delegate(ctx, "agents", {"amount": amount})
        return "none" if reply is None else reply.payload["verdict"]

    @reg.workflow("needs_review", "1")
    async def needs_review(ctx, amount: int) -> str:
        decision = await review(ctx, "finance", {"amount": amount})
        return "none" if decision is None else decision.verdict

    return reg


@pytest.fixture
async def backend(tmp_path):
    b = CairnBackend.configure(
        {"storage": {"type": "filesystem", "path": str(tmp_path / "bucket")}}
    )
    yield b
    await b.close()


@pytest.fixture
def engine(registry: Registry, backend) -> Engine:
    return Engine(backend.ports, Site.local("api-test"), registry=registry)


@pytest.fixture
async def projection(backend, tmp_path):
    p = backend.projection(db_path=str(tmp_path / "wf_view.sqlite"), poll_interval=0.05)
    yield p
    await p.stop()


@pytest.fixture
def principals() -> dict[str, Principal]:
    everything = frozenset(
        {
            WORKFLOWS_READ,
            EXECUTIONS_READ,
            EXECUTIONS_START,
            EXECUTIONS_SIGNAL,
            EXECUTIONS_CANCEL,
            EXECUTIONS_MIGRATE,
            REVIEWS_READ,
            QUEUES_READ,
            EVIDENCE_READ,
            "reviews:decide:finance",
        }
    )
    return {
        OPERATOR: Principal(Actor.human("ops@example.com"), everything),
        READER: Principal(
            Actor.human("reader@example.com"), frozenset({EXECUTIONS_READ, WORKFLOWS_READ})
        ),
        FINANCE: Principal(
            Actor.human("cfo@example.com"),
            frozenset({REVIEWS_READ, "reviews:decide:finance"}),
        ),
        RUNNER: Principal(
            Actor.worker("runner-a"), frozenset({"tasks:consume:agents", EVIDENCE_READ})
        ),
        STRAY: Principal(
            Actor.worker("runner-b"), frozenset({"tasks:consume:default"})
        ),
    }


@pytest.fixture
def app(engine: Engine, projection, principals: dict[str, Principal]):
    return create_app(
        engine,
        authenticator=StaticAuthenticator(principals),
        projection=projection,
        config=ApiConfig(
            queues=("default", "finance", "agents"), wait_ms=200, task_ttl=60.0
        ),
    )


@pytest.fixture
async def client(app):
    """A client with the app's lifespan run, so the projection is live."""
    from asgi_lifespan import LifespanManager

    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://api") as c:
            yield c


def auth(token: str = OPERATOR) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def drain(engine, limit=40) -> int:
    """Run every task the engine has, then return how many ran.

    The worker does not take the queues that humans and agents consume.
    """
    worker = engine.worker(queues=["default"])
    n = 0
    while await worker.run_once():
        n += 1
        assert n < limit, "worker did not settle"
    return n
