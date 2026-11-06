"""`GET /stats/workflows`: one summary per workflow name. Spec 09 section 8.7."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from flowli.adapters.memory import ManualClock
from flowli.domain import Site
from flowli.runtime import Engine, Registry

from .conftest import READER, auth, drain

T0 = datetime(2026, 11, 6, 9, 0, tzinfo=UTC)
INVOICE = {"workflow": "invoice_approval", "version": "3", "args": {"invoice_id": "i"}}
WAITS = {"workflow": "waits", "version": "1"}


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


@pytest.fixture
def engine(registry: Registry, backend, clock) -> Engine:
    return Engine(backend.ports, Site.local("api-test"), registry=registry, clock=clock)


async def _start(client, body) -> str:
    return (await client.post("/executions", json=body, headers=auth())).json()["eid"]


async def _run(client, engine, clock, body, seconds: int) -> str:
    """Start one execution and run it `seconds` after its creation."""
    eid = await _start(client, body)
    clock.advance(timedelta(seconds=seconds))
    await drain(engine)
    clock.advance(timedelta(seconds=1))
    return eid


async def test_no_execution_is_an_empty_list(client, projection):
    await projection.refresh()
    r = await client.get("/stats/workflows", headers=auth(READER))
    assert r.status_code == 200
    assert r.json() == {"items": []}


async def test_a_summary_per_workflow(client, engine, projection, clock):
    for seconds in (2, 4, 6, 8, 10, 12):
        await _run(client, engine, clock, INVOICE, seconds)
    waiting = await _run(client, engine, clock, WAITS, 1)
    await client.post(f"/executions/{waiting}/cancel", headers=auth())
    await drain(engine)
    clock.advance(timedelta(seconds=1))
    await _run(client, engine, clock, WAITS, 1)  # stays suspended
    last = clock().to_iso()
    await _start(client, WAITS)  # stays pending: nothing runs it
    await projection.refresh()

    body = (await client.get("/stats/workflows", headers=auth(READER))).json()
    invoice, waits = body["items"]

    assert invoice["workflow"] == "invoice_approval"
    assert invoice["runs"] == 6 and invoice["active"] == 0
    assert invoice["recent"] == ["completed"] * 5  # the last five only
    assert invoice["finished"] == 6 and invoice["completed"] == 6
    assert invoice["avg_duration_ms"] == 7000  # mean of 2, 4, ..., 12 seconds

    assert waits["workflow"] == "waits"
    assert waits["runs"] == 3 and waits["active"] == 2
    assert waits["recent"] == ["cancelled", "suspended", "pending"]  # oldest first
    assert waits["finished"] == 1 and waits["completed"] == 0
    assert waits["avg_duration_ms"] is None  # nothing completed
    assert waits["last_run_at"] == last


async def test_the_window_bounds_the_rates_and_recent_the_bullets(
    client, engine, projection, clock
):
    for seconds in (100, 2, 4):
        await _run(client, engine, clock, INVOICE, seconds)
    await projection.refresh()
    r = await client.get("/stats/workflows?recent=2&window=2", headers=auth(READER))
    (row,) = r.json()["items"]
    assert row["recent"] == ["completed", "completed"]
    assert row["finished"] == 2
    assert row["avg_duration_ms"] == 3000  # the 100 s run is outside the window


async def test_the_tag_moves_with_a_status(client, engine, projection, clock):
    eid = await _run(client, engine, clock, WAITS, 1)
    await projection.refresh()
    before = await client.get("/stats/workflows", headers=auth())
    same = await client.get(
        "/stats/workflows", headers={**auth(), "If-None-Match": before.headers["etag"]}
    )
    assert same.status_code == 304

    await client.post(
        f"/executions/{eid}/signal", json={"channel": "go", "payload": "x"}, headers=auth()
    )
    await drain(engine)
    await projection.refresh()
    after = await client.get(
        "/stats/workflows", headers={**auth(), "If-None-Match": before.headers["etag"]}
    )
    assert after.status_code == 200
    assert after.json()["items"][0]["recent"] == ["completed"]


@pytest.mark.parametrize("query", ["recent=0", "window=0", "recent=51", "window=501"])
async def test_bounds_are_checked(client, query):
    r = await client.get(f"/stats/workflows?{query}", headers=auth())
    assert r.status_code == 422


async def test_needs_executions_read(client):
    r = await client.get("/stats/workflows", headers={"Authorization": "Bearer token-finance"})
    assert r.status_code == 403
