"""Queue depth, tasks, stats and health. Spec 09 section 8."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

from asgi_lifespan import LifespanManager
from cairndb import Timestamp

from flowli.api import ApiConfig, StaticAuthenticator, create_app
from flowli.api.dto import task_row
from flowli.domain import FrameRef, Task, TaskKind

from .conftest import OPERATOR, READER, auth, drain
from tests.ids import E_ABC

START = {"workflow": "invoice_approval", "version": "3", "args": {"invoice_id": "i-1"}}


async def test_depth_is_read_from_the_queue(client, engine):
    """Depth is live state: the control log records an enqueue and not an ack,
    so a projection cannot count what is on the queue now."""
    before = {q["queue"]: q for q in (await client.get("/queues", headers=auth())).json()["items"]}
    assert before["default"]["total"] == 0

    await client.post("/executions", json=START, headers=auth())
    after = {q["queue"]: q for q in (await client.get("/queues", headers=auth())).json()["items"]}
    assert after["default"]["total"] == 1
    assert after["default"]["visible"] == 1
    assert after["default"]["claimed"] == 0

    await drain(engine)
    settled = {q["queue"]: q for q in (await client.get("/queues", headers=auth())).json()["items"]}
    assert settled["default"]["total"] == 0  # the worker acked it


async def test_pending_tasks(client, engine):
    eid = (await client.post("/executions", json=START, headers=auth())).json()["eid"]
    body = (await client.get("/queues/default/tasks", headers=auth())).json()
    (task,) = body["items"]
    assert task["task_id"] == f"start:{eid}"
    assert task["kind"] == "start"
    assert task["eid"] == eid
    assert task["enqueued_by"] == {"kind": "human", "id": "ops@example.com"}


def test_task_row_reports_every_field(prov):
    """`task_row` mirrors the whole `Task`, not only the fields the HTTP
    scenarios happen to read back -- `queue`, `fid`, `reason`, `key`,
    `not_before` and `enqueued_at` all belong in the row too."""
    not_before = Timestamp(datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC))
    task = Task(
        queue="agents",
        kind=TaskKind.DELEGATE,
        target=FrameRef(E_ABC, "root/x#0"),
        reason="delegate",
        enqueued_by=prov,
        key="k1",
        not_before=not_before,
    )
    assert task_row(task) == {
        "task_id": task.task_id,
        "queue": "agents",
        "kind": "delegate",
        "eid": str(E_ABC),
        "fid": "root/x#0",
        "reason": "delegate",
        "key": "k1",
        "not_before": not_before.to_iso(),
        "enqueued_at": prov.at.to_iso(),
        "enqueued_by": {"kind": "worker", "id": "w-1"},
    }


async def test_queue_reads_need_the_capability(client):
    assert (await client.get("/queues", headers=auth(READER))).status_code == 403


async def test_queues_list_reflects_the_configured_queues(client):
    """The `ApiConfig` passed to `create_app` -- not a fresh default -- is
    what `/queues` iterates: the fixture configures three queues."""
    items = {q["queue"] for q in (await client.get("/queues", headers=auth())).json()["items"]}
    assert items == {"default", "finance", "agents"}


async def test_stats_counts_by_status(client, engine, projection):
    await client.post("/executions", json=START, headers=auth())
    await drain(engine)
    await projection.refresh()
    body = (await client.get("/stats", headers=auth(OPERATOR))).json()
    assert body["by_status"] == {"completed": 1}


async def test_health_reports_the_projection(client):
    body = (await client.get("/health")).json()
    assert body["status"] == "ok"
    assert "checked_at" in body


async def test_app_title_comes_from_config(engine, projection, principals):
    app = create_app(
        engine,
        authenticator=StaticAuthenticator(principals),
        projection=projection,
        config=ApiConfig(title="invoicing-api"),
    )
    assert app.title == "invoicing-api"


async def test_lifespan_owns_the_projection_refresh_loop(engine, projection, principals):
    """`create_app`'s lifespan catches the projection up and starts its poll
    loop at startup, and stops it at shutdown (`08-projection.md` section 4)
    -- it is not enough for the loop to merely exist unwired."""
    refresh, start, stop = (
        AsyncMock(wraps=projection.refresh),
        AsyncMock(wraps=projection.start),
        AsyncMock(wraps=projection.stop),
    )
    projection.refresh, projection.start, projection.stop = refresh, start, stop

    app = create_app(engine, authenticator=StaticAuthenticator(principals), projection=projection)
    assert start.await_count == 0
    async with LifespanManager(app):
        assert refresh.await_count == 1
        assert start.await_count == 1
        assert stop.await_count == 0
    assert stop.await_count == 1
