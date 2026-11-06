"""The tag of a list moves with every change a client can see.

Regression: the tag of `/executions` was built from `eid@updated_at` alone. When
`execution.completed` carried the same instant as the entry before it, the tag
did not move, a client holding it got a 304, and the web application kept showing
a finished execution as running. The engine here runs on a frozen clock, so no
`updated_at` ever moves: only the status does.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from cairndb import Timestamp

from flowli.domain import Site
from flowli.runtime import Engine, Registry

from .conftest import auth, drain

FROZEN = Timestamp(datetime(2026, 11, 6, 9, 0, tzinfo=UTC))


@pytest.fixture
def engine(registry: Registry, backend) -> Engine:
    return Engine(backend.ports, Site.local("api-test"), registry=registry, clock=lambda: FROZEN)


async def _revalidate(client, path: str, tag: str):
    return await client.get(path, headers={**auth(), "If-None-Match": tag})


@pytest.mark.parametrize("path", ["/executions", "/executions?workflow=waits"])
async def test_a_status_change_moves_the_tag_of_a_list(client, engine, projection, path):
    eid = (
        await client.post("/executions", json={"workflow": "waits", "version": "1"}, headers=auth())
    ).json()["eid"]
    await drain(engine)
    await projection.refresh()
    before = await client.get(path, headers=auth())
    assert [i["status"] for i in before.json()["items"]] == ["suspended"]
    assert (await _revalidate(client, path, before.headers["etag"])).status_code == 304

    await client.post(
        f"/executions/{eid}/signal", json={"channel": "go", "payload": "now"}, headers=auth()
    )
    await drain(engine)
    await projection.refresh()

    after = await _revalidate(client, path, before.headers["etag"])
    assert after.status_code == 200
    assert after.headers["etag"] != before.headers["etag"]
    (row,) = after.json()["items"]
    assert row["status"] == "completed"
    assert row["updated_at"] == before.json()["items"][0]["updated_at"]  # the clock is frozen
