from datetime import UTC, datetime

import pytest
from cairndb import Timestamp

from flowlet.domain import Actor, Code, Provenance, Site


@pytest.fixture
def site() -> Site:
    return Site(host="hosta", pid=42, worker_id="w-1", instance="c1", region="eu", epoch=1)


@pytest.fixture
def code() -> Code:
    return Code(workflow="invoice_approval", version="3", frame_kind="step", frame_name="fetch")


@pytest.fixture
def prov(site: Site, code: Code) -> Provenance:
    return Provenance(
        actor=Actor.worker("w-1"),
        site=site,
        code=code,
        attempt=1,
        at=Timestamp(datetime(2026, 9, 7, 9, 0, 0, 123000, tzinfo=UTC)),
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: uses filesystem-backed CairnDB")
