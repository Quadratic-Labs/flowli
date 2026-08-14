"""
Pytest configuration and shared fixtures for flowlet tests.

Provides:
- Async SQLite engine for snapshot database tests
- Registry and FlowController fixtures for controller tests
- run_state / span_record factory fixtures (return a builder callable)
- Mock querier and queue stubs
"""
import pytest
import pytest_asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from flowlet.api.controller import FlowController
from flowlet.api.database import Base
from flowlet.models import FlowJob, RunState, RunStatus, RunType, SpanEvent, SpanRecord
from flowlet.registry import Registry
from uuid import uuid7
from flowlet.types import Timestamp


# ============================================================================
# Builder functions — module-level, used by fixtures below and within conftest
# ============================================================================


def _make_ts(dt: datetime | None = None) -> Timestamp:
    dt = dt or datetime.now(UTC)
    return Timestamp(dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt)


def _make_run_state(
    *,
    run_id: UUID | None = None,
    flow_name: str = "test_flow",
    status: RunStatus = RunStatus.running,
    worker_id: str = "worker-1",
    started_at: Timestamp | None = None,
    ended_at: Timestamp | None = None,
    attempt: int = 1,
    max_retries: int = 3,
    kwargs: dict | None = None,
    cancel_requested: bool = False,
) -> RunState:
    now = _make_ts()
    return RunState(
        run_id=run_id or uuid7(),
        flow_name=flow_name,
        status=status,
        worker_id=worker_id,
        started_at=started_at or now,
        ended_at=ended_at,
        attempt=attempt,
        max_retries=max_retries,
        kwargs=kwargs or {},
        cancel_requested=cancel_requested,
    )


def _seed_lease(
    store,
    state: RunState,
    *,
    holder: str | None = None,
    deadline: datetime | None = None,
    epoch: int = 1,
) -> None:
    """Write a run's lease document directly — test seeding only.

    Defaults model a *released* lease (holder None, deadline = now, the
    release time).  Pass ``holder`` and a future ``deadline`` for a held
    lease, or a past deadline for an expired one.
    """
    import json

    from flowlet.serdes import to_payload

    doc = {
        "epoch": epoch,
        "holder": holder,
        "deadline_at": (deadline or datetime.now(UTC)).isoformat(),
        "state": to_payload(state),
    }
    key = f"state/{state.flow_name}/{state.run_id}.json"
    store.put_object_sync(
        key, json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()
    )


def _make_flow_job(
    *,
    flow_name: str = "test_flow",
    kwargs: dict | None = None,
    max_retries: int = 3,
    timeout_seconds: int | None = None,
) -> FlowJob:
    return FlowJob(
        flow_name=flow_name,
        kwargs=kwargs or {},
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
    )


def _make_span_record(
    *,
    run_id: UUID | None = None,
    span_id: str | None = None,
    parent_span_id: str | None = None,
    name: str = "test_flow",
    flow_name: str = "test_flow",
    attempt: int = 1,
    span_type: RunType = RunType.flow,
    status: RunStatus = RunStatus.completed,
    status_message: str | None = None,
    start_ts: Timestamp | None = None,
    end_ts: Timestamp | None = None,
    events: list[SpanEvent] | None = None,
    attributes: dict | None = None,
) -> SpanRecord:
    import secrets

    return SpanRecord(
        run_id=run_id or uuid7(),
        span_id=span_id or secrets.token_hex(8),
        parent_span_id=parent_span_id,
        name=name,
        flow_name=flow_name,
        attempt=attempt,
        span_type=span_type,
        status=status,
        status_message=status_message,
        start_ts=start_ts or _make_ts(),
        end_ts=end_ts or _make_ts(),
        events=events or [],
        attributes=attributes or {},
    )


# ============================================================================
# Factory fixtures — inject builder callables into tests
# ============================================================================


@pytest.fixture
def make_ts():
    """Return the _make_ts builder callable."""
    return _make_ts


@pytest.fixture
def make_run_state():
    """Return the _make_run_state builder callable."""
    return _make_run_state


@pytest.fixture
def seed_lease():
    """Return the _seed_lease builder callable (writes a lease document)."""
    return _seed_lease


@pytest.fixture
def make_flow_job():
    """Return the _make_flow_job builder callable."""
    return _make_flow_job


@pytest.fixture
def make_span_record():
    """Return the _make_span_record builder callable."""
    return _make_span_record


# ============================================================================
# Registry fixtures
# ============================================================================


@pytest.fixture
def registry() -> Registry:
    """Empty Registry instance."""
    return Registry()


@pytest.fixture
def registry_with_flows(registry: Registry) -> Registry:
    """Registry with two typed flows pre-registered."""

    def my_flow(x: int, y: int) -> int:
        return x + y

    def other_flow(name: str) -> str:
        return f"hello {name}"

    registry.register_flow(my_flow, name="my_flow")
    registry.register_flow(other_flow, name="other_flow")
    return registry


# ============================================================================
# Controller fixtures
# ============================================================================


@pytest.fixture
def mock_querier() -> AsyncMock:
    """AsyncMock stand-in for RunQuery."""
    querier = AsyncMock()
    querier.list_recent_states = AsyncMock(return_value=[])
    querier.get_run = Mock(return_value={})
    return querier


@pytest.fixture
def mock_queue() -> Mock:
    """Mock stand-in for a JobQueue."""
    q = Mock()
    q.enqueue = Mock()
    return q


@pytest.fixture
def controller(registry: Registry) -> FlowController:
    """FlowController with no querier and no queue."""
    return FlowController(registry=registry)


@pytest.fixture
def controller_with_querier(registry: Registry, mock_querier: AsyncMock) -> FlowController:
    """FlowController with a mocked querier (for query endpoint tests)."""
    return FlowController(registry=registry, querier=mock_querier)


@pytest.fixture
def controller_with_queue(registry: Registry, mock_queue: Mock) -> FlowController:
    """FlowController with a mocked queue (for submit endpoint tests)."""
    return FlowController(registry=registry, queue=mock_queue)


# ============================================================================
# Async SQLite fixtures (snapshot database layer)
# ============================================================================


@pytest_asyncio.fixture
async def async_engine():
    """In-memory async SQLite engine, schema created, disposed after test."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session(async_engine):
    """Single async SQLAlchemy session, rolled back after each test."""
    factory = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()
