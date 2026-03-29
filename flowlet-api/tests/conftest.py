"""
Pytest configuration and shared fixtures for flowlet tests.

Provides:
- Async SQLite engine for snapshot database tests
- Registry and FlowController fixtures for controller tests
- run_state / run_log factory fixtures (return a builder callable)
- Mock querier and queue stubs
"""
import pytest
import pytest_asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from flowlet.api.controller import FlowController
from flowlet.api.database import Base
from flowlet.models import FlowJob, RunLog, RunState, RunStatus, RunType
from flowlet.registry import Registry
from flowlet.types import Timestamp, uuid7_desc


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
    heartbeat_at: Timestamp | None = None,
    ended_at: Timestamp | None = None,
    deadline_at: Timestamp | None = None,
    attempt: int = 1,
    max_retries: int = 3,
) -> RunState:
    now = _make_ts()
    return RunState(
        run_id=run_id or uuid7_desc(),
        flow_name=flow_name,
        status=status,
        worker_id=worker_id,
        started_at=started_at or now,
        heartbeat_at=heartbeat_at or now,
        ended_at=ended_at,
        deadline_at=deadline_at,
        attempt=attempt,
        max_retries=max_retries,
    )


def _make_flow_job(
    *,
    flow_name: str = "test_flow",
    kwargs: dict | None = None,
    retry_count: int = 0,
    max_retries: int = 3,
    visibility_timeout: int = 300,
    timeout_seconds: int | None = None,
) -> FlowJob:
    return FlowJob(
        flow_name=flow_name,
        kwargs=kwargs or {},
        retry_count=retry_count,
        max_retries=max_retries,
        visibility_timeout=visibility_timeout,
        timeout_seconds=timeout_seconds,
    )


def _make_run_log(
    *,
    flow_name: str = "test_flow",
    run_id: UUID | None = None,
    span_type: RunType = RunType.flow,
    span_name: str = "test_flow",
    span_id: UUID | None = None,
    parent_span_id: UUID | None = None,
    ts: Timestamp | None = None,
    message: str = "log entry",
    level: str = "INFO",
    extra: dict | None = None,
) -> RunLog:
    return RunLog(
        flow_name=flow_name,
        run_id=run_id or uuid7_desc(),
        span_type=span_type,
        span_name=span_name,
        span_id=span_id or uuid7_desc(),
        parent_span_id=parent_span_id,
        ts=ts or _make_ts(),
        message=message,
        level=level,
        extra=extra or {},
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
def make_flow_job():
    """Return the _make_flow_job builder callable."""
    return _make_flow_job


@pytest.fixture
def make_run_log():
    """Return the _make_run_log builder callable."""
    return _make_run_log


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
