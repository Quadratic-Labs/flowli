"""
Pytest configuration and shared fixtures for flowlet tests.

Provides:
- Async SQLite engine for snapshot database tests
- Registry and FlowController fixtures for controller tests
- run_state / span_record factory fixtures (return a builder callable)
- Mock querier and queue stubs
"""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid7

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from flowlet.api.cache import Base
from flowlet.api.controller import FlowController
from flowlet.models import FlowJob, RunState, RunStatus, RunType, SpanEvent, SpanRecord
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


def _record_from_state(state: RunState) -> "ObligationRecord":
    """Reconstruct an ObligationRecord account matching a RunState spec.

    Test seeding convenience: builders keep describing runs in the flat
    RunState vocabulary, and this expands them into a consistent account —
    ``running`` gets an open in-flight attempt, ``pending`` closed rejected
    attempts, terminal statuses a closed obligation.  The record's
    ``summary()`` round-trips back to the given state.
    """
    from flowlet.models import (
        Attempt,
        AttemptOutcome,
        Obligation,
        ObligationRecord,
        ObligationStatus,
        Verdict,
        VerdictDecision,
    )

    obligation = Obligation(
        id=state.run_id,
        flow_name=state.flow_name,
        kwargs=state.kwargs,
        max_retries=state.max_retries,
        created_at=state.started_at,
        closed_at=state.ended_at,
        cancel_requested=state.cancel_requested,
    )
    record = ObligationRecord(obligation=obligation)

    def _attempt(n: int, outcome, decision) -> Attempt:
        verdict = None
        if decision is not None:
            verdict = Verdict(decision=decision, rendered_at=_make_ts())
        return Attempt(
            n=n,
            executor=state.worker_id,
            started_at=state.started_at,
            ended_at=_make_ts() if outcome is not None else None,
            outcome=outcome,
            verdict=verdict,
        )

    rejected = (AttemptOutcome.raised, VerdictDecision.rejected)
    if state.status == RunStatus.running:
        finals = [(None, None)]
    elif state.status == RunStatus.pending:
        finals = [rejected]
    elif state.status == RunStatus.completed:
        finals = [(AttemptOutcome.returned, VerdictDecision.accepted)]
        obligation.status = ObligationStatus.discharged
        obligation.closed_at = state.ended_at or _make_ts()
    elif state.status == RunStatus.canceled:
        finals = [(AttemptOutcome.interrupted, None)]
        obligation.status = ObligationStatus.abandoned
        obligation.cause = "canceled"
        obligation.cancel_requested = True
        obligation.closed_at = state.ended_at or _make_ts()
    else:  # failed and legacy terminal values
        finals = [rejected]
        obligation.status = ObligationStatus.abandoned
        obligation.cause = "max_retries_exceeded"
        obligation.closed_at = state.ended_at or _make_ts()

    for n in range(1, state.attempt + 1 - len(finals)):
        record.attempts.append(_attempt(n, *rejected))
    for offset, (outcome, decision) in enumerate(finals):
        n = state.attempt - len(finals) + 1 + offset
        if n >= 1:
            record.attempts.append(_attempt(n, outcome, decision))
    return record


def _seed_lease(
    store,
    state,
    *,
    holder: str | None = None,
    deadline: datetime | None = None,
    epoch: int = 1,
) -> None:
    """Write an obligation's lease document directly — test seeding only.

    Accepts either an ObligationRecord or a RunState spec (expanded via
    :func:`_record_from_state`).  Defaults model a *released* lease
    (holder None, deadline = now, the release time).  Pass ``holder`` and a
    future ``deadline`` for a held lease, or a past deadline for an
    expired one.
    """
    import json

    from flowlet.serdes import to_payload

    record = state if not isinstance(state, RunState) else _record_from_state(state)
    doc = {
        "epoch": epoch,
        "holder": holder,
        "deadline_at": (deadline or datetime.now(UTC)).isoformat(),
        "state": to_payload(record),
    }
    obligation = record.obligation
    key = f"state/{obligation.flow_name}/{obligation.id}.json"
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
def make_record():
    """Build an ObligationRecord from RunState-style keyword arguments."""
    return lambda **kwargs: _record_from_state(_make_run_state(**kwargs))


@pytest.fixture
def make_flow_job():
    """Return the _make_flow_job builder callable."""
    return _make_flow_job


@pytest.fixture
def make_span_record():
    """Return the _make_span_record builder callable."""
    return _make_span_record


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
def controller() -> FlowController:
    """FlowController with no querier and no queue."""
    return FlowController()


@pytest.fixture
def controller_with_querier(mock_querier: AsyncMock) -> FlowController:
    """FlowController with a mocked querier (for query endpoint tests)."""
    return FlowController(querier=mock_querier)


@pytest.fixture
def controller_with_queue(mock_queue: Mock) -> FlowController:
    """FlowController with a mocked queue (for submit endpoint tests)."""
    return FlowController(queue=mock_queue)


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
