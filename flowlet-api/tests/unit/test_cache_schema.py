"""
Unit tests for the cache's ORM schema (api/cache.py) — async snapshot functions.

Covers:
- ensure_snapshot_schema creates the obligations table (no RunLink)
- upsert_obligation_summary inserts a new row
- upsert_obligation_summary updates an existing row (idempotent)
"""
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from flowlet.api.cache import ObligationRow, upsert_obligation_summary
from flowlet.models import ReportedStatus


@pytest.mark.unit
class TestEnsureSnapshotSchema:
    """ensure_snapshot_schema creates the obligations table."""

    async def test_obligations_table_exists(self, async_engine):
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='obligations'")
            )
            assert result.fetchone() is not None

    async def test_no_runlink_table(self, async_engine):
        """RunLink has been removed — the table must not exist."""
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='run_links'")
            )
            assert result.fetchone() is None


@pytest.mark.unit
class TestUpsertObligationSummary:
    """upsert_obligation_summary inserts on first call and updates on re-call."""

    async def test_insert(self, async_session: AsyncSession, make_obligation_summary):
        state = make_obligation_summary(flow_name="flow_a")
        await upsert_obligation_summary(async_session, state)
        await async_session.flush()

        row = await async_session.get(ObligationRow, state.obligation_id)
        assert row is not None
        assert row.flow_name == "flow_a"
        assert row.status == ReportedStatus.running.value

    async def test_update_is_idempotent(self, async_session: AsyncSession, make_obligation_summary):
        state = make_obligation_summary(status=ReportedStatus.running)
        await upsert_obligation_summary(async_session, state)
        await async_session.flush()

        from attrs import evolve
        updated = evolve(state, status=ReportedStatus.completed)
        await upsert_obligation_summary(async_session, updated)
        await async_session.flush()

        row = await async_session.get(ObligationRow, state.obligation_id)
        assert row.status == ReportedStatus.completed.value


