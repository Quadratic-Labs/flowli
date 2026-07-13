"""
Unit tests for api/database.py — async snapshot functions.

Covers:
- ensure_snapshot_schema creates the runs table (no RunLink)
- upsert_run_state inserts a new row
- upsert_run_state updates an existing row (idempotent)
"""
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from flowlet.api.database import Run, upsert_run_state
from flowlet.models import RunStatus


@pytest.mark.unit
class TestEnsureSnapshotSchema:
    """ensure_snapshot_schema creates the runs table."""

    async def test_runs_table_exists(self, async_engine):
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
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
class TestUpsertRunState:
    """upsert_run_state inserts on first call and updates on re-call."""

    async def test_insert(self, async_session: AsyncSession, make_run_state):
        state = make_run_state(flow_name="flow_a")
        await upsert_run_state(async_session, state)
        await async_session.flush()

        row = await async_session.get(Run, state.run_id)
        assert row is not None
        assert row.flow_name == "flow_a"
        assert row.status == RunStatus.running.value

    async def test_update_is_idempotent(self, async_session: AsyncSession, make_run_state):
        state = make_run_state(status=RunStatus.running)
        await upsert_run_state(async_session, state)
        await async_session.flush()

        from attrs import evolve
        updated = evolve(state, status=RunStatus.completed)
        await upsert_run_state(async_session, updated)
        await async_session.flush()

        row = await async_session.get(Run, state.run_id)
        assert row.status == RunStatus.completed.value


