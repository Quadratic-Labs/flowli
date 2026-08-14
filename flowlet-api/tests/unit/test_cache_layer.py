"""Unit tests for CacheRepository: pull-based refresh from state files."""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from sqlalchemy import select

from flowlet.api.cache import CacheRepository
from flowlet.api.database import Run
from flowlet.models import RunStatus
from flowlet.repository import StateRepository
from flowlet.types import Timestamp


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store):
    return StateRepository(store=store)


@pytest.fixture
def cache(state_repo, tmp_path):
    return CacheRepository(state_repo=state_repo, store=state_repo.store, ttl=0.0)


async def _rows(cache):
    async with cache.session_factory()() as session:
        result = await session.execute(select(Run))
        return list(result.scalars().all())


@pytest.mark.unit
class TestRefresh:
    async def test_active_states_are_indexed(
        self, cache, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state(flow_name="flow_a")
        seed_lease(store, state)

        await cache.refresh()

        rows = await _rows(cache)
        assert len(rows) == 1
        assert rows[0].run_id == state.run_id
        assert rows[0].status == RunStatus.running.value

    async def test_state_transitions_are_reflected(
        self, cache, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state()
        seed_lease(store, state)
        await cache.refresh()

        state.status = RunStatus.completed
        state.ended_at = Timestamp.now()
        seed_lease(store, state, epoch=2)
        await cache.refresh(force=True)

        rows = await _rows(cache)
        assert rows[0].status == RunStatus.completed.value

    async def test_archived_state_survives_removal_from_active_dir(
        self, cache, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=RunStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)
        state_repo.archive(state.flow_name, state.run_id)
        assert state_repo.list_states() == []

        await cache.refresh()

        rows = await _rows(cache)
        assert len(rows) == 1
        assert rows[0].run_id == state.run_id
        assert rows[0].status == RunStatus.completed.value

    async def test_ttl_throttles_scans(
        self, state_repo, tmp_path, make_run_state, seed_lease, store
    ):
        cache = CacheRepository(state_repo=state_repo, store=state_repo.store, ttl=3600)
        await cache.refresh()

        seed_lease(store, make_run_state())
        await cache.refresh()  # within TTL — must not rescan

        assert await _rows(cache) == []

        await cache.refresh(force=True)
        assert len(await _rows(cache)) == 1
