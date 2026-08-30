"""Unit tests for CacheRepository: pull-based refresh from state files."""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from sqlalchemy import select

from flowlet.api.cache import CacheRepository, ObligationRow
from flowlet.history import RunHistory
from flowlet.models import ReportedStatus
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


@pytest.fixture
def history(store, tmp_path):
    return RunHistory(store=store, db_path=str(tmp_path / "history.db"), ttl=0.0)


async def _rows(cache):
    async with cache.session_factory()() as session:
        result = await session.execute(select(ObligationRow))
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
        assert rows[0].obligation_id == state.obligation_id
        assert rows[0].status == ReportedStatus.running.value

    async def test_state_transitions_are_reflected(
        self, cache, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state()
        seed_lease(store, state)
        await cache.refresh()

        state.status = ReportedStatus.completed
        state.ended_at = Timestamp.now()
        seed_lease(store, state, epoch=2)
        await cache.refresh(force=True)

        rows = await _rows(cache)
        assert rows[0].status == ReportedStatus.completed.value

    async def test_archived_state_survives_removal_from_active_dir(
        self, cache, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=ReportedStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)
        state_repo.archive(state.flow_name, state.obligation_id)
        assert state_repo.list_states() == []

        await cache.refresh()

        rows = await _rows(cache)
        assert len(rows) == 1
        assert rows[0].obligation_id == state.obligation_id
        assert rows[0].status == ReportedStatus.completed.value

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


@pytest.mark.unit
class TestHistorySeeding:
    """When history is configured, a newly-seen archived obligation_id is looked up
    there first — coverage must be identical whether or not it's found."""

    def test_run_known_to_history_is_seeded_without_reading_state_json(
        self, state_repo, history, make_run_state, seed_lease, store
    ):
        import asyncio

        state = make_run_state(
            status=ReportedStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)
        state_repo.archive(state.flow_name, state.obligation_id)

        history.record_many([state])  # sync — must run outside any event loop

        # Prove the cache doesn't need the blob's content: corrupt it before
        # refreshing.  Deleting it outright would also remove it from the
        # listing _read_new_archived_states discovers obligation_ids from in the
        # first place, which would prove nothing either way.
        from flowlet.storage import run_prefix
        key = f"{run_prefix(state.flow_name, state.obligation_id)}/state.json"
        store.put_object_sync(key, b"not valid json")

        cache = CacheRepository(state_repo=state_repo, store=store, history=history, ttl=0.0)
        asyncio.run(cache.refresh())

        rows = asyncio.run(_rows(cache))
        assert len(rows) == 1
        assert rows[0].obligation_id == state.obligation_id
        assert rows[0].status == ReportedStatus.completed.value

    async def test_run_unknown_to_history_falls_back_to_state_json(
        self, state_repo, history, make_run_state, seed_lease, store
    ):
        """A run history has never heard of (e.g. archived before history was
        enabled) is still picked up — history is never a coverage gate."""
        state = make_run_state(
            status=ReportedStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)
        state_repo.archive(state.flow_name, state.obligation_id)
        # Deliberately not recorded to history.

        cache = CacheRepository(state_repo=state_repo, store=store, history=history, ttl=0.0)
        await cache.refresh()

        rows = await _rows(cache)
        assert len(rows) == 1
        assert rows[0].obligation_id == state.obligation_id
        assert rows[0].status == ReportedStatus.completed.value
