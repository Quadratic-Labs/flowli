"""Unit tests for RunQuery: cache + history merge for long-horizon queries."""
import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.api.cache import CacheRepository
from flowlet.api.query import RunQuery
from flowlet.history import RunHistory
from flowlet.models import ReportedStatus
from flowlet.repository import StateRepository
from flowlet.repository.log import LogRepository
from flowlet.types import Timestamp


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path / "bucket")


@pytest.fixture
def state_repo(store):
    return StateRepository(store=store)


@pytest.fixture
def cache(state_repo, store):
    return CacheRepository(state_repo=state_repo, store=store, ttl=0.0)


@pytest.fixture
def history(store, tmp_path):
    return RunHistory(store=store, db_path=str(tmp_path / "history.db"), ttl=0.0)


@pytest.fixture
def query(cache, store, history):
    return RunQuery(cache_repo=cache, log_repo=LogRepository(store), history=history)


def _archived(make_run_state, **overrides):
    defaults: dict = {
        "status": ReportedStatus.completed,
        "ended_at": Timestamp(datetime.now(UTC) - timedelta(hours=2)),
    }
    defaults.update(overrides)
    return make_run_state(**defaults)


@pytest.mark.unit
class TestHistoryMerge:
    """list_recent_states merges the ephemeral cache with the durable history
    projection: additive, deduped by obligation_id, cache wins on conflict."""

    def test_run_visible_only_via_history_is_merged_in(
        self, query, history, make_run_state
    ):
        """A run the cache never saw (nothing under state/ or runs/) still
        surfaces once it's been recorded to history."""
        state = _archived(make_run_state, flow_name="etl")
        history.record_many([state])

        rows = asyncio.run(query.list_recent_states(["etl"]))
        assert [r.obligation_id for r in rows] == [state.obligation_id]

    def test_cache_row_wins_over_history_duplicate(
        self, query, store, history, seed_lease, make_run_state
    ):
        """The same obligation_id known to both sources is returned once, from the cache."""
        state = _archived(make_run_state, flow_name="etl")
        seed_lease(store, state)
        history.record_many([state])

        rows = asyncio.run(query.list_recent_states(["etl"]))
        assert len(rows) == 1
        assert rows[0].obligation_id == state.obligation_id

    def test_history_extends_flow_discovery_when_flow_names_is_none(
        self, query, history, make_run_state
    ):
        """A flow the cache has zero rows for is still discovered via history."""
        state = _archived(make_run_state, flow_name="only_in_history")
        history.record_many([state])

        rows = asyncio.run(query.list_recent_states())
        assert state.obligation_id in {r.obligation_id for r in rows}

    def test_last_n_respected_across_merged_sources(
        self, query, history, make_run_state
    ):
        states = [_archived(make_run_state, flow_name="etl") for _ in range(3)]
        history.record_many(states)

        rows = asyncio.run(query.list_recent_states(["etl"], last_n=2))
        assert len(rows) == 2

    def test_no_history_configured_falls_back_to_cache_only(self, cache, store):
        """RunQuery without a RunHistory behaves exactly as before."""
        query = RunQuery(cache_repo=cache, log_repo=LogRepository(store))
        assert asyncio.run(query.list_recent_states(["etl"])) == []
