"""Unit tests for the run-history CairnDB projection.

Covers:
- RunHistory.record_many: durable append of run.archived events
- refresh_history_db: projection round trip, idempotent upserts, incremental replay
- sweeper integration: record-before-archive ordering and failure handling
"""
import asyncio
import json
from datetime import UTC, datetime, timedelta

import aiosqlite
import pytest
from cairndb.engine.logs import NamespacedStorage
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.history import HISTORY_LOG_NAME, RUN_ARCHIVED, RunHistory, refresh_history_db
from flowlet.models import RunStatus
from flowlet.repository import StateRepository
from flowlet.sweeper import sweep
from flowlet.types import Timestamp


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path / "bucket")


@pytest.fixture
def history_log(store):
    """Namespaced view of the history named log (logs/history/)."""
    return NamespacedStorage(store, f"logs/{HISTORY_LOG_NAME}")


@pytest.fixture
def history(store):
    return RunHistory(store=store)


def _rows(db_path):
    async def inner():
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute(
                "SELECT run_id, flow_name, status, state_json FROM runs ORDER BY run_id"
            )
            return await cursor.fetchall()

    return asyncio.run(inner())


def _closed(make_run_state, **overrides):
    defaults: dict = {
        "status": RunStatus.completed,
        "ended_at": Timestamp(datetime.now(UTC) - timedelta(hours=2)),
    }
    defaults.update(overrides)
    return make_run_state(**defaults)


def _seed(store, state):
    """Write a released lease document for a RunState-spec'd account."""
    from conftest import _seed_lease

    _seed_lease(store, state)


@pytest.mark.unit
class TestRunHistory:
    def test_record_and_project_round_trip(
        self, store, history, tmp_path, make_run_state
    ):
        states = [_closed(make_run_state, flow_name="etl") for _ in range(3)]
        history.record_many(states)

        db_path = str(tmp_path / "history.db")
        asyncio.run(refresh_history_db(store, db_path))

        rows = _rows(db_path)
        assert {row[0] for row in rows} == {str(s.run_id) for s in states}
        assert all(row[1] == "etl" and row[2] == "completed" for row in rows)
        # state_json keeps the full serialized state for forward compatibility
        payload = json.loads(rows[0][3])
        assert payload["max_retries"] == states[0].max_retries

    def test_record_empty_is_noop(self, history_log, history):
        history.record_many([])
        assert asyncio.run(history_log.list_commits()) == []

    def test_re_recording_is_idempotent(
        self, store, history_log, history, tmp_path, make_run_state
    ):
        """A sweep crash between record and archive re-records the run."""
        state = _closed(make_run_state)
        history.record_many([state])
        history.record_many([state])  # second sweep pass

        db_path = str(tmp_path / "history.db")
        asyncio.run(refresh_history_db(store, db_path))

        assert asyncio.run(history_log.list_commits()) == [1, 2]  # two events in the log
        assert len(_rows(db_path)) == 1  # but one projected row

    def test_incremental_refresh_applies_only_the_tail(
        self, store, history, tmp_path, make_run_state
    ):
        db_path = str(tmp_path / "history.db")

        history.record_many([_closed(make_run_state)])
        asyncio.run(refresh_history_db(store, db_path))
        assert len(_rows(db_path)) == 1

        history.record_many([_closed(make_run_state), _closed(make_run_state)])
        asyncio.run(refresh_history_db(store, db_path))
        assert len(_rows(db_path)) == 3

    def test_events_carry_the_archived_type(self, history_log, history, make_run_state):
        from cairndb import Commit

        history.record_many([_closed(make_run_state)])
        raw = asyncio.run(history_log.get_commit(1))
        commit = Commit.from_msgpack(raw)
        assert [e.event_type for e in commit.events] == [RUN_ARCHIVED]


@pytest.mark.unit
class TestSweeperHistoryIntegration:
    class _FakeQueue:
        def enqueue(self, job, delay=0):
            return job.job_id

        def dequeue(self, timeout=None):  # pragma: no cover
            return None

        def ack(self, job_id):  # pragma: no cover
            pass

    def test_sweep_records_then_archives(
        self, store, history_log, history, make_run_state
    ):
        state_repo = StateRepository(store=store)
        state = _closed(make_run_state)
        _seed(store, state)

        stats = sweep(state_repo, self._FakeQueue(), archive_grace=3600, history=history)

        assert stats.archived == 1
        assert state_repo.read(state.flow_name, state.run_id) is None
        assert asyncio.run(history_log.list_commits()) == [1]

    def test_sweep_without_history_archives_as_before(self, store, make_run_state):
        state_repo = StateRepository(store=store)
        state = _closed(make_run_state)
        _seed(store, state)

        stats = sweep(state_repo, self._FakeQueue(), archive_grace=3600)

        assert stats.archived == 1

    def test_failed_recording_blocks_archiving(self, store, make_run_state):
        """If the history write fails the state file must stay for a retry."""

        class BoomHistory:
            def record_many(self, states):
                raise RuntimeError("history log unavailable")

        state_repo = StateRepository(store=store)
        state = _closed(make_run_state)
        _seed(store, state)

        stats = sweep(
            state_repo, self._FakeQueue(), archive_grace=3600, history=BoomHistory()
        )

        assert stats.archived == 0
        assert stats.errors == 1
        assert state_repo.read(state.flow_name, state.run_id) is not None

    def test_recent_closed_run_not_recorded(
        self, store, history_log, history, make_run_state
    ):
        """Runs inside the grace window are neither recorded nor archived."""
        state_repo = StateRepository(store=store)
        state = make_run_state(status=RunStatus.completed, ended_at=Timestamp.now())
        _seed(store, state)

        stats = sweep(state_repo, self._FakeQueue(), archive_grace=3600, history=history)

        assert stats.archived == 0
        assert asyncio.run(history_log.list_commits()) == []
