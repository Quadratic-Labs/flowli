"""
Unit tests for the storage repository layer.

Covers:
- StateRepository: conditional write → read round-trips and CAS semantics
  on the cairndb store (the filesystem backend implements the same
  conditional-write contract as the cloud backends, so these exercise
  genuine CAS without mocks)
- LogRepository: span reading over the runs/<flow>/<date>/<run_id>/ layout
"""
from pathlib import Path
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import SpanRecord
from flowlet.repository.log import LogRepository
from flowlet.repository.state import StateRepository
from flowlet.serdes import to_json
from flowlet.storage import run_prefix


@pytest.fixture
def store(tmp_path: Path) -> FilesystemStorage:
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store) -> StateRepository:
    return StateRepository(store=store)


# ============================================================================
# StateRepository
# ============================================================================


@pytest.mark.unit
class TestStateRepository:
    def test_write_and_read_round_trip(self, state_repo, make_run_state):
        state = make_run_state(flow_name="my_flow")
        ok, etag = state_repo.write(state, etag=None)
        assert ok is True
        assert etag is not None

        result = state_repo.read(state.flow_name, state.run_id)
        assert result is not None
        restored, read_etag = result
        assert restored.run_id == state.run_id
        assert restored.flow_name == state.flow_name
        assert restored.status == state.status
        assert read_etag == etag

    def test_read_missing_returns_none(self, state_repo):
        assert state_repo.read("no_such_flow", uuid7()) is None

    def test_write_returns_false_on_stale_etag(self, state_repo, make_run_state):
        """A write with a stale etag must be rejected."""
        from attrs import evolve
        from flowlet.models import RunStatus

        state = make_run_state(status=RunStatus.running)
        ok, etag_v1 = state_repo.write(state, etag=None)
        assert ok

        # Second write with a correct etag succeeds and mints a new etag.
        ok2, etag_v2 = state_repo.write(evolve(state, status=RunStatus.completed), etag=etag_v1)
        assert ok2
        assert etag_v2 != etag_v1

        # Third write reusing the first (stale) etag must be rejected.
        ok3, etag_v3 = state_repo.write(evolve(state, status=RunStatus.running), etag=etag_v1)
        assert ok3 is False
        assert etag_v3 is None

    def test_write_updates_on_second_call(self, state_repo, make_run_state):
        from attrs import evolve
        from flowlet.models import RunStatus

        state = make_run_state(status=RunStatus.running)
        ok, etag = state_repo.write(state, etag=None)
        assert ok

        ok2, _ = state_repo.write(evolve(state, status=RunStatus.completed), etag=etag)
        assert ok2

        result = state_repo.read(state.flow_name, state.run_id)
        assert result is not None
        restored, _ = result
        assert restored.status == RunStatus.completed

    def test_write_new_fails_when_state_already_exists(self, state_repo, make_run_state):
        """etag=None is a put-if-absent — must fail if the state exists."""
        state = make_run_state()
        ok, _ = state_repo.write(state, etag=None)
        assert ok

        ok2, _ = state_repo.write(state, etag=None)
        assert ok2 is False

    def test_write_with_etag_after_delete_is_rejected(
        self, state_repo, make_run_state
    ):
        state = make_run_state()
        ok, etag = state_repo.write(state, etag=None)
        assert ok

        state_repo.delete(state.flow_name, state.run_id)
        assert state_repo.write(state, etag) == (False, None)

    def test_delete_removes_state(self, state_repo, make_run_state):
        state = make_run_state(flow_name="temp_flow")
        ok, _ = state_repo.write(state, etag=None)
        assert ok
        state_repo.delete(state.flow_name, state.run_id)
        assert state_repo.read(state.flow_name, state.run_id) is None

    def test_list_states_returns_all(self, state_repo, make_run_state):
        states = [make_run_state(flow_name="batch_flow") for _ in range(3)]
        for s in states:
            state_repo.write(s, etag=None)

        listed = state_repo.list_states(flow_name="batch_flow")
        assert {s.run_id for s in listed} == {s.run_id for s in states}

    def test_list_states_filtered_by_flow(self, state_repo, make_run_state):
        state_repo.write(make_run_state(flow_name="flow_a"), etag=None)
        state_repo.write(make_run_state(flow_name="flow_b"), etag=None)

        only_a = state_repo.list_states(flow_name="flow_a")
        assert len(only_a) == 1
        assert only_a[0].flow_name == "flow_a"

    def test_list_states_empty_when_no_state_prefix(self, state_repo):
        assert state_repo.list_states() == []

    def test_state_key_layout(self, state_repo, store, make_run_state):
        """State objects live at state/<flow>/<run_id>.json."""
        state = make_run_state(flow_name="my_flow")
        state_repo.write(state, etag=None)

        keys = store.list_objects_sync("state/")
        assert keys == [f"state/my_flow/{state.run_id}.json"]

    def test_archive_moves_state_into_run_folder(
        self, state_repo, store, make_run_state
    ):
        state = make_run_state(flow_name="my_flow")
        state_repo.write(state, etag=None)

        state_repo.archive(state.flow_name, state.run_id)

        assert state_repo.read(state.flow_name, state.run_id) is None
        archived = store.get_object_sync(
            f"{run_prefix(state.flow_name, state.run_id)}/state.json"
        )
        assert archived is not None
        assert str(state.run_id) in archived.data.decode()


# ============================================================================
# LogRepository helpers
# ============================================================================


def _write_span_file(store, flow_name: str, run_id, spans: list[SpanRecord], attempt: int = 1) -> None:
    """Write a spans-<attempt>.jsonl object in the runs/<flow>/<date>/<run_id>/ layout."""
    key = f"{run_prefix(flow_name, run_id)}/spans-{attempt}.jsonl"
    store.put_object_sync(
        key, "\n".join(to_json(span) for span in spans).encode("utf-8")
    )


@pytest.fixture
def log_repo(store) -> LogRepository:
    return LogRepository(store=store)


@pytest.mark.unit
class TestLogRepository:
    def test_get_spans_missing_folder_returns_empty(self, log_repo):
        assert log_repo.get_spans("ghost_flow", uuid7()) == []

    def test_get_spans_returns_written_records(self, log_repo, store, make_span_record):
        run_id = uuid7()
        spans = [
            make_span_record(run_id=run_id, span_id="a" * 16),
            make_span_record(run_id=run_id, span_id="b" * 16, parent_span_id="a" * 16),
        ]
        _write_span_file(store, "my_flow", run_id, spans)

        assert len(log_repo.get_spans("my_flow", run_id)) == 2

    def test_get_spans_merges_attempts(self, log_repo, store, make_span_record):
        run_id = uuid7()
        _write_span_file(
            store, "my_flow", run_id,
            [make_span_record(run_id=run_id, attempt=1)], attempt=1,
        )
        _write_span_file(
            store, "my_flow", run_id,
            [make_span_record(run_id=run_id, attempt=2)], attempt=2,
        )

        spans = log_repo.get_spans("my_flow", run_id)
        assert sorted(s.attempt for s in spans) == [1, 2]

    def test_list_run_ids_for_flow(self, log_repo, store, make_span_record):
        run_id = uuid7()
        _write_span_file(store, "my_flow", run_id, [make_span_record(run_id=run_id)])

        results = log_repo.list_run_ids(flow_name="my_flow")
        assert results == [("my_flow", run_id)]

    def test_list_run_ids_empty_when_no_runs_prefix(self, log_repo):
        assert log_repo.list_run_ids() == []

    def test_list_run_ids_sorted_chronologically(self, log_repo, store, make_span_record):
        first, second = uuid7(), uuid7()
        _write_span_file(store, "my_flow", second, [make_span_record(run_id=second)])
        _write_span_file(store, "my_flow", first, [make_span_record(run_id=first)])

        results = log_repo.list_run_ids(flow_name="my_flow")
        assert [r for _, r in results] == sorted([first, second], key=str)

    def test_malformed_lines_are_skipped(self, log_repo, store, make_span_record):
        run_id = uuid7()
        span = make_span_record(run_id=run_id)
        key = f"{run_prefix('my_flow', run_id)}/spans-1.jsonl"
        store.put_object_sync(
            key, (to_json(span) + "\n{not json}\n").encode("utf-8")
        )

        assert len(log_repo.get_spans("my_flow", run_id)) == 1
