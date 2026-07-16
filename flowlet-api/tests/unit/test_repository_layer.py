"""
Unit tests for the storage repository layer.

Covers:
- StateRepository: write → read round-trip on local filesystem
- StateRepository: missing file returns None
- StateRepository: delete removes state file
- StateRepository: list_states returns persisted states
- LogRepository: get_logs returns empty list when file absent
- LogRepository: list_run_ids returns ids for present log files
- LogRepository: get_logs_recursive follows child references
"""
from pathlib import Path

import pytest

from flowlet.models import RunType, SpanRecord
from flowlet.repository.log import LogRepository, run_folder
from flowlet.repository.state import StateRepository
from flowlet.serdes import to_json
from uuid import uuid7


# ============================================================================
# StateRepository
# ============================================================================


@pytest.fixture
def state_repo(tmp_path: Path) -> StateRepository:
    return StateRepository(root=tmp_path)


@pytest.mark.unit
class TestStateRepository:
    def test_write_and_read_round_trip(self, state_repo, make_run_state):
        state = make_run_state(flow_name="my_flow")
        ok, etag = state_repo.write(state, etag=None)
        assert ok is True
        assert etag is not None

        result = state_repo.read(state.flow_name, state.run_id)
        assert result is not None
        restored, _ = result
        assert restored.run_id == state.run_id
        assert restored.flow_name == state.flow_name
        assert restored.status == state.status

    def test_read_missing_returns_none(self, state_repo):
        assert state_repo.read("no_such_flow", uuid7()) is None

    def test_write_returns_false_on_stale_etag(self, state_repo, make_run_state):
        """A write with a stale ETag must be rejected."""
        from attrs import evolve
        from flowlet.models import RunStatus

        state = make_run_state(status=RunStatus.running)
        ok, etag_v1 = state_repo.write(state, etag=None)
        assert ok

        # Second write with a correct ETag succeeds and mints a new ETag.
        ok2, etag_v2 = state_repo.write(evolve(state, status=RunStatus.completed), etag=etag_v1)
        assert ok2
        assert etag_v2 != etag_v1

        # Third write reusing the first (stale) ETag must be rejected.
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

    def test_write_new_fails_when_file_already_exists(self, state_repo, make_run_state):
        """etag=None asserts the file does not exist — must fail if it does."""
        state = make_run_state()
        ok, _ = state_repo.write(state, etag=None)
        assert ok

        ok2, _ = state_repo.write(state, etag=None)
        assert ok2 is False

    def test_delete_removes_file(self, state_repo, make_run_state):
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

    def test_list_states_empty_when_no_state_dir(self, state_repo):
        assert state_repo.list_states() == []


# ============================================================================
# StateRepository — remote branch (etag CAS via the chroniql object store)
# ============================================================================


@pytest.fixture
def remote_repo(tmp_path: Path) -> StateRepository:
    """Repository whose remote branch runs against a real chroniql store.

    The chroniql filesystem backend implements the same conditional-write
    contract as the cloud backends, so these tests exercise genuine CAS
    semantics without mocks.
    """
    from chroniql.storage.filesystem import FilesystemStorage

    return StateRepository(
        root=tmp_path, object_store=FilesystemStorage(tmp_path / "bucket")
    )


@pytest.mark.unit
class TestStateRepositoryRemote:
    def test_write_and_read_round_trip(self, remote_repo, make_run_state):
        state = make_run_state(flow_name="my_flow")
        sp = remote_repo._state_path(state.flow_name, state.run_id)

        ok, etag = remote_repo._write_remote(sp, state, etag=None)
        assert ok is True
        assert etag is not None

        result = remote_repo._read_remote(sp)
        assert result is not None
        restored, read_etag = result
        assert restored.run_id == state.run_id
        assert restored.status == state.status
        assert read_etag == etag

    def test_read_missing_returns_none(self, remote_repo, make_run_state):
        state = make_run_state()
        sp = remote_repo._state_path(state.flow_name, state.run_id)
        assert remote_repo._read_remote(sp) is None

    def test_create_fails_when_object_exists(self, remote_repo, make_run_state):
        """etag=None asserts the object does not exist — put-if-absent."""
        state = make_run_state()
        sp = remote_repo._state_path(state.flow_name, state.run_id)

        assert remote_repo._write_remote(sp, state, etag=None)[0] is True
        assert remote_repo._write_remote(sp, state, etag=None) == (False, None)

    def test_stale_etag_is_rejected(self, remote_repo, make_run_state):
        from attrs import evolve
        from flowlet.models import RunStatus

        state = make_run_state(status=RunStatus.running)
        sp = remote_repo._state_path(state.flow_name, state.run_id)

        ok, etag_v1 = remote_repo._write_remote(sp, state, etag=None)
        assert ok

        ok2, etag_v2 = remote_repo._write_remote(
            sp, evolve(state, status=RunStatus.completed), etag=etag_v1
        )
        assert ok2
        assert etag_v2 != etag_v1

        # Reusing the stale etag must lose ownership.
        assert remote_repo._write_remote(sp, state, etag=etag_v1) == (False, None)

        restored, _ = remote_repo._read_remote(sp)
        assert restored.status == RunStatus.completed

    def test_write_with_etag_after_delete_is_rejected(
        self, remote_repo, make_run_state
    ):
        state = make_run_state()
        sp = remote_repo._state_path(state.flow_name, state.run_id)
        ok, etag = remote_repo._write_remote(sp, state, etag=None)
        assert ok

        remote_repo.object_store.delete_object_sync(
            remote_repo._state_key(state.flow_name, state.run_id)
        )
        assert remote_repo._write_remote(sp, state, etag=etag) == (False, None)

    def test_missing_object_store_raises(self, tmp_path, make_run_state):
        repo = StateRepository(root=tmp_path)  # no object_store
        state = make_run_state()
        sp = repo._state_path(state.flow_name, state.run_id)

        with pytest.raises(RuntimeError, match="chroniql object store"):
            repo._write_remote(sp, state, etag=None)

    def test_state_key_layout(self, remote_repo, make_run_state):
        """Remote keys mirror the state/<flow>/<run_id>.json path layout."""
        state = make_run_state(flow_name="my_flow")
        sp = remote_repo._state_path(state.flow_name, state.run_id)
        remote_repo._write_remote(sp, state, etag=None)

        keys = remote_repo.object_store.list_objects_sync("state/")
        assert keys == [f"state/my_flow/{state.run_id}.json"]


# ============================================================================
# LogRepository helpers
# ============================================================================


def _write_span_file(base: Path, flow_name: str, run_id, spans: list[SpanRecord], attempt: int = 1) -> None:
    """Write a spans-<attempt>.jsonl file in the runs/<flow>/<date>/<run_id>/ layout."""
    folder = run_folder(base, flow_name, run_id)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"spans-{attempt}.jsonl").write_text(
        "\n".join(to_json(span) for span in spans), encoding="utf-8"
    )


@pytest.fixture
def log_repo(tmp_path: Path) -> LogRepository:
    return LogRepository(base_path=tmp_path)


@pytest.mark.unit
class TestLogRepository:
    def test_get_spans_missing_folder_returns_empty(self, log_repo):
        assert log_repo.get_spans("ghost_flow", uuid7()) == []

    def test_get_spans_returns_written_records(self, log_repo, tmp_path, make_span_record):
        run_id = uuid7()
        spans = [
            make_span_record(run_id=run_id, span_id="a" * 16),
            make_span_record(run_id=run_id, span_id="b" * 16, parent_span_id="a" * 16),
        ]
        _write_span_file(tmp_path, "my_flow", run_id, spans)

        assert len(log_repo.get_spans("my_flow", run_id)) == 2

    def test_get_spans_merges_attempts(self, log_repo, tmp_path, make_span_record):
        run_id = uuid7()
        _write_span_file(
            tmp_path, "my_flow", run_id,
            [make_span_record(run_id=run_id, attempt=1)], attempt=1,
        )
        _write_span_file(
            tmp_path, "my_flow", run_id,
            [make_span_record(run_id=run_id, attempt=2)], attempt=2,
        )

        spans = log_repo.get_spans("my_flow", run_id)
        assert sorted(s.attempt for s in spans) == [1, 2]

    def test_list_run_ids_for_flow(self, log_repo, tmp_path, make_span_record):
        run_id = uuid7()
        _write_span_file(tmp_path, "my_flow", run_id, [make_span_record(run_id=run_id)])

        results = log_repo.list_run_ids(flow_name="my_flow")
        assert results == [("my_flow", run_id)]

    def test_list_run_ids_empty_when_no_runs_dir(self, log_repo):
        assert log_repo.list_run_ids() == []

    def test_list_run_ids_sorted_chronologically(self, log_repo, tmp_path, make_span_record):
        first, second = uuid7(), uuid7()
        _write_span_file(tmp_path, "my_flow", second, [make_span_record(run_id=second)])
        _write_span_file(tmp_path, "my_flow", first, [make_span_record(run_id=first)])

        results = log_repo.list_run_ids(flow_name="my_flow")
        assert [r for _, r in results] == sorted([first, second], key=str)

    def test_malformed_lines_are_skipped(self, log_repo, tmp_path, make_span_record):
        run_id = uuid7()
        span = make_span_record(run_id=run_id)
        folder = run_folder(tmp_path, "my_flow", run_id)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "spans-1.jsonl").write_text(
            to_json(span) + "\n{not json}\n", encoding="utf-8"
        )

        assert len(log_repo.get_spans("my_flow", run_id)) == 1
