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

from flowlet.models import RunLog, RunType
from flowlet.repository.log import LogRepository
from flowlet.repository.state import StateRepository
from flowlet.serdes import to_json
from flowlet.types import uuid7_desc


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
        assert state_repo.write(state) is True

        restored = state_repo.read(state.flow_name, state.run_id)
        assert restored is not None
        assert restored.run_id == state.run_id
        assert restored.flow_name == state.flow_name
        assert restored.status == state.status

    def test_read_missing_returns_none(self, state_repo):
        assert state_repo.read("no_such_flow", uuid7_desc()) is None

    def test_write_updates_on_second_call(self, state_repo, make_run_state):
        from attrs import evolve
        from flowlet.models import RunStatus

        state   = make_run_state(status=RunStatus.running)
        state_repo.write(state)
        state_repo.write(evolve(state, status=RunStatus.completed))

        restored = state_repo.read(state.flow_name, state.run_id)
        assert restored.status == RunStatus.completed

    def test_delete_removes_file(self, state_repo, make_run_state):
        state = make_run_state(flow_name="temp_flow")
        state_repo.write(state)
        state_repo.delete(state.flow_name, state.run_id)
        assert state_repo.read(state.flow_name, state.run_id) is None

    def test_list_states_returns_all(self, state_repo, make_run_state):
        states = [make_run_state(flow_name="batch_flow") for _ in range(3)]
        for s in states:
            state_repo.write(s)

        listed = state_repo.list_states(flow_name="batch_flow")
        assert {s.run_id for s in listed} == {s.run_id for s in states}

    def test_list_states_filtered_by_flow(self, state_repo, make_run_state):
        state_repo.write(make_run_state(flow_name="flow_a"))
        state_repo.write(make_run_state(flow_name="flow_b"))

        only_a = state_repo.list_states(flow_name="flow_a")
        assert len(only_a) == 1
        assert only_a[0].flow_name == "flow_a"

    def test_list_states_empty_when_no_state_dir(self, state_repo):
        assert state_repo.list_states() == []


# ============================================================================
# LogRepository helpers
# ============================================================================


def _write_log_file(base: Path, flow_name: str, span_id, logs: list[RunLog]) -> None:
    """Write a .jsonl log file in the expected runs/<name>/<uuid>.jsonl layout."""
    span_dir = base / "runs" / flow_name
    span_dir.mkdir(parents=True, exist_ok=True)
    (span_dir / f"{span_id}.jsonl").write_text(
        "\n".join(to_json(log) for log in logs), encoding="utf-8"
    )


@pytest.fixture
def log_repo(tmp_path: Path) -> LogRepository:
    return LogRepository(base_path=tmp_path)


@pytest.mark.unit
class TestLogRepository:
    def test_get_logs_missing_file_returns_empty(self, log_repo):
        assert log_repo.get_logs("ghost_flow", uuid7_desc()) == []

    def test_get_logs_returns_written_entries(self, log_repo, tmp_path, make_run_log):
        run_id  = uuid7_desc()
        span_id = uuid7_desc()
        entries = [
            make_run_log(run_id=run_id, span_id=span_id, level="INFO"),
            make_run_log(run_id=run_id, span_id=span_id, level="SUCCESS"),
        ]
        _write_log_file(tmp_path, "my_flow", span_id, entries)

        assert len(log_repo.get_logs("my_flow", span_id)) == 2

    def test_list_run_ids_for_flow(self, log_repo, tmp_path, make_run_log):
        span_id = uuid7_desc()
        _write_log_file(tmp_path, "my_flow", span_id, [make_run_log(span_id=span_id)])

        results = log_repo.list_run_ids(flow_name="my_flow")
        assert results == [("my_flow", span_id)]

    def test_list_run_ids_empty_when_no_runs_dir(self, log_repo):
        assert log_repo.list_run_ids() == []

    def test_get_logs_recursive_follows_child_references(
        self, log_repo, tmp_path, make_run_log
    ):
        run_id       = uuid7_desc()
        flow_span_id = uuid7_desc()
        task_span_id = uuid7_desc()

        parent_log = make_run_log(
            flow_name="my_flow", run_id=run_id, span_id=flow_span_id,
            span_type=RunType.flow, level="INFO",
            extra={"child_span_id": str(task_span_id), "child_span_name": "my_task"},
        )
        child_log = make_run_log(
            flow_name="my_flow", run_id=run_id, span_id=task_span_id,
            span_type=RunType.task, level="SUCCESS",
        )
        _write_log_file(tmp_path, "my_flow", flow_span_id, [parent_log])
        _write_log_file(tmp_path, "my_task", task_span_id, [child_log])

        all_logs  = log_repo.get_logs_recursive("my_flow", flow_span_id)
        span_ids  = {log.span_id for log in all_logs}
        assert len(all_logs) == 2
        assert flow_span_id in span_ids
        assert task_span_id in span_ids

    def test_get_logs_recursive_guards_against_cycles(
        self, log_repo, tmp_path, make_run_log
    ):
        span_id   = uuid7_desc()
        log_entry = make_run_log(
            span_id=span_id, level="INFO",
            extra={"child_span_id": str(span_id), "child_span_name": "my_flow"},
        )
        _write_log_file(tmp_path, "my_flow", span_id, [log_entry])

        logs = log_repo.get_logs_recursive("my_flow", span_id)
        assert len(logs) == 1
