"""
Unit tests for domain model layer.

Covers:
- RunStatus helpers: from_log_level, is_closed
- RunSummary.duration computed property
- RunState construction and defaults
- FlowJob defaults
"""
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from flowlet.models import FlowJob, RunStatus, RunSummary, RunType
from flowlet.types import Timestamp, uuid7_desc
from flowlet.worker import JobState, existing_state_case


@pytest.mark.unit
class TestRunStatus:
    """from_log_level maps logging levels; is_closed gates terminal states."""

    @pytest.mark.parametrize(
        "level, expected",
        [
            ("SUCCESS", RunStatus.completed),
            ("success", RunStatus.completed),
            ("WARNING", RunStatus.warning),
            ("warning", RunStatus.warning),
            ("ERROR", RunStatus.failed),
            ("CRITICAL", RunStatus.failed),
            ("INFO", RunStatus.running),
            ("DEBUG", RunStatus.running),
        ],
    )
    def test_from_log_level(self, level: str, expected: RunStatus):
        assert RunStatus.from_log_level(level) == expected

    @pytest.mark.parametrize(
        "status, closed",
        [
            (RunStatus.completed, True),
            (RunStatus.failed, True),
            (RunStatus.warning, True),
            (RunStatus.canceled, True),
            (RunStatus.running, False),
            (RunStatus.pending, False),
            (RunStatus.retry, False),
            (RunStatus.stopped, False),
        ],
    )
    def test_is_closed(self, status: RunStatus, closed: bool):
        assert status.is_closed() is closed


@pytest.mark.unit
class TestRunSummary:
    """RunSummary.duration is the elapsed time between start and end."""

    def test_duration_with_timestamps(self, make_ts):
        start = make_ts(datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC))
        end   = make_ts(datetime(2024, 1, 1, 12, 0, 5, tzinfo=UTC))
        span  = RunSummary(
            span_id=uuid7_desc(),
            span_name="my_flow",
            span_type=RunType.flow,
            status=RunStatus.completed,
            start_ts=start,
            end_ts=end,
        )
        assert span.duration == timedelta(seconds=5)

    def test_duration_none_when_missing_start(self, make_ts):
        span = RunSummary(
            span_id=uuid7_desc(),
            span_name="my_flow",
            span_type=RunType.flow,
            status=RunStatus.running,
            start_ts=None,
            end_ts=make_ts(),
        )
        assert span.duration is None

    def test_duration_none_when_missing_end(self, make_ts):
        span = RunSummary(
            span_id=uuid7_desc(),
            span_name="my_flow",
            span_type=RunType.flow,
            status=RunStatus.running,
            start_ts=make_ts(),
            end_ts=None,
        )
        assert span.duration is None

    def test_children_defaults_to_empty_list(self):
        span = RunSummary(
            span_id=uuid7_desc(),
            span_name="my_flow",
            span_type=RunType.flow,
            status=RunStatus.pending,
        )
        assert span.children == []


@pytest.mark.unit
class TestRunState:
    """RunState construction and default values."""

    def test_basic_construction(self, make_run_state):
        state = make_run_state()
        assert state.flow_name == "test_flow"
        assert state.status == RunStatus.running
        assert state.attempt == 1
        assert state.max_retries == 3
        assert state.ended_at is None

    def test_ended_at_can_be_set(self, make_run_state, make_ts):
        end_ts = make_ts()
        state  = make_run_state(status=RunStatus.completed, ended_at=end_ts)
        assert state.ended_at is end_ts
        assert state.status.is_closed()

    def test_run_id_is_uuid(self, make_run_state):
        state = make_run_state()
        assert isinstance(state.run_id, UUID)


@pytest.mark.unit
class TestExistingStateCase:
    """existing_state_case maps RunState (or None) to JobState correctly."""

    _STALE_THRESHOLD = 90  # seconds

    def test_no_state_returns_new(self):
        assert existing_state_case(None, self._STALE_THRESHOLD) == JobState.new

    def test_closed_status_returns_closed(self, make_run_state):
        for status in (RunStatus.completed, RunStatus.failed, RunStatus.canceled):
            state = make_run_state(status=status)
            assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.closed

    def test_pending_status_returns_ready(self, make_run_state):
        state = make_run_state(status=RunStatus.pending)
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.ready

    def test_live_heartbeat_no_deadline_returns_busy(self, make_run_state):
        state = make_run_state(status=RunStatus.running, heartbeat_at=Timestamp.now())
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.busy

    def test_stale_heartbeat_under_max_retries_returns_stale(self, make_run_state, make_ts):
        old_hb = make_ts(datetime.now(UTC) - timedelta(seconds=200))
        state = make_run_state(status=RunStatus.running, heartbeat_at=old_hb, attempt=1, max_retries=3)
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.stale

    def test_stale_heartbeat_at_max_retries_returns_failed(self, make_run_state, make_ts):
        old_hb = make_ts(datetime.now(UTC) - timedelta(seconds=200))
        state = make_run_state(status=RunStatus.running, heartbeat_at=old_hb, attempt=3, max_retries=3)
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.failed

    def test_live_heartbeat_past_deadline_returns_stale(self, make_run_state, make_ts):
        """A live heartbeat should not block takeover once deadline_at has passed."""
        past_deadline = make_ts(datetime.now(UTC) - timedelta(seconds=1))
        state = make_run_state(
            status=RunStatus.running,
            heartbeat_at=Timestamp.now(),
            deadline_at=past_deadline,
            attempt=1,
            max_retries=3,
        )
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.stale

    def test_live_heartbeat_past_deadline_at_max_retries_returns_failed(self, make_run_state, make_ts):
        past_deadline = make_ts(datetime.now(UTC) - timedelta(seconds=1))
        state = make_run_state(
            status=RunStatus.running,
            heartbeat_at=Timestamp.now(),
            deadline_at=past_deadline,
            attempt=3,
            max_retries=3,
        )
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.failed

    def test_live_heartbeat_before_deadline_returns_busy(self, make_run_state, make_ts):
        future_deadline = make_ts(datetime.now(UTC) + timedelta(seconds=300))
        state = make_run_state(
            status=RunStatus.running,
            heartbeat_at=Timestamp.now(),
            deadline_at=future_deadline,
        )
        assert existing_state_case(state, self._STALE_THRESHOLD) == JobState.busy


@pytest.mark.unit
class TestFlowJob:
    """FlowJob defaults are sensible and job_id / run_id are auto-generated."""

    def test_defaults(self):
        job = FlowJob(flow_name="my_flow")
        assert job.flow_name == "my_flow"
        assert job.kwargs == {}
        assert job.retry_count == 0
        assert job.max_retries == 3
        assert job.visibility_timeout == 300
        assert job.job_id is not None
        assert job.run_id is not None

    def test_job_id_differs_from_run_id(self):
        job = FlowJob(flow_name="my_flow")
        assert job.job_id != job.run_id

    def test_kwargs_passed_through(self):
        job = FlowJob(flow_name="my_flow", kwargs={"x": 1, "y": 2})
        assert job.kwargs == {"x": 1, "y": 2}
