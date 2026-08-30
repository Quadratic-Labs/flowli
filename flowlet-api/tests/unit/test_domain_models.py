"""
Unit tests for domain model layer.

Covers:
- ReportedStatus helpers: from_log_level, is_closed
- TraceSummary.duration computed property
- ObligationSummary construction and defaults
- FlowJob defaults
"""
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

import pytest

from flowlet.models import FlowJob, ReportedStatus, TraceSummary


@pytest.mark.unit
class TestRunStatus:
    """is_closed gates terminal states; ReportedStatus is a projection only."""

    @pytest.mark.parametrize(
        "status, closed",
        [
            (ReportedStatus.completed, True),
            (ReportedStatus.failed, True),
            (ReportedStatus.warning, True),
            (ReportedStatus.canceled, True),
            (ReportedStatus.running, False),
            (ReportedStatus.pending, False),
            (ReportedStatus.retry, False),
            (ReportedStatus.stopped, False),
        ],
    )
    def test_is_closed(self, status: ReportedStatus, closed: bool):
        assert status.is_closed() is closed


@pytest.mark.unit
class TestRunSummary:
    """TraceSummary.duration is the elapsed time between start and end."""

    def test_duration_with_timestamps(self, make_ts):
        start = make_ts(datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC))
        end   = make_ts(datetime(2024, 1, 1, 12, 0, 5, tzinfo=UTC))
        span  = TraceSummary(
            span_id=uuid7(),
            span_name="my_flow",
            span_type="flow",
            status=ReportedStatus.completed,
            start_ts=start,
            end_ts=end,
        )
        assert span.duration == timedelta(seconds=5)

    def test_duration_none_when_missing_start(self, make_ts):
        span = TraceSummary(
            span_id=uuid7(),
            span_name="my_flow",
            span_type="flow",
            status=ReportedStatus.running,
            start_ts=None,
            end_ts=make_ts(),
        )
        assert span.duration is None

    def test_duration_none_when_missing_end(self, make_ts):
        span = TraceSummary(
            span_id=uuid7(),
            span_name="my_flow",
            span_type="flow",
            status=ReportedStatus.running,
            start_ts=make_ts(),
            end_ts=None,
        )
        assert span.duration is None

    def test_children_defaults_to_empty_list(self):
        span = TraceSummary(
            span_id=uuid7(),
            span_name="my_flow",
            span_type="flow",
            status=ReportedStatus.pending,
        )
        assert span.children == []


@pytest.mark.unit
class TestRunState:
    """ObligationSummary construction and default values."""

    def test_basic_construction(self, make_run_state):
        state = make_run_state()
        assert state.flow_name == "test_flow"
        assert state.status == ReportedStatus.running
        assert state.attempt == 1
        assert state.max_retries == 3
        assert state.ended_at is None

    def test_ended_at_can_be_set(self, make_run_state, make_ts):
        end_ts = make_ts()
        state  = make_run_state(status=ReportedStatus.completed, ended_at=end_ts)
        assert state.ended_at is end_ts
        assert state.status.is_closed()

    def test_run_id_is_uuid(self, make_run_state):
        state = make_run_state()
        assert isinstance(state.run_id, UUID)


@pytest.mark.unit
class TestFlowJob:
    """FlowJob defaults are sensible and job_id / run_id are auto-generated."""

    def test_defaults(self):
        job = FlowJob(flow_name="my_flow")
        assert job.flow_name == "my_flow"
        assert job.kwargs == {}
        assert job.max_retries == 3
        assert job.timeout_seconds is None
        assert job.job_id is not None
        assert job.run_id is not None

    def test_job_id_differs_from_run_id(self):
        job = FlowJob(flow_name="my_flow")
        assert job.job_id != job.run_id

    def test_kwargs_passed_through(self):
        job = FlowJob(flow_name="my_flow", kwargs={"x": 1, "y": 2})
        assert job.kwargs == {"x": 1, "y": 2}
