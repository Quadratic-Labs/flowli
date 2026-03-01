"""
Unit tests for analysis.summarise.

Covers:
- Single-span run: correct status and timestamps
- Parent-child hierarchy: children wired to parent
- Status derived from the last log's level
- ValueError raised when no root span can be found
"""
from datetime import UTC, datetime

import pytest

from flowlet.analysis import summarise
from flowlet.models import RunStatus, RunType
from flowlet.types import uuid7_desc


def _ts(make_ts, second: int):
    return make_ts(datetime(2024, 1, 1, 0, 0, second, tzinfo=UTC))


@pytest.mark.unit
class TestSummarise:
    """analysis.summarise derives RunSummary from a flat RunLog list."""

    def test_single_span_status_and_timestamps(self, make_run_log, make_ts):
        run_id  = uuid7_desc()
        span_id = uuid7_desc()
        logs = [
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 0), level="INFO"),
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 5), level="SUCCESS"),
        ]

        summary = summarise(logs)

        assert summary.span_id == span_id
        assert summary.status == RunStatus.completed
        assert summary.start_ts == _ts(make_ts, 0)
        assert summary.end_ts == _ts(make_ts, 5)
        assert summary.children == []

    def test_single_span_failed_status(self, make_run_log, make_ts):
        run_id  = uuid7_desc()
        span_id = uuid7_desc()
        logs = [
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 0), level="INFO"),
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 2), level="ERROR"),
        ]

        summary = summarise(logs)

        assert summary.status == RunStatus.failed

    def test_parent_child_hierarchy(self, make_run_log, make_ts):
        run_id       = uuid7_desc()
        flow_span_id = uuid7_desc()
        task_span_id = uuid7_desc()

        flow_logs = [
            make_run_log(run_id=run_id, span_id=flow_span_id, span_type=RunType.flow,
                         ts=_ts(make_ts, 0), level="INFO"),
            make_run_log(run_id=run_id, span_id=flow_span_id, span_type=RunType.flow,
                         ts=_ts(make_ts, 10), level="SUCCESS"),
        ]
        task_logs = [
            make_run_log(run_id=run_id, span_id=task_span_id, parent_span_id=flow_span_id,
                         span_type=RunType.task, span_name="my_task",
                         ts=_ts(make_ts, 2), level="INFO"),
            make_run_log(run_id=run_id, span_id=task_span_id, parent_span_id=flow_span_id,
                         span_type=RunType.task, span_name="my_task",
                         ts=_ts(make_ts, 8), level="SUCCESS"),
        ]

        summary = summarise(flow_logs + task_logs)

        assert summary.span_id == flow_span_id
        assert len(summary.children) == 1
        child = summary.children[0]
        assert child.span_id == task_span_id
        assert child.status == RunStatus.completed

    def test_no_root_span_raises(self):
        """An empty log list produces no spans at all → ValueError."""
        with pytest.raises(ValueError, match="No root span"):
            summarise([])

    def test_status_uses_last_log_level(self, make_run_log, make_ts):
        """Status is determined by the last log's level (sorted by ts)."""
        run_id  = uuid7_desc()
        span_id = uuid7_desc()
        # Provide logs out of order; the latest (ts=2) level should win
        logs = [
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 2), level="WARNING"),
            make_run_log(run_id=run_id, span_id=span_id, ts=_ts(make_ts, 0), level="INFO"),
        ]

        summary = summarise(logs)

        assert summary.status == RunStatus.warning
