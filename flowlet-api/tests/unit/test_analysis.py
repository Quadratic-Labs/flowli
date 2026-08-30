"""
Unit tests for analysis.summarise.

Covers:
- Single-span run: status and timestamps taken straight from the span
- Parent-child hierarchy wired from parent_span_id
- Latest attempt selected when spans from several attempts are present
- ValueError raised when no spans / no root span
"""
from datetime import UTC, datetime
from uuid import uuid7

import pytest

from flowlet.analysis import summarise
from flowlet.models import ReportedStatus


def _ts(make_ts, second: int):
    return make_ts(datetime(2024, 1, 1, 0, 0, second, tzinfo=UTC))


@pytest.mark.unit
class TestSummarise:
    """analysis.summarise derives the TraceSummary tree from SpanRecords."""

    def test_single_span_status_and_timestamps(self, make_span_record, make_ts):
        run_id = uuid7()
        span = make_span_record(
            run_id=run_id,
            status=ReportedStatus.completed,
            start_ts=_ts(make_ts, 0),
            end_ts=_ts(make_ts, 5),
        )

        summary = summarise([span])

        assert summary.span_id == span.span_id
        assert summary.status == ReportedStatus.completed
        assert summary.start_ts == _ts(make_ts, 0)
        assert summary.end_ts == _ts(make_ts, 5)
        assert summary.children == []

    def test_single_span_failed_status(self, make_span_record):
        span = make_span_record(status=ReportedStatus.failed)
        summary = summarise([span])
        assert summary.status == ReportedStatus.failed

    def test_parent_child_hierarchy(self, make_span_record, make_ts):
        run_id = uuid7()
        root = make_span_record(
            run_id=run_id,
            span_id="aaaaaaaaaaaaaaaa",
            span_type="flow",
            start_ts=_ts(make_ts, 0),
            end_ts=_ts(make_ts, 10),
        )
        child = make_span_record(
            run_id=run_id,
            span_id="bbbbbbbbbbbbbbbb",
            parent_span_id="aaaaaaaaaaaaaaaa",
            name="my_task",
            span_type="task",
            start_ts=_ts(make_ts, 2),
            end_ts=_ts(make_ts, 8),
        )

        summary = summarise([child, root])  # order must not matter

        assert summary.span_id == root.span_id
        assert len(summary.children) == 1
        assert summary.children[0].span_id == child.span_id
        assert summary.children[0].span_name == "my_task"

    def test_latest_attempt_wins(self, make_span_record):
        run_id = uuid7()
        first = make_span_record(
            run_id=run_id, span_id="a" * 16, attempt=1, status=ReportedStatus.failed
        )
        second = make_span_record(
            run_id=run_id, span_id="b" * 16, attempt=2, status=ReportedStatus.completed
        )

        summary = summarise([first, second])

        assert summary.span_id == second.span_id
        assert summary.status == ReportedStatus.completed

    def test_empty_spans_raise(self):
        with pytest.raises(ValueError, match="No spans"):
            summarise([])
