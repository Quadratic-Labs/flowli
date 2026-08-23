"""
Unit tests for the serdes module.

Covers two anchor regions:
- @serdes.dict: destructure / structure round-trips over JSON-safe trees
- @serdes.json: to_json / from_json round-trips (the wire format)
"""
import json
from uuid import UUID

import pytest

from flowlet.models import FlowJob, RunState, RunStatus, RunType, SpanEvent, SpanRecord
from flowlet.serdes import (
    destructure,
    from_json,
    structure,
    to_json,
)
from flowlet.types import Timestamp

# =============================================================================
# @serdes.dict — destructure / structure
# =============================================================================


@pytest.mark.unit
class TestDestructure:
    """destructure converts domain models to JSON-safe trees."""

    def test_passthrough_for_primitives(self):
        assert destructure(42) == 42
        assert destructure("hello") == "hello"
        assert destructure(None) is None

    def test_list_is_mapped_recursively(self):
        assert destructure([1, 2, 3]) == [1, 2, 3]

    def test_dict_is_mapped_recursively(self):
        assert destructure({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_span_record_tree_is_json_safe(self, make_span_record):
        record = make_span_record()
        d = destructure(record)

        assert d["run_id"] == str(record.run_id)
        assert isinstance(d["span_id"], str)      # OTel hex span id
        assert d["start_ts"] == record.start_ts.to_iso()
        assert d["span_type"] == str(record.span_type)
        assert d["status"] == str(record.status)
        assert d["name"] == record.name
        json.dumps(d)  # must not raise: the tree is the wire format as data

    def test_span_record_events_destructured(self, make_span_record, make_ts):
        ts = make_ts()
        record = make_span_record(
            events=[SpanEvent(ts=ts, message="hello", attributes={"log.level": "INFO"})]
        )
        d = destructure(record)

        assert d["events"] == [
            {"ts": ts.to_iso(), "message": "hello", "attributes": {"log.level": "INFO"}}
        ]


@pytest.mark.unit
class TestStructure:
    """structure reconstructs domain models from JSON-safe trees."""

    def test_span_record_round_trip(self, make_span_record):
        original = make_span_record()
        d = destructure(original)
        restored = structure(SpanRecord)(d)

        assert restored.run_id == original.run_id
        assert restored.span_id == original.span_id
        assert restored.start_ts == original.start_ts
        assert restored.status == original.status
        assert restored.span_type == original.span_type

    def test_run_state_round_trip(self, make_run_state):
        original = make_run_state()
        d = destructure(original)
        restored = structure(RunState)(d)

        assert restored.run_id == original.run_id
        assert restored.flow_name == original.flow_name
        assert restored.status == original.status
        assert restored.started_at == original.started_at
        assert restored.ended_at is None

    def test_run_state_with_optional_fields_round_trip(self, make_run_state, make_ts):
        end_ts = make_ts()
        original = make_run_state(status=RunStatus.completed, ended_at=end_ts)
        d = destructure(original)
        restored = structure(RunState)(d)

        assert restored.ended_at == original.ended_at
        assert restored.status == RunStatus.completed

    def test_list_of_span_records_round_trip(self, make_span_record):
        records = [make_span_record(), make_span_record()]
        dicts = [destructure(r) for r in records]
        restored = structure(list[SpanRecord])(dicts)

        assert len(restored) == 2
        assert all(isinstance(r, SpanRecord) for r in restored)
        assert restored[0].run_id == records[0].run_id
        assert restored[1].run_id == records[1].run_id

    def test_unregistered_type_passthrough(self):
        assert structure(int)(42) == 42
        assert structure(str)("hello") == "hello"


# =============================================================================
# @serdes.json — to_json / from_json
# =============================================================================


@pytest.mark.unit
class TestToJson:
    """to_json serialises domain models to JSON strings."""

    def test_span_record_produces_valid_json(self, make_span_record):
        record = make_span_record()
        raw = to_json(record)

        parsed = json.loads(raw)
        assert parsed["flow_name"] == record.flow_name
        assert parsed["name"] == record.name
        assert parsed["status"] == str(record.status)

    def test_span_record_uuid_serialised_as_str(self, make_span_record):
        record = make_span_record()
        parsed = json.loads(to_json(record))

        UUID(parsed["run_id"])   # must not raise
        assert isinstance(parsed["span_id"], str)

    def test_span_record_timestamp_serialised_as_iso_str(self, make_span_record):
        record = make_span_record()
        parsed = json.loads(to_json(record))

        ts = Timestamp.from_iso(parsed["start_ts"])  # must not raise
        assert ts == record.start_ts

    def test_span_record_enum_serialised_as_str(self, make_span_record):
        record = make_span_record(span_type=RunType.task)
        parsed = json.loads(to_json(record))

        assert parsed["span_type"] == "task"


@pytest.mark.unit
class TestFromJson:
    """from_json deserialises JSON strings back to typed domain models."""

    def test_span_record_round_trip(self, make_span_record, make_ts):
        ts = make_ts()
        original = make_span_record(
            events=[SpanEvent(ts=ts, message="evt", attributes={"log.level": "INFO"})]
        )
        restored = from_json(SpanRecord)(to_json(original))

        assert isinstance(restored, SpanRecord)
        assert restored.run_id == original.run_id
        assert isinstance(restored.run_id, UUID)
        assert restored.span_id == original.span_id
        assert restored.start_ts == original.start_ts
        assert restored.span_type == original.span_type
        assert restored.status == original.status
        assert len(restored.events) == 1
        assert restored.events[0].message == "evt"
        assert restored.events[0].ts == ts

    def test_span_record_parent_none_round_trip(self, make_span_record):
        original = make_span_record(parent_span_id=None)
        restored = from_json(SpanRecord)(to_json(original))
        assert restored.parent_span_id is None

    def test_span_record_parent_set_round_trip(self, make_span_record):
        original = make_span_record(parent_span_id="00f067aa0ba902b7")
        restored = from_json(SpanRecord)(to_json(original))
        assert restored.parent_span_id == "00f067aa0ba902b7"

    def test_run_state_round_trip(self, make_run_state):
        original = make_run_state()
        restored = from_json(RunState)(to_json(original))

        assert isinstance(restored, RunState)
        assert restored.run_id == original.run_id
        assert isinstance(restored.run_id, UUID)
        assert restored.status == original.status
        assert isinstance(restored.status, RunStatus)
        assert restored.started_at == original.started_at
        assert isinstance(restored.started_at, Timestamp)
        assert restored.ended_at is None

    def test_run_state_optional_fields_restored(self, make_run_state, make_ts):
        end_ts = make_ts()
        original = make_run_state(status=RunStatus.completed, ended_at=end_ts)
        restored = from_json(RunState)(to_json(original))

        assert isinstance(restored.ended_at, Timestamp)
        assert restored.ended_at == original.ended_at
        assert restored.status == RunStatus.completed

    def test_run_state_attempt_and_max_retries_preserved(self, make_run_state):
        original = make_run_state(attempt=2, max_retries=5)
        restored = from_json(RunState)(to_json(original))

        assert restored.attempt == 2
        assert restored.max_retries == 5

    def test_flow_job_round_trip(self, make_flow_job):
        original = make_flow_job(kwargs={"x": 1}, max_retries=5)
        restored = from_json(FlowJob)(to_json(original))

        assert isinstance(restored, FlowJob)
        assert restored.job_id == original.job_id
        assert isinstance(restored.job_id, UUID)
        assert restored.run_id == original.run_id
        assert restored.flow_name == original.flow_name
        assert restored.kwargs == {"x": 1}
        assert restored.submitted_at == original.submitted_at
        assert isinstance(restored.submitted_at, Timestamp)
        assert restored.max_retries == 5
        assert restored.timeout_seconds is None

    def test_flow_job_with_timeout_seconds(self, make_flow_job):
        original = make_flow_job(timeout_seconds=120)
        restored = from_json(FlowJob)(to_json(original))

        assert restored.timeout_seconds == 120

    def test_unregistered_type_returns_raw_parsed_json(self):
        raw = json.dumps({"key": "value"})
        result = from_json(dict)(raw)

        # plain containers structure as themselves
        assert result == {"key": "value"}
