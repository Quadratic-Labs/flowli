"""
Unit tests for the serdes module.

Covers three anchor regions:
- @valuedispatch: ValueDispatch curried dispatch on type values and generic aliases
- @serdes.dict:   destructure / structure round-trips (lossless, no type coercion)
- @serdes.json:   to_json / from_json round-trips (with full type coercion)
"""
import json
from uuid import UUID

import pytest

from flowlet.context import RunContext
from flowlet.models import FlowJob, RunLog, RunState, RunStatus, RunSummary, RunType
from flowlet.serdes import (
    ValueDispatch,
    destructure,
    from_json,
    structure,
    to_json,
)
from flowlet.types import Timestamp, uuid7_desc


# =============================================================================
# @valuedispatch — ValueDispatch
# =============================================================================


@pytest.mark.unit
class TestValueDispatch:
    """Curried dispatch on type values and generic aliases."""

    def test_registered_type_returns_handler_result(self):
        dispatch = ValueDispatch(default=lambda x: x)
        dispatch.register(int)(lambda data: data * 2)

        assert dispatch(int)(3) == 6

    def test_unregistered_type_returns_default(self):
        sentinel = object()
        dispatch = ValueDispatch(default=lambda x: sentinel)

        assert dispatch(str)("hello") is sentinel

    def test_default_callable_is_used_as_fallback(self):
        dispatch = ValueDispatch(default=lambda x: x)

        assert dispatch(float)(3.14) == 3.14

    def test_generic_alias_resolved_via_origin_and_args(self):
        dispatch = ValueDispatch(default=lambda x: x)
        # register list origin: takes an element deserializer, returns a list handler
        dispatch.register(list)(lambda elem_deser: lambda data: [elem_deser(x) for x in data])
        dispatch.register(int)(lambda data: data + 1)

        handler = dispatch(list[int])
        assert handler([1, 2, 3]) == [2, 3, 4]

    def test_generic_alias_falls_back_to_default_when_origin_unregistered(self):
        dispatch = ValueDispatch(default=lambda x: x)
        # list[int] origin (list) not registered
        handler = dispatch(list[int])
        assert handler is dispatch._default

    def test_nested_generic_alias(self):
        dispatch = ValueDispatch(default=lambda x: x)
        dispatch.register(list)(lambda elem_deser: lambda data: [elem_deser(x) for x in data])
        dispatch.register(int)(lambda data: data * 10)

        handler = dispatch(list[list[int]])
        assert handler([[1, 2], [3]]) == [[10, 20], [30]]

    def test_multiple_types_registered_independently(self):
        dispatch = ValueDispatch(default=lambda x: None)
        dispatch.register(int)(lambda data: "int")
        dispatch.register(str)(lambda data: "str")

        assert dispatch(int)(0) == "int"
        assert dispatch(str)("") == "str"


# =============================================================================
# @serdes.dict — destructure / structure
# =============================================================================


@pytest.mark.unit
class TestDestructure:
    """destructure converts domain models to plain dicts preserving typed values."""

    def test_passthrough_for_primitives(self):
        assert destructure(42) == 42
        assert destructure("hello") == "hello"
        assert destructure(None) is None

    def test_list_is_mapped_recursively(self):
        assert destructure([1, 2, 3]) == [1, 2, 3]

    def test_dict_is_mapped_recursively(self):
        assert destructure({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_run_context_preserves_uuid_and_enum(self, make_run_state):
        run_id = uuid7_desc()
        span_id = uuid7_desc()
        ctx = RunContext(
            flow_name="my_flow",
            run_id=run_id,
            span_name="my_flow",
            span_type=RunType.flow,
            span_id=span_id,
            parent_span_id=None,
        )
        d = destructure(ctx)

        assert d["run_id"] is run_id          # UUID preserved, NOT str
        assert d["span_id"] is span_id
        assert d["span_type"] is RunType.flow  # StrEnum preserved, NOT str
        assert d["parent_span_id"] is None
        assert d["flow_name"] == "my_flow"
        assert d["span_name"] == "my_flow"

    def test_run_log_preserves_timestamp_and_uuid(self, make_run_log):
        log = make_run_log()
        d = destructure(log)

        assert isinstance(d["run_id"], UUID)
        assert isinstance(d["span_id"], UUID)
        assert isinstance(d["ts"], Timestamp)   # Timestamp preserved, NOT str
        assert d["level"] == log.level
        assert d["message"] == log.message
        assert d["extra"] == log.extra

    def test_run_state_preserves_timestamps(self, make_run_state, make_ts):
        ts = make_ts()
        state = make_run_state(started_at=ts, heartbeat_at=ts)
        d = destructure(state)

        assert isinstance(d["run_id"], UUID)
        assert d["started_at"] is ts           # Timestamp preserved
        assert d["heartbeat_at"] is ts
        assert d["ended_at"] is None
        assert d["status"] is RunStatus.running

    def test_run_summary_children_destructured_recursively(self, make_ts):
        child = RunSummary(
            span_id=uuid7_desc(),
            span_name="child",
            span_type=RunType.task,
            status=RunStatus.completed,
        )
        parent = RunSummary(
            span_id=uuid7_desc(),
            span_name="parent",
            span_type=RunType.flow,
            status=RunStatus.completed,
            children=[child],
        )
        d = destructure(parent)

        assert len(d["children"]) == 1
        assert isinstance(d["children"][0], dict)
        assert d["children"][0]["span_name"] == "child"

    def test_run_state_with_optional_fields(self, make_run_state, make_ts):
        end_ts = make_ts()
        state = make_run_state(status=RunStatus.completed, ended_at=end_ts)
        d = destructure(state)

        assert d["ended_at"] is end_ts

    def test_flow_job_preserves_uuid_and_timestamp(self, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})
        d = destructure(job)

        assert isinstance(d["job_id"], UUID)
        assert isinstance(d["run_id"], UUID)
        assert isinstance(d["submitted_at"], Timestamp)  # preserved, NOT str
        assert d["flow_name"] == job.flow_name
        assert d["kwargs"] == {"x": 1}
        assert d["retry_count"] == 0
        assert d["timeout_seconds"] is None


@pytest.mark.unit
class TestStructure:
    """structure reconstructs domain models from dicts (typed values assumed)."""

    def test_run_context_round_trip(self):
        run_id = uuid7_desc()
        span_id = uuid7_desc()
        original = RunContext(
            flow_name="my_flow",
            run_id=run_id,
            span_name="my_flow",
            span_type=RunType.flow,
            span_id=span_id,
            parent_span_id=None,
        )
        d = destructure(original)
        restored = structure(RunContext)(d)

        assert restored.run_id == original.run_id
        assert restored.span_id == original.span_id
        assert restored.span_type == original.span_type
        assert restored.parent_span_id is None

    def test_run_log_round_trip(self, make_run_log):
        original = make_run_log()
        d = destructure(original)
        restored = structure(RunLog)(d)

        assert restored.run_id == original.run_id
        assert restored.span_id == original.span_id
        assert restored.ts == original.ts
        assert restored.message == original.message
        assert restored.level == original.level

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

    def test_list_of_run_logs_round_trip(self, make_run_log):
        logs = [make_run_log(), make_run_log()]
        dicts = [destructure(log) for log in logs]
        restored = structure(list[RunLog])(dicts)

        assert len(restored) == 2
        assert all(isinstance(r, RunLog) for r in restored)
        assert restored[0].run_id == logs[0].run_id
        assert restored[1].run_id == logs[1].run_id

    def test_unregistered_type_passthrough(self):
        assert structure(int)(42) == 42
        assert structure(str)("hello") == "hello"


# =============================================================================
# @serdes.json — to_json / from_json
# =============================================================================


@pytest.mark.unit
class TestToJson:
    """to_json serialises domain models to JSON strings."""

    def test_run_log_produces_valid_json(self, make_run_log):
        log = make_run_log()
        raw = to_json(log)

        parsed = json.loads(raw)
        assert parsed["flow_name"] == log.flow_name
        assert parsed["message"] == log.message
        assert parsed["level"] == log.level

    def test_run_log_uuid_serialised_as_str(self, make_run_log):
        log = make_run_log()
        parsed = json.loads(to_json(log))

        UUID(parsed["run_id"])   # must not raise
        UUID(parsed["span_id"])

    def test_run_log_timestamp_serialised_as_iso_str(self, make_run_log):
        log = make_run_log()
        parsed = json.loads(to_json(log))

        ts = Timestamp.from_iso(parsed["ts"])  # must not raise
        assert ts == log.ts

    def test_run_log_enum_serialised_as_str(self, make_run_log):
        log = make_run_log(span_type=RunType.task)
        parsed = json.loads(to_json(log))

        assert parsed["span_type"] == "task"

    def test_run_state_produces_valid_json(self, make_run_state):
        state = make_run_state()
        raw = to_json(state)

        parsed = json.loads(raw)
        assert parsed["flow_name"] == state.flow_name
        assert parsed["status"] == state.status.value

    def test_run_state_optional_fields_null_when_none(self, make_run_state):
        state = make_run_state()
        parsed = json.loads(to_json(state))

        assert parsed["ended_at"] is None
        assert parsed["deadline_at"] is None

    def test_run_state_optional_fields_serialised_when_set(self, make_run_state, make_ts):
        end_ts = make_ts()
        state = make_run_state(status=RunStatus.completed, ended_at=end_ts)
        parsed = json.loads(to_json(state))

        assert parsed["ended_at"] == end_ts.to_iso()

    def test_flow_job_produces_valid_json(self, make_flow_job):
        job = make_flow_job(kwargs={"file": "data.csv"})
        raw = to_json(job)

        parsed = json.loads(raw)
        assert parsed["flow_name"] == job.flow_name
        assert parsed["kwargs"] == {"file": "data.csv"}
        assert parsed["retry_count"] == job.retry_count
        assert parsed["max_retries"] == job.max_retries
        assert parsed["visibility_timeout"] == job.visibility_timeout

    def test_flow_job_uuids_serialised_as_str(self, make_flow_job):
        job = make_flow_job()
        parsed = json.loads(to_json(job))

        UUID(parsed["job_id"])   # must not raise
        UUID(parsed["run_id"])

    def test_flow_job_timestamp_serialised_as_iso_str(self, make_flow_job):
        job = make_flow_job()
        parsed = json.loads(to_json(job))

        ts = Timestamp.from_iso(parsed["submitted_at"])  # must not raise
        assert ts == job.submitted_at

    def test_flow_job_optional_timeout_seconds_null_when_none(self, make_flow_job):
        job = make_flow_job()
        parsed = json.loads(to_json(job))

        assert parsed["timeout_seconds"] is None

    def test_json_default_raises_for_unserializable_type(self):
        from flowlet.serdes import _json_default

        with pytest.raises(TypeError, match="not JSON serialisable"):
            _json_default(object())


@pytest.mark.unit
class TestFromJson:
    """from_json deserialises JSON strings back to typed domain models."""

    def test_run_context_round_trip(self):
        run_id = uuid7_desc()
        span_id = uuid7_desc()
        original = RunContext(
            flow_name="my_flow",
            run_id=run_id,
            span_name="my_flow",
            span_type=RunType.flow,
            span_id=span_id,
            parent_span_id=None,
        )
        restored = from_json(RunContext)(to_json(original))

        assert restored.run_id == original.run_id
        assert isinstance(restored.run_id, UUID)
        assert restored.span_type == RunType.flow
        assert restored.parent_span_id is None

    def test_run_context_with_parent_span_id(self):
        run_id = uuid7_desc()
        parent_id = uuid7_desc()
        span_id = uuid7_desc()
        original = RunContext(
            flow_name="my_flow",
            run_id=run_id,
            span_name="child_task",
            span_type=RunType.task,
            span_id=span_id,
            parent_span_id=parent_id,
        )
        restored = from_json(RunContext)(to_json(original))

        assert isinstance(restored.parent_span_id, UUID)
        assert restored.parent_span_id == parent_id

    def test_run_log_round_trip(self, make_run_log):
        original = make_run_log()
        restored = from_json(RunLog)(to_json(original))

        assert isinstance(restored, RunLog)
        assert restored.run_id == original.run_id
        assert isinstance(restored.run_id, UUID)
        assert restored.ts == original.ts
        assert isinstance(restored.ts, Timestamp)
        assert restored.span_type == original.span_type
        assert restored.message == original.message
        assert restored.level == original.level
        assert restored.extra == original.extra

    def test_run_log_with_parent_span_id_none(self, make_run_log):
        original = make_run_log(parent_span_id=None)
        restored = from_json(RunLog)(to_json(original))

        assert restored.parent_span_id is None

    def test_run_log_with_parent_span_id_set(self, make_run_log):
        parent_id = uuid7_desc()
        original = make_run_log(parent_span_id=parent_id)
        restored = from_json(RunLog)(to_json(original))

        assert isinstance(restored.parent_span_id, UUID)
        assert restored.parent_span_id == parent_id

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
        original = make_flow_job(kwargs={"x": 1}, retry_count=1, max_retries=5)
        restored = from_json(FlowJob)(to_json(original))

        assert isinstance(restored, FlowJob)
        assert restored.job_id == original.job_id
        assert isinstance(restored.job_id, UUID)
        assert restored.run_id == original.run_id
        assert restored.flow_name == original.flow_name
        assert restored.kwargs == {"x": 1}
        assert restored.submitted_at == original.submitted_at
        assert isinstance(restored.submitted_at, Timestamp)
        assert restored.retry_count == 1
        assert restored.max_retries == 5
        assert restored.visibility_timeout == original.visibility_timeout
        assert restored.timeout_seconds is None

    def test_flow_job_with_timeout_seconds(self, make_flow_job):
        original = make_flow_job(timeout_seconds=120)
        restored = from_json(FlowJob)(to_json(original))

        assert restored.timeout_seconds == 120

    def test_unregistered_type_returns_raw_parsed_json(self):
        raw = json.dumps({"key": "value"})
        result = from_json(dict)(raw)

        # default handler: json.loads
        assert result == {"key": "value"}
