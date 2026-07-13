import functools
import json
from typing import Any, Callable, get_args, get_origin
from uuid import UUID

from .models import FlowJob, RunStatus, RunState, RunType, RunSummary, SpanEvent, SpanRecord
from .types import JsonAtom, Timestamp


# region @valuedispatch
# ---
# role: util
# intent: curried function dispatch on a type argument, with generic alias support
# description: >
#   Similar to singledispatch but dispatches on a type value (not the runtime type
#   of the argument). The __call__ returns a Callable (deserializer), making it
#   curried: dispatch(Type)(data).
#   For generic aliases (e.g. list[RunLog]), get_origin/get_args are used to
#   resolve type parameters into deserializers before passing them to the factory
#   handler, so composition is automatic: dispatch(list[list[RunLog]]) works.
# rules:
#   - Non-generic handlers have signature fn(data) -> T.
#   - Generic-origin handlers have signature fn(*resolved_deserializers) -> Callable.
#   - The default is returned as-is for unregistered non-generic types.
# dependencies:
# aliases:
# triggers:
# ---

class ValueDispatch:
    def __init__(self, default: Callable):
        self._default = default
        self._registry: dict[object, Callable] = {}

    def register(self, target_type: type):
        def decorator(fn: Callable):
            self._registry[target_type] = fn
            return fn
        return decorator

    def __call__(self, target_type: type) -> Callable:
        origin = get_origin(target_type)
        if origin is not None:
            fn = self._registry.get(origin)
            if fn is None:
                return self._default
            resolved = [self(t) for t in get_args(target_type)]
            return fn(*resolved)
        return self._registry.get(target_type, self._default)


valuedispatch = ValueDispatch

# ---
# endregion


# region @serdes.dict
# ---
# role: util
# intent: structural serdes between domain models and typed dicts
# description: >
#   to_dict converts domain models to plain dicts, preserving typed values
#   (UUID, Timestamp, StrEnum) as-is. list and dict containers are handled
#   recursively. This is the lossless intermediate layer: to_dict / from_dict
#   round-trip without any type coercion.
#   from_dict is curried via ValueDispatch: from_dict(RunLog)(data) or
#   from_dict(list[RunLog])(data). Generic type args are pre-resolved to
#   deserializers before being passed to factory handlers.
# rules:
#   - to_dict MUST NOT convert UUID, Timestamp, or StrEnum to str.
#   - from_dict handlers MUST assume typed values (UUID, Timestamp already resolved).
#   - Generic handlers (list, dict) MUST accept resolved deserializers, not raw types.
# dependencies:
# aliases:
# triggers:
# ---

@functools.singledispatch
def destructure(data: Any) -> Any:
    return data


@destructure.register(list)
def _(data: list) -> list:
    return [destructure(item) for item in data]


@destructure.register(dict)
def _(data: dict) -> dict:
    return {key: destructure(value) for key, value in data.items()}


@destructure.register(SpanEvent)
def _(data: SpanEvent) -> dict:
    return {
        "ts": data.ts,
        "message": data.message,
        "attributes": dict(data.attributes),
    }


@destructure.register(SpanRecord)
def _(data: SpanRecord) -> dict:
    return {
        "run_id": data.run_id,
        "span_id": data.span_id,
        "parent_span_id": data.parent_span_id,
        "name": data.name,
        "flow_name": data.flow_name,
        "attempt": data.attempt,
        "span_type": data.span_type,
        "status": data.status,
        "status_message": data.status_message,
        "start_ts": data.start_ts,
        "end_ts": data.end_ts,
        "events": destructure(data.events),
        "attributes": data.attributes,
    }


@destructure.register(RunSummary)
def _(data: RunSummary) -> dict:
    return {
        "span_id": data.span_id,
        "span_name": data.span_name,
        "span_type": data.span_type,
        "status": data.status,
        "start_ts": data.start_ts,
        "end_ts": data.end_ts,
        "children": destructure(data.children),
    }


@destructure.register(RunState)
def _(data: RunState) -> dict:
    return {
        "run_id": data.run_id,
        "flow_name": data.flow_name,
        "status": data.status,
        "worker_id": data.worker_id,
        "started_at": data.started_at,
        "ended_at": data.ended_at,
        "deadline_at": data.deadline_at,
        "attempt": data.attempt,
        "max_retries": data.max_retries,
        "kwargs": data.kwargs,
    }


@destructure.register(FlowJob)
def _(data: FlowJob) -> dict:
    return {
        "job_id": data.job_id,
        "run_id": data.run_id,
        "flow_name": data.flow_name,
        "kwargs": data.kwargs,
        "submitted_at": data.submitted_at,
        "max_retries": data.max_retries,
        "timeout_seconds": data.timeout_seconds,
    }


@valuedispatch
def structure(data):
    return data


@structure.register(list)
def _(elem_deser: Callable) -> Callable[[list], list]:
    return lambda data: [elem_deser(item) for item in data]


@structure.register(SpanEvent)
def _(data: dict) -> SpanEvent:
    return SpanEvent(
        ts=data["ts"],
        message=data["message"],
        attributes=data.get("attributes") or {},
    )


@structure.register(SpanRecord)
def _(data: dict) -> SpanRecord:
    return SpanRecord(
        run_id=data["run_id"],
        span_id=data["span_id"],
        parent_span_id=data.get("parent_span_id"),
        name=data["name"],
        flow_name=data["flow_name"],
        attempt=data.get("attempt", 1),
        span_type=data["span_type"],
        status=data["status"],
        status_message=data.get("status_message"),
        start_ts=data["start_ts"],
        end_ts=data.get("end_ts"),
        events=data.get("events") or [],
        attributes=data.get("attributes") or {},
    )


@structure.register(RunState)
def _(data: dict) -> RunState:
    return RunState(
        run_id=data["run_id"],
        flow_name=data["flow_name"],
        status=data["status"],
        worker_id=data["worker_id"],
        started_at=data["started_at"],
        ended_at=data.get("ended_at"),
        deadline_at=data.get("deadline_at"),
        attempt=data.get("attempt", 1),
        max_retries=data.get("max_retries", 3),
        kwargs=data.get("kwargs") or {},
    )

# ---
# endregion


# region @serdes.json
# ---
# role: util
# intent: serdes between domain models and JSON strings, with type coercion
# description: >
#   to_json converts a domain model to a JSON string via to_dict + _json_default.
#   _json_default is a singledispatch encoder for types that json.dumps cannot
#   handle natively (UUID -> str, Timestamp -> ISO str).
#   from_json is curried: from_json(RunLog)(raw_str) -> RunLog.
#   from_json handlers own the full coercion pipeline (json.loads + str -> UUID,
#   str -> Timestamp), making them the single source of truth for the wire format.
# rules:
#   - _json_default MUST use singledispatch (not isinstance chains).
#   - from_json handlers MUST call json.loads internally.
#   - StrEnum values are handled natively by json.dumps (no _json_default needed).
# dependencies:
#   - serdes.dict
# aliases:
# triggers:
# ---

@functools.singledispatch
def _json_default(obj) -> JsonAtom:
    raise TypeError(f"Object of type {type(obj).__name__!r} is not JSON serialisable")


@_json_default.register(UUID)
def _(obj: UUID) -> str:
    return str(obj)


@_json_default.register(Timestamp)
def _(obj: Timestamp) -> str:
    return obj.to_iso()


@functools.singledispatch
def to_json(data: Any) -> str:
    return json.dumps(destructure(data), default=_json_default)


@valuedispatch
def from_json(data: str) -> Any:
    return json.loads(data)


@from_json.register(SpanRecord)
def _(data: str) -> SpanRecord:
    raw = json.loads(data)
    return SpanRecord(
        run_id=UUID(raw["run_id"]),
        span_id=raw["span_id"],
        parent_span_id=raw.get("parent_span_id"),
        name=raw["name"],
        flow_name=raw["flow_name"],
        attempt=raw.get("attempt", 1),
        span_type=RunType(raw["span_type"]),
        status=RunStatus(raw["status"]),
        status_message=raw.get("status_message"),
        start_ts=Timestamp.from_iso(raw["start_ts"]),
        end_ts=Timestamp.from_iso(raw["end_ts"]) if raw.get("end_ts") else None,
        events=[
            SpanEvent(
                ts=Timestamp.from_iso(e["ts"]),
                message=e["message"],
                attributes=e.get("attributes") or {},
            )
            for e in raw.get("events") or []
        ],
        attributes=raw.get("attributes") or {},
    )


@from_json.register(RunState)
def _(data: str) -> RunState:
    raw = json.loads(data)
    return RunState(
        run_id=UUID(raw["run_id"]),
        flow_name=raw["flow_name"],
        status=RunStatus(raw["status"]),
        worker_id=raw["worker_id"],
        started_at=Timestamp.from_iso(raw["started_at"]),
        ended_at=Timestamp.from_iso(raw["ended_at"]) if raw.get("ended_at") else None,
        deadline_at=Timestamp.from_iso(raw["deadline_at"]) if raw.get("deadline_at") else None,
        attempt=raw.get("attempt", 1),
        max_retries=raw.get("max_retries", 3),
        kwargs=raw.get("kwargs") or {},
    )


@from_json.register(FlowJob)
def _(data: str) -> FlowJob:
    raw = json.loads(data)
    return FlowJob(
        job_id=UUID(raw["job_id"]),
        run_id=UUID(raw["run_id"]),
        flow_name=raw["flow_name"],
        kwargs=raw["kwargs"],
        submitted_at=Timestamp.from_iso(raw["submitted_at"]),
        max_retries=raw.get("max_retries", 3),
        timeout_seconds=raw.get("timeout_seconds"),
    )

# ---
# endregion
