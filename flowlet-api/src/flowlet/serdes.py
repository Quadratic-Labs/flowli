from datetime import datetime, timezone
import functools
import json
from typing import Any, Callable
from uuid import UUID

from .models import RunContext, SpanLog, RunSummary
from .types import RunStatus, SpanType, JsonData


class ValueDispatch[T]:
    def __init__(self, default: Callable):
        self._default = default
        self._registry: dict[object, Callable] = {}

    def register(self, target_type: type):
        def decorator(fn: Callable):
            self._registry[target_type] = fn
            return fn
        return decorator

    def __call__(self, target_type: type, data, *args, **kwargs):
        try:
            fn = self._registry[target_type]
        except KeyError:
            fn = self._default
        return fn(data, *args, **kwargs)


valuedispatch = ValueDispatch


@functools.singledispatch
def to_json_data(data: Any) -> JsonData:
    return str(data)


@to_json_data.register(dict)
def _(data: dict) -> JsonData:
    return {str(key): to_json_data(value) for key, value in data.items()}


@to_json_data.register(RunContext)
def _(data: RunContext) -> JsonData:
    return {
        "flow_name": data.flow_name,
        "run_id": str(data.run_id),
        "span_name": data.span_name,
        "span_type": str(data.span_type),
        "span_id": str(data.span_id),
        "parent_span_id": str(data.parent_span_id) if data.parent_span_id is not None else None
    }

@to_json_data.register(SpanLog)
def _(data: SpanLog) -> JsonData:
    return {
        "flow_name": data.flow_name,
        "run_id": str(data.run_id),
        "span_name": data.span_name,
        "span_type": str(data.span_type),
        "span_id": str(data.span_id),
        "parent_span_id": str(data.parent_span_id) if data.parent_span_id is not None else None,
        "ts": data.ts.isoformat().replace('+00:00', 'Z'),
        "message": data.message,
        "level": data.level,
        "extra": to_json_data(data.extra),
    }


@functools.singledispatch
def to_json(data: Any) -> str:
    return json.dumps(to_json_data(data))


@valuedispatch
def from_json_data(_, data: JsonData) -> Any:
    return data


@from_json_data.register(RunContext)
def _(data: dict[str, Any]) -> RunContext:
    return RunContext(
        flow_name = data["flow_name"],
        run_id = UUID(data["run_id"]),
        span_name = data["span_name"],
        span_type = SpanType(data["span_type"]),
        span_id = UUID(data["span_id"]),
        parent_span_id = UUID(data["parent_span_id"]),
    )


@from_json_data.register(type(SpanLog))
def _(data: dict[str, Any]) -> SpanLog:
    return SpanLog(
        flow_name = data["flow_name"],
        run_id = UUID(data["run_id"]),
        span_type = SpanType(data["span_type"]),
        span_name = data["span_name"],
        span_id = UUID(data["span_id"]),
        parent_span_id = UUID(data["parent_span_id"]) if data["parent_span_id"] is not None else None,
        ts = datetime.fromisoformat(data["ts"]),
        message = data["message"],
        level = data["level"],
        extra = data["extra"],
    )


@functools.singledispatch
def from_json(data: str) -> Any:
    return from_json_data(object, json.loads(data))