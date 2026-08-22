import functools
import json
from collections.abc import Callable
from typing import Any, get_args, get_origin
from uuid import UUID

from flowlet.models import (
    Attempt,
    AttemptOutcome,
    Effect,
    FlowJob,
    Obligation,
    ObligationRecord,
    ObligationStatus,
    RunState,
    RunStatus,
    RunSummary,
    RunType,
    SpanEvent,
    SpanRecord,
    Verdict,
    VerdictDecision,
)
from flowlet.types import JsonAtom, Timestamp

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
        "attempt": data.attempt,
        "max_retries": data.max_retries,
        "kwargs": data.kwargs,
        "cancel_requested": data.cancel_requested,
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
        "parent_id": data.parent_id,
        "root_id": data.root_id,
        "caused_by": data.caused_by,
    }


@destructure.register(Verdict)
def _(data: Verdict) -> dict:
    return {
        "decision": data.decision,
        "by": data.by,
        "rendered_at": data.rendered_at,
        "reason": data.reason,
    }


@destructure.register(Attempt)
def _(data: Attempt) -> dict:
    return {
        "n": data.n,
        "executor": data.executor,
        "started_at": data.started_at,
        "ended_at": data.ended_at,
        "outcome": data.outcome,
        "error": data.error,
        "verdict": destructure(data.verdict) if data.verdict is not None else None,
    }


@destructure.register(Obligation)
def _(data: Obligation) -> dict:
    return {
        "id": data.id,
        "flow_name": data.flow_name,
        "kwargs": data.kwargs,
        "parent_id": data.parent_id,
        "root_id": data.root_id,
        "max_retries": data.max_retries,
        "adjudication": data.adjudication,
        "caused_by": data.caused_by,
        "created_at": data.created_at,
        "closed_at": data.closed_at,
        "status": data.status,
        "cause": data.cause,
        "cancel_requested": data.cancel_requested,
    }


@destructure.register(Effect)
def _(data: Effect) -> dict:
    return {
        "name": data.name,
        "occurrence": data.occurrence,
        "attempt_n": data.attempt_n,
        "produced_at": data.produced_at,
        "result_ref": data.result_ref,
    }


@destructure.register(ObligationRecord)
def _(data: ObligationRecord) -> dict:
    return {
        "obligation": destructure(data.obligation),
        "attempts": [destructure(a) for a in data.attempts],
        "effects": [destructure(e) for e in data.effects],
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
        attempt=data.get("attempt", 1),
        max_retries=data.get("max_retries", 3),
        kwargs=data.get("kwargs") or {},
        cancel_requested=data.get("cancel_requested", False),
    )


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
        attempt=raw.get("attempt", 1),
        max_retries=raw.get("max_retries", 3),
        kwargs=raw.get("kwargs") or {},
        cancel_requested=raw.get("cancel_requested", False),
    )


def to_payload(data: Any) -> Any:
    """Convert a domain model to a JSON-safe tree (the wire format as data).

    Used where a model travels inside another JSON document — e.g. RunState
    as the state payload of a cairndb lease document — so the embedded form
    is byte-equivalent to the ``to_json`` wire format.
    """
    return json.loads(to_json(data))


def from_payload(model: type) -> Callable[[Any], Any]:
    """Curried inverse of :func:`to_payload`: ``from_payload(RunState)(tree)``.

    Round-trips through the ``from_json`` handlers so they remain the single
    source of truth for the wire format.
    """
    deser = from_json(model)
    return lambda payload: deser(json.dumps(payload))


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
        parent_id=UUID(raw["parent_id"]) if raw.get("parent_id") else None,
        root_id=UUID(raw["root_id"]) if raw.get("root_id") else None,
        caused_by=raw.get("caused_by"),
    )


def _verdict_from_raw(raw: dict | None) -> Verdict | None:
    if raw is None:
        return None
    return Verdict(
        decision=VerdictDecision(raw["decision"]),
        by=raw.get("by", "auto"),
        rendered_at=Timestamp.from_iso(raw["rendered_at"]),
        reason=raw.get("reason"),
    )


def _attempt_from_raw(raw: dict) -> Attempt:
    return Attempt(
        n=raw["n"],
        executor=raw["executor"],
        started_at=Timestamp.from_iso(raw["started_at"]),
        ended_at=Timestamp.from_iso(raw["ended_at"]) if raw.get("ended_at") else None,
        outcome=AttemptOutcome(raw["outcome"]) if raw.get("outcome") else None,
        error=raw.get("error"),
        verdict=_verdict_from_raw(raw.get("verdict")),
    )


def _obligation_from_raw(raw: dict) -> Obligation:
    return Obligation(
        id=UUID(raw["id"]),
        flow_name=raw["flow_name"],
        kwargs=raw.get("kwargs") or {},
        parent_id=UUID(raw["parent_id"]) if raw.get("parent_id") else None,
        root_id=UUID(raw["root_id"]) if raw.get("root_id") else None,
        max_retries=raw.get("max_retries", 3),
        adjudication=raw.get("adjudication", "auto"),
        caused_by=raw.get("caused_by"),
        created_at=Timestamp.from_iso(raw["created_at"]),
        closed_at=Timestamp.from_iso(raw["closed_at"]) if raw.get("closed_at") else None,
        status=ObligationStatus(raw.get("status", "open")),
        cause=raw.get("cause"),
        cancel_requested=raw.get("cancel_requested", False),
    )


def _effect_from_raw(raw: dict) -> Effect:
    return Effect(
        name=raw["name"],
        occurrence=raw.get("occurrence", "1"),
        attempt_n=raw["attempt_n"],
        produced_at=Timestamp.from_iso(raw["produced_at"]),
        result_ref=raw.get("result_ref"),
    )


@from_json.register(ObligationRecord)
def _(data: str) -> ObligationRecord:
    raw = json.loads(data)
    return ObligationRecord(
        obligation=_obligation_from_raw(raw["obligation"]),
        attempts=[_attempt_from_raw(a) for a in raw.get("attempts") or []],
        effects=[_effect_from_raw(e) for e in raw.get("effects") or []],
    )
