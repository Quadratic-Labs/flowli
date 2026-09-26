"""Request bodies and response shapes. See specs/09-http-api.md section 8.

A response body mirrors the projection row or the domain object it comes
from. Nothing here invents a field the account does not hold.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from flowli.adapters.cairndb_projection import AnnouncementRow, ExecutionRow, ReviewRow
from flowli.domain import Entry, Execution, ExecutionStatus, QueueDepth, Sequenced, Task


class Strict(BaseModel):
    """A body that refuses what it does not know.

    `extra="forbid"` is what makes an actor in a body a 400 instead of a
    silently ignored field (`09-http-api.md`, section 5.2).
    """

    model_config = ConfigDict(extra="forbid")


class StartRequest(Strict):
    workflow: str
    version: str
    args: dict[str, Any] = Field(default_factory=dict)
    queue: str | None = None
    dispatch_key: str | None = None


class SignalRequest(Strict):
    channel: str
    payload: Any = None
    correlation: str | None = None


class MigrateRequest(Strict):
    version: str


class DecideRequest(Strict):
    verdict: str
    data: Any = None


def execution_row(row: ExecutionRow) -> dict[str, Any]:
    return {
        "eid": str(row.eid),
        "workflow": row.workflow,
        "version": row.version,
        "status": row.status.value,
        "queue": row.queue,
        "parent_eid": row.parent_eid,
        "parent_fid": row.parent_fid,
        "dispatch_key": row.dispatch_key,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "last_type": row.last_type,
        "epoch": row.epoch,
        "suspended_on": row.suspended_on,
        "result": row.result,
        "error": None
        if row.error_type is None
        else {"type": row.error_type, "message": row.error_message},
        "created_by": {"kind": row.created_by_kind, "id": row.created_by_id},
        "worker_id": row.worker_id,
        "host": row.host,
        "archived_at": row.archived_at,
    }


def execution_record(execution: Execution, status: ExecutionStatus) -> dict[str, Any]:
    """The answer before the projection has seen the execution (section 8.2)."""
    return {
        "eid": str(execution.eid),
        "workflow": execution.workflow,
        "version": execution.version,
        "status": status.value,
        "queue": execution.queue,
        "parent_eid": None if execution.parent is None else str(execution.parent.eid),
        "parent_fid": None if execution.parent is None else execution.parent.fid,
        "dispatch_key": execution.dispatch_key,
        "created_at": execution.created_by.at.to_iso(),
        "updated_at": execution.created_by.at.to_iso(),
        "last_type": None,
        "epoch": None,
        "suspended_on": None,
        "result": None,
        "error": None,
        "created_by": {
            "kind": execution.created_by.actor.kind,
            "id": execution.created_by.actor.id,
        },
        "worker_id": None,
        "host": None,
        "archived_at": None,
    }


def journal_entry(s: Sequenced[Entry]) -> dict[str, Any]:
    e = s.item
    return {
        "seq": s.seq,
        "type": str(e.type),
        "fid": e.fid,
        "payload": e.payload,
        "at": e.provenance.at.to_iso(),
        "attempt": e.provenance.attempt,
        "actor": {
            "kind": e.provenance.actor.kind,
            "id": e.provenance.actor.id,
            "on_behalf_of": e.provenance.actor.on_behalf_of,
        },
        "site": {
            "host": e.provenance.site.host,
            "worker_id": e.provenance.site.worker_id,
            "epoch": e.provenance.site.epoch,
        },
        "code": {
            "workflow": e.provenance.code.workflow,
            "version": e.provenance.code.version,
            "frame_kind": e.provenance.code.frame_kind,
            "frame_name": e.provenance.code.frame_name,
            "code_ref": e.provenance.code.code_ref,
        },
    }


def review_row(row: ReviewRow) -> dict[str, Any]:
    eid = None if row.eid is None else str(row.eid)  # pragma: no mutate
    return {
        "rid": row.rid,
        "eid": eid,
        "queue": row.queue,
        "status": row.status,
        "payload": row.payload,
        "deadline": row.deadline,
        "requested_at": row.requested_at,
        "decided_at": row.decided_at,
        "verdict": row.verdict,
        "decided_by": row.decided_by,
    }


def announcement_row(row: AnnouncementRow) -> dict[str, Any]:
    return {
        "seq": row.seq,
        "kind": row.kind,
        "eid": row.eid,
        "fid": row.fid,
        "at": row.at,
        "actor": {"kind": row.actor_kind, "id": row.actor_id},
        "payload": row.payload,
    }


def task_row(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "queue": task.queue,
        "kind": task.kind.value,
        "eid": str(task.target.eid),
        "fid": task.target.fid,
        "reason": task.reason,
        "key": task.key,
        "not_before": None if task.not_before is None else task.not_before.to_iso(),
        "enqueued_at": task.enqueued_by.at.to_iso(),
        "enqueued_by": {"kind": task.enqueued_by.actor.kind, "id": task.enqueued_by.actor.id},
    }


def queue_depth(queue: str, depth: QueueDepth) -> dict[str, Any]:
    return {
        "queue": queue,
        "total": depth.total,
        "visible": depth.visible,
        # An upper bound: a dead holder's lease document stays until a steal
        # or an ack (specs/03-ports.md, section 7).
        "claimed": depth.claimed,
    }


# --- worker plane (section 9) ---------------------------------------------------------


class DequeueRequest(Strict):
    ttl_seconds: float | None = None
    wait_seconds: float = 0.0


class HolderRequest(Strict):
    holder: str
    ttl_seconds: float | None = None


class AckRequest(Strict):
    holder: str


class NackRequest(Strict):
    holder: str
    delay_seconds: float = 5.0


class StateRequest(Strict):
    holder: str
    state: dict[str, Any]


class DeliverRequest(Strict):
    queue: str
    task_id: str
    holder: str
    channel: str
    payload: Any = None


def delegate_task(task: Task) -> dict[str, Any]:
    """A DELEGATE task as its consumer sees it (specs/06-patterns.md)."""
    payload = task.payload or {}
    target = payload.get("target") or {}
    return {
        "task_id": task.task_id,
        "queue": task.queue,
        "kind": task.kind.value,
        "eid": target.get("eid", str(task.target.eid)),
        "fid": target.get("fid", task.target.fid),
        "reason": task.reason,
        "reply_channel": payload.get("reply_channel"),
        "payload": payload.get("payload"),
        "enqueued_at": task.enqueued_by.at.to_iso(),
    }
