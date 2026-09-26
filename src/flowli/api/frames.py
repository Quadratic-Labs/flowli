"""The frame tree of one execution. See specs/09-http-api.md section 8.3.

A frame id is a path (`01-domain-model.md`, section 3.1), so the tree is in
the journal already. Nothing here reconstructs a hierarchy from spans.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any

from flowli.domain import ROOT_FID, Entry, EntryType, Sequenced


@dataclass
class FrameNode:
    fid: str
    kind: str
    name: str
    status: str  # running | completed | failed | suspended | cancelled
    attempts: int = 1
    started_at: str | None = None
    ended_at: str | None = None
    value: Any = None
    error: dict[str, Any] | None = None
    suspended_on: str | None = None
    deadline: str | None = None
    evidence: bool = False  # an attempt of this frame wrote a log or an attachment
    children: list[FrameNode] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fid": self.fid,
            "kind": self.kind,
            "name": self.name,
            "status": self.status,
            "attempts": self.attempts,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "value": self.value,
            "error": self.error,
            "suspended_on": self.suspended_on,
            "deadline": self.deadline,
            "evidence": self.evidence,
            "children": [c.as_dict() for c in self.children],
        }


def _parent(fid: str) -> str | None:
    head, sep, _ = fid.rpartition("/")
    return head if sep else None


def build(entries: list[Sequenced[Entry]], with_evidence: Collection[str] = ()) -> FrameNode | None:
    """Fold a journal into the tree of its frames, in journal order.

    `with_evidence` names the frames that wrote evidence, so the interface can
    offer a drill-down and read nothing until a person asks for it.
    """
    nodes: dict[str, FrameNode] = {}
    order: list[str] = []

    def node(fid: str, kind: str = "step", name: str = "") -> FrameNode:
        if fid not in nodes:
            nodes[fid] = FrameNode(fid, kind, name or fid.rpartition("/")[2], "running")
            order.append(fid)
        return nodes[fid]

    for s in entries:
        e: Entry = s.item
        at = e.provenance.at.to_iso()
        p = e.payload
        match e.type:
            case EntryType.EXECUTION_STARTED:
                root = node(ROOT_FID, "root", e.provenance.code.workflow)
                root.started_at = root.started_at or at
            case EntryType.EXECUTION_SUSPENDED:
                root = node(ROOT_FID, "root", e.provenance.code.workflow)
                root.status = "suspended"
                on = p.get("on") or []
                root.suspended_on = ", ".join(on) if on else None
            case EntryType.EXECUTION_RESUMED:
                node(ROOT_FID, "root", e.provenance.code.workflow).status = "running"
            case EntryType.EXECUTION_COMPLETED:
                root = node(ROOT_FID, "root", e.provenance.code.workflow)
                root.status, root.ended_at, root.value = "completed", at, p.get("value")
                root.suspended_on = None
            case EntryType.EXECUTION_FAILED:
                root = node(ROOT_FID, "root", e.provenance.code.workflow)
                root.status, root.ended_at = "failed", at
                root.error = {"type": p.get("error_type"), "message": p.get("message")}
                root.suspended_on = None
            case EntryType.EXECUTION_CANCELLED:
                root = node(ROOT_FID, "root", e.provenance.code.workflow)
                root.status, root.ended_at = "cancelled", at
                root.suspended_on = None
            case EntryType.FRAME_STARTED:
                n = node(e.fid, p.get("kind", "step"), p.get("name", ""))
                n.attempts = max(n.attempts, int(p.get("attempt", 1)))
                n.started_at = n.started_at or at
                n.status, n.ended_at, n.suspended_on = "running", None, None
            case EntryType.FRAME_COMPLETED:
                n = node(e.fid)
                n.status, n.ended_at, n.value = "completed", at, p.get("value")
                n.suspended_on = None
            case EntryType.FRAME_FAILED:
                n = node(e.fid)
                n.status, n.ended_at = "failed", at
                n.error = {
                    "type": p.get("error_type"),
                    "message": p.get("message"),
                    "retryable": p.get("retryable", True),
                    "retry_at": p.get("retry_at"),
                }
            case EntryType.FRAME_SUSPENDED:
                n = node(e.fid)
                n.status, n.suspended_on = "suspended", p.get("on")
                n.deadline = p.get("deadline")
            case EntryType.FRAME_FULFILLED:
                n = node(e.fid)
                n.status, n.suspended_on, n.deadline = "running", None, None
            case _:  # pragma: no mutate
                pass

    for fid in with_evidence:
        if fid in nodes:
            nodes[fid].evidence = True
    if ROOT_FID not in nodes:
        return None
    for fid in order:
        parent = _parent(fid)
        if parent is None:
            continue
        # A frame under a gone parent hangs from the root rather than vanishing.
        nodes.get(parent, nodes[ROOT_FID]).children.append(nodes[fid])
    return nodes[ROOT_FID]
