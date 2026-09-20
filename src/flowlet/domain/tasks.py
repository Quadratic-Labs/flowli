"""Tasks on queues. See docs/specs/01-domain-model.md section 6."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from cairndb import Timestamp

from .frames import FrameRef
from .names import Eid, parse_eid
from .provenance import Provenance


class TaskKind(StrEnum):
    START = "start"
    RESUME = "resume"
    RUN_STEP = "run_step"
    DELEGATE = "delegate"  # consumed outside the engine: a human queue or an external system


@dataclass(frozen=True, slots=True)
class DelegateTask:
    """The payload of a DELEGATE task, as its consumer sees it.

    The contract between the pattern that enqueues (`06-patterns.md`, section
    1) and the consumer that answers (`10-agent-runner.md`, section 2), so it
    belongs to neither of them.
    """

    eid: Eid
    fid: str  # the frame that delegated, which is the parent of the one that waits
    reply_channel: str
    payload: Any
    reply_fid: str | None = None  # the frame that waits, when the sender named it

    @classmethod
    def from_task_payload(cls, payload: dict[str, Any]) -> DelegateTask:
        target = payload["target"]
        return cls(
            parse_eid(target["eid"]),
            target["fid"],
            payload["reply_channel"],
            payload.get("payload"),
            payload.get("reply_fid"),
        )

    @property
    def waiting_fid(self) -> str:
        """The frame a consumer files its evidence under."""
        return self.reply_fid or self.fid


@dataclass(frozen=True, slots=True)
class Task:
    """A unit of work on a queue. Its id is derived: `{kind}:{eid}` plus `:{key}` when set.

    `key` is the dedup discriminator inside one kind and target: the message that caused a
    resume, the frame of a detached step, the name of a delegate. Two tasks with the same
    id are one task, so the enqueue of a duplicate is a no-op.
    """

    queue: str
    kind: TaskKind
    target: FrameRef
    reason: str
    enqueued_by: Provenance
    key: str = ""
    not_before: Timestamp | None = None
    payload: Any = None

    @staticmethod
    def id_for(kind: TaskKind, eid: Eid, key: str = "") -> str:
        return f"{kind.value}:{eid}" + (f":{key}" if key else "")

    @property
    def task_id(self) -> str:
        return self.id_for(self.kind, self.target.eid, self.key)

    def visible_at(self, default: Timestamp) -> Timestamp:
        """The instant from which a worker may dequeue this task."""
        return default if self.not_before is None else self.not_before
