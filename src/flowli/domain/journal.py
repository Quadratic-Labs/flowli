"""Journal entries, conditions, and the memo table. See specs/02-journal.md."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .frames import ROOT_FID, Completed, Failed
from .provenance import Provenance

SCHEMA_VERSION = "1.0.0"

# region ----- EntryType -----

class EntryType(StrEnum):
    # journal and control log
    EXECUTION_STARTED = "execution.started"
    EXECUTION_SUSPENDED = "execution.suspended"
    EXECUTION_RESUMED = "execution.resumed"
    EXECUTION_COMPLETED = "execution.completed"
    EXECUTION_FAILED = "execution.failed"
    EXECUTION_CANCELLED = "execution.cancelled"
    # journal only
    FRAME_STARTED = "frame.started"
    FRAME_COMPLETED = "frame.completed"
    FRAME_FAILED = "frame.failed"
    FRAME_SUSPENDED = "frame.suspended"
    FRAME_FULFILLED = "frame.fulfilled"
    EXECUTION_MIGRATED = "execution.migrated"  # journal and control log, status unchanged
    # control log only
    EXECUTION_CREATED = "execution.created"
    EXECUTION_ARCHIVED = "execution.archived"
    TASK_ENQUEUED = "task.enqueued"


ANNOUNCE_PREFIX = "announce."

TERMINAL_TYPES = frozenset(
    {
        EntryType.EXECUTION_COMPLETED,
        EntryType.EXECUTION_FAILED,
        EntryType.EXECUTION_CANCELLED,
    }
)

# endregion

# region ----- conditions -----

class Condition:
    """The `on` field of frame.suspended / frame.fulfilled. A prefixed string."""

    CHANNEL = "channel"
    TIMER = "timer"
    CHILD = "child"
    OPERATOR = "operator"

    @staticmethod
    def channel(name: str) -> str:
        return f"channel:{name}"

    @staticmethod
    def timer(timer_id: str) -> str:
        return f"timer:{timer_id}"

    @staticmethod
    def child(eid: str) -> str:
        return f"child:{eid}"

    @staticmethod
    def operator() -> str:
        return "operator"

    @staticmethod
    def parse(on: str) -> tuple[str, str | None]:
        kind, sep, rest = on.partition(":")
        return kind, (rest if sep else None)

# endregion

# region ----- entries -----

@dataclass(frozen=True, slots=True)
class Entry:
    """One event in the journal or in the control log.

    `type` is an EntryType value or an 'announce.*' string.
    """

    type: str
    fid: str
    payload: dict[str, Any]
    provenance: Provenance

    def __post_init__(self) -> None:
        if not (self.type in EntryType._value2member_map_ or self.type.startswith(ANNOUNCE_PREFIX)):
            raise ValueError(f"unknown entry type {self.type!r}")

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_TYPES

    # constructors for each type -------------------------------------------

    @classmethod
    def execution_started(cls, prov: Provenance, args: Any) -> Entry:
        return cls(EntryType.EXECUTION_STARTED, ROOT_FID, {"args": args}, prov)

    @classmethod
    def frame_started(
        cls, prov: Provenance, fid: str, kind: str, name: str, args_digest: str, attempt: int
    ) -> Entry:
        payload = {"kind": kind, "name": name, "args_digest": args_digest, "attempt": attempt}
        return cls(EntryType.FRAME_STARTED, fid, payload, prov)

    @classmethod
    def frame_completed(cls, prov: Provenance, fid: str, attempt: int, value: Any) -> Entry:
        return cls(EntryType.FRAME_COMPLETED, fid, {"attempt": attempt, "value": value}, prov)

    @classmethod
    def frame_failed(
        cls, prov: Provenance, fid: str, attempt: int, failed: Failed, retry_at: str | None = None
    ) -> Entry:
        payload: dict[str, Any] = {
            "attempt": attempt,
            "error_type": failed.error_type,
            "message": failed.message,
            "retryable": failed.retryable,
        }
        if retry_at is not None:
            payload["retry_at"] = retry_at
        return cls(EntryType.FRAME_FAILED, fid, payload, prov)

    @classmethod
    def frame_suspended(
        cls, prov: Provenance, fid: str, attempt: int, on: str, deadline: str | None = None
    ) -> Entry:
        payload: dict[str, Any] = {"attempt": attempt, "on": on}
        if deadline is not None:
            payload["deadline"] = deadline
        return cls(EntryType.FRAME_SUSPENDED, fid, payload, prov)

    @classmethod
    def frame_fulfilled(
        cls, prov: Provenance, fid: str, attempt: int, on: str, message_seq: int | None = None
    ) -> Entry:
        payload: dict[str, Any] = {"attempt": attempt, "on": on, "message_seq": message_seq}
        return cls(EntryType.FRAME_FULFILLED, fid, payload, prov)

    @classmethod
    def execution_suspended(cls, prov: Provenance, on: list[str]) -> Entry:
        return cls(EntryType.EXECUTION_SUSPENDED, ROOT_FID, {"on": list(on)}, prov)

    @classmethod
    def execution_resumed(cls, prov: Provenance, epoch: int, reason: str) -> Entry:
        return cls(EntryType.EXECUTION_RESUMED, ROOT_FID, {"epoch": epoch, "reason": reason}, prov)

    @classmethod
    def execution_completed(cls, prov: Provenance, value: Any) -> Entry:
        return cls(EntryType.EXECUTION_COMPLETED, ROOT_FID, {"value": value}, prov)

    @classmethod
    def execution_failed(cls, prov: Provenance, failed: Failed) -> Entry:
        payload = {"error_type": failed.error_type, "message": failed.message}
        return cls(EntryType.EXECUTION_FAILED, ROOT_FID, payload, prov)

    @classmethod
    def execution_cancelled(cls, prov: Provenance, by: dict[str, Any]) -> Entry:
        return cls(EntryType.EXECUTION_CANCELLED, ROOT_FID, {"by": by}, prov)

    @classmethod
    def execution_migrated(cls, prov: Provenance, from_version: str, version: str) -> Entry:
        payload = {"from_version": from_version, "version": version}
        return cls(EntryType.EXECUTION_MIGRATED, ROOT_FID, payload, prov)

    @classmethod
    def announce(cls, prov: Provenance, fid: str, kind: str, payload: dict[str, Any]) -> Entry:
        return cls(ANNOUNCE_PREFIX + kind, fid, dict(payload), prov)

    @classmethod
    def task_enqueued(cls, task: Any) -> Entry:
        """Control-log twin of a Task. `task` is a domain Task (kept untyped to avoid a cycle)."""
        payload = {
            "task_id": task.task_id,
            "queue": task.queue,
            "kind": task.kind.value,
            "target": {"eid": str(task.target.eid), "fid": task.target.fid},
            "reason": task.reason,
        }
        return cls(EntryType.TASK_ENQUEUED, task.target.fid, payload, task.enqueued_by)

# endregion

# region ----- memo table -----

@dataclass(frozen=True, slots=True)
class Sequenced[T]:
    seq: int
    item: T

@dataclass
class MemoTable:
    """Derived state of one journal. See specs/02-journal.md section 4."""

    memos: dict[str, Completed] = field(default_factory=dict)
    failures: dict[str, list[Failed]] = field(default_factory=dict)
    suspended: dict[str, str] = field(default_factory=dict)
    fulfilled: dict[str, int | None] = field(default_factory=dict)
    deadlines: dict[str, str] = field(default_factory=dict)  # fid -> ISO deadline of the wait
    retry_at: dict[str, str] = field(default_factory=dict)  # fid -> ISO instant of the next attempt
    digests: dict[str, str] = field(default_factory=dict)
    consumed: dict[str, int] = field(default_factory=dict)  # channel -> max consumed seq
    terminal: Completed | Failed | None = None
    cancelled: bool = False
    tail: int = 0  # sequence of the last entry folded

    @classmethod
    def build(cls, entries: list[Sequenced[Entry]]) -> MemoTable:
        table = cls()
        for s in entries:
            table.apply(s)
        return table

    def apply(self, s: Sequenced[Entry]) -> None:
        e = s.item
        p = e.payload
        match e.type:
            case EntryType.FRAME_STARTED:
                self.digests.setdefault(e.fid, p["args_digest"])
            case EntryType.FRAME_COMPLETED:
                self.memos.setdefault(e.fid, Completed(p["value"]))
            case EntryType.FRAME_FAILED:
                self.failures.setdefault(e.fid, []).append(
                    Failed(p["error_type"], p["message"], p.get("retryable", True))
                )
                if p.get("retry_at") is not None:
                    self.retry_at[e.fid] = p["retry_at"]
                else:
                    self.retry_at.pop(e.fid, None)
            case EntryType.FRAME_SUSPENDED:
                self.suspended[e.fid] = p["on"]
                if p.get("deadline") is not None:
                    self.deadlines[e.fid] = p["deadline"]
            case EntryType.FRAME_FULFILLED:
                self.suspended.pop(e.fid, None)
                self.deadlines.pop(e.fid, None)
                seq = p.get("message_seq")
                self.fulfilled[e.fid] = seq
                kind, name = Condition.parse(p["on"])
                if kind == Condition.CHANNEL and name is not None and seq is not None:
                    self.consumed[name] = max(self.consumed.get(name, 0), seq)
            case EntryType.EXECUTION_COMPLETED:
                if self.terminal is None:
                    self.terminal = Completed(p["value"])
            case EntryType.EXECUTION_FAILED:
                if self.terminal is None:
                    self.terminal = Failed(p["error_type"], p["message"], retryable=False)
            case EntryType.EXECUTION_CANCELLED:
                self.cancelled = True
            case _:  # pragma: no mutate
                pass
        self.tail = max(self.tail, s.seq)

    # queries -----------------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        return self.terminal is not None or self.cancelled

    def next_attempt(self, fid: str) -> int:
        return 1 + len(self.failures.get(fid, []))

    def last_consumed(self, channel: str) -> int:
        return self.consumed.get(channel, 0)

    def check_digest(self, fid: str, computed: str) -> str | None:
        """Return the recorded digest when it differs from `computed`, else None."""
        recorded = self.digests.get(fid)
        if recorded is not None and recorded != computed:
            return recorded
        return None

# endregion
