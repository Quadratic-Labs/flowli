"""Execution and its status machine. See specs/01-domain-model.md section 2."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .frames import FrameRef
from .names import Eid
from .provenance import Provenance


class ExecutionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = frozenset(
    {ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}
)

TRANSITIONS: dict[ExecutionStatus, frozenset[ExecutionStatus]] = {
    ExecutionStatus.PENDING: frozenset({ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED}),
    ExecutionStatus.RUNNING: frozenset(
        {
            ExecutionStatus.SUSPENDED,
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }
    ),
    ExecutionStatus.SUSPENDED: frozenset({ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED}),
    ExecutionStatus.COMPLETED: frozenset(),
    ExecutionStatus.FAILED: frozenset(),
    ExecutionStatus.CANCELLED: frozenset(),
}


def can_transition(src: ExecutionStatus, dst: ExecutionStatus) -> bool:
    return dst in TRANSITIONS[src]


@dataclass(frozen=True, slots=True)
class Execution:
    eid: Eid
    workflow: str
    version: str
    args: Any  # JSON-compatible
    created_by: Provenance
    parent: FrameRef | None = None
    dispatch_key: str | None = None
    queue: str = "default"
