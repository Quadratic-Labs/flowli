"""Frames, frame ids, attempts, outcomes, retry. See docs/specs/01-domain-model.md sections 3-5."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any

from .channels import execution_channel
from .errors import DuplicateFrameError
from .names import FLOWLET2_NAMESPACE, Eid, check_frame_name, digest_text
from .provenance import Provenance

ROOT_FID = "root"


class FrameKind(StrEnum):
    ROOT = "root"
    STEP = "step"
    CHILD = "child"
    RECEIVE = "receive"
    SLEEP = "sleep"


@dataclass(frozen=True, slots=True)
class FrameRef:
    """One frame of one execution. Everything derived from that pair is derived here."""

    eid: Eid
    fid: str

    @property
    def child_eid(self) -> Eid:
        """The id of the child execution this frame starts: a UUIDv5, so it is deterministic."""
        return uuid.uuid5(FLOWLET2_NAMESPACE, f"{self.eid}/{self.fid}")

    @property
    def child_channel(self) -> str:
        """Where the child execution reports its terminal outcome to this frame."""
        return execution_channel(self.eid, f"child.{self.child_eid}")

    @property
    def step_channel(self) -> str:
        """Where a detached run of this step frame reports its outcome."""
        return execution_channel(self.eid, f"step.{digest_text(self.fid)[:16]}")

    def reply_channel(self, frame: str) -> str:
        """Where a delegate sub-frame `frame` of this frame receives its answer."""
        return execution_channel(self.eid, f"reply.{digest_text(self.fid + '/' + frame)[:16]}")


def child_fid(parent: str, name: str, *, ordinal: int | None = None, key: str | None = None) -> str:
    """The frame id of a child of `parent`. Exactly one of ordinal or key is given."""
    check_frame_name(name)
    if (ordinal is None) == (key is None):
        raise ValueError("give exactly one of ordinal or key")
    if key is not None:
        check_frame_name(key)
        return f"{parent}/{name}:{key}"
    return f"{parent}/{name}#{ordinal}"


class FrameIdAllocator:
    """Assigns frame ids under one parent during one replay.

    Ordinals count same-named frames in call order. Keys must be unique.
    """

    def __init__(self, parent: str) -> None:
        self.parent = parent
        self._counts: dict[str, int] = {}
        self._used: set[str] = set()

    def allocate(self, name: str, key: str | None = None) -> str:
        if key is not None:
            fid = child_fid(self.parent, name, key=key)
            if fid in self._used:
                raise DuplicateFrameError(fid)
        else:
            n = self._counts.get(name, 0)
            self._counts[name] = n + 1
            fid = child_fid(self.parent, name, ordinal=n)
        self._used.add(fid)
        return fid


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: timedelta = timedelta(0)
    backoff_factor: float = 1.0
    max_backoff: timedelta | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.backoff_factor < 1.0:
            raise ValueError("backoff_factor must be >= 1.0")

    def allows_retry(self, failed_attempt: int, retryable: bool) -> bool:
        """True when attempt `failed_attempt` failed and another attempt is allowed."""
        return retryable and failed_attempt < self.max_attempts

    def delay_after(self, failed_attempt: int) -> timedelta:
        """Delay before attempt `failed_attempt + 1`. See 05-protocols.md section 6."""
        delay = self.backoff * (self.backoff_factor ** (failed_attempt - 1))
        if self.max_backoff is not None and delay > self.max_backoff:  # pragma: no mutate
            return self.max_backoff
        return delay


@dataclass(frozen=True, slots=True)
class Frame:
    ref: FrameRef
    kind: FrameKind
    name: str
    args_digest: str
    retry: RetryPolicy = field(default_factory=RetryPolicy)


@dataclass(frozen=True, slots=True)
class Attempt:
    frame: FrameRef
    number: int
    provenance: Provenance

    def __post_init__(self) -> None:
        if self.number < 1:
            raise ValueError("attempt number starts at 1")


@dataclass(frozen=True, slots=True)
class Completed:
    value: Any


@dataclass(frozen=True, slots=True)
class Failed:
    error_type: str
    message: str
    retryable: bool = True

    @classmethod
    def from_exception(cls, exc: BaseException, retryable: bool = True) -> Failed:
        return cls(error_type=type(exc).__name__, message=str(exc), retryable=retryable)


Outcome = Completed | Failed
