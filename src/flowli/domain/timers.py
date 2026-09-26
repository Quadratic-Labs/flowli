"""Timers. See specs/01-domain-model.md section 8."""

from __future__ import annotations

from dataclasses import dataclass

from cairndb import Timestamp

from .frames import FrameRef
from .names import digest_text


@dataclass(frozen=True, slots=True)
class Timer:
    """A future instant at which the sweeper resumes one frame.

    The id is derived, not stored: `{due_at_iso}-{eid}-{fid_digest}`. It starts with the
    instant, so ids sort by due time, and two timers for one frame and instant are one.
    """

    due_at: Timestamp
    target: FrameRef

    @property
    def timer_id(self) -> str:
        return f"{self.due_at.to_iso()}-{self.target.eid}-{digest_text(self.target.fid)[:16]}"

    def is_due(self, now: Timestamp | None=None) -> bool:
        now = now or Timestamp.now()
        return self.due_at <= now
