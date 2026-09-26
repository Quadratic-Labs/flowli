"""Attempt logs: what a step wrote while it ran. See specs/03-ports.md section 12.

An author writes `log.info("fetched", n=12)` and nothing more. `log.py` binds
`eid`, `fid` and `attempt` to every event of a worker already, and the
`capture` processor below puts the event in the buffer of the running
attempt. The buffer flushes as one object per flush, never one object per
event.

Evidence is never authority. A write of it is best effort: an error is
logged and is not raised into the frame, because a frame must not fail
because a log did not write.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator, Callable, MutableMapping
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from flowli.domain import Eid, Evidence, EvidenceItem, EvidenceRef

DEFAULT_LIMIT = 1 << 20  # 1 MiB per attempt
DEFAULT_FLUSH_INTERVAL = 10.0  # seconds
TAIL_LINES = 64  # what survives past the limit, with the head already written

_current: ContextVar[AttemptBuffer | None] = ContextVar("flowli_evidence", default=None)


def _line(event: dict[str, Any]) -> bytes:
    try:
        return (json.dumps(event, default=str, sort_keys=True) + "\n").encode()
    except Exception:  # a value that json cannot render must not break the step
        fallback = {"event": str(event.get("event")), "unrenderable": True}
        return (json.dumps(fallback) + "\n").encode()


@dataclass
class AttemptBuffer:
    """The lines of one attempt, between two flushes.

    Past `limit` the buffer keeps a bounded tail only and counts what it
    dropped, so a runaway step costs a known amount of memory and of storage.
    """

    ref: EvidenceRef
    limit: int = DEFAULT_LIMIT
    pending: list[bytes] = None  # type: ignore[assignment]
    written: int = 0
    dropped: int = 0
    tail: deque[bytes] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.pending = []
        self.tail = deque(maxlen=TAIL_LINES)

    def add(self, event: dict[str, Any]) -> None:
        line = _line(event)
        if self.written + sum(len(x) for x in self.pending) + len(line) > self.limit:
            self.dropped += 1
            self.tail.append(line)
            return
        self.pending.append(line)

    def drain(self) -> bytes | None:
        """The next part to write, or None when there is nothing new."""
        if not self.pending:
            return None
        part = b"".join(self.pending)
        self.pending.clear()
        self.written += len(part)
        return part

    def final(self) -> bytes | None:
        """The last part: what is pending, then the truncation marker and the tail."""
        if not self.dropped:
            return self.drain()
        head = self.drain() or b""
        marker = _line({"event": "evidence_truncated", "dropped": self.dropped})
        return head + marker + b"".join(self.tail)


def capture(
    _logger: Any, _method: str, event: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """A structlog processor. It adds the event to the buffer of this attempt.

    It never changes the event and never raises: logging must work whether or
    not an attempt is running.
    """
    buffer = _current.get()
    if buffer is not None:
        with suppress(Exception):
            buffer.add(dict(event))
    return event


class EvidenceWriter:
    """Buffers the log of an attempt and writes it to the `Evidence` port."""

    def __init__(
        self,
        port: Evidence,
        *,
        limit: int = DEFAULT_LIMIT,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.port = port
        self.limit = limit
        self.flush_interval = flush_interval
        self._on_error = on_error

    async def _write(self, ref: EvidenceRef, part: bytes | None) -> None:
        if not part:
            return
        try:
            await self.port.append_log(ref, part)
        except Exception as exc:  # evidence never fails a frame
            if self._on_error is not None:
                self._on_error(exc)

    @asynccontextmanager
    async def collecting(self, ref: EvidenceRef) -> AsyncIterator[AttemptBuffer]:
        """Capture what this attempt logs, and flush it when the attempt ends."""
        buffer = AttemptBuffer(ref, limit=self.limit)
        token = _current.set(buffer)
        ticker = asyncio.create_task(self._tick(buffer)) if self.flush_interval > 0 else None
        try:
            yield buffer
        finally:
            _current.reset(token)
            if ticker is not None:
                ticker.cancel()
                with suppress(asyncio.CancelledError):
                    await ticker
            await self._write(ref, buffer.final())

    async def _tick(self, buffer: AttemptBuffer) -> None:
        """Flush a long attempt while it runs, so a crash keeps what it said."""
        while True:
            await asyncio.sleep(self.flush_interval)
            await self._write(buffer.ref, buffer.drain())


class NullEvidence:
    """The default: accept every write, store nothing.

    A process that wants attempt logs configures a real adapter. One that does
    not pays nothing for the machinery.
    """

    async def append_log(self, ref: EvidenceRef, part: bytes) -> None:
        return None

    async def read_log(self, ref: EvidenceRef) -> bytes:
        return b""

    async def put(self, ref: EvidenceRef, name: str, data: bytes, media_type: str) -> str:
        return ""

    async def get(self, ref: EvidenceRef, name: str) -> bytes | None:
        return None

    async def list(self, eid: Eid) -> list[EvidenceItem]:
        return []

    async def delete_for(self, eid: Eid) -> int:
        return 0


__all__ = [
    "AttemptBuffer",
    "EvidenceWriter",
    "NullEvidence",
    "capture",
]
