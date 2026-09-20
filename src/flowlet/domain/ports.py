"""Ports the domain requires. See docs/specs/03-ports.md.

One adapter implements all of them. The domain never imports the adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol, runtime_checkable

from cairndb import Timestamp

from .channels import Message
from .execution import Execution
from .frames import FrameRef
from .journal import Entry, Sequenced
from .names import Eid
from .provenance import Provenance
from .tasks import Task
from .timers import Timer


@runtime_checkable
class Lease(Protocol):
    """Fenced ownership. renew() raises LeaseLost when fenced."""

    @property
    def epoch(self) -> int: ...

    @property
    def state(self) -> Any: ...

    @property
    def deadline_at(self) -> Timestamp | None:
        """When the lease expires unless it is renewed."""
        ...

    async def renew(self) -> None: ...

    async def release(self, state: Any = None) -> None: ...

    async def refresh_state(self) -> Any:
        """Absorb cooperative writes. Return the fresh state."""
        ...

    async def update_state(self, fn: Callable[[Any], Any]) -> Any:
        """Apply `fn` to the freshest state and write the result. `fn` must be pure.

        The state of a task lease belongs to its holder. A consumer of a long
        task keeps the address of its work there (spec 10, section 4.2).
        """
        ...


class Journal(Protocol):
    """Trace and memo of one execution. Spec 03 section 3."""

    async def append(self, eid: Eid, entry: Entry) -> int: ...

    async def read(self, eid: Eid, after: int = 0) -> list[Sequenced[Entry]]: ...

    async def tail(self, eid: Eid) -> int: ...

    async def delete(self, eid: Eid) -> None:
        """Delete the whole journal. Retention only, after the archive is written."""
        ...


class ControlLog(Protocol):
    """Lifecycle entries of all executions. Spec 03 section 4."""

    async def announce(self, entry: Entry) -> int: ...

    async def read(self, after: int = 0) -> list[Sequenced[Entry]]: ...


@dataclass(frozen=True, slots=True)
class LeaseInfo:
    """A read of a lease document, without acquisition."""

    epoch: int
    holder: str | None
    deadline_at: Timestamp | None
    released: bool
    state: Any

    def is_expired(self, now: Timestamp | None=None) -> bool:
        """True when nobody holds the lease: released, or past its deadline."""
        now = now or Timestamp.now()
        return self.released or self.deadline_at is None or self.deadline_at <= now


class Ownership(Protocol):
    """One owner per execution. Spec 03 section 5."""

    async def acquire(self, eid: Eid, holder: str, ttl: float) -> Lease | None: ...

    async def request_cancel(self, eid: Eid, by: Provenance) -> None: ...

    async def inspect(self, eid: Eid) -> LeaseInfo | None:
        """Read the lease document. None when no lease was ever acquired."""
        ...

    async def delete(self, eid: Eid) -> None:
        """Delete the lease document. Retention only."""
        ...


class Dispatch(Protocol):
    """Idempotent start, and generic exactly-one claims. Spec 03 section 6."""

    async def claim_start(self, key: str, eid: Eid) -> tuple[bool, Eid]:
        """Return (won, eid). The loser gets the winner's eid."""
        ...

    async def claim(self, key: str, value: Any) -> tuple[bool, Any]:
        """Put-if-absent of a JSON value under `wf/claims/{key}`. Return (won, winner's value)."""
        ...


@dataclass(frozen=True, slots=True)
class QueueDepth:
    """What a queue holds now. See docs/specs/03-ports.md section 7."""

    total: int
    visible: int
    claimed: int


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    task: Task
    key: str
    lease: Lease


class Queue(Protocol):
    """Tasks. Spec 03 section 7."""

    async def enqueue(self, task: Task) -> bool:
        """Return True when this call created the task. False is a no-op."""
        ...

    async def ensure(self, task: Task) -> bool:
        """`enqueue`, and repair an enqueue marker that outlived its task.

        An adapter that dedups with a separate marker writes the marker first,
        so a crash between the two writes leaves a marker that refuses every
        later `enqueue` of that task id. Repair paths only: it costs one
        listing, where `enqueue` costs none.
        """
        ...

    async def dequeue(self, queue: str, worker: str, ttl: float) -> ClaimedTask | None: ...

    async def take(self, queue: str, task_id: str, worker: str, ttl: float) -> ClaimedTask | None:
        """Lease one specific task. None when absent, hidden, or held by someone else."""
        ...

    async def ack(self, claimed: ClaimedTask) -> None: ...

    async def nack(self, claimed: ClaimedTask, delay: timedelta) -> None: ...

    async def depth(self, queue: str) -> QueueDepth:
        """Counts only: one list of the tasks and one of the leases."""
        ...

    async def pending(self, queue: str, limit: int = 100) -> list[Task]:
        """The tasks on the queue, oldest first."""
        ...

    async def peek(self, queue: str, task_id: str) -> Task | None:
        """Read one task. A read takes no lease: ownership is for removal."""
        ...

    async def attach(
        self, queue: str, task_id: str, holder: str, ttl: float
    ) -> ClaimedTask | None:
        """Re-attach to a task that `holder` holds now. Writes nothing.

        None when the task is gone, when the holder does not match, or when
        the lease expired. See spec 10, sections 4.3 and 4.4.
        """
        ...

    async def request_cancel(self, queue: str, task_id: str, by: Provenance) -> None:
        """Ask the holder of a task to stop. It fences nobody.

        The holder reads it on its next renew. This is
        `Ownership.request_cancel` one level down.
        """
        ...


class Channel(Protocol):
    """Messages. Spec 03 section 8."""

    async def send(self, message: Message) -> int: ...

    async def read(self, channel: str, after: int = 0) -> list[Message]: ...

    async def register_wait(self, channel: str, ref: FrameRef) -> None: ...

    async def clear_wait(self, channel: str, ref: FrameRef) -> None: ...

    async def waiters(self, channel: str) -> list[FrameRef]: ...

    async def all_waits(self) -> list[tuple[str, FrameRef]]:
        """Every wait marker, as (channel, ref). For the sweeper."""
        ...

    async def scoped(self, eid: Eid) -> list[str]:
        """Names of the channels scoped to eid: those with the prefix '{eid}.'."""
        ...

    async def delete_channel(self, channel: str) -> None:
        """Delete a channel's log. Retention only."""
        ...


class Timers(Protocol):
    """Future resumes. Spec 03 section 9."""

    async def schedule(self, timer: Timer) -> None: ...

    async def due(self, now: Timestamp) -> list[Timer]: ...

    async def remove(self, timer: Timer) -> None: ...

    async def remove_for(self, eid: Eid) -> int:
        """Remove every timer of one execution. Return how many. Retention only."""
        ...


class ExecutionStore(Protocol):
    """The `wf/exec/{eid}/meta` object. Put-if-absent, then CAS for migrate."""

    async def create(self, execution: Execution) -> bool: ...

    async def read(self, eid: Eid) -> Execution | None: ...

    async def replace(self, eid: Eid, fn: Callable[[Execution], Execution]) -> Execution | None:
        """Read-modify-write with CAS. `fn` must be pure. None when the record is absent."""
        ...

    async def delete(self, eid: Eid) -> None: ...


class Archive(Protocol):
    """Folded journals of finished executions. `wf/archive/{eid}`. Put-if-absent."""

    async def write(self, eid: Eid, data: dict[str, Any]) -> bool: ...

    async def read(self, eid: Eid) -> dict[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """One attempt of one frame. The key of evidence is the attempt, because a
    retry writes a second log."""

    eid: Eid
    fid: str
    attempt: int


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    ref: EvidenceRef
    name: str  # "log", or the name of an attachment
    media_type: str
    size: int


class Evidence(Protocol):
    """What a step or an agent wrote while it ran. Spec 03 section 12.

    The journal says what happened. Evidence explains it. The engine never
    reads evidence, and no decision depends on it.
    """

    async def append_log(self, ref: EvidenceRef, part: bytes) -> None: ...

    async def read_log(self, ref: EvidenceRef) -> bytes: ...

    async def put(
        self, ref: EvidenceRef, name: str, data: bytes, media_type: str
    ) -> str: ...

    async def get(self, ref: EvidenceRef, name: str) -> bytes | None: ...

    async def list(self, eid: Eid) -> list[EvidenceItem]: ...

    async def delete_for(self, eid: Eid) -> int:
        """Retention only. Returns how many objects were removed."""
        ...


@dataclass(frozen=True, slots=True)
class Ports:
    """Everything the engine needs, bundled."""

    journal: Journal
    control: ControlLog
    ownership: Ownership
    dispatch: Dispatch
    queue: Queue
    channel: Channel
    timers: Timers
    executions: ExecutionStore
    archive: Archive
    evidence: Evidence
