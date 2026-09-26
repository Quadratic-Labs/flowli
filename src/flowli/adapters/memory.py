"""In-memory implementation of every port. For tests and local runs.

Semantics follow specs/03-ports.md. Time is injectable through `ManualClock`
so tests can expire leases and fire timers without sleeping.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any

from cairndb import Timestamp

from flowli.adapters import evidence as keys
from flowli.codec import canonical_json, structure, unstructure
from flowli.domain import (
    ClaimedTask,
    Eid,
    Entry,
    EvidenceItem,
    EvidenceRef,
    Execution,
    FrameRef,
    LeaseInfo,
    LeaseLost,
    Message,
    Ports,
    Provenance,
    QueueDepth,
    Sequenced,
    Task,
    Timer,
    check_channel,
    check_queue,
)

Now = Callable[[], Timestamp]


def _ts(value: Timestamp | datetime) -> Timestamp:
    return value if isinstance(value, Timestamp) else Timestamp(value)


class ManualClock:
    """A clock that moves only when told to."""

    def __init__(self, start: Timestamp | datetime | None = None) -> None:
        self._now = Timestamp.now() if start is None else _ts(start)

    def __call__(self) -> Timestamp:
        return self._now

    def advance(self, delta: timedelta) -> Timestamp:
        self._now += delta
        return self._now

    def set(self, at: Timestamp | datetime) -> None:
        self._now = _ts(at)


# --- leases -----------------------------------------------------------------


@dataclass
class _LeaseDoc:
    epoch: int = 0
    holder: str | None = None
    deadline_at: Timestamp | None = None
    state: Any = None
    released: bool = True


class MemoryLease:
    """Fenced lease handle. Every write checks that the doc epoch is still ours."""

    def __init__(self, table: _LeaseTable, key: str, epoch: int, ttl: float) -> None:
        self._table = table
        self._key = key
        self._epoch = epoch
        self._ttl = ttl
        self._state: Any = table.docs[key].state

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def state(self) -> Any:
        return self._state

    @property
    def deadline_at(self) -> Timestamp | None:
        doc = self._table.docs.get(self._key)
        return None if doc is None else doc.deadline_at

    def _doc(self) -> _LeaseDoc:
        doc = self._table.docs.get(self._key)
        if doc is None or doc.epoch != self._epoch or doc.released:
            raise LeaseLost(f"lease {self._key} epoch {self._epoch} was fenced")
        return doc

    async def renew(self) -> None:
        doc = self._doc()
        doc.deadline_at = self._table.now() + timedelta(seconds=self._ttl)
        self._state = doc.state

    async def release(self, state: Any = None) -> None:
        doc = self._doc()
        if state is not None:
            doc.state = state
        doc.released = True
        doc.holder = None
        self._state = doc.state

    async def refresh_state(self) -> Any:
        doc = self._doc()
        self._state = doc.state
        return doc.state

    async def update_state(self, fn: Callable[[Any], Any]) -> Any:
        doc = self._doc()
        doc.state = fn(doc.state)
        self._state = doc.state
        return doc.state


class _LeaseTable:
    """Shared by ownership and queue: key -> lease document."""

    def __init__(self, now: Now) -> None:
        self.now = now
        self.docs: dict[str, _LeaseDoc] = {}

    def is_held(self, key: str) -> bool:
        doc = self.docs.get(key)
        if doc is None or doc.released:
            return False
        return doc.deadline_at is not None and doc.deadline_at > self.now()

    async def acquire(self, key: str, holder: str, ttl: float) -> MemoryLease | None:
        if self.is_held(key):
            return None
        doc = self.docs.setdefault(key, _LeaseDoc())
        doc.epoch += 1
        doc.holder = holder
        doc.deadline_at = self.now() + timedelta(seconds=ttl)
        doc.released = False
        return MemoryLease(self, key, doc.epoch, ttl)

    def cooperative_write(self, key: str, fn: Callable[[Any], Any]) -> None:
        doc = self.docs.setdefault(key, _LeaseDoc())
        doc.state = fn(doc.state)

    def delete(self, key: str) -> None:
        self.docs.pop(key, None)


# --- ports ------------------------------------------------------------------


class MemoryJournal:
    def __init__(self) -> None:
        self.logs: dict[Eid, list[Sequenced[Entry]]] = {}

    async def append(self, eid: Eid, entry: Entry) -> int:
        log = self.logs.setdefault(eid, [])
        seq = len(log) + 1
        log.append(Sequenced(seq, entry))
        return seq

    async def read(self, eid: Eid, after: int = 0) -> list[Sequenced[Entry]]:
        return [s for s in self.logs.get(eid, []) if s.seq > after]

    async def tail(self, eid: Eid) -> int:
        return len(self.logs.get(eid, []))

    async def delete(self, eid: Eid) -> None:
        self.logs.pop(eid, None)


class MemoryControlLog:
    def __init__(self) -> None:
        self.entries: list[Sequenced[Entry]] = []

    async def announce(self, entry: Entry) -> int:
        seq = len(self.entries) + 1
        self.entries.append(Sequenced(seq, entry))
        return seq

    async def read(self, after: int = 0) -> list[Sequenced[Entry]]:
        return [s for s in self.entries if s.seq > after]


class MemoryOwnership:
    def __init__(self, leases: _LeaseTable) -> None:
        self._leases = leases

    @staticmethod
    def key(eid: Eid) -> str:
        return f"wf/exec/{eid}/lease"

    async def acquire(self, eid: Eid, holder: str, ttl: float) -> MemoryLease | None:
        return await self._leases.acquire(self.key(eid), holder, ttl)

    async def request_cancel(self, eid: Eid, by: Provenance) -> None:
        flag = unstructure(by)
        self._leases.cooperative_write(
            self.key(eid), lambda s: {**(s or {}), "cancel_requested": flag}
        )

    async def inspect(self, eid: Eid) -> LeaseInfo | None:
        doc = self._leases.docs.get(self.key(eid))
        if doc is None:
            return None
        return LeaseInfo(doc.epoch, doc.holder, doc.deadline_at, doc.released, doc.state)

    async def delete(self, eid: Eid) -> None:
        self._leases.delete(self.key(eid))

    def lease_doc(self, eid: Eid) -> _LeaseDoc | None:
        return self._leases.docs.get(self.key(eid))


class MemoryDispatch:
    def __init__(self) -> None:
        self.claims: dict[str, Eid] = {}
        self.generic: dict[str, Any] = {}

    async def claim_start(self, key: str, eid: Eid) -> tuple[bool, Eid]:
        if key in self.claims:
            return False, self.claims[key]
        self.claims[key] = eid
        return True, eid

    async def claim(self, key: str, value: Any) -> tuple[bool, Any]:
        if key in self.generic:
            return False, self.generic[key]
        self.generic[key] = value
        return True, value


@dataclass
class _QueuedTask:
    key: str
    task: Task
    visible_at: Timestamp


class MemoryQueue:
    def __init__(self, leases: _LeaseTable, now: Now) -> None:
        self._leases = leases
        self._now = now
        self.queues: dict[str, dict[str, _QueuedTask]] = {}

    def _key(self, task: Task, visible_at: Timestamp) -> str:
        return f"wf/queues/{task.queue}/{visible_at.to_iso()}-{task.task_id}"

    async def enqueue(self, task: Task) -> bool:
        check_queue(task.queue)
        q = self.queues.setdefault(task.queue, {})
        if task.task_id in q:
            return False
        visible_at = task.visible_at(self._now())
        q[task.task_id] = _QueuedTask(self._key(task, visible_at), task, visible_at)
        return True

    async def ensure(self, task: Task) -> bool:
        """The task table is its own dedup index here, so no marker can outlive a task."""
        return await self.enqueue(task)

    async def dequeue(self, queue: str, worker: str, ttl: float) -> ClaimedTask | None:
        now = self._now()
        candidates = sorted(self.queues.get(queue, {}).values(), key=lambda t: t.key)
        for qt in candidates:
            if qt.visible_at > now:
                # `candidates` is sorted by key, which is prefixed by visible_at.to_iso()
                # (fixed-width, so string order == chronological order), so visible_at is
                # non-decreasing across this loop: once one candidate isn't due yet, no
                # later one can be either. `continue` and `break` are then equivalent here.
                continue  # pragma: no mutate
            lease = await self._leases.acquire(qt.key + ".lease", worker, ttl)
            if lease is not None:
                return ClaimedTask(task=qt.task, key=qt.key, lease=lease)
        return None

    async def take(self, queue: str, task_id: str, worker: str, ttl: float) -> ClaimedTask | None:
        qt = self.queues.get(queue, {}).get(task_id)
        if qt is None or qt.visible_at > self._now():
            return None
        lease = await self._leases.acquire(qt.key + ".lease", worker, ttl)
        if lease is None:
            return None
        return ClaimedTask(task=qt.task, key=qt.key, lease=lease)

    async def ack(self, claimed: ClaimedTask) -> None:
        await claimed.lease.release()
        self.queues.get(claimed.task.queue, {}).pop(claimed.task.task_id, None)
        self._leases.delete(claimed.key + ".lease")

    async def nack(self, claimed: ClaimedTask, delay: timedelta) -> None:
        task = replace(claimed.task, not_before=self._now() + delay)
        await self.ack(claimed)
        await self.enqueue(task)

    async def pending(self, queue: str, limit: int = 100) -> list[Task]:
        ordered = sorted(self.queues.get(queue, {}).values(), key=lambda t: t.key)
        return [qt.task for qt in ordered[:limit]]

    async def peek(self, queue: str, task_id: str) -> Task | None:
        qt = self.queues.get(queue, {}).get(task_id)
        return None if qt is None else qt.task

    async def attach(self, queue: str, task_id: str, holder: str, ttl: float) -> ClaimedTask | None:
        qt = self.queues.get(queue, {}).get(task_id)
        if qt is None:
            return None
        key = qt.key + ".lease"
        doc = self._leases.docs.get(key)
        if doc is None or doc.holder != holder or not self._leases.is_held(key):
            return None
        lease = MemoryLease(self._leases, key, doc.epoch, ttl)
        return ClaimedTask(task=qt.task, key=qt.key, lease=lease)

    async def request_cancel(self, queue: str, task_id: str, by: Provenance) -> None:
        qt = self.queues.get(queue, {}).get(task_id)
        if qt is None:
            return
        self._leases.cooperative_write(
            qt.key + ".lease", lambda s: {**(s or {}), "cancel_requested": unstructure(by)}
        )

    async def depth(self, queue: str) -> QueueDepth:
        now = self._now()
        tasks = list(self.queues.get(queue, {}).values())
        return QueueDepth(
            total=len(tasks),
            visible=sum(1 for qt in tasks if qt.visible_at <= now),
            claimed=sum(1 for qt in tasks if self._leases.is_held(qt.key + ".lease")),
        )


class MemoryEvidence:
    """Evidence in a dict. The key layout is the CairnDB one (adapters/evidence.py)."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    async def append_log(self, ref: EvidenceRef, part: bytes) -> None:
        self.objects.setdefault(
            keys.meta_key(ref), (keys.meta_bytes(ref), "application/json")
        )  # pragma: no mutate
        n = sum(
            1 for k in self.objects if k.startswith(f"{keys.base(ref)}/{keys.LOG_NAME}.")
        )  # pragma: no mutate
        self.objects[keys.log_key(ref, n)] = (part, keys.NDJSON)

    async def read_log(self, ref: EvidenceRef) -> bytes:
        prefix = f"{keys.base(ref)}/{keys.LOG_NAME}."
        return b"".join(self.objects[k][0] for k in sorted(self.objects) if k.startswith(prefix))

    async def put(self, ref: EvidenceRef, name: str, data: bytes, media_type: str) -> str:
        self.objects.setdefault(
            keys.meta_key(ref), (keys.meta_bytes(ref), "application/json")
        )  # pragma: no mutate
        key = keys.attachment_key(ref, name)
        self.objects[key] = (data, media_type)
        return key

    async def get(self, ref: EvidenceRef, name: str) -> bytes | None:
        if name == keys.LOG_NAME:
            log = await self.read_log(ref)
            return log or None
        item = self.objects.get(keys.attachment_key(ref, name))
        return None if item is None else item[0]

    async def list(self, eid: Eid) -> list[EvidenceItem]:
        prefix = keys.eid_prefix(eid)
        refs: dict[str, EvidenceRef] = {}
        for key, (data, _) in self.objects.items():
            if key.startswith(prefix) and key.endswith("/meta"):
                refs[key.rsplit("/", 1)[0]] = keys.ref_from_meta(eid, data)
        items: list[EvidenceItem] = []
        for frame_base, ref in sorted(refs.items()):
            size = sum(
                len(d)
                for k, (d, _) in self.objects.items()
                if k.startswith(f"{frame_base}/{keys.LOG_NAME}.")
            )
            if size:
                items.append(EvidenceItem(ref, keys.LOG_NAME, keys.NDJSON, size))
            for key, (data, media) in sorted(self.objects.items()):
                if key.startswith(f"{frame_base}/a/"):
                    items.append(EvidenceItem(ref, key.rsplit("/", 1)[1], media, len(data)))
        return items

    async def delete_for(self, eid: Eid) -> int:
        prefix = keys.eid_prefix(eid)
        gone = [k for k in self.objects if k.startswith(prefix)]
        for k in gone:
            del self.objects[k]
        return len(gone)


class MemoryChannel:
    def __init__(self) -> None:
        self.logs: dict[str, list[Message]] = {}
        self.waits: dict[str, dict[Eid, str]] = {}  # channel -> eid -> fid

    async def send(self, message: Message) -> int:
        check_channel(message.channel)
        log = self.logs.setdefault(message.channel, [])
        seq = len(log) + 1
        log.append(replace(message, seq=seq))
        return seq

    async def read(self, channel: str, after: int = 0) -> list[Message]:
        return [m for m in self.logs.get(channel, []) if m.seq > after]

    async def register_wait(self, channel: str, ref: FrameRef) -> None:
        self.waits.setdefault(channel, {})[ref.eid] = ref.fid

    async def clear_wait(self, channel: str, ref: FrameRef) -> None:
        self.waits.get(channel, {}).pop(ref.eid, None)

    async def waiters(self, channel: str) -> list[FrameRef]:
        entries = self.waits.get(channel, {})
        items = sorted(entries.items(), key=lambda kv: str(kv[0]))  # pragma: no mutate
        return [FrameRef(eid, fid) for eid, fid in items]

    async def all_waits(self) -> list[tuple[str, FrameRef]]:
        def sorted_items(channel: str) -> list[tuple[Eid, str]]:
            return sorted(
                self.waits[channel].items(), key=lambda kv: str(kv[0])
            )  # pragma: no mutate

        return [
            (channel, FrameRef(eid, fid))
            for channel in sorted(self.waits)
            for eid, fid in sorted_items(channel)
        ]

    async def scoped(self, eid: Eid) -> list[str]:
        return sorted(c for c in self.logs if c.startswith(f"{eid}."))

    async def delete_channel(self, channel: str) -> None:
        self.logs.pop(channel, None)


class MemoryTimers:
    def __init__(self) -> None:
        self.timers: dict[str, Timer] = {}

    async def schedule(self, timer: Timer) -> None:
        self.timers.setdefault(timer.timer_id, timer)

    async def due(self, now: Timestamp) -> list[Timer]:
        return sorted((t for t in self.timers.values() if t.is_due(now)), key=lambda t: t.timer_id)

    async def remove(self, timer: Timer) -> None:
        self.timers.pop(timer.timer_id, None)

    async def remove_for(self, eid: Eid) -> int:
        doomed = [k for k, t in self.timers.items() if t.target.eid == eid]
        for k in doomed:
            del self.timers[k]
        return len(doomed)


class MemoryExecutionStore:
    """Records, round-tripped through the codec on every read.

    A store that hands back the object it holds would let a workflow mutate
    its own arguments and see that mutation on the next replay. A real store
    deserializes each time, so this one does too.
    """

    def __init__(self) -> None:
        self.records: dict[Eid, str] = {}

    def _put(self, execution: Execution) -> None:
        self.records[execution.eid] = canonical_json(unstructure(execution))

    async def create(self, execution: Execution) -> bool:
        if execution.eid in self.records:
            return False
        self._put(execution)
        return True

    async def read(self, eid: Eid) -> Execution | None:
        raw = self.records.get(eid)
        return None if raw is None else structure(json.loads(raw), Execution)

    async def replace(self, eid: Eid, fn: Callable[[Execution], Execution]) -> Execution | None:
        current = await self.read(eid)
        if current is None:
            return None
        self._put(fn(current))
        return await self.read(eid)

    async def delete(self, eid: Eid) -> None:
        self.records.pop(eid, None)


class MemoryArchive:
    def __init__(self) -> None:
        self.archives: dict[str, dict[str, Any]] = {}  # keyed by str(eid)

    async def write(self, eid: Eid, data: dict[str, Any]) -> bool:
        if str(eid) in self.archives:
            return False
        self.archives[str(eid)] = dict(data)
        return True

    async def read(self, eid: Eid) -> dict[str, Any] | None:
        data = self.archives.get(str(eid))
        return None if data is None else dict(data)


@dataclass
class MemoryBackend:
    """All ports over one shared clock. `backend.ports` is what the engine takes."""

    clock: ManualClock = field(default_factory=ManualClock)

    def __post_init__(self) -> None:
        self.leases = _LeaseTable(self.clock)
        self.journal = MemoryJournal()
        self.control = MemoryControlLog()
        self.ownership = MemoryOwnership(self.leases)
        self.dispatch = MemoryDispatch()
        self.queue = MemoryQueue(self.leases, self.clock)
        self.channel = MemoryChannel()
        self.timers = MemoryTimers()
        self.executions = MemoryExecutionStore()
        self.archive = MemoryArchive()
        self.evidence = MemoryEvidence()

    @property
    def ports(self) -> Ports:
        return Ports(
            journal=self.journal,
            control=self.control,
            ownership=self.ownership,
            dispatch=self.dispatch,
            queue=self.queue,
            channel=self.channel,
            timers=self.timers,
            executions=self.executions,
            archive=self.archive,
            evidence=self.evidence,
        )
