"""CairnDB implementation of every port. See specs/03-ports.md.

Key layout (spec 03 section 2):

    logs/wf/                         control log
    logs/wf.exec.{eid}/              journal
    logs/wf.ch.{channel}/            channel messages
    wf/exec/{eid}/meta               Execution record          put-if-absent
    wf/exec/{eid}/lease              ownership lease           CairnDB lease
    wf/dispatch/{key}                {"eid": ...}              claim
    wf/claims/{key}                  any JSON value            claim (generic)
    wf/queues/{q}/t/{iso}-{task_id}  Task                      put-if-absent
    wf/queues/{q}/l/{iso}-{task_id}  task lease                CairnDB lease
    wf/queues/{q}/ids/{task_id}      dedup marker: the task key put-if-absent
    wf/waits/{channel}/{eid}         {"fid": ...}              put, delete
    wf/timers/{timer_id}             Timer                     put-if-absent, delete
    wf/archive/{eid}.msgpack         folded journal + channels put-if-absent

Sequence numbers: CairnDB orders events by (commit, index). The adapter encodes
that pair as ``commit * SEQ_BASE + index`` so the domain sees one ordered int.
"""

from __future__ import annotations

import json
import uuid
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from datetime import timedelta
from typing import Any

import cairndb
import msgpack
from cairndb import CairnDB, Event, EventType, SchemaVersion, SequenceNumber, Timestamp
from cairndb.engine.logs import Log, NamespacedStorage

from flowli.adapters import evidence as keys
from flowli.codec import structure, unstructure
from flowli.domain import (
    ROOT_FID,
    SCHEMA_VERSION,
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
    parse_eid,
)

SEQ_BASE = 1_000_000
ISO_WIDTH = len("2026-09-07T09:11:00.123456Z")  # width of Timestamp.to_iso(), sorts lexically

Now = Callable[[], Timestamp]


def encode_seq(sn: SequenceNumber) -> int:
    return sn.commit * SEQ_BASE + sn.index


def _json_default(value: Any) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"not JSON-compatible: {type(value).__name__}")


def _dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=_json_default).encode()


def _loads(data: bytes) -> Any:
    return json.loads(data)


# --- logs -----------------------------------------------------------------------


class _LogCache:
    """Open logs, most recent last. Evicted logs are closed (their committer stops)."""

    def __init__(self, db: CairnDB, maxsize: int = 128) -> None:
        self._db = db
        self._maxsize = maxsize
        self._logs: OrderedDict[str, Log] = OrderedDict()

    async def get(self, name: str) -> Log:
        log = self._logs.get(name)
        if log is None:
            log = Log(self._db.storage, name)
            self._logs[name] = log
            while len(self._logs) > self._maxsize:
                _, old = self._logs.popitem(last=False)
                await old.close()
        else:
            self._logs.move_to_end(name)
        return log

    async def close(self) -> None:
        for log in self._logs.values():
            await log.close()
        self._logs.clear()

    async def drop(self, name: str) -> None:
        """Close and forget a log, then delete every commit and snapshot it holds."""
        log = self._logs.pop(name, None)
        if log is not None:
            await log.close()
        ns = NamespacedStorage(self._db.storage, f"logs/{name}")
        await ns.delete_commits_before(2**62)
        for schema in ("1", "1.0.0"):
            await ns.delete_snapshots_before(schema, 2**62)


def _entry_event(entry: Entry) -> Event:
    """The CairnDB Event form of a journal entry: provenance and fid ride in metadata."""
    return Event(
        event_type=EventType(entry.type),
        timestamp=entry.provenance.at,
        payload=entry.payload,
        schema_version=SchemaVersion(SCHEMA_VERSION),
        metadata={"provenance": unstructure(entry.provenance), "fid": entry.fid},
    )


def _event_entry(event: Event) -> Entry:
    meta = event.metadata or {}
    return Entry(
        type=str(event.event_type),
        fid=meta.get("fid", ROOT_FID),
        payload=dict(event.payload),
        provenance=structure(meta["provenance"], Provenance),
    )


async def _read_entries(log: Log, after: int) -> list[Sequenced[Entry]]:
    out: list[Sequenced[Entry]] = []
    # Same reasoning as CairnChannel.read: shifting this lower bound by one
    # commit can only make it scan one more already-seen commit, which the
    # `seq > after` filter below discards -- never changes what is returned.
    async for s in log.read(after=max(after // SEQ_BASE - 1, 0)):  # pragma: no mutate
        seq = encode_seq(s.sequence)
        if seq > after:
            out.append(Sequenced(seq, _event_entry(s.event)))
    return out


class CairnJournal:
    def __init__(self, logs: _LogCache) -> None:
        self._logs = logs

    @staticmethod
    def name(eid: Eid) -> str:
        return f"wf.exec.{parse_eid(eid)}"

    async def append(self, eid: Eid, entry: Entry) -> int:
        log = await self._logs.get(self.name(eid))
        return encode_seq(await log.append(_entry_event(entry)))

    async def read(self, eid: Eid, after: int = 0) -> list[Sequenced[Entry]]:  # pragma: no mutate
        # See CairnChannel.read: real `seq` values start at SEQ_BASE, so this
        # default's exact value doesn't matter.
        return await _read_entries(await self._logs.get(self.name(eid)), after)

    async def tail(self, eid: Eid) -> int:
        log = await self._logs.get(self.name(eid))
        last_commit = await log.current_tail()
        if last_commit == 0:
            return 0
        seq = 0  # pragma: no mutate
        async for s in log.read(after=last_commit - 1):
            seq = encode_seq(s.sequence)
        return seq

    async def delete(self, eid: Eid) -> None:
        await self._logs.drop(self.name(eid))


class CairnControlLog:
    NAME = "wf"

    def __init__(self, logs: _LogCache) -> None:
        self._logs = logs

    async def announce(self, entry: Entry) -> int:
        log = await self._logs.get(self.NAME)
        return encode_seq(await log.append(_entry_event(entry)))

    async def read(self, after: int = 0) -> list[Sequenced[Entry]]:  # pragma: no mutate
        # See CairnChannel.read: real `seq` values start at SEQ_BASE, so this
        # default's exact value doesn't matter.
        return await _read_entries(await self._logs.get(self.NAME), after)


# --- leases ---------------------------------------------------------------------


class CairnLease:
    """Wraps a cairndb Lease and maps its LeaseLost to the domain's."""

    def __init__(self, inner: cairndb.Lease) -> None:
        self._inner = inner

    @property
    def epoch(self) -> int:
        return int(self._inner.epoch)

    @property
    def state(self) -> Any:
        return self._inner.state

    @property
    def deadline_at(self) -> Timestamp | None:
        value: Timestamp | None = self._inner.deadline_at
        return value

    async def renew(self) -> None:
        try:
            await self._inner.renew()
        except cairndb.LeaseLost as exc:
            raise LeaseLost(str(exc)) from exc

    async def release(self, state: Any = None) -> None:
        try:
            await self._inner.release(state)
        except cairndb.LeaseLost as exc:
            raise LeaseLost(str(exc)) from exc

    async def refresh_state(self) -> Any:
        return await self.update_state(lambda s: s)

    async def update_state(self, fn: Callable[[Any], Any]) -> Any:
        try:
            return await self._inner.update_state(fn)
        except cairndb.LeaseLost as exc:
            raise LeaseLost(str(exc)) from exc


async def _acquire(db: CairnDB, key: str, holder: str, ttl: float) -> CairnLease | None:
    # `steal_if_expired=True` is spelled out for clarity; it is `db.lease`'s
    # own default, so omitting it is behaviorally identical.
    inner = await db.lease(key, ttl=ttl, holder=holder, steal_if_expired=True)  # pragma: no mutate
    return None if inner is None else CairnLease(inner)


class CairnOwnership:
    def __init__(self, db: CairnDB) -> None:
        self._db = db

    @staticmethod
    def key(eid: Eid) -> str:
        return f"wf/exec/{parse_eid(eid)}/lease"

    async def acquire(self, eid: Eid, holder: str, ttl: float) -> CairnLease | None:
        return await _acquire(self._db, self.key(eid), holder, ttl)

    async def request_cancel(self, eid: Eid, by: Provenance) -> None:
        key = self.key(eid)
        flag = unstructure(by)

        def fn(s: Any) -> Any:
            return {**(s if isinstance(s, dict) else {}), "cancel_requested": flag}

        if await self._db.cooperative_write(key, fn) is not None:
            return
        # No lease document yet: the execution never ran. Create one, released, with the flag.
        lease = await self._db.lease(
            key, ttl=1, holder=f"cancel:{by.actor.id}", state_fn=fn
        )  # pragma: no mutate
        if lease is not None:
            await lease.release()
        else:
            await self._db.cooperative_write(key, fn)

    async def delete(self, eid: Eid) -> None:
        await self._db.objects.delete(self.key(eid))

    async def inspect(self, eid: Eid) -> LeaseInfo | None:
        obj = await self._db.objects.get(self.key(eid))
        if obj is None:
            return None
        doc = _loads(obj.data)
        deadline = doc.get("deadline_at")
        return LeaseInfo(
            epoch=int(doc["epoch"]),
            holder=doc.get("holder"),
            deadline_at=None if deadline is None else Timestamp.from_iso(deadline),
            released=doc.get("holder") is None,
            state=doc.get("state"),
        )


class CairnDispatch:
    def __init__(self, db: CairnDB) -> None:
        self._db = db

    async def claim_start(self, key: str, eid: Eid) -> tuple[bool, Eid]:
        result = await self._db.claim(f"wf/dispatch/{key}", {"eid": str(eid)})
        return result.won, parse_eid(result.value["eid"])

    async def claim(self, key: str, value: Any) -> tuple[bool, Any]:
        result = await self._db.claim(f"wf/claims/{key}", value)
        return result.won, result.value


# --- queue -------------------------------------------------------------------------


class CairnQueue:
    def __init__(self, db: CairnDB, now: Now) -> None:
        self._db = db
        self._now = now

    @staticmethod
    def _base(queue: str) -> str:
        return f"wf/queues/{check_queue(queue)}"

    def _task_key(self, task: Task, visible_at: Timestamp) -> str:
        return f"{self._base(task.queue)}/t/{visible_at.to_iso()}-{task.task_id}"

    @staticmethod
    def _lease_key(task_key: str) -> str:
        return task_key.replace("/t/", "/l/", 1)

    def _id_key(self, task: Task) -> str:
        return f"{self._base(task.queue)}/ids/{task.task_id}"

    def _fresh_key(self, task: Task) -> str:
        """The key this task takes if this call is the one that places it."""
        return self._task_key(task, task.visible_at(self._now()))

    async def _write_task(self, task: Task, key: str) -> bool:
        """Place the task at `key`. True when this call is the one that created it."""
        put = await self._db.objects.put(key, _dumps(unstructure(task)), if_absent=True)
        return put is not None

    async def _claim_key(self, task: Task) -> str:
        """The one key this task id occupies.

        The marker is the rendezvous. Whoever writes it names the key, and
        everyone else converges on that name, so two writers of one task id
        aim at one key and put-if-absent on the task settles which of them
        creates it.
        """
        mine = self._fresh_key(task)
        id_key = self._id_key(task)
        while True:
            if await self._db.objects.put(id_key, mine.encode(), if_absent=True) is not None:
                return mine
            marker = await self._db.objects.get(id_key)
            if marker is None:
                continue  # acked between the put and the read: the id is free again
            if marker.data:
                return marker.data.decode()
            # A marker from before markers named their key. Adopt it under CAS,
            # so that two repairs of one stranded id still agree on one key.
            if await self._db.objects.put(id_key, mine.encode(), if_match=marker.etag) is not None:
                return mine

    async def enqueue(self, task: Task) -> bool:
        key = self._fresh_key(task)
        if await self._db.objects.put(self._id_key(task), key.encode(), if_absent=True) is None:
            return False
        return await self._write_task(task, key)

    async def ensure(self, task: Task) -> bool:
        """Write the task when nothing answers to its id. Repair paths only.

        `enqueue` writes the marker, then the task, so a crash between the two
        leaves a marker that refuses every later enqueue of that id. This puts
        the task back, at the key the marker names: a concurrent `enqueue` and
        this call therefore converge on one object rather than queue the task
        twice, and put-if-absent decides which of them created it. It stays
        marker-first itself, so its own interruption is repaired by the next
        call. It costs one read, where `enqueue` costs none.
        """
        return await self._write_task(task, await self._claim_key(task))

    async def dequeue(self, queue: str, worker: str, ttl: float) -> ClaimedTask | None:
        prefix = f"{self._base(queue)}/t/"
        now_iso = self._now().to_iso()
        for key in await self._db.objects.list(prefix):
            visible_iso = key[len(prefix) :][:ISO_WIDTH]
            if visible_iso > now_iso:
                break  # keys sort by visible time  # pragma: no mutate
            lease = await _acquire(self._db, self._lease_key(key), worker, ttl)
            if lease is None:
                continue
            obj = await self._db.objects.get(key)
            if obj is None:  # acked between the list and the lease
                await self._drop_stale_lease(lease, key)
                continue
            return ClaimedTask(task=structure(_loads(obj.data), Task), key=key, lease=lease)
        return None

    async def _drop_stale_lease(self, lease: CairnLease, key: str) -> None:
        """The task is gone: the acker may have deleted our lease document already."""
        with suppress(LeaseLost):
            await lease.release()
        await self._db.objects.delete(self._lease_key(key))

    async def take(self, queue: str, task_id: str, worker: str, ttl: float) -> ClaimedTask | None:
        prefix = f"{self._base(queue)}/t/"
        now_iso = self._now().to_iso()
        suffix = f"-{task_id}"
        for key in await self._db.objects.list(prefix):
            if not key.endswith(suffix):
                continue
            if key[len(prefix) :][:ISO_WIDTH] > now_iso:
                return None
            lease = await _acquire(self._db, self._lease_key(key), worker, ttl)
            if lease is None:
                return None
            obj = await self._db.objects.get(key)
            if obj is None:
                await self._drop_stale_lease(lease, key)
                return None
            return ClaimedTask(task=structure(_loads(obj.data), Task), key=key, lease=lease)
        return None

    async def ack(self, claimed: ClaimedTask) -> None:
        await claimed.lease.release()
        await self._db.objects.delete(claimed.key)
        await self._db.objects.delete(self._id_key(claimed.task))
        await self._db.objects.delete(self._lease_key(claimed.key))

    async def nack(self, claimed: ClaimedTask, delay: timedelta) -> None:
        task = replace(claimed.task, not_before=self._now() + delay)
        key = self._fresh_key(task)  # marker stays: same task_id, new visible time
        await self._write_task(task, key)
        # Repoint the marker before the old key goes, so that an `ensure` in
        # between always reads the name of a key that exists.
        await self._db.objects.put(self._id_key(task), key.encode())
        await claimed.lease.release()
        await self._db.objects.delete(claimed.key)
        await self._db.objects.delete(self._lease_key(claimed.key))

    async def pending(self, queue: str, limit: int = 100) -> list[Task]:
        prefix = f"{self._base(queue)}/t/"
        out: list[Task] = []
        for key in await self._db.objects.list(prefix):
            if len(out) >= limit:
                break
            obj = await self._db.objects.get(key)
            if obj is not None:
                out.append(structure(_loads(obj.data), Task))
        return out

    async def peek(self, queue: str, task_id: str) -> Task | None:
        key = await self._key_of(queue, task_id)
        if key is None:
            return None
        obj = await self._db.objects.get(key)
        return None if obj is None else structure(_loads(obj.data), Task)

    async def attach(self, queue: str, task_id: str, holder: str, ttl: float) -> ClaimedTask | None:
        key = await self._key_of(queue, task_id)
        if key is None:
            return None
        inner = await self._db.attach_lease(self._lease_key(key), holder=holder, ttl=ttl)
        if inner is None:
            return None
        obj = await self._db.objects.get(key)
        if obj is None:
            return None
        return ClaimedTask(task=structure(_loads(obj.data), Task), key=key, lease=CairnLease(inner))

    async def request_cancel(self, queue: str, task_id: str, by: Provenance) -> None:
        key = await self._key_of(queue, task_id)
        if key is None:
            return
        await self._db.cooperative_write(
            self._lease_key(key), lambda s: {**(s or {}), "cancel_requested": unstructure(by)}
        )

    async def _key_of(self, queue: str, task_id: str) -> str | None:
        prefix = f"{self._base(queue)}/t/"
        suffix = f"-{task_id}"
        for key in await self._db.objects.list(prefix):
            if key.endswith(suffix):
                return key
        return None

    async def depth(self, queue: str) -> QueueDepth:
        base = self._base(queue)
        prefix = f"{base}/t/"
        now_iso = self._now().to_iso()
        keys = await self._db.objects.list(prefix)
        # A lease document of a dead holder stays until a steal or an ack, so
        # `claimed` is an upper bound. See specs/03-ports.md section 7.
        leases = await self._db.objects.list(f"{base}/l/")
        return QueueDepth(
            total=len(keys),
            visible=sum(1 for k in keys if k[len(prefix) :][:ISO_WIDTH] <= now_iso),
            claimed=len(leases),
        )


class CairnEvidence:
    """Evidence as plain objects under `wf/evidence/`. Nothing here is authority."""

    def __init__(self, db: CairnDB) -> None:
        self._db = db

    async def _ensure_meta(self, ref: EvidenceRef) -> None:
        # `if_absent` only ever matters for the FIRST writer: meta_bytes(ref) is a
        # pure function of ref, so a second writer's put-if-absent and an
        # unconditional put would land byte-identical content either way.
        await self._db.objects.put(
            keys.meta_key(ref), keys.meta_bytes(ref), if_absent=True
        )  # pragma: no mutate

    async def append_log(self, ref: EvidenceRef, part: bytes) -> None:
        await self._ensure_meta(ref)
        prefix = f"{keys.base(ref)}/{keys.LOG_NAME}."
        n = len(await self._db.objects.list(prefix))
        # A part key is put-if-absent, so two flushes never overwrite one another.
        for candidate in range(n, n + 8):
            if (
                await self._db.objects.put(keys.log_key(ref, candidate), part, if_absent=True)
                is not None
            ):
                return
        raise RuntimeError(f"evidence log of {ref.eid} {ref.fid} could not settle")

    async def read_log(self, ref: EvidenceRef) -> bytes:
        prefix = f"{keys.base(ref)}/{keys.LOG_NAME}."
        parts = []
        for key in sorted(await self._db.objects.list(prefix)):
            obj = await self._db.objects.get(key)
            if obj is not None:
                parts.append(obj.data)
        return b"".join(parts)

    async def put(self, ref: EvidenceRef, name: str, data: bytes, media_type: str) -> str:
        await self._ensure_meta(ref)
        key = keys.attachment_key(ref, name)
        await self._db.objects.put(key, data)
        await self._db.objects.put(keys.media_type_key(ref, name), media_type.encode())
        return key

    async def get(self, ref: EvidenceRef, name: str) -> bytes | None:
        if name == keys.LOG_NAME:
            log = await self.read_log(ref)
            return log or None
        obj = await self._db.objects.get(keys.attachment_key(ref, name))
        return None if obj is None else obj.data

    async def list(self, eid: Eid) -> list[EvidenceItem]:
        prefix = keys.eid_prefix(eid)
        all_keys = sorted(await self._db.objects.list(prefix))
        items: list[EvidenceItem] = []
        for meta_key in [k for k in all_keys if k.endswith("/meta")]:
            obj = await self._db.objects.get(meta_key)
            if obj is None:
                continue
            ref = keys.ref_from_meta(eid, obj.data)
            frame_base = meta_key.rsplit("/", 1)[0]
            size = 0
            for key in [k for k in all_keys if k.startswith(f"{frame_base}/{keys.LOG_NAME}.")]:
                part = await self._db.objects.get(key)
                size += 0 if part is None else len(part.data)
            if size:
                items.append(EvidenceItem(ref, keys.LOG_NAME, keys.NDJSON, size))
            for key in [k for k in all_keys if k.startswith(f"{frame_base}/a/")]:
                obj = await self._db.objects.get(key)
                if obj is not None:
                    name = key.rsplit("/", 1)[1]
                    mt_obj = await self._db.objects.get(keys.media_type_key(ref, name))
                    media_type = (
                        "application/octet-stream" if mt_obj is None else mt_obj.data.decode()
                    )
                    items.append(EvidenceItem(ref, name, media_type, len(obj.data)))
        return items

    async def delete_for(self, eid: Eid) -> int:
        gone = await self._db.objects.list(keys.eid_prefix(eid))
        for key in gone:
            await self._db.objects.delete(key)
        return len(gone)


# --- channel -----------------------------------------------------------------------


class CairnChannel:
    def __init__(self, db: CairnDB, logs: _LogCache) -> None:
        self._db = db
        self._logs = logs

    @staticmethod
    def name(channel: str) -> str:
        return f"wf.ch.{check_channel(channel)}"

    async def send(self, message: Message) -> int:
        log = await self._logs.get(self.name(message.channel))
        body = unstructure(message)
        body.pop("seq")
        event = Event(
            event_type=EventType("message"),
            timestamp=message.sent_by.at,
            payload=body,
            schema_version=SchemaVersion(SCHEMA_VERSION),
        )
        return encode_seq(await log.append(event))

    async def read(self, channel: str, after: int = 0) -> list[Message]:  # pragma: no mutate
        log = await self._logs.get(self.name(channel))
        out: list[Message] = []
        # Commits start at 1 (see CairnJournal.tail), so the smallest real `seq`
        # is SEQ_BASE: shifting this window's lower bound by one commit, or the
        # `after=0` default by one, can only make it scan one more already-seen
        # commit that the `seq > after` filter below discards -- never change
        # what is returned.
        async for s in log.read(after=max(after // SEQ_BASE - 1, 0)):  # pragma: no mutate
            seq = encode_seq(s.sequence)
            if seq > after:
                out.append(structure({**s.event.payload, "seq": seq}, Message))
        return out

    @staticmethod
    def _wait_key(channel: str, eid: Eid) -> str:
        return f"wf/waits/{check_channel(channel)}/{eid}"

    async def register_wait(self, channel: str, ref: FrameRef) -> None:
        await self._db.objects.put(self._wait_key(channel, ref.eid), _dumps({"fid": ref.fid}))

    async def clear_wait(self, channel: str, ref: FrameRef) -> None:
        await self._db.objects.delete(self._wait_key(channel, ref.eid))

    async def waiters(self, channel: str) -> list[FrameRef]:
        return [ref for _, ref in await self._list_waits(f"wf/waits/{check_channel(channel)}/")]

    async def all_waits(self) -> list[tuple[str, FrameRef]]:
        return await self._list_waits("wf/waits/")

    async def scoped(self, eid: Eid) -> list[str]:
        prefix = f"logs/wf.ch.{parse_eid(eid)}."
        names: set[str] = set()
        for key in await self._db.objects.list(prefix):
            rest = key[len("logs/wf.ch.") :]
            # Only [0] is kept, so any maxsplit >= 1 yields the same first
            # segment -- the value of the limit itself is unobservable here.
            names.add(rest.split("/", 1)[0])  # pragma: no mutate
        return sorted(names)

    async def delete_channel(self, channel: str) -> None:
        await self._logs.drop(self.name(channel))

    async def _list_waits(self, prefix: str) -> list[tuple[str, FrameRef]]:
        out: list[tuple[str, FrameRef]] = []
        for key in await self._db.objects.list(prefix):
            # A wait key is always exactly "wf/waits/{channel}/{eid}" (3 slashes;
            # channel and eid never contain one), so any maxsplit >= 3, in
            # either direction, yields this same 4-way split.
            _, _, channel, eid = key.split("/", 3)  # pragma: no mutate
            obj = await self._db.objects.get(key)
            if obj is not None:
                out.append((channel, FrameRef(parse_eid(eid), _loads(obj.data)["fid"])))
        return out


# --- timers, executions ----------------------------------------------------------------


class CairnTimers:
    PREFIX = "wf/timers/"

    def __init__(self, db: CairnDB) -> None:
        self._db = db

    async def schedule(self, timer: Timer) -> None:
        # `timer_id` is derived from `(due_at, target)` (see Timer's docstring):
        # a second `schedule` of the same id always serializes to the same
        # bytes, so `if_absent` vs. an unconditional put is unobservable.
        key = self.PREFIX + timer.timer_id
        data = _dumps(unstructure(timer))
        await self._db.objects.put(key, data, if_absent=True)  # pragma: no mutate

    async def due(self, now: Timestamp) -> list[Timer]:
        now_iso = now.to_iso()
        out: list[Timer] = []
        for key in await self._db.objects.list(self.PREFIX):
            if key[len(self.PREFIX) :][:ISO_WIDTH] > now_iso:
                break
            obj = await self._db.objects.get(key)
            if obj is not None:
                out.append(structure(_loads(obj.data), Timer))
        return out

    async def remove(self, timer: Timer) -> None:
        await self._db.objects.delete(self.PREFIX + timer.timer_id)

    async def remove_for(self, eid: Eid) -> int:
        marker = f"-{parse_eid(eid)}-"
        n = 0
        for key in await self._db.objects.list(self.PREFIX):
            if marker in key[len(self.PREFIX) :]:
                await self._db.objects.delete(key)
                n += 1
        return n


class CairnExecutionStore:
    def __init__(self, db: CairnDB) -> None:
        self._db = db

    @staticmethod
    def key(eid: Eid) -> str:
        return f"wf/exec/{parse_eid(eid)}/meta"

    async def create(self, execution: Execution) -> bool:
        etag = await self._db.objects.put(
            self.key(execution.eid), _dumps(unstructure(execution)), if_absent=True
        )
        return etag is not None

    async def read(self, eid: Eid) -> Execution | None:
        obj = await self._db.objects.get(self.key(eid))
        return None if obj is None else structure(_loads(obj.data), Execution)

    async def replace(self, eid: Eid, fn: Callable[[Execution], Execution]) -> Execution | None:
        key = self.key(eid)
        for _ in range(8):
            obj = await self._db.objects.get(key)
            if obj is None:
                return None
            new = fn(structure(_loads(obj.data), Execution))
            if await self._db.objects.put(key, _dumps(unstructure(new)), if_match=obj.etag):
                return new
        raise RuntimeError(f"execution record {eid} kept changing under replace")

    async def delete(self, eid: Eid) -> None:
        await self._db.objects.delete(self.key(eid))


class CairnArchive:
    PREFIX = "wf/archive/"

    def __init__(self, db: CairnDB) -> None:
        self._db = db

    def key(self, eid: Eid) -> str:
        return f"{self.PREFIX}{parse_eid(eid)}.msgpack"

    async def write(self, eid: Eid, data: dict[str, Any]) -> bool:
        packed: bytes = msgpack.packb(  # type: ignore[no-untyped-call]
            data, use_bin_type=True, default=_json_default
        )
        return await self._db.objects.put(self.key(eid), packed, if_absent=True) is not None

    async def read(self, eid: Eid) -> dict[str, Any] | None:
        obj = await self._db.objects.get(self.key(eid))
        if obj is None:
            return None
        # `raw` only changes how msgpack's legacy combined str/bytes type
        # decodes; `write` always packs with `use_bin_type=True`, which never
        # produces that legacy type, so `raw`'s value has no effect here.
        data = msgpack.unpackb(obj.data, raw=False)  # type: ignore[attr-defined]  # pragma: no mutate
        return dict(data)


# --- backend -------------------------------------------------------------------------------


class CairnBackend:
    """All ports over one CairnDB engine."""

    def __init__(self, db: CairnDB, *, now: Now = Timestamp.now, log_cache_size: int = 128) -> None:
        self.db = db
        self.logs = _LogCache(db, log_cache_size)
        self.journal = CairnJournal(self.logs)
        self.control = CairnControlLog(self.logs)
        self.ownership = CairnOwnership(db)
        self.dispatch = CairnDispatch(db)
        self.queue = CairnQueue(db, now)
        self.channel = CairnChannel(db, self.logs)
        self.timers = CairnTimers(db)
        self.executions = CairnExecutionStore(db)
        self.archive = CairnArchive(db)
        self.evidence = CairnEvidence(db)

    @classmethod
    def configure(cls, config: dict[str, Any], **kwargs: Any) -> CairnBackend:
        return cls(CairnDB.configure(config), **kwargs)

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

    def projection(self, **kwargs: Any) -> Any:
        """The SQLite projection of the control log. See cairndb_projection.py."""
        from .cairndb_projection import WorkflowProjection

        return WorkflowProjection(self.db, **kwargs)

    async def close(self) -> None:
        await self.logs.close()
        await self.db.close()

    async def __aenter__(self) -> CairnBackend:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


__all__ = ["SEQ_BASE", "CairnBackend", "CairnLease", "encode_seq"]
