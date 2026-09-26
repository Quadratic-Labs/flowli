"""SQLite projection of the control log, on CairnDB's replay machinery.

Tables:

    executions     one row per execution, status from the last lifecycle entry
    tasks          one row per task.enqueued entry (audit)
    announcements  one row per announce.* entry (generic)
    reviews        folded from announce.review.requested / .decided / .expired

The projection is derived, read-only state. `refresh()` catches up now, `start()`
polls in the background, `wait_for(seq)` gives read-your-writes for an announce
sequence returned by the ControlLog port.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiosqlite
from cairndb import CairnDB, SequencedEvent, SequenceNumber, Timestamp
from cairndb.client.config import ClientConfig
from cairndb.client.registry import HandlerRegistry
from cairndb.client.updater import BackgroundUpdater
from cairndb.engine.logs import Log

from flowli.domain import (
    ANNOUNCE_PREFIX,
    Eid,
    EntryType,
    ExecutionStatus,
    parse_eid,
)
from flowli.runtime.sweeper import KnownExecution

from .cairndb import SEQ_BASE, CairnControlLog, encode_seq

Handler = Callable[[aiosqlite.Connection, SequencedEvent], Awaitable[None]]

_STATUS_OF: dict[str, ExecutionStatus] = {
    EntryType.EXECUTION_CREATED: ExecutionStatus.PENDING,
    EntryType.EXECUTION_STARTED: ExecutionStatus.RUNNING,
    EntryType.EXECUTION_RESUMED: ExecutionStatus.RUNNING,
    EntryType.EXECUTION_SUSPENDED: ExecutionStatus.SUSPENDED,
    EntryType.EXECUTION_COMPLETED: ExecutionStatus.COMPLETED,
    EntryType.EXECUTION_FAILED: ExecutionStatus.FAILED,
    EntryType.EXECUTION_CANCELLED: ExecutionStatus.CANCELLED,
}
_TERMINAL = {s.value for s in ExecutionStatus if s.is_terminal}

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    eid TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    parent_eid TEXT,
    parent_fid TEXT,
    dispatch_key TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_type TEXT NOT NULL,
    last_seq TEXT NOT NULL,
    epoch INTEGER,
    suspended_on TEXT,
    result TEXT,
    error_type TEXT,
    error_message TEXT,
    created_by_kind TEXT,
    created_by_id TEXT,
    worker_id TEXT,
    host TEXT,
    archived_at TEXT
);
CREATE INDEX IF NOT EXISTS executions_status ON executions(status, updated_at);
CREATE INDEX IF NOT EXISTS executions_parent ON executions(parent_eid);
CREATE INDEX IF NOT EXISTS executions_workflow ON executions(workflow, version);

CREATE TABLE IF NOT EXISTS tasks (
    seq TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    eid TEXT NOT NULL,
    fid TEXT NOT NULL,
    reason TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    actor_kind TEXT,
    actor_id TEXT
);
CREATE INDEX IF NOT EXISTS tasks_eid ON tasks(eid);

CREATE TABLE IF NOT EXISTS announcements (
    seq TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    eid TEXT,
    fid TEXT,
    at TEXT NOT NULL,
    actor_kind TEXT,
    actor_id TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS announcements_kind ON announcements(kind, at);
CREATE INDEX IF NOT EXISTS announcements_eid ON announcements(eid);

CREATE TABLE IF NOT EXISTS reviews (
    rid TEXT PRIMARY KEY,
    eid TEXT NOT NULL,
    queue TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT,
    deadline TEXT,
    requested_at TEXT NOT NULL,
    decided_at TEXT,
    verdict TEXT,
    decided_by TEXT
);
CREATE INDEX IF NOT EXISTS reviews_queue_status ON reviews(queue, status);
"""


async def init_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(SCHEMA)
        await conn.commit()


class PrefixRegistry(HandlerRegistry):
    """A HandlerRegistry that also matches event types by prefix."""

    def __init__(self) -> None:
        super().__init__()  # type: ignore[no-untyped-call]
        self._prefixes: list[tuple[str, Handler]] = []

    def register_prefix(self, prefix: str, handler: Handler) -> None:
        self._prefixes.append((prefix, handler))

    def has_handler(self, event_type: str) -> bool:
        return super().has_handler(event_type) or any(
            event_type.startswith(p) for p, _ in self._prefixes
        )

    def get_handler(self, event_type: str) -> Handler:
        if super().has_handler(event_type):
            return super().get_handler(event_type)
        for prefix, handler in self._prefixes:
            if event_type.startswith(prefix):
                return handler
        return super().get_handler(event_type)


# --- rows -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExecutionRow:
    eid: Eid
    workflow: str
    version: str
    status: ExecutionStatus
    queue: str
    parent_eid: str | None
    parent_fid: str | None
    dispatch_key: str | None
    created_at: str
    updated_at: str
    last_type: str
    epoch: int | None
    suspended_on: list[str] | None
    result: Any
    error_type: str | None
    error_message: str | None
    created_by_kind: str | None
    created_by_id: str | None
    worker_id: str | None
    host: str | None
    archived_at: str | None = None

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> ExecutionRow:  # pragma: no mutate block
        return cls(
            eid=parse_eid(r["eid"]),
            workflow=r["workflow"],
            version=r["version"],
            status=ExecutionStatus(r["status"]),
            queue=r["queue"],
            parent_eid=r["parent_eid"],
            parent_fid=r["parent_fid"],
            dispatch_key=r["dispatch_key"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
            last_type=r["last_type"],
            epoch=r["epoch"],
            suspended_on=None if r["suspended_on"] is None else json.loads(r["suspended_on"]),
            result=None if r["result"] is None else json.loads(r["result"]),
            error_type=r["error_type"],
            error_message=r["error_message"],
            created_by_kind=r["created_by_kind"],
            created_by_id=r["created_by_id"],
            worker_id=r["worker_id"],
            host=r["host"],
            archived_at=r["archived_at"],
        )


@dataclass(frozen=True, slots=True)
class ReviewRow:
    rid: str
    eid: Eid | None
    queue: str
    status: str  # pending | decided | expired
    payload: Any
    deadline: str | None
    requested_at: str
    decided_at: str | None
    verdict: str | None
    decided_by: dict[str, Any] | None

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> ReviewRow:  # pragma: no mutate block
        return cls(
            rid=r["rid"],
            eid=parse_eid(r["eid"]) if r["eid"] else None,
            queue=r["queue"],
            status=r["status"],
            payload=None if r["payload"] is None else json.loads(r["payload"]),
            deadline=r["deadline"],
            requested_at=r["requested_at"],
            decided_at=r["decided_at"],
            verdict=r["verdict"],
            decided_by=None if r["decided_by"] is None else json.loads(r["decided_by"]),
        )


@dataclass(frozen=True, slots=True)
class TaskRow:
    """One `task.enqueued` entry. An ack leaves no entry, so this is an audit
    of enqueues and never a count of what is on the queue now."""

    seq: str
    task_id: str
    queue: str
    kind: str
    eid: str | None
    fid: str | None
    reason: str | None
    enqueued_at: str
    actor_kind: str | None
    actor_id: str | None

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> TaskRow:  # pragma: no mutate block
        return cls(
            seq=r["seq"],
            task_id=r["task_id"],
            queue=r["queue"],
            kind=r["kind"],
            eid=r["eid"],
            fid=r["fid"],
            reason=r["reason"],
            enqueued_at=r["enqueued_at"],
            actor_kind=r["actor_kind"],
            actor_id=r["actor_id"],
        )


@dataclass(frozen=True, slots=True)
class AnnouncementRow:
    seq: str
    kind: str
    eid: str | None
    fid: str | None
    at: str
    actor_kind: str | None
    actor_id: str | None
    payload: dict[str, Any]

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> AnnouncementRow:  # pragma: no mutate block
        return cls(
            seq=r["seq"],
            kind=r["kind"],
            eid=r["eid"],
            fid=r["fid"],
            at=r["at"],
            actor_kind=r["actor_kind"],
            actor_id=r["actor_id"],
            payload=json.loads(r["payload"]),
        )


# --- projection --------------------------------------------------------------------


def _prov(entry: SequencedEvent) -> dict[str, Any]:
    meta = entry.event.metadata or {}
    return dict(meta.get("provenance") or {})


class WorkflowProjection:
    NAME = "wf_view"

    def __init__(
        self,
        db: CairnDB,
        *,
        db_path: str | None = None,
        version: str = "1",
        poll_interval: float = 5.0,
    ) -> None:
        storage = Log(db.storage, CairnControlLog.NAME).storage
        self.config = ClientConfig(
            db_path=db_path or f"./{self.NAME}.v{version}.sqlite",
            schema_version=version,
            poll_interval_seconds=poll_interval,
        )
        self.registry = PrefixRegistry()
        self._register()
        self._updater = BackgroundUpdater(
            self.config, storage, self.registry, init_schema=init_schema
        )

    # -- lifecycle ------------------------------------------------------------------

    async def refresh(self) -> int | None:
        await self._updater.trigger_update()
        current = await self._updater.projector.get_current_sequence()
        return None if current is None else encode_seq(current)

    async def start(self) -> None:
        await self._updater.start()

    async def stop(self) -> None:
        await self._updater.stop()

    async def wait_for(self, seq: int, timeout: float = 30.0) -> bool:
        target = SequenceNumber(commit=seq // SEQ_BASE, index=seq % SEQ_BASE)
        return bool(await self._updater.wait_for_sequence(str(target), timeout))

    @property
    def path(self) -> str:
        return self.config.db_path

    @property
    def ready(self) -> bool:
        """False until the first refresh wrote the file.

        A control log with no commit yet leaves no file, and a read-only
        open of a file that is not there fails. A projection with no file
        has seen nothing, so every query answers empty.
        """
        return os.path.exists(self.path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    # -- handlers --------------------------------------------------------------------

    def _register(self) -> None:
        self.registry.register(EntryType.EXECUTION_CREATED, self._on_created)
        for t in (
            EntryType.EXECUTION_STARTED,
            EntryType.EXECUTION_RESUMED,
            EntryType.EXECUTION_SUSPENDED,
            EntryType.EXECUTION_COMPLETED,
            EntryType.EXECUTION_FAILED,
            EntryType.EXECUTION_CANCELLED,
        ):
            self.registry.register(t, self._on_lifecycle)
        self.registry.register(EntryType.TASK_ENQUEUED, self._on_task)
        self.registry.register(EntryType.EXECUTION_MIGRATED, self._on_migrated)
        self.registry.register(EntryType.EXECUTION_ARCHIVED, self._on_archived)
        self.registry.register_prefix(ANNOUNCE_PREFIX, self._on_announce)

    async def _on_migrated(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        p = entry.payload
        sql = "UPDATE executions SET version = ?, updated_at = ? WHERE eid = ?"  # pragma: no mutate
        at = _prov(entry).get("at", "")
        await conn.execute(sql, (p["version"], at, p["eid"]))

    async def _on_archived(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        p = entry.payload
        sql = "UPDATE executions SET archived_at = ? WHERE eid = ?"  # pragma: no mutate
        at = _prov(entry).get("at", "")
        await conn.execute(sql, (at, p["eid"]))

    async def _on_created(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        p = entry.payload
        prov = _prov(entry)
        actor = prov.get("actor") or {}
        parent = p.get("parent") or {}
        await conn.execute(
            """
            INSERT INTO executions (eid, workflow, version, status, queue, parent_eid, parent_fid,
                dispatch_key, created_at, updated_at, last_type, last_seq,
                created_by_kind, created_by_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(eid) DO NOTHING
            """,
            (
                p["eid"],
                p["workflow"],
                p["version"],
                ExecutionStatus.PENDING.value,
                p.get("queue", "default"),
                parent.get("eid"),
                parent.get("fid"),
                p.get("dispatch_key"),
                prov.get("at", ""),
                prov.get("at", ""),
                entry.event_type,
                str(entry.sequence),
                actor.get("kind"),
                actor.get("id"),
            ),
        )

    async def _on_lifecycle(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        p = entry.payload
        eid = p["eid"]
        etype = str(entry.event_type)
        status = _STATUS_OF[etype]
        prov = _prov(entry)
        site = prov.get("site") or {}
        code = prov.get("code") or {}
        workflow = code.get("workflow", "-")  # pragma: no mutate
        version = code.get("version", "-")  # pragma: no mutate
        at = prov.get("at", "")  # pragma: no mutate
        completed = etype == EntryType.EXECUTION_COMPLETED.value
        suspended = etype == EntryType.EXECUTION_SUSPENDED.value
        failed = etype == EntryType.EXECUTION_FAILED.value
        result = json.dumps(p.get("value")) if completed else None
        suspended_on = json.dumps(p.get("on")) if suspended else None
        error_type = p.get("error_type") if failed else None  # pragma: no mutate
        error_message = p.get("message") if failed else None  # pragma: no mutate
        epoch = p.get("epoch", site.get("epoch"))  # pragma: no mutate
        worker_id = site.get("worker_id")
        host = site.get("host")
        # a row may be missing when the log was pruned before execution.created. The
        # fallback insert below carries the SAME fields the update would set, since a
        # freshly-inserted row already in a terminal status is invisible to that
        # update's own WHERE clause (it only ever touches non-terminal rows).
        await conn.execute(
            """
            INSERT INTO executions (eid, workflow, version, status, created_at, updated_at,
                last_type, last_seq, epoch, suspended_on, result, error_type, error_message,
                worker_id, host)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(eid) DO NOTHING
            """,
            (
                eid,
                workflow,
                version,
                status.value,
                at,
                at,
                etype,
                str(entry.sequence),
                epoch,
                suspended_on,
                result,
                error_type,
                error_message,
                worker_id,
                host,
            ),
        )
        await conn.execute(
            """
            UPDATE executions SET
                status = ?, updated_at = ?, last_type = ?, last_seq = ?,
                epoch = COALESCE(?, epoch),
                suspended_on = ?,
                result = COALESCE(?, result),
                error_type = COALESCE(?, error_type),
                error_message = COALESCE(?, error_message),
                worker_id = COALESCE(?, worker_id),
                host = COALESCE(?, host)
            WHERE eid = ? AND status NOT IN ('completed', 'failed', 'cancelled')
            """,
            (
                status.value,
                at,
                etype,
                str(entry.sequence),
                epoch,
                suspended_on,
                result,
                error_type,
                error_message,
                worker_id,
                host,
                eid,
            ),
        )

    async def _on_task(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        p = entry.payload
        prov = _prov(entry)
        actor = prov.get("actor") or {}
        target = p.get("target") or {}
        await conn.execute(
            """
            INSERT OR IGNORE INTO tasks (seq, task_id, queue, kind, eid, fid, reason, enqueued_at,
                actor_kind, actor_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(entry.sequence),
                p["task_id"],
                p["queue"],
                p["kind"],
                target.get("eid", ""),
                target.get("fid", ""),
                p.get("reason", ""),
                prov.get("at", ""),
                actor.get("kind"),
                actor.get("id"),
            ),
        )

    async def _on_announce(self, conn: aiosqlite.Connection, entry: SequencedEvent) -> None:
        kind = entry.event_type[len(ANNOUNCE_PREFIX) :]
        p = entry.payload
        prov = _prov(entry)
        actor = prov.get("actor") or {}
        meta = entry.event.metadata or {}
        eid = p.get("eid") or None
        await conn.execute(
            """
            INSERT OR IGNORE INTO announcements
                (seq, kind, eid, fid, at, actor_kind, actor_id, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(entry.sequence),
                kind,
                eid,
                meta.get("fid"),
                prov.get("at", ""),
                actor.get("kind"),
                actor.get("id"),
                json.dumps(p, sort_keys=True),
            ),
        )
        if kind == "review.requested":
            await conn.execute(
                """
                INSERT OR IGNORE INTO reviews
                    (rid, eid, queue, status, payload, deadline, requested_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    p["rid"],
                    p.get("eid") or "",
                    p.get("queue", "default"),
                    json.dumps(p.get("payload")),
                    p.get("deadline"),
                    prov.get("at", ""),
                ),
            )
        elif kind == "review.decided":
            by = p.get("by")
            await conn.execute(
                """
                UPDATE reviews SET status = 'decided', decided_at = ?, verdict = ?, decided_by = ?
                WHERE rid = ? AND status = 'pending'
                """,
                (
                    prov.get("at", ""),
                    p.get("verdict"),
                    None if by is None else json.dumps(by),
                    p["rid"],
                ),
            )
        elif kind == "review.expired":
            await conn.execute(
                "UPDATE reviews SET status = 'expired', decided_at = ? "
                "WHERE rid = ? AND status = 'pending'",
                (prov.get("at", ""), p["rid"]),
            )

    # -- queries -----------------------------------------------------------------------

    def execution(self, eid: Eid | str) -> ExecutionRow | None:
        if not self.ready:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM executions WHERE eid = ?", (str(eid),)
            ).fetchone()  # pragma: no mutate
        return None if row is None else ExecutionRow.from_row(row)

    def executions(
        self,
        *,
        status: ExecutionStatus | None = None,
        workflow: str | None = None,
        parent_eid: Eid | str | None = None,
        limit: int = 100,
        after: tuple[str, str] | None = None,
    ) -> list[ExecutionRow]:
        """Newest first. `after` is the (updated_at, eid) of the last row of the
        previous page: a keyset cursor over the same order."""
        if not self.ready:
            return []
        clauses, params = [], []
        if status is not None:
            clauses.append("status = ?")  # pragma: no mutate
            params.append(status.value)
        if workflow is not None:
            clauses.append("workflow = ?")  # pragma: no mutate
            params.append(workflow)
        if parent_eid is not None:
            clauses.append("parent_eid = ?")  # pragma: no mutate
            params.append(str(parent_eid))
        if after is not None:
            clauses.append("(updated_at < ? OR (updated_at = ? AND eid > ?))")  # pragma: no mutate
            params.extend([after[0], after[0], after[1]])
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM executions {where} ORDER BY updated_at DESC, eid LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [ExecutionRow.from_row(r) for r in rows]

    def children(self, eid: Eid | str) -> list[ExecutionRow]:
        return self.executions(parent_eid=eid, limit=10_000)

    def counts_by_status(self) -> dict[ExecutionStatus, int]:
        if not self.ready:
            return {}
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM executions GROUP BY status"
            ).fetchall()  # pragma: no mutate
        return {ExecutionStatus(r["status"]): r["n"] for r in rows}  # pragma: no mutate

    def tasks(
        self,
        *,
        eid: Eid | str | None = None,
        queue: str | None = None,
        kind: str | None = None,
        limit: int = 100,
    ) -> list[TaskRow]:
        """Enqueued tasks, newest first. See the note on `TaskRow`."""
        if not self.ready:
            return []
        clauses, params = [], []
        if eid is not None:
            clauses.append("eid = ?")  # pragma: no mutate
            params.append(str(eid))
        if queue is not None:
            clauses.append("queue = ?")  # pragma: no mutate
            params.append(queue)
        if kind is not None:
            clauses.append("kind = ?")  # pragma: no mutate
            params.append(kind)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""  # pragma: no mutate
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks {where} ORDER BY seq DESC LIMIT ?", (*params, limit)
            ).fetchall()
        return [TaskRow.from_row(r) for r in rows]

    def pending_reviews(self, queue: str | None = None) -> list[ReviewRow]:
        if not self.ready:
            return []
        sql = "SELECT * FROM reviews WHERE status = 'pending'"  # pragma: no mutate
        params: tuple[Any, ...] = ()
        if queue is not None:
            sql += " AND queue = ?"  # pragma: no mutate
            params = (queue,)
        with self.connect() as conn:
            rows = conn.execute(
                sql + " ORDER BY requested_at, rid", params
            ).fetchall()  # pragma: no mutate
        return [ReviewRow.from_row(r) for r in rows]

    def review(self, rid: str) -> ReviewRow | None:
        if not self.ready:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM reviews WHERE rid = ?", (rid,)
            ).fetchone()  # pragma: no mutate
        return None if row is None else ReviewRow.from_row(row)

    def announcements(
        self, *, kind: str | None = None, eid: Eid | str | None = None, limit: int = 100
    ) -> list[AnnouncementRow]:
        if not self.ready:
            return []
        clauses, params = [], []
        if kind is not None:
            clauses.append("kind = ?")  # pragma: no mutate
            params.append(kind)
        if eid is not None:
            clauses.append("eid = ?")  # pragma: no mutate
            params.append(str(eid))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""  # pragma: no mutate
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM announcements {where} ORDER BY seq LIMIT ?", (*params, limit)
            ).fetchall()
        return [AnnouncementRow.from_row(r) for r in rows]

    # -- sweeper source ----------------------------------------------------------------

    async def snapshot(self) -> dict[Eid, KnownExecution]:
        """ControlSource for the Sweeper: refresh, then the non-terminal executions."""
        await self.refresh()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM executions WHERE status NOT IN ('completed', 'failed', 'cancelled')"
            ).fetchall()  # pragma: no mutate
        return {
            parse_eid(r["eid"]): _known(ExecutionRow.from_row(r)) for r in rows
        }  # pragma: no mutate

    async def terminal_before(self, before: Timestamp) -> dict[Eid, KnownExecution]:
        """ControlSource for Retention: terminal, not archived, last entry older than `before`."""
        await self.refresh()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM executions
                WHERE status IN ('completed', 'failed', 'cancelled')
                  AND archived_at IS NULL AND updated_at < ?
                """,
                (before.to_iso(),),
            ).fetchall()
        return {
            parse_eid(r["eid"]): _known(ExecutionRow.from_row(r)) for r in rows
        }  # pragma: no mutate


def _known(row: ExecutionRow) -> KnownExecution:
    return KnownExecution(
        row.status,
        row.last_type,
        Timestamp.from_iso(row.updated_at),
        row.queue,
        archived=row.archived_at is not None,
    )


__all__ = [
    "SCHEMA",
    "AnnouncementRow",
    "ExecutionRow",
    "PrefixRegistry",
    "ReviewRow",
    "WorkflowProjection",
    "init_schema",
]
