"""Durable run history as a CairnDB event-sourced projection.

Archived runs are immutable facts, which makes them a natural fit for a
CairnDB commit log: the sweeper records a ``run.archived`` event just
before a closed run's state leaves the active prefix, and readers project
the log into a durable SQLite ``runs`` table that answers long-horizon
dashboard queries without rescanning state objects.

This is strictly additive to the control plane: leases, the worker state
machine, and the pull-based cache are untouched.  The history log is the
CairnDB **named log** ``history`` (keys under ``logs/history/``) on the
same store as everything else, so it never collides with the ``runs/``
and ``state/`` planes.
"""
import asyncio
import json
import logging

from attrs import define
from cairndb import Event, EventType, SchemaVersion
from cairndb import Timestamp as CairnTimestamp
from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.engine.logs import Log
from cairndb.storage.base import BlobStorage

from .models import RunState
from .serdes import to_json

logger = logging.getLogger(__name__)

RUN_ARCHIVED = "run.archived"
HISTORY_SCHEMA_VERSION = "1.0.0"
HISTORY_LOG_NAME = "history"


# region @history
# ---
# role: storage
# intent: record archived runs to the cairndb history log and project them into SQLite
# description: >
#   RunHistory appends one run.archived event per archived run to the
#   cairndb named log "history" (group-committed, durable once record_many
#   returns).  history_registry projects those events into a flat runs
#   table via INSERT OR REPLACE keyed on run_id, so re-recording a run —
#   possible when a sweep crashes between recording and archiving — is
#   idempotent.  refresh_history_db() replays only the log tail using the
#   last-applied sequence cairndb tracks inside the projection database.
# rules:
#   - Events MUST be recorded before the state file is archived, never after.
#   - The projection handler MUST be idempotent per run_id (INSERT OR REPLACE).
#   - MUST NOT be used for live run state; the control plane owns liveness.
# dependencies:
#   - models.run
#   - serdes.json
# aliases:
#   - run-history
#   - history-projection
# triggers:
#   - where are archived runs recorded
#   - how to query old runs
#   - run history dashboard
# ---


history_registry = HandlerRegistry()
"""Handlers projecting history events into the SQLite ``runs`` table."""


async def init_history_schema(db_path: str) -> None:
    """Create the history projection tables on a fresh database.

    Args:
        db_path: Path of the SQLite projection database.
    """
    import aiosqlite

    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                flow_name TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                started_at TEXT,
                ended_at TEXT,
                attempt INTEGER,
                max_retries INTEGER,
                state_json TEXT NOT NULL
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_flow ON runs (flow_name, ended_at)"
        )
        await db.commit()


@history_registry.handler(RUN_ARCHIVED)
async def _handle_run_archived(db, entry) -> None:
    """Upsert one archived run; idempotent per run_id."""
    payload = entry.payload
    await db.execute(
        """
        INSERT OR REPLACE INTO runs
            (run_id, flow_name, status, worker_id, started_at, ended_at,
             attempt, max_retries, state_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["run_id"],
            payload["flow_name"],
            payload["status"],
            payload.get("worker_id"),
            payload.get("started_at"),
            payload.get("ended_at"),
            payload.get("attempt"),
            payload.get("max_retries"),
            json.dumps(payload),
        ),
    )


def _archived_event(state: RunState) -> Event:
    """Build the run.archived event carrying the full serialized state."""
    return Event(
        event_type=EventType(RUN_ARCHIVED),
        timestamp=CairnTimestamp.now(),
        payload=json.loads(to_json(state)),
        schema_version=SchemaVersion(HISTORY_SCHEMA_VERSION),
    )


@define(slots=True, kw_only=True)
class RunHistory:
    """Writer for the run-history event log.

    Synchronous façade over the cairndb named log so the (synchronous)
    sweeper can call it directly.  All events of one call are group-committed;
    when :meth:`record_many` returns they are durably in the log.

    Attributes:
        store: The framework's blob store; events land on the named log
            ``history`` (keys under ``logs/history/``).
    """

    store: BlobStorage

    def record_many(self, states: list[RunState]) -> None:
        """Durably append one run.archived event per state.

        Args:
            states: Closed runs about to be archived.

        Raises:
            Exception: Whatever the committer raises when the log cannot be
                written; callers must then *not* archive the states, so the
                runs are re-recorded on the next sweep (idempotent).
        """
        if not states:
            return
        asyncio.run(self._record(states))
        logger.info("history_recorded", extra={"count": len(states)})

    async def _record(self, states: list[RunState]) -> None:
        log = Log(self.store, HISTORY_LOG_NAME)
        try:
            await log.append_many([_archived_event(s) for s in states])
        finally:
            await log.close()


async def refresh_history_db(store: BlobStorage, db_path: str) -> None:
    """Bring a local history projection up to date with the event log.

    Creates the schema on first use, then replays only the commits after the
    last applied sequence recorded inside the projection database.  Safe to
    call repeatedly and from a cron job; the projection file is a disposable
    cache, fully rebuildable from the log.

    Args:
        store: The framework's blob store (same value the sweeper writes to).
        db_path: Path of the SQLite projection database.
    """
    await init_history_schema(db_path)
    engine = ReplayEngine(Log(store, HISTORY_LOG_NAME).storage, history_registry)
    await engine.initialize_metadata_table(db_path)
    last = await engine.get_last_applied_sequence(db_path)
    await engine.replay(db_path, after=last.commit if last else 0)


# ---
# endregion
