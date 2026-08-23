"""Durable run history as a CairnDB event-sourced projection.

Archived runs are immutable facts, which makes them a natural fit for a
CairnDB commit log: the sweeper records a ``run.archived`` event just
before a closed run's state leaves the active prefix, and readers project
the log into a durable SQLite ``runs`` table that answers long-horizon
dashboard queries without rescanning state objects.

This is strictly additive to the control plane: leases and the worker
state machine are untouched.  CacheRepository's archived-run scan consults
it (get_states()) to avoid a blob GET per archived run on a cold seed, but
falls back to reading state.json directly for any run history it doesn't
have — the cache never depends on history being enabled.  The history log
is the CairnDB **named log** ``history`` (keys under ``logs/history/``) on
the same store as everything else, so it never collides with the ``runs/``
and ``state/`` planes.

History covers only the log plane: *active* run state stays in mutable
lease documents and is never logged, which is why live reads go through
the pull-based cache scan rather than a projection — see
``docs/architecture.md`` § "Why the pull cache scans storage" for the
full rationale.
"""
import asyncio
import json
import logging
import time
from collections.abc import Iterable
from uuid import UUID

from attrs import define, field
from cairndb import Event, EventType, SchemaVersion
from cairndb import Timestamp as CairnTimestamp
from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.engine.logs import Log
from cairndb.storage.base import BlobStorage

from flowlet.models import RunState
from flowlet.serdes import from_json, to_json

logger = logging.getLogger(__name__)

RUN_ARCHIVED = "run.archived"
HISTORY_SCHEMA_VERSION = "1.0.0"
HISTORY_LOG_NAME = "history"
DEFAULT_TTL_SECONDS = 5.0


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
    """Reader/writer for the run-history event log and its SQLite projection.

    Synchronous façade over the cairndb named log so the (synchronous)
    sweeper can call :meth:`record_many` directly.  All events of one call
    are group-committed; when it returns they are durably in the log.  The
    read side is async: :meth:`refresh` (TTL-throttled, mirrors
    ``CacheRepository``) replays the log tail into the local projection at
    ``db_path``, and :meth:`list_states` reads it back.

    Attributes:
        store: The framework's blob store; events land on the named log
            ``history`` (keys under ``logs/history/``).
        db_path: Local filesystem path for the SQLite projection.  Disposable
            — fully rebuildable by replaying the log from scratch — but
            persisting it between refreshes is what makes replay incremental.
        ttl: Minimum seconds between two projection replays.
    """

    store: BlobStorage
    db_path: str = "./flowlet_history.sqlite"
    ttl: float = DEFAULT_TTL_SECONDS
    _last_refresh: float = field(default=0.0, alias="_last_refresh")

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

    async def refresh(self, force: bool = False) -> None:
        """Bring the local projection up to date, at most once per TTL.

        Args:
            force: When True, replay regardless of the TTL.
        """
        now = time.monotonic()
        if not force and now - self._last_refresh < self.ttl:
            return
        self._last_refresh = now
        await refresh_history_db(self.store, self.db_path)

    async def known_flow_names(self) -> list[str]:
        """Distinct flow names present in the refreshed projection."""
        import aiosqlite

        await self.refresh()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT DISTINCT flow_name FROM runs")
            return [row[0] for row in await cursor.fetchall()]

    async def list_states(
        self, flow_names: list[str] | None = None, *, last_n: int = 5
    ) -> list[RunState]:
        """Fetch the most recent *last_n* archived RunStates for each flow.

        Refreshes the projection first (TTL-throttled).  Intended as an
        additive source RunQuery merges with the ephemeral cache — this is
        the read side of the "long-horizon" queries the history log exists
        for.

        Args:
            flow_names: Flows to include, or ``None`` for every flow the
                projection has ever recorded.
            last_n: Maximum number of rows to return per flow.

        Returns:
            RunState objects reconstructed from the projection, newest
            first within each flow.
        """
        import aiosqlite

        await self.refresh()
        names = flow_names if flow_names is not None else await self.known_flow_names()
        if not names:
            return []

        states: list[RunState] = []
        async with aiosqlite.connect(self.db_path) as db:
            for name in names:
                cursor = await db.execute(
                    "SELECT state_json FROM runs WHERE flow_name = ? "
                    "ORDER BY run_id DESC LIMIT ?",
                    (name, last_n),
                )
                states.extend(
                    from_json(RunState)(row[0]) for row in await cursor.fetchall()
                )
        return states

    async def get_states(self, run_ids: Iterable[UUID]) -> dict[UUID, RunState]:
        """Look up specific archived runs by ID from the local projection.

        Bulk point-lookup for CacheRepository's archived-run seeding: a
        handful of local SQLite reads instead of one blob GET per run.
        Refreshes first (TTL-throttled).

        Args:
            run_ids: Run IDs to look up.

        Returns:
            Mapping of the run_ids that were found to their RunState. A
            run_id absent from the projection — archived before history was
            enabled, or not yet replayed — is simply absent from the
            result; callers should fall back to reading its state.json
            directly.
        """
        import aiosqlite

        ids = list(run_ids)
        if not ids:
            return {}
        await self.refresh()
        placeholders = ",".join("?" for _ in ids)
        results: dict[UUID, RunState] = {}
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                f"SELECT run_id, state_json FROM runs WHERE run_id IN ({placeholders})",
                [str(rid) for rid in ids],
            )
            for run_id_str, state_json in await cursor.fetchall():
                results[UUID(run_id_str)] = from_json(RunState)(state_json)
        return results


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

