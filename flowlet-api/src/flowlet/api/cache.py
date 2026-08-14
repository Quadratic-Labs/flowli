"""
Pull-based run cache: an ephemeral SQLite index rebuilt from storage on demand.

Replaces the pubsub → subscriber → snapshot pipeline: the API never receives
events.  On each (TTL-throttled) refresh it scans

- ``state/`` — the small active-runs directory, always re-read fully, and
- ``runs/<flow>/<date>/<run_id>/state.json`` — archived terminal states,
  immutable, read once per run and remembered,

and upserts both into a local in-memory SQLite the query layer reads.  The
cache has no correctness role: it can be dropped and rebuilt from storage at
any time, which is what makes the API scale-to-zero safe.
"""
import logging
import time
from uuid import UUID

from attrs import Factory, define, field
from cairndb.storage.base import BlobStorage
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..models import RunState
from ..repository.state import StateRepository
from ..serdes import from_json
from .database import ensure_snapshot_schema, upsert_run_state

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 5.0


# region @cache.repository
# ---
# role: domain data access
# intent: TTL-refreshed local SQLite index over state files and archived runs
# description: >
#   CacheRepository owns the API's only SQLite engine (in-memory).  refresh()
#   is called by the query layer before reads: within the TTL it is a no-op;
#   otherwise it re-reads every active state file and any archived state.json
#   not yet seen, upserting them into the runs table.  Archived states are
#   immutable so they are ingested exactly once per process lifetime.
# rules:
#   - The cache MUST be reconstructible from storage alone (no correctness role).
#   - refresh() MUST be cheap within the TTL (single timestamp check).
#   - MUST only be used from a single event loop (the API's) — no threads.
#   - Archived state.json files MUST be treated as immutable.
# dependencies:
#   - state_repository
#   - database.snapshot
#   - database.models
#   - storage.keys
# aliases:
#   - run-cache
# triggers:
#   - how does the api see run states
#   - how is the dashboard data refreshed
# ---


@define(slots=False, kw_only=True)
class CacheRepository:
    """Ephemeral SQLite index over active and archived run states.

    Attributes:
        state_repo: Repository for the active ``state/`` directory.
        store: CairnDB blob store containing the ``runs/`` tree.
        ttl: Minimum seconds between two storage scans.
        engine: Async in-memory SQLite engine the query layer reads from.
    """

    state_repo: StateRepository
    store: BlobStorage
    ttl: float = DEFAULT_TTL_SECONDS
    engine: AsyncEngine = Factory(
        lambda: create_async_engine("sqlite+aiosqlite:///:memory:")
    )
    _last_refresh: float = field(default=0.0, alias="_last_refresh")
    _schema_ready: bool = field(default=False, alias="_schema_ready")
    _seen_archived: set[UUID] = field(factory=set, alias="_seen_archived")

    @classmethod
    def from_deps(cls, *, state_repo, configs, **_) -> "CacheRepository":
        """Construct from the configure() dependency dict.

        Args:
            state_repo: Active-state repository.
            configs: Application config providing the blob store.
            **_: Absorbs unused dependency keys.
        """
        return cls(state_repo=state_repo, store=configs.store)

    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Session factory bound to the cache engine."""
        return async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def refresh(self, force: bool = False) -> None:
        """Bring the cache up to date with storage, at most once per TTL.

        Args:
            force: When True, scan regardless of the TTL.
        """
        now = time.monotonic()
        if not force and now - self._last_refresh < self.ttl:
            return
        self._last_refresh = now

        if not self._schema_ready:
            await ensure_snapshot_schema(self.engine)
            self._schema_ready = True

        active = self.state_repo.list_states()
        archived = self._read_new_archived_states()

        async with self.session_factory()() as session:
            for state in active:
                await upsert_run_state(session, state)
            for state in archived:
                await upsert_run_state(session, state)
                self._seen_archived.add(state.run_id)
            await session.commit()

        logger.debug(
            "cache_refreshed",
            extra={"active": len(active), "new_archived": len(archived)},
        )

    def _read_new_archived_states(self) -> list[RunState]:
        """Scan run folders for archived state.json objects not yet ingested.

        Archived states are immutable, so each is read exactly once per
        process lifetime (tracked in ``_seen_archived``).
        """
        states: list[RunState] = []
        for key in self.store.list_objects_sync("runs/"):
            # runs/<flow>/<date>/<run_id>/state.json
            parts = key.split("/")
            if len(parts) != 5 or parts[4] != "state.json":
                continue
            try:
                run_id = UUID(parts[3])
            except ValueError:
                continue
            if run_id in self._seen_archived:
                continue
            obj = self.store.get_object_sync(key)
            if obj is None:
                continue
            try:
                states.append(_parse_archived_state(obj.data.decode("utf-8")))
            except Exception:
                logger.warning(
                    "cache_archived_state_unreadable", extra={"key": key}
                )
        return states


def _parse_archived_state(raw: str) -> RunState:
    """Parse an archived state.json into the RunState read shape.

    Current archives hold the full ObligationRecord account; archives from
    before the account model hold a bare RunState — both remain readable.
    """
    import json

    from ..models import ObligationRecord

    if "obligation" in json.loads(raw):
        return from_json(ObligationRecord)(raw).summary()
    return from_json(RunState)(raw)

# ---
# endregion
