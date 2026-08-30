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

Why scan storage instead of reading a CairnDB projection?  CairnDB
projections read a *log* and can always catch up to its tail (``refresh``/
``wait_for`` — snapshot lag only affects bootstrap cost, never read
freshness), but *active* run state is deliberately not in any log: it lives
in the lease document at ``state/{run_id}.json``, mutated in place under
CAS, because the lease is the correctness mechanism (ownership, fencing,
takeover).  Logging every transition would serialize runs through one log
sequencer or need a log per run, and would duplicate the source of truth.
So the cache bridges the two planes on the read side: re-read the mutable
object plane in full (cheap — the active set is small by construction) and
fold the immutable archive plane incrementally.  Archived runs *are* logged
(``run.archived`` in the ``history`` named log), which is why history can
serve as a cold-seed fast path below.  Reads are at most one TTL behind
storage; ``refresh(force=True)`` closes the gap on demand.  See
``docs/architecture.md`` § "Why the pull cache scans storage".
"""
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from attrs import Factory, define, field
from cairndb.storage.base import BlobStorage
from sqlalchemy import DateTime, String, Uuid
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from flowlet.models import ObligationSummary
from flowlet.repository.state import StateRepository
from flowlet.serdes import from_json

if TYPE_CHECKING:
    from flowlet.history import RunHistory

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 5.0


class Base(DeclarativeBase):
    """Base class for the cache's SQLAlchemy ORM models."""


class ObligationRow(Base):
    """Flat snapshot row for a single flow run — the cache's one table.

    One row per run; upserted on every state transition.  Hierarchy is NOT
    stored here — query logs instead.

    Attributes:
        run_id: Unique identifier (UUIDv7 — chronologically ordered, the
            largest run_id is the most recent run).
        flow_name: Name of the flow being executed.
        status: Current execution status string.
        worker_id: Identifier of the owning worker.
        started_at: Wall-clock time when the run started.
        ended_at: Wall-clock time when the run finished (NULL if ongoing).
        attempt: Current attempt number (1-based).
        max_retries: Maximum allowed retries.
    """
    __tablename__ = "runs"

    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    flow_name: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[str] = mapped_column(String, index=True)
    worker_id: Mapped[str | None] = mapped_column(String, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt: Mapped[int] = mapped_column(default=1)
    max_retries: Mapped[int] = mapped_column(default=3)


async def ensure_snapshot_schema(engine: AsyncEngine) -> None:
    """Create the ``runs`` table in the cache's SQLite engine if not present.

    Args:
        engine: Async SQLAlchemy engine bound to the cache's SQLite database.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def upsert_run_state(session: AsyncSession, state: ObligationSummary) -> None:
    """Upsert a single ObligationSummary into the flat runs table.

    Uses SQLite's ``ON CONFLICT DO UPDATE`` so re-processing an existing
    run_id updates the row instead of raising.

    Args:
        session: Active async SQLAlchemy session for the cache engine.
        state: ObligationSummary to persist.
    """
    started_at = state.started_at.value
    ended_at = state.ended_at.value if state.ended_at is not None else None

    await session.execute(
        sqlite_insert(ObligationRow).values(
            run_id=state.run_id,
            flow_name=state.flow_name,
            status=state.status.value,
            worker_id=state.worker_id,
            started_at=started_at,
            ended_at=ended_at,
            attempt=state.attempt,
            max_retries=state.max_retries,
        ).on_conflict_do_update(
            index_elements=[ObligationRow.run_id],
            set_=dict(
                flow_name=state.flow_name,
                status=state.status.value,
                worker_id=state.worker_id,
                ended_at=ended_at,
                attempt=state.attempt,
            ),
        )
    )


@define(slots=False, kw_only=True)
class CacheRepository:
    """Ephemeral SQLite index over active and archived run states.

    Attributes:
        state_repo: Repository for the active ``state/`` directory.
        store: CairnDB blob store containing the ``runs/`` tree.
        history: Optional RunHistory consulted to seed newly-seen archived
            run_ids without a blob GET per run; None falls back to reading
            every archived run's state.json directly (unchanged behavior).
        ttl: Minimum seconds between two storage scans.
        engine: Async in-memory SQLite engine the query layer reads from.
    """

    state_repo: StateRepository
    store: BlobStorage
    history: "RunHistory | None" = None
    ttl: float = DEFAULT_TTL_SECONDS
    engine: AsyncEngine = Factory(
        lambda: create_async_engine("sqlite+aiosqlite:///:memory:")
    )
    _last_refresh: float = field(default=0.0, alias="_last_refresh")
    _schema_ready: bool = field(default=False, alias="_schema_ready")
    _seen_archived: set[UUID] = field(factory=set, alias="_seen_archived")

    @classmethod
    def from_deps(cls, *, state_repo, configs, history=None, **_) -> "CacheRepository":
        """Construct from the configure() dependency dict.

        Args:
            state_repo: Active-state repository.
            configs: Application config providing the blob store.
            history: Optional RunHistory (built earlier in configure() than
                this component, so it's already in deps when RunQuery(**deps)
                needs it too).
            **_: Absorbs unused dependency keys.
        """
        return cls(state_repo=state_repo, store=configs.store, history=history)

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
        archived = await self._read_new_archived_states()

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

    async def _read_new_archived_states(self) -> list[ObligationSummary]:
        """Scan run folders for archived state.json objects not yet ingested.

        Archived states are immutable, so each is read exactly once per
        process lifetime (tracked in ``_seen_archived``).  When ``history``
        is configured, newly-seen run_ids are looked up there first — a
        handful of local SQLite reads instead of one blob GET per run; any
        run_id history doesn't have falls back to reading state.json
        directly, so coverage never depends on history being enabled or
        caught up.
        """
        new_keys: dict[UUID, str] = {}
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
            new_keys[run_id] = key
        if not new_keys:
            return []

        from_history: dict[UUID, ObligationSummary] = {}
        if self.history is not None:
            from_history = await self.history.get_states(new_keys.keys())

        states: list[ObligationSummary] = list(from_history.values())
        for run_id, key in new_keys.items():
            if run_id in from_history:
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


def _parse_archived_state(raw: str) -> ObligationSummary:
    """Parse an archived state.json into the ObligationSummary read shape.

    Current archives hold the full ObligationRecord account; archives from
    before the account model hold a bare ObligationSummary — both remain readable.
    """
    import json

    from flowlet.models import ObligationRecord

    if "obligation" in json.loads(raw):
        return from_json(ObligationRecord)(raw).summary()
    return from_json(ObligationSummary)(raw)
