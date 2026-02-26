"""Snapshot lifecycle management for Flowlet run history.

Layer separation
----------------
- path layer:       SnapshotPath — structured /snapshots/<start>--<end>.sqlite path.
                    Pure construction and validation, no I/O.
- rollout layer:    Strategy classes (TimeRolloutStrategy, CountRolloutStrategy,
                    AnyRolloutStrategy) controlling when and how the hot snapshot
                    is archived into a frozen file.
- repository layer: SnapshotRepository — single owner of all snapshot I/O.
                    Bridges remote storage with the active SQLAlchemy engine.
"""
import logging
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol, Self
from uuid import UUID

from attrs import define, field, Factory
from sqlalchemy import delete as sa_delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from ..models import RunSummary
from ..storage.types import StoragePath
from ..types import PeriodUUID, Timestamp
from .database import (
    Run,
    RunLink,
    ensure_snapshot_schema,
    get_snapshot_period,
    insert_new_summaries,
)


logger = logging.getLogger(__name__)


# region @snapshot.path
# ---
# role: datatype
# intent: structured /snapshots/<start>--<end>.sqlite path with derived period
# description: >
#   SnapshotPath couples a StoragePath with the PeriodUUID parsed from the stem.
#   path is the single source of truth; period is derived in __attrs_post_init__.
#   Use try_from() to parse an existing path and build() to construct a new one.
#   '--' is a safe separator because UUIDv7 strings use only single hyphens.
# rules:
#   - path MUST match …/snapshots/<start_uuid>--<end_uuid>.sqlite; raises ValueError otherwise.
#   - period MUST NOT be set directly; mutate path and re-parse instead.
#   - try_from MUST return None for any non-conforming path rather than raising.
# dependencies:
#   - types.time
#   - storage.types
# ---


@define(frozen=True, slots=True, kw_only=True)
class SnapshotPath:
    """Snapshot-file path structured under the ``/snapshots/<start>--<end>.sqlite`` schema.

    ``path`` is the single source of truth. ``period`` is derived from it on
    construction and stored for fast access.

    Attributes:
        path: Full storage path to the snapshot file.
        period: Period (start and end UUIDs) encoded in the filename.
    """

    path: StoragePath = field()
    period: PeriodUUID = field(init=False)

    def __attrs_post_init__(self) -> None:
        parts = self.path.parts
        if (
            self.path.suffix != ".sqlite"
            or len(parts) < 2
            or parts[-2] != "snapshots"
        ):
            raise ValueError(
                f"{self.path!r} does not match /snapshots/<start>--<end>.sqlite"
            )
        object.__setattr__(self, "period", PeriodUUID.from_str(self.path.stem))

    @classmethod
    def try_from(cls, path: StoragePath) -> Self | None:
        """Parse a SnapshotPath from a storage path, returning None on failure.

        Args:
            path: Storage path to parse.

        Returns:
            Parsed SnapshotPath, or None if the path does not match the schema.
        """
        try:
            return cls(path=path)
        except Exception:
            return None

    @classmethod
    def build(cls, root: StoragePath, period: PeriodUUID) -> Self:
        """Construct a SnapshotPath under ``root/snapshots/<start>--<end>.sqlite``.

        Args:
            root: Root storage directory.
            period: Period to encode in the filename.

        Returns:
            A SnapshotPath pointing to ``root/snapshots/<period>.sqlite``.
        """
        return cls(path=root / "snapshots" / f"{period}.sqlite")

    def get_engine(self) -> AsyncEngine:
        """Create a SQLAlchemy engine bound to this snapshot file.

        Raises:
            ValueError: If the snapshot path is not a local filesystem path.
        """
        if not isinstance(self.path, Path):
            raise ValueError(f"No support for SQLite connections to remote paths: {self.path}")
        return create_async_engine(f"sqlite+aiosqlite:///{self.path}")

# ---
# endregion


# region @snapshot.rollout
# ---
# role: datatype
# intent: rollout strategies controlling when and how the hot snapshot is archived
# description: >
#   A rollout strategy decides when the hot snapshot has grown too large and
#   archives the oldest runs into a new frozen snapshot file, then prunes them
#   from the hot session.  Two concrete strategies are provided:
#     - TimeRolloutStrategy: triggers when the covered period exceeds a time
#       threshold; archives runs older than rollout_seconds_min.
#     - CountRolloutStrategy: triggers when the run count exceeds max_runs;
#       archives the oldest runs down to min_runs.
#   AnyRolloutStrategy delegates to the first strategy that fires.
# rules:
#   - snapshot MUST return None when no rollout is needed.
#   - snapshot MUST prune archived runs from the hot session before returning.
#   - snapshot MUST write the frozen snapshot to storage_root before returning.
#   - Strategies MUST preserve the partition invariant: each run_id in exactly one file.
# dependencies:
#   - snapshot.path
#   - database.snapshot
#   - database.models
# ---


async def _archive_runs(
    session: AsyncSession,
    archive_ids: list[UUID],
    storage_root: StoragePath,
    cache_dir: Path,
) -> SnapshotPath:
    """Copy the given runs to a frozen snapshot, upload it, and prune the hot session.

    Args:
        session: Active async session for the hot snapshot DB.
        archive_ids: Ordered list of run_ids to archive (oldest first).
        storage_root: Storage root where the frozen file is written.
        cache_dir: Local directory for the temporary archive file.

    Returns:
        SnapshotPath for the newly written frozen snapshot under storage_root.
    """
    runs_result = await session.execute(select(Run).where(Run.run_id.in_(archive_ids)))
    runs = list(runs_result.scalars().all())
    links_result = await session.execute(
        select(RunLink).where(
            RunLink.parent_run_id.in_(archive_ids) & RunLink.child_run_id.in_(archive_ids)
        )
    )
    links = list(links_result.scalars().all())

    archived_period = PeriodUUID(start=archive_ids[0], end=archive_ids[-1])
    local_snapshot = SnapshotPath.build(Path(cache_dir), archived_period)
    local_snapshot.path.parent.mkdir(parents=True, exist_ok=True)

    arch_engine = create_async_engine(f"sqlite+aiosqlite:///{local_snapshot.path}")
    try:
        await ensure_snapshot_schema(arch_engine)
        arch_factory = async_sessionmaker(arch_engine, class_=AsyncSession, expire_on_commit=False)
        async with arch_factory() as arch_session:
            for run in runs:
                arch_session.add(Run(**run.to_dict()))
            for link in links:
                arch_session.add(RunLink(**link.to_dict()))
            await arch_session.commit()
    finally:
        await arch_engine.dispose()

    remote_snapshot = SnapshotPath.build(storage_root, archived_period)
    remote_snapshot.path.parent.mkdir(parents=True, exist_ok=True)
    remote_snapshot.path.write_bytes(local_snapshot.path.read_bytes())

    await session.execute(
        sa_delete(RunLink).where(
            RunLink.parent_run_id.in_(archive_ids) | RunLink.child_run_id.in_(archive_ids)
        )
    )
    await session.execute(sa_delete(Run).where(Run.run_id.in_(archive_ids)))
    await session.commit()

    return remote_snapshot


class RolloutStrategyProtocol(Protocol):
    cache_dir: Path

    async def should_rollout(self, session: AsyncSession) -> bool:
        """Return True when the hot snapshot should be rolled out.

        Args:
            session: Active async session for the hot snapshot DB.

        Returns:
            True if the strategy recommends archiving; False otherwise.
        """
        ...

    async def snapshot(self, session: AsyncSession) -> SnapshotPath | None:
        """Archive the oldest runs if the rollout threshold is exceeded.

        Args:
            session: Active async session for the hot snapshot DB.

        Returns:
            SnapshotPath for the newly written frozen snapshot, or None if no
            rollout was performed.
        """
        ...


@define(slots=True, kw_only=True)
class TimeRolloutStrategy:
    """Archives runs by time: triggers when the covered period exceeds a threshold.

    Attributes:
        cache_dir: Local directory for temporary archive files.
        storage_root: Storage root where frozen snapshots are written.
        rollout_seconds_min: Minimum window (in seconds) to keep in the hot
            snapshot after archiving.  Default: 4 weeks.
        rollout_seconds_max: Total period (in seconds) that triggers a rollout.
            Default: 12 weeks.
    """

    cache_dir: Path
    storage_root: StoragePath
    rollout_seconds_min: int = 60 * 60 * 24 * 7 * 4
    rollout_seconds_max: int = 60 * 60 * 24 * 7 * 12

    async def should_rollout(self, session: AsyncSession) -> bool:
        """Return True when the covered period exceeds rollout_seconds_max.

        Args:
            session: Active async session for the hot snapshot DB.
        """
        period = await get_snapshot_period(session)
        duration = period.to_timestamp().duration()
        return duration is not None and duration.total_seconds() > self.rollout_seconds_max

    async def snapshot(self, session: AsyncSession) -> SnapshotPath | None:
        """Archive runs older than rollout_seconds_min if the period is too large.

        Args:
            session: Active async session for the hot snapshot DB.

        Returns:
            SnapshotPath for the frozen archive, or None if no rollout triggered.
        """
        period = await get_snapshot_period(session)
        duration = period.to_timestamp().duration()
        if duration is None or duration.total_seconds() <= self.rollout_seconds_max:
            return None

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.rollout_seconds_min)
        result = await session.execute(
            select(Run.run_id).where(Run.start_ts < cutoff).order_by(Run.run_id)
        )
        archive_ids = list(result.scalars().all())
        if not archive_ids:
            return None

        return await _archive_runs(session, archive_ids, self.storage_root, self.cache_dir)


@define(slots=True, kw_only=True)
class CountRolloutStrategy:
    """Archives runs by count: triggers when the hot snapshot exceeds max_runs.

    Attributes:
        cache_dir: Local directory for temporary archive files.
        storage_root: Storage root where frozen snapshots are written.
        max_runs: Run count above which a rollout is triggered.
        min_runs: Target count to keep in the hot snapshot after archiving.
    """

    cache_dir: Path
    storage_root: StoragePath
    max_runs: int = 10_000
    min_runs: int = 5_000

    async def should_rollout(self, session: AsyncSession) -> bool:
        """Return True when the run count exceeds max_runs.

        Args:
            session: Active async session for the hot snapshot DB.
        """
        result = await session.execute(select(func.count()).select_from(Run))
        return result.scalar_one() > self.max_runs

    async def snapshot(self, session: AsyncSession) -> SnapshotPath | None:
        """Archive the oldest runs if the count exceeds max_runs.

        Args:
            session: Active async session for the hot snapshot DB.

        Returns:
            SnapshotPath for the frozen archive, or None if no rollout triggered.
        """
        result = await session.execute(select(func.count()).select_from(Run))
        count = result.scalar_one()
        if count <= self.max_runs:
            return None

        excess = count - self.min_runs
        result = await session.execute(
            select(Run.run_id).order_by(Run.run_id).limit(excess)
        )
        archive_ids = list(result.scalars().all())
        if not archive_ids:
            return None

        return await _archive_runs(session, archive_ids, self.storage_root, self.cache_dir)


@define(slots=True, kw_only=True)
class AnyRolloutStrategy:
    """Delegates to the first sub-strategy that triggers a rollout.

    Strategies are tried in order; the first non-None result wins and subsequent
    strategies are skipped.

    Attributes:
        cache_dir: Local directory, forwarded to sub-strategies.
        strategies: Ordered list of candidate rollout strategies.
    """

    cache_dir: Path
    strategies: list[RolloutStrategyProtocol]

    async def should_rollout(self, session: AsyncSession) -> bool:
        """Return True if any sub-strategy recommends a rollout.

        Args:
            session: Active async session for the hot snapshot DB.
        """
        for strategy in self.strategies:
            if await strategy.should_rollout(session):
                return True
        return False

    async def snapshot(self, session: AsyncSession) -> SnapshotPath | None:
        """Run each strategy in order and return the first archive produced.

        Args:
            session: Active async session for the hot snapshot DB.

        Returns:
            SnapshotPath from the first strategy that fires, or None if none trigger.
        """
        for strategy in self.strategies:
            result = await strategy.snapshot(session)
            if result is not None:
                return result
        return None


# ---
# endregion


# region @snapshot.repository
# ---
# role: both
# intent: unified snapshot lifecycle — list, fetch, load, and update with rollout
# description: >
#   SnapshotRepository is the single owner of all snapshot I/O:
#     - ls()      — enumerate snapshot files without opening them.
#     - find()    — resolve the snapshot covering a point in time.
#     - fetch()   — download the right SQLite to the local cache.
#     - load()    — fetch and bind self.engine to the hot snapshot.
#     - update()  — ingest new runs, then roll out when rollout_strategy fires.
#   No manifest file is maintained; the directory listing is the manifest.
# rules:
#   - MUST delegate all DB writes to database.snapshot pure functions.
#   - MUST NOT embed business logic; keep it as thin glue.
#   - update MUST preserve the partition invariant: each run_id in exactly one file.
#   - fetch MUST NOT modify any remote file.
# dependencies:
#   - snapshot.path
#   - snapshot.rollout
#   - database.snapshot
#   - storage.types
# ---


@define(slots=True, kw_only=True)
class SnapshotRepository:
    """Repository for snapshot lifecycle management.

    Owns discovery, download, ingestion, and archiving of SQLite snapshot files.
    Remote snapshots live under storage_path; downloaded copies are cached under
    cache_path.

    Attributes:
        storage_path: Remote (or local) directory holding snapshot files.
        cache_path: Local directory for cached SQLite copies used for queries.
        engine: Active SQLAlchemy engine for the current snapshot (in-memory by
            default; updated by load()).
        rollout_strategy: Optional strategy controlling when and how the hot
            snapshot is archived.
    """

    storage_path: StoragePath
    cache_path: Path | None = None
    engine: AsyncEngine = Factory(lambda: create_async_engine("sqlite+aiosqlite:///:memory:"))
    rollout_strategy: RolloutStrategyProtocol | None = None

    @classmethod
    def from_configs(cls, *, configs, **_) -> Self:
        """Construct from application configs.

        Args:
            configs: Application config with snapshot_storage_path and
                snapshot_cache_path attributes.

        Returns:
            A SnapshotRepository bound to the configured paths.
        """
        return cls(
            storage_path=configs.snapshot_storage_path,
            cache_path=configs.snapshot_cache_path,
        )

    def ls(self, cached: bool = False) -> list[SnapshotPath]:
        """Enumerate all snapshot files, oldest-first by end UUID.

        Args:
            cached: When True, list from cache_path instead of storage_path.

        Returns:
            Sorted list of SnapshotPath objects.  The last element is the most
            recent (hot) snapshot.
        """
        snapshot_dir = self.cache_path if cached else self.storage_path
        if snapshot_dir is None or not snapshot_dir.exists():
            return []
        results = [
            snapshot
            for path in snapshot_dir.iterdir()
            if (snapshot := SnapshotPath.try_from(path)) is not None
        ]
        results.sort(key=lambda p: p.path.stem)
        return results

    def find(self, at: UUID | Timestamp | None, cached: bool = False) -> SnapshotPath | None:
        """Return the snapshot whose period covers *at*, or the latest.

        Args:
            at: UUID, Timestamp, or None (for the latest snapshot).
            cached: When True, search cache_path instead of storage_path.

        Returns:
            The matching SnapshotPath, or None if no snapshots exist.
        """
        snapshots = self.ls(cached=cached)
        if not snapshots:
            return None
        if at is None:
            return snapshots[-1]
        if isinstance(at, Timestamp):
            at = at.to_uuid7()
        for path in snapshots:
            if path.period.covers(at):
                return path
        return snapshots[-1]

    def fetch(self, at: UUID | Timestamp | None, force: bool = False) -> SnapshotPath | None:
        """Download the snapshot covering *at* into the local cache.

        Args:
            at: Point in time to look up, or None for the latest.
            force: When True, overwrite the local cache even if it exists.

        Returns:
            SnapshotPath pointing to the local cached file, or None if no remote
            snapshot exists.  Returns the remote path directly when cache_path is
            not configured.
        """
        remote = self.find(at=at, cached=False)
        if remote is None:
            return None
        if self.cache_path is None:
            return remote
        local = SnapshotPath.build(self.cache_path, remote.period)
        if not force and local.path.exists():
            return local
        local.path.parent.mkdir(parents=True, exist_ok=True)
        local.path.write_bytes(remote.path.read_bytes())
        return local

    def load(self) -> None:
        """Fetch the latest snapshot and bind self.engine to the local cached file."""
        current = self.fetch(at=None)
        if current is None:
            return
        self.engine = current.get_engine()

    async def update(self, summaries: dict[UUID, RunSummary]) -> int:
        """Ingest new run summaries into the hot snapshot, then roll out if needed.

        Copies the current remote hot snapshot to a temp file, upserts new
        summaries, optionally archives the oldest runs via rollout_strategy, and
        writes the result back to storage_path.  The old file is removed when the
        period name changes.

        Args:
            summaries: New run trees to persist, keyed by flow run_id (UUIDv7).

        Returns:
            Number of runs newly inserted (already-present IDs are skipped).
        """
        current = self.find(at=None, cached=False)
        tmp_dir = tempfile.mkdtemp()
        local_path = Path(tmp_dir) / "snapshot.db"
        try:
            if current is not None and current.path.exists():
                local_path.write_bytes(current.path.read_bytes())

            engine = create_async_engine(f"sqlite+aiosqlite:///{local_path}")
            try:
                await ensure_snapshot_schema(engine)
                session_factory = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with session_factory() as session:
                    new_count = await insert_new_summaries(session, summaries)
                    if new_count == 0:
                        return 0
                    if self.rollout_strategy is not None:
                        archived = await self.rollout_strategy.snapshot(session)
                        if archived is not None:
                            logger.info(
                                "snapshot_rollout",
                                extra={"archived": str(archived.period)},
                            )
                    hot_period = await get_snapshot_period(session)
            finally:
                await engine.dispose()

            if hot_period.end is not None:
                new_snapshot = SnapshotPath.build(self.storage_path, hot_period)
                new_snapshot.path.parent.mkdir(parents=True, exist_ok=True)
                new_snapshot.path.write_bytes(local_path.read_bytes())
                if current is not None and current.period != hot_period:
                    current.path.unlink(missing_ok=True)

            logger.info(
                "snapshot_update_done",
                extra={"new_runs": new_count, "hot_end": str(hot_period.end)},
            )
            return new_count
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

# ---
# endregion
