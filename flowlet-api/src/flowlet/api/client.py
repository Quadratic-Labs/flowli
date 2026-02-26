"""
High-level client API for querying Flowlet run history.

Provides a convenient interface for downloading snapshots, syncing with
new logs, and querying run data using SQLAlchemy ORM.
"""
import logging
from pathlib import Path
from ..storage import StoragePath
from uuid import UUID

from attrs import define, field
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

from ..models import RunSummary
from ..storage.database import Run, RunLink, DatabaseSettings
from ..types import RunStatus
from .run_view_service import RunViewService
from .syncer import IncrementalSyncer, BackgroundSyncer



logger = logging.getLogger(__name__)


@define(slots=True, kw_only=True)
class FlowletClientConfig:
    """
    Client-side configuration.

    Attributes:
        storage_root: Root path for blob storage (Path or AzureBlobPath).
        cache_dir: Local directory for caching snapshots.
        poll_interval_seconds: Interval for background sync polling.
        auto_sync: Whether to enable automatic background sync.
    """
    storage_root: StoragePath
    cache_dir: Path = Path.home() / ".flowlet" / "cache"
    poll_interval_seconds: float = 30.0
    auto_sync: bool = True


@define(slots=True, kw_only=True)
class FlowletClient:
    """
    Client for querying Flowlet run history.

    Provides high-level API for downloading snapshots, syncing with
    new logs, and querying run data.

    Example:
        >>> config = FlowletClientConfig(
        ...     storage_root=Path("/path/to/storage"),
        ...     cache_dir=Path("~/.flowlet/cache"),
        ...     auto_sync=True
        ... )
        >>> client = FlowletClient(config=config)
        >>> await client.initialize()
        >>>
        >>> # Query runs
        >>> runs = client.list_runs(flow_name="my_flow", limit=10)
        >>> run = client.get_run(run_id)
        >>> summary = client.get_run_summary(run_id)
        >>>
        >>> await client.close()
    """
    config: FlowletClientConfig
    downloader: SnapshotDownloader = field(init=False)
    syncer: IncrementalSyncer = field(init=False)
    background_syncer: BackgroundSyncer = field(init=False)
    db_session: Session | None = None
    _initialized: bool = False

    def __attrs_post_init__(self):
        """Initialize components after attrs initialization."""
        from ..storage.snapshot import LogReader

        # Create downloader
        self.downloader = SnapshotDownloader(
            storage_root=self.config.storage_root,
            local_cache_dir=self.config.cache_dir
        )

        # Create log reader
        log_reader = LogReader(storage_root=self.config.storage_root)

        # Create syncer (will be configured after download)
        self.syncer = IncrementalSyncer(
            storage_root=self.config.storage_root,
            local_db_path=self.config.cache_dir / "flowlet-hot.db",
            log_reader=log_reader
        )

        # Create background syncer
        self.background_syncer = BackgroundSyncer(
            syncer=self.syncer,
            poll_interval_seconds=self.config.poll_interval_seconds
        )

    async def initialize(self, background_sync: bool | None = None) -> None:
        """
        Initialize client.

        1. Download hot snapshot
        2. Sync with latest logs
        3. Open SQLAlchemy session
        4. Optionally start background sync

        Args:
            background_sync: Override config.auto_sync setting.
        """
        if self._initialized:
            logger.warning("Client already initialized")
            return

        logger.info("Initializing Flowlet client...")

        # Download hot snapshot
        local_db = await self.downloader.download_hot_snapshot()
        if local_db is None:
            raise RuntimeError("Failed to download hot snapshot")

        # Perform initial sync
        updated, new_runs = await self.syncer.sync()
        if updated:
            logger.info(f"Initial sync: {new_runs} new runs")

        # Setup SQLAlchemy session
        db_settings = DatabaseSettings(url=f"sqlite:///{local_db}")
        self.db_session = db_settings.db_session_factory()

        # Start background sync if enabled
        if background_sync is None:
            background_sync = self.config.auto_sync

        if background_sync:
            await self.background_syncer.start()

        self._initialized = True
        logger.info("Flowlet client initialized")

    async def close(self) -> None:
        """
        Shutdown client.

        Stops background sync and closes database session.
        """
        if not self._initialized:
            return

        logger.info("Closing Flowlet client...")

        # Stop background sync
        await self.background_syncer.stop()

        # Close database session
        if self.db_session:
            self.db_session.close()
            self.db_session = None

        self._initialized = False
        logger.info("Flowlet client closed")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    def _ensure_initialized(self) -> None:
        """Raise error if client not initialized."""
        if not self._initialized or self.db_session is None:
            raise RuntimeError("Client not initialized. Call initialize() first.")

    def get_run(self, run_id: UUID) -> Run | None:
        """
        Query run by ID.

        Args:
            run_id: UUID of the run to fetch.

        Returns:
            Run object if found, None otherwise.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        return self.db_session.query(Run).filter_by(run_id=run_id).first()

    def list_runs(
        self,
        flow_name: str | None = None,
        status: RunStatus | None = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[Run]:
        """
        List runs with optional filtering.

        Args:
            flow_name: Filter by flow/task name.
            status: Filter by run status.
            limit: Maximum number of runs to return.
            offset: Number of runs to skip.

        Returns:
            List of Run objects.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        query = self.db_session.query(Run)

        if flow_name:
            query = query.filter_by(name=flow_name)

        if status:
            query = query.filter_by(status=status.value)

        return query.order_by(Run.start_ts.desc()).limit(limit).offset(offset).all()

    def get_run_summary(self, run_id: UUID) -> RunSummary | None:
        """
        Get hierarchical RunSummary for a run.

        Reconstructs tree from Run/RunLink tables.

        Args:
            run_id: UUID of the run.

        Returns:
            RunSummary tree, or None if not found.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        # Get root run
        run = self.get_run(run_id)
        if not run:
            return None

        # Build tree recursively
        return self._build_summary_tree(run)

    def _build_summary_tree(self, run: Run) -> RunSummary:
        """
        Recursively build RunSummary tree from Run/RunLink records.

        Args:
            run: Root Run object.

        Returns:
            RunSummary tree.
        """
        assert self.db_session is not None

        # Get children via run_links
        links = self.db_session.query(RunLink).filter_by(parent_run_id=run.run_id).all()

        children = []
        for link in links:
            child_run = self.db_session.query(Run).filter_by(run_id=link.child_run_id).first()
            if child_run:
                children.append(self._build_summary_tree(child_run))

        # Create RunSummary
        from ..types import Timestamp

        return RunSummary(
            span_id=run.run_id,
            span_name=run.name,
            status=RunStatus(run.status),
            start_ts=Timestamp.from_datetime(run.start_ts) if run.start_ts else None,
            end_ts=Timestamp.from_datetime(run.end_ts) if run.end_ts else None,
            children=children
        )

    async def sync_now(self) -> tuple[bool, int]:
        """
        Manually trigger immediate sync.

        Returns:
            Tuple of (updated: bool, new_runs: int).
        """
        self._ensure_initialized()
        return await self.syncer.sync()

    def count_runs(
        self,
        flow_name: str | None = None,
        status: RunStatus | None = None
    ) -> int:
        """
        Count runs matching filters.

        Args:
            flow_name: Filter by flow/task name.
            status: Filter by run status.

        Returns:
            Number of matching runs.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        query = self.db_session.query(Run)

        if flow_name:
            query = query.filter_by(name=flow_name)

        if status:
            query = query.filter_by(status=status.value)

        return query.count()

    def get_run_children(self, run_id: UUID) -> list[Run]:
        """
        Get direct children of a run.

        Args:
            run_id: Parent run UUID.

        Returns:
            List of child Run objects.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        # Get child run IDs from links
        links = self.db_session.query(RunLink).filter_by(parent_run_id=run_id).all()
        child_ids = [link.child_run_id for link in links]

        # Get child runs
        return self.db_session.query(Run).filter(Run.run_id.in_(child_ids)).all()

    def get_run_parent(self, run_id: UUID) -> Run | None:
        """
        Get parent of a run.

        Args:
            run_id: Child run UUID.

        Returns:
            Parent Run object, or None if no parent.
        """
        self._ensure_initialized()
        assert self.db_session is not None

        # Get parent run ID from link
        link = self.db_session.query(RunLink).filter_by(child_run_id=run_id).first()
        if not link:
            return None

        return self.db_session.query(Run).filter_by(run_id=link.parent_run_id).first()

    async def refresh(self) -> None:
        """
        Refresh client data.

        Re-downloads hot snapshot and syncs with latest logs.
        Useful after significant updates to remote storage.
        """
        logger.info("Refreshing client data...")

        # Re-download hot snapshot
        local_db = await self.downloader.download_hot_snapshot(force=True)
        if local_db is None:
            raise RuntimeError("Failed to download hot snapshot")

        # Sync with latest logs
        updated, new_runs = await self.syncer.sync()
        if updated:
            logger.info(f"Refresh sync: {new_runs} new runs")

        # Recreate database session
        if self.db_session:
            self.db_session.close()

        db_settings = DatabaseSettings(url=f"sqlite:///{local_db}")
        self.db_session = db_settings.db_session_factory()

        logger.info("Client data refreshed")
