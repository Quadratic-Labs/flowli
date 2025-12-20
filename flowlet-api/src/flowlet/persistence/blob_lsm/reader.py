"""Multi-tier reader for querying across L0/L1/L2 tiers."""

import json
from datetime import datetime, UTC, timedelta
from uuid import UUID
from typing import AsyncIterator

from flowlet.persistence.azure_path import AzureBlobPath
from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel, RunModel
from .config import BlobLSMSettings
from .cache import HotCache


class MultiTierReader:
    """Coordinates queries across L0 (hot), L1 (warm), and L2 (cold) tiers.

    Query strategy:
    1. Check hot cache first (10ms)
    2. Query L0 JSONL files (20-100ms)
    3. Query L1 Parquet files if needed (100-500ms)
    4. Query L2 compressed archives if needed (500-5000ms)

    Features:
    - Cache-aware querying
    - Drill-down by run_id (optimized path)
    - Drill-down by flow_name (secondary index)
    - Streaming results to avoid memory bloat
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize multi-tier reader.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings
        self.blob_root = AzureBlobPath.from_connection_string(
            connection_string=settings.connection_string,
            container=settings.container_name
        )
        self.cache = HotCache(settings)

    async def get_run_logs(
        self,
        run_id: UUID,
        limit: int | None = None,
        offset: int = 0
    ) -> AsyncIterator[RunLogAttrModel]:
        """Get all logs for a specific run (drill-down optimized).

        Query path:
        1. Check cache
        2. Query L0 by-run index (direct path)
        3. Query L1 by-run partition (Hive pruning)
        4. Query L2 by-run sorted table

        Args:
            run_id: Run ID to get logs for
            limit: Maximum number of logs to return
            offset: Number of logs to skip

        Yields:
            Log entries in chronological order
        """
        # Check cache first
        cached_logs = self.cache.get_logs(run_id)
        if cached_logs:
            count = 0
            for log in cached_logs:
                if count >= offset:
                    yield log
                    if limit and (count - offset) >= limit:
                        return
                count += 1
            return

        # Cache miss - query tiers
        count = 0
        all_logs = []

        # Query L0 (hot) - direct path lookup
        async for log in self._get_run_logs_l0(run_id):
            all_logs.append(log)
            if count >= offset:
                yield log
                if limit and (count - offset) >= limit:
                    # Cache what we've read so far
                    self.cache.put_logs(run_id, all_logs)
                    return
            count += 1

        # Query L1 (warm) - Hive partition pruning
        # TODO: Implement L1 Parquet reading when compaction is ready

        # Query L2 (cold) - sorted archive
        # TODO: Implement L2 compressed Parquet reading

        # Cache all logs for this run
        if all_logs:
            self.cache.put_logs(run_id, all_logs)

    async def _get_run_logs_l0(self, run_id: UUID) -> AsyncIterator[RunLogAttrModel]:
        """Query L0 logs for a specific run.

        Path: logs/l0/by-run/{date}/run-{run_id}/logs.jsonl

        Args:
            run_id: Run ID

        Yields:
            Log entries from L0
        """
        # Query recent dates (last 7 days per hot tier retention)
        for days_ago in range(self.settings.hot_tier_retention_days):
            date = datetime.now(UTC) - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            log_path = self.blob_root / f"logs/l0/by-run/{date_str}/run-{run_id}/logs.jsonl"

            if not log_path.exists():
                continue

            # Read JSONL file
            content = log_path.read_text()
            for line in content.strip().split('\n'):
                if not line:
                    continue

                data = json.loads(line)
                yield RunLogAttrModel(
                    log_id=UUID(data['log_id']),
                    run_id=UUID(data['run_id']),
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    status=data['status'],
                    log=data['log']
                )

    async def get_flow_logs(
        self,
        flow_name: str,
        date_range: tuple[datetime, datetime] | None = None,
        limit: int | None = None
    ) -> AsyncIterator[RunLogAttrModel]:
        """Get all logs for a flow across all runs (drill-down by flow).

        Query path:
        1. Query L0 by-flow index (scan flow directory)
        2. Query L1 by-flow partition (Hive pruning on flow_name)
        3. Query L2 by-flow sorted table

        Args:
            flow_name: Flow name to get logs for
            date_range: Optional date range filter
            limit: Maximum number of logs to return

        Yields:
            Log entries for all runs of this flow
        """
        count = 0

        # Determine date range
        if date_range:
            start_date, end_date = date_range
        else:
            end_date = datetime.now(UTC)
            start_date = end_date - timedelta(days=self.settings.hot_tier_retention_days)

        # Query L0 (hot) - by-flow index
        # Note: L0 by-flow index is not implemented in writer yet
        # For now, we'd need to scan by-run and filter by flow name
        # TODO: Implement by-flow index in L0 writer

        # Query L1 (warm) - by-flow partition
        # TODO: Implement L1 Parquet reading with flow_name partition

        # Query L2 (cold) - by-flow sorted table
        # TODO: Implement L2 compressed Parquet reading

    async def get_run_tree(
        self,
        run_id: UUID,
        include_logs: bool = True
    ) -> RunModel:
        """Reconstruct full run hierarchy from blob storage.

        Args:
            run_id: Root run ID
            include_logs: Whether to include logs in the tree

        Returns:
            Complete run tree with children and optionally logs
        """
        # Check cache first
        cached_tree = self.cache.get_run_tree(run_id)
        if cached_tree:
            return cached_tree

        # Get run metadata
        run = await self._get_run(run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")

        # Get logs if requested
        logs = []
        if include_logs:
            async for log in self.get_run_logs(run_id):
                logs.append(log)

        # Get children (from links)
        children = await self._get_child_runs(run_id, include_logs)

        # Get parent (from links)
        parent = await self._get_parent_run(run_id)

        # Build tree
        tree = RunModel(
            run=run,
            logs=logs,
            parent=parent,
            children=children
        )

        # Cache the tree
        self.cache.put_run_tree(tree)

        return tree

    async def _get_run(self, run_id: UUID) -> RunAttrModel | None:
        """Get run metadata from storage.

        Args:
            run_id: Run ID

        Returns:
            Run metadata or None if not found
        """
        # Check cache
        cached_run = self.cache.get_run(run_id)
        if cached_run:
            return cached_run

        # Query L0 (hot)
        for days_ago in range(self.settings.hot_tier_retention_days):
            date = datetime.now(UTC) - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            # Scan all run files for this date
            runs_dir = self.blob_root / f"runs/l0/{date_str}"
            if not runs_dir.is_dir():
                continue

            for run_file in runs_dir.iterdir():
                if not run_file.name.endswith('.jsonl'):
                    continue

                content = run_file.read_text()
                for line in content.strip().split('\n'):
                    if not line:
                        continue

                    data = json.loads(line)
                    if data['run_id'] == str(run_id):
                        run = RunAttrModel(
                            run_id=UUID(data['run_id']),
                            run_type=data['run_type'],
                            name=data['name']
                        )
                        self.cache.put_run(run)
                        return run

        # Query L1 (warm) - TODO: Implement Parquet reading
        # Query L2 (cold) - TODO: Implement compressed Parquet reading

        return None

    async def _get_child_runs(
        self,
        parent_id: UUID,
        include_logs: bool
    ) -> list[RunModel]:
        """Get child runs for a parent.

        Args:
            parent_id: Parent run ID
            include_logs: Whether to include logs in children

        Returns:
            List of child run models
        """
        children = []

        # Get links from storage
        child_ids = await self._get_linked_children(parent_id)

        # Recursively build child trees
        for child_id in child_ids:
            child_tree = await self.get_run_tree(child_id, include_logs)
            children.append(child_tree)

        return children

    async def _get_parent_run(self, child_id: UUID) -> RunAttrModel | None:
        """Get parent run for a child.

        Args:
            child_id: Child run ID

        Returns:
            Parent run metadata or None
        """
        # Get parent link from storage
        parent_id = await self._get_linked_parent(child_id)
        if not parent_id:
            return None

        return await self._get_run(parent_id)

    async def _get_linked_children(self, parent_id: UUID) -> list[UUID]:
        """Get child run IDs linked to a parent.

        Args:
            parent_id: Parent run ID

        Returns:
            List of child run IDs
        """
        children = []

        # Query L0 hierarchy
        for days_ago in range(self.settings.hot_tier_retention_days):
            date = datetime.now(UTC) - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            links_dir = self.blob_root / f"hierarchy/l0/{date_str}"
            if not links_dir.is_dir():
                continue

            for link_file in links_dir.iterdir():
                if not link_file.name.endswith('.jsonl'):
                    continue

                content = link_file.read_text()
                for line in content.strip().split('\n'):
                    if not line:
                        continue

                    data = json.loads(line)
                    if data['parent_run_id'] == str(parent_id):
                        children.append(UUID(data['child_run_id']))

        return children

    async def _get_linked_parent(self, child_id: UUID) -> UUID | None:
        """Get parent run ID linked to a child.

        Args:
            child_id: Child run ID

        Returns:
            Parent run ID or None
        """
        # Query L0 hierarchy
        for days_ago in range(self.settings.hot_tier_retention_days):
            date = datetime.now(UTC) - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            links_dir = self.blob_root / f"hierarchy/l0/{date_str}"
            if not links_dir.is_dir():
                continue

            for link_file in links_dir.iterdir():
                if not link_file.name.endswith('.jsonl'):
                    continue

                content = link_file.read_text()
                for line in content.strip().split('\n'):
                    if not line:
                        continue

                    data = json.loads(line)
                    if data['child_run_id'] == str(child_id):
                        return UUID(data['parent_run_id'])

        return None

    async def stream_recent_runs(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> AsyncIterator[RunModel]:
        """Stream recent runs from hot tier.

        Args:
            limit: Maximum number of runs to return
            offset: Number of runs to skip

        Yields:
            Recent run models
        """
        count = 0
        total = 0

        # Query L0 (hot) - most recent first
        for days_ago in range(self.settings.hot_tier_retention_days):
            date = datetime.now(UTC) - timedelta(days=days_ago)
            date_str = date.strftime("%Y-%m-%d")

            runs_dir = self.blob_root / f"runs/l0/{date_str}"
            if not runs_dir.is_dir():
                continue

            # Read all run files for this date
            for run_file in sorted(runs_dir.iterdir(), reverse=True):
                if not run_file.name.endswith('.jsonl'):
                    continue

                content = run_file.read_text()
                for line in reversed(content.strip().split('\n')):
                    if not line:
                        continue

                    if total < offset:
                        total += 1
                        continue

                    data = json.loads(line)
                    run = RunAttrModel(
                        run_id=UUID(data['run_id']),
                        run_type=data['run_type'],
                        name=data['name']
                    )

                    # Get full tree
                    tree = await self.get_run_tree(run.run_id, include_logs=False)
                    yield tree

                    count += 1
                    total += 1

                    if count >= limit:
                        return
