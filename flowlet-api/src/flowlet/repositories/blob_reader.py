"""Blob-native read repository implementation."""

from datetime import datetime
from typing import AsyncIterator
from uuid import UUID

from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel, RunModel
from flowlet.persistence.blob_lsm import BlobLSMSettings, LSMTierManager


class BlobFlowReader:
    """Implementation of BlobFlowReader protocol.

    Provides streaming queries with drill-down support across all tiers.
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize blob flow reader.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings
        self.lsm = LSMTierManager(settings)

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
        async for run in self.lsm.reader.stream_recent_runs(limit, offset):
            yield run

    async def query_runs_by_filters(
        self,
        flow_name: str | None = None,
        status: str | None = None,
        date_range: tuple[datetime, datetime] | None = None,
        batch_size: int = 1000
    ) -> AsyncIterator[RunModel]:
        """Stream runs matching filters.

        Args:
            flow_name: Filter by flow name
            status: Filter by status
            date_range: Filter by date range
            batch_size: Batch size for streaming

        Yields:
            Matching run models
        """
        # For now, scan all runs and filter client-side
        # TODO: Implement server-side filtering with Parquet predicate pushdown
        async for run in self.stream_recent_runs(limit=batch_size):
            # Apply filters
            if flow_name and run.run.name != flow_name:
                continue

            if status and run.logs:
                latest_status = run.logs[-1].status if run.logs else None
                if latest_status != status:
                    continue

            yield run

    async def get_run_tree(
        self,
        run_id: UUID,
        include_logs: bool = True
    ) -> RunModel:
        """Reconstruct full run hierarchy from blob storage.

        Args:
            run_id: Run ID
            include_logs: Whether to include logs in the tree

        Returns:
            Complete run tree with children and logs
        """
        return await self.lsm.reader.get_run_tree(run_id, include_logs)

    async def get_run_logs(
        self,
        run_id: UUID,
        limit: int | None = None,
        offset: int = 0
    ) -> AsyncIterator[RunLogAttrModel]:
        """Drill down: Stream all logs for a specific run.

        Args:
            run_id: Run ID
            limit: Maximum number of logs
            offset: Number of logs to skip

        Yields:
            Log entries in chronological order
        """
        async for log in self.lsm.reader.get_run_logs(run_id, limit, offset):
            yield log

    async def get_flow_logs(
        self,
        flow_name: str,
        date_range: tuple[datetime, datetime] | None = None,
        limit: int | None = None
    ) -> AsyncIterator[RunLogAttrModel]:
        """Drill down: Stream all logs across all runs of a flow.

        Args:
            flow_name: Flow name
            date_range: Optional date range filter
            limit: Maximum number of logs

        Yields:
            Log entries for all runs of this flow
        """
        async for log in self.lsm.reader.get_flow_logs(flow_name, date_range, limit):
            yield log

    def get_cache_stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        return self.lsm.cache.get_stats()
