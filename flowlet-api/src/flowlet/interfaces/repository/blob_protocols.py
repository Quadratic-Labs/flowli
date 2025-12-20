"""Protocol definitions for blob-native repository interfaces."""

from datetime import datetime
from typing import AsyncIterator, Protocol, Literal
from uuid import UUID
import pyarrow as pa

from .models import RunAttrModel, RunLogAttrModel, RunModel, FlowSummary


class BlobFlowWriter(Protocol):
    """Blob-native write interface optimized for batching and streaming.

    Designed for high-throughput writes with minimal blob operations.
    """

    async def write_run_batch(
        self,
        runs: list[RunAttrModel]
    ) -> list[UUID]:
        """Write multiple runs in a single operation.

        Args:
            runs: List of runs to write

        Returns:
            List of run IDs that were written
        """
        ...

    async def append_logs_stream(
        self,
        run_id: UUID,
        logs: AsyncIterator[RunLogAttrModel]
    ) -> int:
        """Stream logs directly to blob storage.

        Uses Azure AppendBlob for efficient streaming writes.

        Args:
            run_id: Run ID these logs belong to
            logs: Async iterator of log entries

        Returns:
            Number of logs written
        """
        ...

    async def link_runs_batch(
        self,
        links: list[tuple[UUID, UUID]]
    ) -> None:
        """Create parent-child links in batch.

        Args:
            links: List of (parent_id, child_id) tuples
        """
        ...

    async def flush(self) -> None:
        """Force flush in-memory buffers to blob storage."""
        ...


class BlobFlowReader(Protocol):
    """Blob-native read interface with streaming and drill-down support.

    Optimized for both operational (recent data) and analytical (historical) queries.
    """

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
        ...

    async def query_runs_by_filters(
        self,
        flow_name: str | None = None,
        status: str | None = None,
        date_range: tuple[datetime, datetime] | None = None,
        batch_size: int = 1000
    ) -> AsyncIterator[RunModel]:
        """Stream runs matching filters.

        Uses Parquet predicate pushdown for efficient filtering.

        Args:
            flow_name: Filter by flow name
            status: Filter by status
            date_range: Filter by date range
            batch_size: Batch size for streaming

        Yields:
            Matching run models
        """
        ...

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
        ...

    async def get_run_logs(
        self,
        run_id: UUID,
        limit: int | None = None,
        offset: int = 0
    ) -> AsyncIterator[RunLogAttrModel]:
        """Drill down: Stream all logs for a specific run.

        Optimized path with direct file lookup in L0 and partition pruning in L1/L2.

        Args:
            run_id: Run ID
            limit: Maximum number of logs
            offset: Number of logs to skip

        Yields:
            Log entries in chronological order
        """
        ...

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
        ...


class BlobAnalyticsReader(Protocol):
    """Pre-computed analytics via materialized views.

    Provides fast access to aggregated metrics without scanning raw data.
    """

    async def get_flow_summary(
        self,
        flow_name: str
    ) -> FlowSummary:
        """Get summary statistics for a flow from materialized view.

        Args:
            flow_name: Flow name

        Returns:
            Flow summary with latest status, duration, etc.
        """
        ...

    async def get_success_rate_trend(
        self,
        flow_name: str,
        granularity: Literal['hourly', 'daily', 'weekly'] = 'daily'
    ) -> list[tuple[datetime, float]]:
        """Get success rate trend over time.

        Args:
            flow_name: Flow name
            granularity: Time granularity for aggregation

        Returns:
            List of (timestamp, success_rate) tuples
        """
        ...

    async def query_parquet_raw(
        self,
        tier: Literal['l1', 'l2'],
        partition_path: str,
        filters: list[tuple] | None = None
    ) -> pa.Table:
        """Direct Parquet query for custom analytics (power users).

        Args:
            tier: Which tier to query (l1 or l2)
            partition_path: Partition path within tier
            filters: Optional PyArrow filters

        Returns:
            PyArrow table with results
        """
        ...
