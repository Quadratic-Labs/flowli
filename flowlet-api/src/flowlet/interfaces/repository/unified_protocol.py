


class FlowReader(Protocol):
    """Read interface with streaming and drill-down support.

    Optimized for both operational (recent data) and analytical (historical) queries.
    """
    def list_flows(self, n_last_runs: int=1, db: Session | None=None) -> list[models.FlowSummary]:
        """List all registered flows with execution summaries.

        Args:
            n_last_runs: Number of recent runs to include in summary.
            db: Optional database session.

        Returns:
            list[FlowSummary]: Flow summaries with execution statistics.
        """
        ...

    def list_runs(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List recent runs with pagination.

        Args:
            offset: Number of runs to skip.
            limit: Maximum number of runs to return.
            db: Optional database session.

        Returns:
            list[FlowRunSummary]: Run summaries ordered by recency.
        """
        ...

    def list_runs_by_flow_name(self, name: str, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List runs for a specific flow.

        Args:
            name: Name of the flow.
            offset: Number of runs to skip.
            limit: Maximum number of runs to return.
            db: Optional database session.

        Returns:
            list[FlowRunSummary]: Run summaries for the specified flow.
        """
        ...

    def get_run_by_id(self, run_id: UUID, db: Session | None=None) -> models.RunModel | None:
        """Get detailed information for a specific run.

        Args:
            run_id: Unique identifier of the run.
            db: Optional database session.

        Returns:
            RunModel | None: Detailed run model, or None if not found.
        """
        ...
    async def runs_stream(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> AsyncIterator[RunModel]:
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