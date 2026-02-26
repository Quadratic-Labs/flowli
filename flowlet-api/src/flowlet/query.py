"""Repository for querying flow and task execution history.

This module provides the AzureQuery class for read operations on
flow execution data, along with SQL query definitions.
"""
import json
from pathlib import Path
from typing import Iterator, Sequence
from uuid import UUID

from jsonry.model import Query
import jsonry.execution.in_memory

from ...interfaces.registry import RegistryProtocol
from ...interfaces.query import RunQueryProtocol
from ...interfaces.tracker import TrackerProtocol
from ...models import RunResult, RunSummaryResult, SpanLog
from ...serdes import from_json, to_dict
from ...types import Period


from flowlet.storage import StoragePath


class FileQuery(RunQueryProtocol):
    """Query historical run logs from Azure storage.

    Provides query methods for retrieving execution history, flow summaries, and
    run details. Combines data from the flow register with execution records from
    the database.

    Responsibilities:
        - Query flow runs and summaries
        - Query task runs
        - Aggregate execution statistics
        - Build hierarchical run models with parent/child relationships

    Attributes:
        register: Flow register for accessing registered flow names.

    Example:
        >>> repo = FlowQueryRepository(register=register, db_session_factory=factory)
        >>> flows = repo.list_flows()
        >>> run = repo.get_run_by_id(run_id)
    """
    def __init__(
        self,
        *,
        registry: RegistryProtocol,
        tracker: TrackerProtocol,
        base_path: StoragePath,
        **_
    ):
        """Initialize the query repository.
        """
        self.registry = registry
        self.tracker = tracker
        self.base_path = base_path

    def list_summaries(
        self,
        names: Sequence[str] | None = None,
        query: Query | None = None,
    ) -> Iterator[RunSummaryResult]:
        """
        List execution summaries for registered flows.

        Returns RunSummary models when possible (full schema), or dicts when projections are used.

        Args:
            names: include flow's with flow_names in `names`, or all if None.
            query: post-transformation on the results.

        Returns:
            Iterator[RunSummary]
        """
        flow_names = self.registry.list_flows()
        flow_names_set = set(flow_names)
        names = names if names is not None else flow_names
        for name in names:
            path = self.base_path / "runs" / f"{name}.jsonl"
            if not path.is_file():
                if name not in flow_names_set:
                    raise ValueError(f"{name} is not a registered Flow")
                continue
            result = self._load(path)
            if query is not None:
                result = jsonry.execution.in_memory.apply(query, result)
            yield from result

    def list_runs(
        self,
        runs: Sequence[UUID] | Period | None = None,
        query: Query | None = None
    ) -> Iterator[RunResult]:
        """Get runs with logs and summaries.

        Returns Run models (logs + summary) when possible, or dicts when projections are used.

        Args:
            runs: Run UUIDs or time period to filter by, or None for all runs
            query: Optional jsonry query for filtering/projection/transformation

        Returns:
            Iterator[Run]
        """
        path = self.base_path / "logs"
        if isinstance(runs, Period):
            names = (
                name
                for name in path.iterdir()
                if runs.to_uuid7().covers(UUID(self._extract_from_path(name)))
            )
        elif runs is not None:  # Sequence[UUID]
            names = (path / f"{uid}.jsonl" for uid in runs)
        else:
            names = path.iterdir()

        for name in names:
            if not name.is_file():
                raise ValueError(f"Run {self._extract_from_path(name)} does not exist")
            run = [from_json(SpanLog, l) for l in name.read_text().splitlines()]
            summary = self.tracker.summarise(run)

            # Apply query to logs if provided
            if query is not None:
                run = jsonry.execution.in_memory.apply(query, run)

            result = to_dict(summary)
            result["logs"] = run
            yield result

    @classmethod
    def _extract_from_path(cls, path: StoragePath):
        return Path(path.name).stem

    @classmethod
    def _load(cls, path: StoragePath):
        return [json.loads(log) for log in path.read_text().splitlines()]
