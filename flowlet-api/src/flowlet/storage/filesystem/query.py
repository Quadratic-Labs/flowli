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
from ...types import Period
from ..azure.path import AzureBlobPath


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
    def __init__(self, *, registry: RegistryProtocol, base_path: AzureBlobPath | Path, **_):
        """Initialize the query repository.
        """
        self.registry = registry
        self.base_path = base_path

    def list_runs(
        self,
        names: Sequence[str] | None = None,
        query: Query | None = None,
    ) -> Iterator:
        """
        List all registered flows with execution summaries.

        Optionally, we can post-transform results using `query`.

        Args:
            names: include flow's with flow_names in `names`, or all if None.
            query: post-transformation on the results.

        Returns:
            Iterator over results, the type depending on query.
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
            result =  self._load(path)
            if query is not None:
                result = jsonry.execution.in_memory.apply(query, result)
            yield from result

    def list_logs(
        self,
        runs: Sequence[UUID] | Period | None = None,
        query: Query | None = None
    ) -> Iterator:
        """Get logs for run's given by run_ids.

        Args:
            run_ids: 
            db: Optional database session.

        Returns:
            RunModel | None: Detailed run model, or None if not found.
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
            result =  self._load(name)
            if query is not None:
                result = jsonry.execution.in_memory.apply(query, result)
            yield from result

    @classmethod
    def _extract_from_path(cls, path: AzureBlobPath | Path):
        return Path(path.name).stem

    @classmethod
    def _load(cls, path: AzureBlobPath | Path):
        return [json.loads(log) for log in path.read_text().splitlines()]
