"""Protocol definitions for repository interfaces.

This module defines structural typing protocols for repository classes,
enabling type checking and dependency injection without concrete dependencies.
"""
from typing import AsyncIterator, Iterator, Protocol, Sequence
from uuid import UUID

from jsonry.model import Query

from ..types import Period
from .registry import RegistryProtocol


class RunQueryProtocol(Protocol):
    """Protocol for flow query repository (read operations).

    Defines the interface for querying flow and task execution history.
    Implementations should provide efficient read-only access to execution data.
    """
    registry: RegistryProtocol

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
        ...

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
        ...


class AsyncRunQueryProtocol(Protocol):
    """Protocol for flow query repository (read operations).

    Defines the interface for querying flow and task execution history.
    Implementations should provide efficient read-only access to execution data.

    Attributes:
        db_session_factory: SQLAlchemy session factory for database access.
    """
    registry: RegistryProtocol

    async def list_runs(
        self,
        names: Sequence[str] | None = None,
        query: Query | None = None,
    ) -> AsyncIterator:
        """
        List all registered flows with execution summaries.

        Optionally, we can post-transform results using `query`.

        Args:
            names: include flow's with flow_names in `names`, or all if None.
            query: post-transformation on the results.

        Returns:
            Iterator over results, the type depending on query.
        """
        ...

    async def list_logs(
        self,
        runs: Sequence[UUID] | Period | None = None,
        query: Query | None = None
    ) -> AsyncIterator:
        """Get logs for run's given by run_ids.

        Args:
            run_ids: 
            db: Optional database session.

        Returns:
            RunModel | None: Detailed run model, or None if not found.
        """
        ...