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

    def list_summaries(
        self,
        names: Sequence[str] | None = None,
        query: Query | None = None,
    ) -> Iterator:
        """
        List execution summaries for registered flows.

        Returns RunSummary models when possible (full schema), or dicts when projections are used.

        Args:
            names: include flow's with flow_names in `names`, or all if None.
            query: post-transformation on the results.

        Returns:
            Iterator[RunSummary | dict]: RunSummary models if full schema is available,
                                         otherwise dicts with projected fields
        """
        ...

    def list_runs(
        self,
        runs: Sequence[UUID] | Period | None = None,
        query: Query | None = None
    ) -> Iterator:
        """Get runs with logs and summaries.

        Returns Run models (logs + summary) when possible, or dicts when projections are used.

        Args:
            runs: Run UUIDs or time period to filter by, or None for all runs
            query: Optional jsonry query for filtering/projection/transformation

        Returns:
            Iterator[Run | dict]: Run models (with logs and summary) if full schema is available,
                                  otherwise dicts with projected fields
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

    async def list_summaries(
        self,
        names: Sequence[str] | None = None,
        query: Query | None = None,
    ) -> AsyncIterator:
        """
        List execution summaries for registered flows.

        Returns RunSummary models when possible (full schema), or dicts when projections are used.

        Args:
            names: include flow's with flow_names in `names`, or all if None.
            query: post-transformation on the results.

        Returns:
            AsyncIterator[RunSummary | dict]: RunSummary models if full schema is available,
                                               otherwise dicts with projected fields
        """
        ...

    async def list_runs(
        self,
        runs: Sequence[UUID] | Period | None = None,
        query: Query | None = None
    ) -> AsyncIterator:
        """Get runs with logs and summaries.

        Returns Run models (logs + summary) when possible, or dicts when projections are used.

        Args:
            runs: Run UUIDs or time period to filter by, or None for all runs
            query: Optional jsonry query for filtering/projection/transformation

        Returns:
            AsyncIterator[Run | dict]: Run models (with logs and summary) if full schema is available,
                                        otherwise dicts with projected fields
        """
        ...