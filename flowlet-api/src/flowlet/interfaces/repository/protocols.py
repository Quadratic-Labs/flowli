"""Protocol definitions for repository interfaces.

This module defines structural typing protocols for repository classes,
enabling type checking and dependency injection without concrete dependencies.
"""
from typing import Protocol, Self
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from . import models


class FlowQueryRepositoryProtocol(Protocol):
    """Protocol for flow query repository (read operations).

    Defines the interface for querying flow and task execution history.
    Implementations should provide efficient read-only access to execution data.

    Attributes:
        db_session_factory: SQLAlchemy session factory for database access.
    """
    db_session_factory: sessionmaker

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


class FlowTrackerProtocol(Protocol):
    """Protocol for flow tracker (write operations).

    Defines the interface for recording flow and task execution lifecycle events.
    Implementations should handle database writes for runs, logs, and relationships.

    Attributes:
        db_session_factory: SQLAlchemy session factory for database access.
    """
    db_session_factory: sessionmaker

    def create_run(self, data: models.RunAttrModel, db: Session | None = None) -> models.RunAttrModel:
        """Create a new run record in the database.

        Args:
            data: Run attributes including name, type, and ID.
            db: Optional database session.

        Returns:
            RunAttrModel: The created run attributes.
        """
        ...

    def link_runs(self, parent: models.RunAttrModel | None, child: models.RunAttrModel | None, db: Session | None = None) -> None:
        """Create a parent-child relationship between runs.

        Args:
            parent: Parent run attributes, or None.
            child: Child run attributes, or None.
            db: Optional database session.
        """
        ...

    def log(self, data: models.RunLogAttrModel, db: Session | None = None) -> models.RunLogAttrModel:
        """Add a log entry to a run.

        Args:
            data: Log entry data including run_id, status, and message.
            db: Optional database session.

        Returns:
            RunLogAttrModel: The created log entry attributes.
        """
        ...