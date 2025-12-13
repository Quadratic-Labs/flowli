"""Repository for tracking flow and task execution lifecycle events.

This module provides the FlowTracker class which handles all write operations
for recording flow and task execution in the database.
"""
from uuid import uuid4

from sqlalchemy.orm import Session

from ..database import Run as RunORM, RunLink as RunLinkORM, RunLog as RunLogORM
from ..interfaces.repository.models import RunAttrModel, RunLogAttrModel


class FlowTracker:
    """Repository for tracking flow and task execution (write operations only).

    Used exclusively by execution context managers (ExecutionContext, FlowContext,
    TaskContext) to record execution lifecycle events. Has no dependencies on
    FlowRegister to maintain separation of concerns.

    Responsibilities:
        - Create run records for flows and tasks
        - Log status changes and errors
        - Manage parent-child relationships between runs

    Attributes:
        db_session_factory: SQLAlchemy session factory for database access.

    Example:
        >>> tracker = FlowTracker(db_session_factory=session_factory)
        >>> run = tracker.create_run(RunAttrModel(name="my_flow", run_type="flow"))
        >>> tracker.log(RunLogAttrModel(run_id=run.run_id, status="running"))
    """
    def __init__(self, *, db_session_factory, **_):
        """Initialize the flow tracker.

        Args:
            db_session_factory: SQLAlchemy session factory.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.db_session_factory = db_session_factory

    def create_run(self, data: RunAttrModel, db: Session | None=None) -> RunAttrModel:
        """Create a new run record in the database.

        Args:
            data: Run attributes including name, type, and ID.
            db: Optional existing database session. If None, creates a new session.

        Returns:
            RunAttrModel: The run attributes (same as input).

        Example:
            >>> run = tracker.create_run(
            ...     RunAttrModel(name="my_flow", run_type="flow", run_id=uuid4())
            ... )
        """
        if db is None:
            with self.db_session_factory() as db:
                run = self.create_run(data, db=db)
            return run

        # Create run record
        run = RunORM.from_attrs(data)
        db.add(run)
        db.commit()
        return data

    def link_runs(self, parent: RunAttrModel | None, child: RunAttrModel | None, db: Session | None=None) -> None:
        """Create a parent-child relationship between two runs.

        Links a task run to its parent flow run, or a nested task to its parent task.
        Does nothing if either parent or child is None.

        Args:
            parent: Parent run attributes, or None.
            child: Child run attributes, or None.
            db: Optional existing database session. If None, creates a new session.

        Example:
            >>> tracker.link_runs(flow_run, task_run)
        """
        if db is None:
            with self.db_session_factory() as db:
                result = self.link_runs(parent, child, db=db)
            return result

        if parent is None or child is None:
            return
        link = RunLinkORM(
            link_id=uuid4(),
            parent_run_id=parent.run_id,
            child_run_id=child.run_id,
        )
        db.add(link)
        db.commit()

    def log(self, data: RunLogAttrModel, db: Session | None = None) -> RunLogAttrModel:
        """Add a log entry to a run.

        Records status changes and error messages for a run. Logs are append-only
        and provide an audit trail of execution.

        Args:
            data: Log entry data including run_id, status, timestamp, and optional log message.
            db: Optional existing database session. If None, creates a new session.

        Returns:
            RunLogAttrModel: The log entry attributes (same as input).

        Example:
            >>> tracker.log(RunLogAttrModel(
            ...     run_id=run.run_id,
            ...     status="success",
            ...     log="Flow completed successfully"
            ... ))
        """
        if db is None:
            with self.db_session_factory() as db:
                result = self.log(data, db=db)
            return result

        db.add(RunLogORM.from_attrs(data))
        db.commit()
        return data
