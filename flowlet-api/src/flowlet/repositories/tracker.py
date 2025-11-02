from uuid import UUID

from ..database import Run as RunORM, RunLog as RunLogORM, RunLink as RunLinkORM

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from ..interfaces.repository.models import RunAttrModel, RunLogAttrModel


class FlowTracker:
    """
    Repository for tracking flow and task execution (write operations only).

    Used exclusively by decorators (ExecutionContext, FlowContext, TaskContext) to record
    execution lifecycle events. Has no dependencies on FlowRegister.

    Responsibilities:
    - Create unified run records (flows and tasks)
    - Update run status, timing, and errors
    - Manage run links (parent-child relationships)
    """
    def __init__(self, *, db_session_factory, **_):
        self.db_session_factory = db_session_factory

    def create_run(self, data: RunAttrModel, db: Session | None=None) -> RunAttrModel:
        """
        Create a new run record (flow or task).

        Args:
            run_id: Unique identifier for the run
            name: Name of the flow or task
            run_type: "flow" or "task"
            started_at: Start timestamp
            parent_run_id: Optional parent run ID for nested contexts
            status: Initial status (default "running")
            db: Optional database session
        """
        if db is None:
            with self.db_session_factory() as db:
                run = self.create_run(data, db=db)
            return run

        # Create run record
        run = RunORM.from_attr(data)
        db.add(run)
        db.commit()
        return data

    def link_runs(self, parent: RunAttrModel | None, child: RunAttrModel | None, db: Session | None=None) -> None:
        if db is None:
            with self.db_session_factory() as db:
                result = self.link_runs(parent, child, db=db)
            return result

        if parent is None or child is None:
            return
        link = RunLinkORM(
            parent_run_id=parent.run_id,
            child_run_id=child.run_id,
        )
        db.add(link)
        db.commit()

    def log(self, data: RunLogAttrModel, db: Session | None = None) -> RunLogAttrModel:
        """
        Add a log record to an existing run record.

        Args:
        """
        if db is None:
            with self.db_session_factory() as db:
                result = self.log(data, db=db)
            return result

        db.add(RunLogORM.from_attr(data))
        db.commit()
        return data
