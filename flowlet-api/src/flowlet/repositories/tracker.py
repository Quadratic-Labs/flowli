from datetime import datetime, UTC
from uuid import UUID

from sqlalchemy.orm import Session

from ..interfaces.repository.models import FlowRun, FlowRunLog
from ..database import Run as RunORM, RunLog as RunLogORM, RunLink as RunLinkORM
from ..database import FlowRunLog as FlowRunLogORM, TaskRun as TaskRunORM


class FlowTracker:
    """
    Repository for tracking flow and task execution (write operations only).

    Used exclusively by decorators (FlowContext, TaskContext) to record
    execution lifecycle events. Has no dependencies on FlowRegister.

    Responsibilities:
    - Create flow/task run records
    - Update flow/task run status, timing, and errors
    """
    def __init__(self, *, db_session_factory, **_):
        self.db_session_factory = db_session_factory

    def create_flow_run(self, run_id: UUID, flow_name: str, started_at: datetime,
                       status: str = "running", db: Session | None = None) -> RunORM:
        """Create a new flow run record."""
        if db is None:
            with self.db_session_factory() as db:
                flow_run = self.create_flow_run(
                    run_id=run_id,
                    flow_name=flow_name,
                    started_at=started_at,
                    status=status,
                    db=db
                )
            return flow_run

        flow_run = RunORM(
            run_id=run_id,
            name=flow_name,
            run_type="flow",
        )
        db.add(flow_run)

        # Create initial log entry
        log_entry = RunLogORM(
            run_id=run_id,
            timestamp=started_at,
            status=status,
            log="",
        )
        db.add(log_entry)
        db.commit()
        return flow_run

    def update_flow_run(self, flow_run: RunORM, finished_at: datetime | None = None,
                       status: str | None = None, error: str | None = None,
                       db: Session | None = None) -> None:
        """Update an existing flow run record by adding a log entry."""
        if db is None:
            with self.db_session_factory() as db:
                self.update_flow_run(
                    flow_run=flow_run,
                    finished_at=finished_at,
                    status=status,
                    error=error,
                    db=db
                )
            return

        # Create a log entry for the update
        if status is not None or error is not None:
            log_entry = RunLogORM(
                run_id=flow_run.run_id,
                timestamp=finished_at or datetime.now(UTC),
                status=status or "",
                log=error or "",
            )
            db.add(log_entry)
            db.commit()

    def create_task_run(self, run_id: UUID, task_name: str, flow_run_id: UUID,
                       flow_name: str, started_at: datetime, status: str = "running",
                       db: Session | None = None) -> RunORM:
        """Create a new task run record."""
        if db is None:
            with self.db_session_factory() as db:
                task_run = self.create_task_run(
                    run_id=run_id,
                    task_name=task_name,
                    flow_run_id=flow_run_id,
                    flow_name=flow_name,
                    started_at=started_at,
                    status=status,
                    db=db
                )
            return task_run

        task_run = RunORM(
            run_id=run_id,
            name=task_name,
            run_type="task",
        )
        db.add(task_run)

        # Create initial log entry
        log_entry = RunLogORM(
            run_id=run_id,
            timestamp=started_at,
            status=status,
            log="",
        )
        db.add(log_entry)

        # Create link between task and flow
        link = RunLinkORM(
            parent_run_id=flow_run_id,
            child_run_id=run_id,
        )
        db.add(link)

        db.commit()
        return task_run

    def update_task_run(self, task_run: RunORM, finished_at: datetime | None = None,
                       status: str | None = None, result: str | None = None,
                       error: str | None = None, db: Session | None = None) -> None:
        """Update an existing task run record by adding a log entry."""
        if db is None:
            with self.db_session_factory() as db:
                self.update_task_run(
                    task_run=task_run,
                    finished_at=finished_at,
                    status=status,
                    result=result,
                    error=error,
                    db=db
                )
            return

        # Create a log entry for the update
        if status is not None or error is not None or result is not None:
            log_text = error or result or ""
            log_entry = RunLogORM(
                run_id=task_run.run_id,
                timestamp=finished_at or datetime.now(UTC),
                status=status or "",
                log=log_text,
            )
            db.add(log_entry)
            db.commit()