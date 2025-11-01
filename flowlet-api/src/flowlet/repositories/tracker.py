from sqlalchemy.orm import Session

from ..interfaces.repository.models import FlowRun, FlowRunLog
from ..database import FlowRunLog as FlowRunLogORM


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

    def log_flow_run(self, data: FlowRunLog, db: Session | None = None) -> FlowRun:
        """Create a new flow run record."""
        if db is None:
            with self.db_session_factory() as db:
                flow_run = self.log_flow_run(data, db)
            return flow_run
        db.add(flow_run)
        db.commit()
        return flow_run

    def update_flow_run(self, flow_run: FlowRunORM, finished_at: datetime | None = None,
                       status: str | None = None, error: str | None = None,
                       db: Session | None = None) -> None:
        """Update an existing flow run record."""
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

        if finished_at is not None:
            flow_run.finished_at = finished_at
        if status is not None:
            flow_run.status = status
        if error is not None:
            flow_run.error = error

        db.add(flow_run)
        db.commit()

    def create_task_run(self, run_id: UUID, task_name: str, flow_run_id: UUID,
                       flow_name: str, started_at: datetime, status: str = "running",
                       db: Session | None = None) -> TaskRunORM:
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

        task_run = TaskRunORM(
            run_id=run_id,
            task_name=task_name,
            flow_run_id=flow_run_id,
            flow_name=flow_name,
            started_at=started_at,
            status=status,
        )
        db.add(task_run)
        db.commit()
        return task_run

    def update_task_run(self, task_run: TaskRunORM, finished_at: datetime | None = None,
                       status: str | None = None, result: str | None = None,
                       error: str | None = None, db: Session | None = None) -> None:
        """Update an existing task run record."""
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

        if finished_at is not None:
            task_run.finished_at = finished_at
        if status is not None:
            task_run.status = status
        if result is not None:
            task_run.result = result
        if error is not None:
            task_run.error = error

        db.add(task_run)
        db.commit()