from datetime import UTC, datetime, timedelta
from typing import Protocol, Self
from uuid import UUID

from attrs import define, field
from sqlalchemy.orm import Session, aliased
from sqlalchemy import bindparam, func, select

from .database import FlowRun as FlowRunORM, TaskRun as TaskRunORM


class FlowRegisterProtocol(Protocol):
    """Protocol defining the interface for flow registration.

    This protocol breaks the circular dependency between FlowRepository
    and FlowRegister by defining only the interface that FlowRepository
    needs, without importing FlowRegister directly.
    """

    def list_flows(self) -> list[str]:
        """Return list of registered flow names."""
        ...

    def list_tasks(self) -> list[str]:
        """Return list of registered task names."""
        ...


@define(slots=True)
class FlowSummary:
    """Domain model for a flow runs' summary."""
    name: str
    last_status: str | None = field(default=None)
    finished_ago: timedelta | None = field(default=None)
    duration: timedelta | None = field(default=None)
    doc: str | None = field(default=None)


@define(slots=True)
class FlowRun:
    """Domain model for a flow run."""
    run_id: UUID
    flow_name: str
    status: str
    started_at: datetime
    finished_at: datetime | None = field(default=None)
    error: str | None = field(default=None)

    @classmethod
    def from_orm(cls, orm: FlowRunORM) -> Self:
        """Create domain model from ORM object."""
        return cls(
            run_id=orm.run_id,
            flow_name=orm.flow_name,
            started_at=orm.started_at,
            finished_at=orm.finished_at,
            status=orm.status,
            error=orm.error,
        )


@define(slots=True)
class TaskRun:
    """Domain model for a task run."""
    run_id: UUID
    task_name: str
    flow_run_id: UUID
    flow_name: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    result: str | None
    error: str | None

    @classmethod
    def from_orm(cls, orm: TaskRunORM) -> Self:
        """Create domain model from ORM object."""
        return cls(
            run_id=orm.run_id,
            task_name=orm.task_name,
            flow_run_id=orm.flow_run_id,
            flow_name=orm.flow_name,
            started_at=orm.started_at,
            finished_at=orm.finished_at,
            status=orm.status,
            result=orm.result,
            error=orm.error,
        )


class SQL:
    read_ranked_flow = (
        select(
            FlowRunORM,
            func.row_number()
            .over(
                partition_by=FlowRunORM.flow_name,
                order_by=FlowRunORM.started_at.desc(),
            )
            .label("rnk"),
        )
    )
    _ranked = read_ranked_flow.subquery()
    _ranked_alias = aliased(FlowRunORM, _ranked)
    read_flow_many = (
        select(_ranked_alias)
        .where(_ranked.c.rnk <= bindparam("limit"))
        .order_by(_ranked.c.flow_name, _ranked.c.started_at.desc())
    )
    read_flow_run = (
        select(FlowRunORM)
        .where(FlowRunORM.run_id == bindparam("run_id"))
    )
    read_flow_run_many = (
        select(FlowRunORM)
        .order_by(FlowRunORM.started_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )
    read_run_task_many = (
        select(TaskRunORM)
        .where(TaskRunORM.flow_run_id == bindparam("run_id"))
    )


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
                       status: str = "running", db: Session | None = None) -> FlowRunORM:
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

        flow_run = FlowRunORM(
            run_id=run_id,
            flow_name=flow_name,
            started_at=started_at,
            status=status,
        )
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


class FlowQueryRepository:
    """
    Repository for querying flow and task execution data (read operations only).

    Used by API endpoints and UI to retrieve execution history and statistics.
    Depends on FlowRegisterProtocol to combine registered flows with their runs.

    Responsibilities:
    - Query flow runs and summaries
    - Query task runs
    - Aggregate execution statistics
    """

    def __init__(self, *, register: FlowRegisterProtocol, db_session_factory, **_):
        self.register = register
        self.db_session_factory = db_session_factory

    def read_flow_many(self, limit: int=1, db: Session | None=None) -> list[FlowSummary]:
        if db is None:
            with self.db_session_factory() as db:
                results = self.read_flow_many(limit=limit, db=db)
            return results
        results = []
        runs = db.scalars(SQL.read_flow_many, {"limit": limit}).all()
        runs_idx = {}
        for r in runs:
            name = r.flow_name
            runs_idx[name] = runs_idx.get(name, [])
            runs_idx[name].append(r)
        now = datetime.now(UTC)

        for name in self.register.list_flows():
            last_runs = runs_idx.get(name)
            res = FlowSummary(name=name)
            if last_runs:
                last_run = last_runs[0]
                res.last_status = last_run.status
                if last_run.finished_at and last_run.started_at:
                    res.duration = (last_run.finished_at - last_run.started_at).total_seconds()
                if last_run.finished_at:
                    res.finished_ago = now - last_run.finished_at
            results.append(res)
        return results

    def read_flow_run(self, run_id: UUID, db: Session | None=None) -> FlowRun | None:
        if db is None:
            with self.db_session_factory() as db:
                run = self.read_flow_run(run_id=run_id, db=db)
            return run
        run_orm = db.scalar(SQL.read_flow_run, {"run_id": run_id})
        if run_orm is not None:
            return FlowRun.from_orm(run_orm)
        return None

    def read_flow_run_many(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[FlowRun]:
        if db is None:
            with self.db_session_factory() as db:
                runs = self.read_flow_run_many(offset=offset, limit=limit, db=db)
            return runs
        params = {"offset": offset, "limit": limit}
        runs_orm = db.scalars(SQL.read_flow_run_many, params).all()
        return [FlowRun.from_orm(run) for run in runs_orm]

    def read_run_task_many(self, run_id: UUID, db: Session | None=None) -> list[TaskRun]:
        if db is None:
            with self.db_session_factory() as db:
                tasks = self.read_run_task_many(run_id=run_id, db=db)
            return tasks
        tasks_orm = db.scalars(SQL.read_run_task_many, {"run_id": run_id}).all()
        return [TaskRun.from_orm(task) for task in tasks_orm]
