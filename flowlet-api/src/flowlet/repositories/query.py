from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID

from attrs import define, field
from sqlalchemy.orm import Session, aliased
from sqlalchemy import bindparam, func, select

from ..database import FlowRun as FlowRunORM, FlowRunLog as FlowRunLogORM
from ..database import TaskRun as TaskRunORM, TaskRunLog as TaskRunLogORM
from ..interfaces.register import FlowRegisterProtocol
from ..interfaces.repository import models


class SQL:
    flow_times = (
        select(
            FlowRunLogORM.run_id.label("run_id"),
            func.min(FlowRunLogORM.at).label("created_at"),
            func.max(FlowRunLogORM.at).label("latest_at"),
        )
        .group_by(FlowRunLogORM.run_id)
        .cte("flow_times")
    )
    ranked_flows = (
        select(
            FlowRunORM,
            flow_times.c.created_at,
            flow_times.c.latest_at,
            func.row_number()
            .over(
                partition_by=FlowRunORM.flow_name,
                order_by=flow_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .join(flow_times, FlowRunORM.run_id == flow_times.c.run_id)
        .cte("ranked_flows")
    )
    read_flow_many = select(ranked_flows).where(ranked_flows.c.rnk <= bindparam("limit"))
    read_flow_run = (
        select(FlowRunORM)
        .where(FlowRunORM.run_id == bindparam("run_id"))
    )
    read_flow_run_many = (
        select(FlowRunORM)
        .select_from(ranked_flows)
        .order_by(ranked_flows.c.created_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )
    read_flow_task_run_many = (
        select(TaskRunORM)
        .where(TaskRunORM.flow_run_id == bindparam("run_id"))
    )

    task_times = (
        select(
            TaskRunLogORM.run_id.label("run_id"),
            func.min(TaskRunLogORM.at).label("created_at"),
            func.max(TaskRunLogORM.at).label("latest_at"),
        )
        .group_by(TaskRunLogORM.run_id)
        .cte("task_times")
    )
    ranked_tasks = (
        select(
            TaskRunORM,
            task_times.c.created_at,
            task_times.c.latest_at,
            func.row_number()
            .over(
                partition_by=TaskRunORM.task_name,
                order_by=task_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .join(task_times, TaskRunORM.run_id == task_times.c.run_id)
        .cte("ranked_tasks")
    )
    read_task_many = select(ranked_tasks).where(ranked_tasks.c.rnk <= bindparam("n_last"))
    read_task_run = (
        select(TaskRunORM)
        .where(TaskRunORM.run_id == bindparam("run_id"))
    )
    read_task_run_many = (
        select(TaskRunORM)
        .select_from(ranked_tasks)
        .order_by(ranked_tasks.c.created_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )




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

    def read_flow_many(self, n_last: int=1, db: Session | None=None) -> list[models.FlowSummary]:
        if db is None:
            with self.db_session_factory() as db:
                results = self.read_flow_many(n_last=n_last, db=db)
            return results
        results = []
        runs = db.execute(SQL.read_flow_many, {"n_last": n_last}).all()
        runs_idx = {}
        for r in runs:
            name = r.flow_name
            runs_idx[name] = runs_idx.get(name, [])
            runs_idx[name].append(r)
        now = datetime.now(UTC)

        for name in self.register.list_flows():
            last_runs = runs_idx.get(name)
            res = models.FlowSummary(name=name)
            if last_runs:
                last_run = last_runs[0]
                res.last_status = last_run.status
                if last_run.finished_at and last_run.started_at:
                    res.duration = (last_run.finished_at - last_run.started_at).total_seconds()
                if last_run.finished_at:
                    res.finished_ago = now - last_run.finished_at
            results.append(res)
        return results

    def read_flow_run(self, run_id: UUID, db: Session | None=None) -> models.FlowRun | None:
        if db is None:
            with self.db_session_factory() as db:
                run = self.read_flow_run(run_id=run_id, db=db)
            return run
        run_orm = db.scalar(SQL.read_flow_run, {"run_id": run_id})
        if run_orm is not None:
            return models.FlowRun.from_orm(run_orm)
        return None

    def read_flow_run_many(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRun]:
        if db is None:
            with self.db_session_factory() as db:
                runs = self.read_flow_run_many(offset=offset, limit=limit, db=db)
            return runs
        params = {"offset": offset, "limit": limit}
        runs_orm = db.scalars(SQL.read_flow_run_many, params).all()
        return [models.FlowRun.from_orm(run) for run in runs_orm]

    def read_run_task_many(self, run_id: UUID, db: Session | None=None) -> list[models.TaskRun]:
        if db is None:
            with self.db_session_factory() as db:
                tasks = self.read_run_task_many(run_id=run_id, db=db)
            return tasks
        tasks_orm = db.scalars(SQL.read_run_task_many, {"run_id": run_id}).all()
        return [models.TaskRun.from_orm(task) for task in tasks_orm]
