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
    # CTE to get flow run time ranges from logs
    flow_times = (
        select(
            FlowRunLogORM.run_id.label("run_id"),
            func.min(FlowRunLogORM.timestamp).label("created_at"),
            func.max(FlowRunLogORM.timestamp).label("latest_at"),
        )
        .group_by(FlowRunLogORM.run_id)
        .cte("flow_times")
    )

    # CTE to get the latest status for each flow run
    latest_flow_status = (
        select(
            FlowRunLogORM.run_id.label("run_id"),
            FlowRunLogORM.status.label("status"),
            FlowRunLogORM.log.label("error"),
            FlowRunLogORM.timestamp.label("status_at"),
        )
        .distinct(FlowRunLogORM.run_id)
        .order_by(FlowRunLogORM.run_id, FlowRunLogORM.timestamp.desc())
        .cte("latest_flow_status")
    )

    # CTE to get task run time ranges from logs
    task_times = (
        select(
            TaskRunLogORM.run_id.label("run_id"),
            func.min(TaskRunLogORM.timestamp).label("created_at"),
            func.max(TaskRunLogORM.timestamp).label("latest_at"),
        )
        .group_by(TaskRunLogORM.run_id)
        .cte("task_times")
    )

    # CTE to get the latest status for each task run
    latest_task_status = (
        select(
            TaskRunLogORM.run_id.label("run_id"),
            TaskRunLogORM.status.label("status"),
            TaskRunLogORM.log.label("log"),
            TaskRunLogORM.timestamp.label("status_at"),
        )
        .distinct(TaskRunLogORM.run_id)
        .order_by(TaskRunLogORM.run_id, TaskRunLogORM.timestamp.desc())
        .cte("latest_task_status")
    )

    # CTE to rank flows by recency within each flow name
    ranked_flows = (
        select(
            FlowRunORM.run_id,
            FlowRunORM.flow_name,
            flow_times.c.created_at,
            flow_times.c.latest_at,
            latest_flow_status.c.status,
            latest_flow_status.c.error,
            func.row_number()
            .over(
                partition_by=FlowRunORM.flow_name,
                order_by=flow_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .join(flow_times, FlowRunORM.run_id == flow_times.c.run_id)
        .join(latest_flow_status, FlowRunORM.run_id == latest_flow_status.c.run_id)
        .cte("ranked_flows")
    )

    # CTE to rank tasks by recency within each task name
    ranked_tasks = (
        select(
            TaskRunORM.run_id,
            TaskRunORM.task_name,
            TaskRunORM.flow_run_id,
            task_times.c.created_at,
            task_times.c.latest_at,
            latest_task_status.c.status,
            latest_task_status.c.log,
            func.row_number()
            .over(
                partition_by=TaskRunORM.task_name,
                order_by=task_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .join(task_times, TaskRunORM.run_id == task_times.c.run_id)
        .join(latest_task_status, TaskRunORM.run_id == latest_task_status.c.run_id)
        .cte("ranked_tasks")
    )

    # Flow queries
    read_flow_many = select(ranked_flows).where(ranked_flows.c.rnk <= bindparam("n_last"))

    read_flow_run = (
        select(
            FlowRunORM.run_id,
            FlowRunORM.flow_name,
            flow_times.c.created_at.label("started_at"),
            flow_times.c.latest_at.label("finished_at"),
            latest_flow_status.c.status,
            latest_flow_status.c.error,
        )
        .join(flow_times, FlowRunORM.run_id == flow_times.c.run_id)
        .join(latest_flow_status, FlowRunORM.run_id == latest_flow_status.c.run_id)
        .where(FlowRunORM.run_id == bindparam("run_id"))
    )

    read_flow_run_many = (
        select(
            FlowRunORM.run_id,
            FlowRunORM.flow_name,
            flow_times.c.created_at.label("started_at"),
            flow_times.c.latest_at.label("finished_at"),
            latest_flow_status.c.status,
            latest_flow_status.c.error,
        )
        .join(flow_times, FlowRunORM.run_id == flow_times.c.run_id)
        .join(latest_flow_status, FlowRunORM.run_id == latest_flow_status.c.run_id)
        .select_from(ranked_flows)
        .where(FlowRunORM.run_id == ranked_flows.c.run_id)
        .order_by(flow_times.c.created_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )

    # Task queries
    read_flow_task_run_many = (
        select(
            TaskRunORM.run_id,
            TaskRunORM.task_name,
            TaskRunORM.flow_run_id,
            FlowRunORM.flow_name,
            task_times.c.created_at.label("started_at"),
            task_times.c.latest_at.label("finished_at"),
            latest_task_status.c.status,
            latest_task_status.c.log,
        )
        .join(FlowRunORM, TaskRunORM.flow_run_id == FlowRunORM.run_id)
        .join(task_times, TaskRunORM.run_id == task_times.c.run_id)
        .join(latest_task_status, TaskRunORM.run_id == latest_task_status.c.run_id)
        .where(TaskRunORM.flow_run_id == bindparam("run_id"))
    )

    read_task_many = select(ranked_tasks).where(ranked_tasks.c.rnk <= bindparam("n_last"))

    read_task_run = (
        select(
            TaskRunORM.run_id,
            TaskRunORM.task_name,
            TaskRunORM.flow_run_id,
            task_times.c.created_at.label("started_at"),
            task_times.c.latest_at.label("finished_at"),
            latest_task_status.c.status,
            latest_task_status.c.log,
        )
        .join(task_times, TaskRunORM.run_id == task_times.c.run_id)
        .join(latest_task_status, TaskRunORM.run_id == latest_task_status.c.run_id)
        .where(TaskRunORM.run_id == bindparam("run_id"))
    )

    read_task_run_many = (
        select(
            TaskRunORM.run_id,
            TaskRunORM.task_name,
            TaskRunORM.flow_run_id,
            task_times.c.created_at.label("started_at"),
            task_times.c.latest_at.label("finished_at"),
            latest_task_status.c.status,
            latest_task_status.c.log,
        )
        .join(task_times, TaskRunORM.run_id == task_times.c.run_id)
        .join(latest_task_status, TaskRunORM.run_id == latest_task_status.c.run_id)
        .select_from(ranked_tasks)
        .where(TaskRunORM.run_id == ranked_tasks.c.run_id)
        .order_by(task_times.c.created_at.desc())
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
        for row in runs:
            name = row.flow_name
            runs_idx[name] = runs_idx.get(name, [])
            runs_idx[name].append(row)
        now = datetime.now(UTC)

        for name in self.register.list_flows():
            last_runs = runs_idx.get(name)
            res = models.FlowSummary(name=name)
            if last_runs:
                last_run = last_runs[0]
                res.last_status = last_run.status
                if last_run.latest_at and last_run.created_at:
                    res.duration = last_run.latest_at - last_run.created_at
                if last_run.latest_at:
                    res.finished_ago = now - last_run.latest_at
            results.append(res)
        return results

    def read_flow_run(self, run_id: UUID, db: Session | None=None) -> models.FlowRun | None:
        if db is None:
            with self.db_session_factory() as db:
                run = self.read_flow_run(run_id=run_id, db=db)
            return run
        row = db.execute(SQL.read_flow_run, {"run_id": run_id}).first()
        if row is not None:
            return models.FlowRun(
                run_id=row.run_id,
                flow_name=row.flow_name,
                status=row.status,
                started_at=row.started_at,
                finished_at=row.finished_at if row.status in ("success", "failed") else None,
                error=row.error if row.status == "failed" else None,
            )
        return None

    def read_flow_run_many(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRun]:
        if db is None:
            with self.db_session_factory() as db:
                runs = self.read_flow_run_many(offset=offset, limit=limit, db=db)
            return runs
        params = {"offset": offset, "limit": limit}
        rows = db.execute(SQL.read_flow_run_many, params).all()
        return [
            models.FlowRun(
                run_id=row.run_id,
                flow_name=row.flow_name,
                status=row.status,
                started_at=row.started_at,
                finished_at=row.finished_at if row.status in ("success", "failed") else None,
                error=row.error if row.status == "failed" else None,
            )
            for row in rows
        ]

    def read_run_task_many(self, run_id: UUID, db: Session | None=None) -> list[models.TaskRun]:
        if db is None:
            with self.db_session_factory() as db:
                tasks = self.read_run_task_many(run_id=run_id, db=db)
            return tasks
        rows = db.execute(SQL.read_flow_task_run_many, {"run_id": run_id}).all()
        return [
            models.TaskRun(
                run_id=row.run_id,
                task_name=row.task_name,
                flow_run_id=row.flow_run_id,
                flow_name=row.flow_name,
                started_at=row.started_at,
                finished_at=row.finished_at if row.status in ("success", "failed") else None,
                status=row.status,
                result=row.log if row.status == "success" else None,
                error=row.log if row.status == "failed" else None,
            )
            for row in rows
        ]
