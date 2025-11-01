from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID

from attrs import define, field
from sqlalchemy.orm import Session, aliased
from sqlalchemy import bindparam, func, select

from ..database import Run as RunORM, RunLog as RunLogORM, RunLink as RunLinkORM
from ..database import FlowRun as FlowRunORM, FlowRunLog as FlowRunLogORM
from ..database import TaskRun as TaskRunORM, TaskRunLog as TaskRunLogORM
from ..interfaces.register import FlowRegisterProtocol
from ..interfaces.repository import models


class SQL:
    # CTE to get run time ranges from logs
    run_times = (
        select(
            RunLogORM.run_id.label("run_id"),
            func.min(RunLogORM.timestamp).label("created_at"),
            func.max(RunLogORM.timestamp).label("latest_at"),
        )
        .group_by(RunLogORM.run_id)
        .cte("run_times")
    )

    # CTE to get the latest status for each run
    latest_run_status = (
        select(
            RunLogORM.run_id.label("run_id"),
            RunLogORM.status.label("status"),
            RunLogORM.log.label("log"),
            RunLogORM.timestamp.label("status_at"),
        )
        .distinct(RunLogORM.run_id)
        .order_by(RunLogORM.run_id, RunLogORM.timestamp.desc())
        .cte("latest_run_status")
    )

    # Backward compatibility aliases
    flow_times = run_times
    latest_flow_status = latest_run_status
    task_times = run_times
    latest_task_status = latest_run_status

    # CTE to rank flows by recency within each flow name
    ranked_flows = (
        select(
            RunORM.run_id,
            RunORM.name.label("flow_name"),
            run_times.c.created_at,
            run_times.c.latest_at,
            latest_run_status.c.status,
            latest_run_status.c.log.label("error"),
            func.row_number()
            .over(
                partition_by=RunORM.name,
                order_by=run_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .where(RunORM.run_type == "flow")
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .cte("ranked_flows")
    )

    # CTE to rank tasks by recency within each task name
    ranked_tasks = (
        select(
            RunORM.run_id,
            RunORM.name.label("task_name"),
            RunLinkORM.parent_run_id.label("flow_run_id"),
            run_times.c.created_at,
            run_times.c.latest_at,
            latest_run_status.c.status,
            latest_run_status.c.log,
            func.row_number()
            .over(
                partition_by=RunORM.name,
                order_by=run_times.c.created_at.desc(),
            )
            .label("rnk"),
        )
        .where(RunORM.run_type == "task")
        .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .cte("ranked_tasks")
    )

    # Flow queries
    read_flow_many = select(ranked_flows).where(ranked_flows.c.rnk <= bindparam("n_last"))

    read_flow_run = (
        select(
            RunORM.run_id,
            RunORM.name.label("flow_name"),
            run_times.c.created_at.label("started_at"),
            run_times.c.latest_at.label("finished_at"),
            latest_run_status.c.status,
            latest_run_status.c.log.label("error"),
        )
        .where(RunORM.run_type == "flow")
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .where(RunORM.run_id == bindparam("run_id"))
    )

    read_flow_run_many = (
        select(
            RunORM.run_id,
            RunORM.name.label("flow_name"),
            run_times.c.created_at.label("started_at"),
            run_times.c.latest_at.label("finished_at"),
            latest_run_status.c.status,
            latest_run_status.c.log.label("error"),
        )
        .where(RunORM.run_type == "flow")
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .select_from(ranked_flows)
        .where(RunORM.run_id == ranked_flows.c.run_id)
        .order_by(run_times.c.created_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )

    # Task queries
    FlowRunAlias = aliased(RunORM)
    read_flow_task_run_many = (
        select(
            RunORM.run_id,
            RunORM.name.label("task_name"),
            RunLinkORM.parent_run_id.label("flow_run_id"),
            FlowRunAlias.name.label("flow_name"),
            run_times.c.created_at.label("started_at"),
            run_times.c.latest_at.label("finished_at"),
            latest_run_status.c.status,
            latest_run_status.c.log,
        )
        .where(RunORM.run_type == "task")
        .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
        .join(FlowRunAlias, RunLinkORM.parent_run_id == FlowRunAlias.run_id)
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .where(RunLinkORM.parent_run_id == bindparam("run_id"))
    )

    read_task_many = select(ranked_tasks).where(ranked_tasks.c.rnk <= bindparam("n_last"))

    read_task_run = (
        select(
            RunORM.run_id,
            RunORM.name.label("task_name"),
            RunLinkORM.parent_run_id.label("flow_run_id"),
            run_times.c.created_at.label("started_at"),
            run_times.c.latest_at.label("finished_at"),
            latest_run_status.c.status,
            latest_run_status.c.log,
        )
        .where(RunORM.run_type == "task")
        .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .where(RunORM.run_id == bindparam("run_id"))
    )

    read_task_run_many = (
        select(
            RunORM.run_id,
            RunORM.name.label("task_name"),
            RunLinkORM.parent_run_id.label("flow_run_id"),
            run_times.c.created_at.label("started_at"),
            run_times.c.latest_at.label("finished_at"),
            latest_run_status.c.status,
            latest_run_status.c.log,
        )
        .where(RunORM.run_type == "task")
        .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
        .join(run_times, RunORM.run_id == run_times.c.run_id)
        .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
        .select_from(ranked_tasks)
        .where(RunORM.run_id == ranked_tasks.c.run_id)
        .order_by(run_times.c.created_at.desc())
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
