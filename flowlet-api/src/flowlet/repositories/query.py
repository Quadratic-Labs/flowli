from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy import bindparam, func, select

from ..database import Run as RunORM, RunLog as RunLogORM
from ..interfaces.register import FlowRegisterProtocol
from ..interfaces.repository import models


class SQL:
    get_run_by_id = (
        select(RunORM)
        .where(RunORM.run_id == bindparam("run_id"))
    )

    run_times = (
        select(
            RunLogORM.run_id.label("run_id"),
            func.min(RunLogORM.timestamp).label("earliest_at"),
            func.max(RunLogORM.timestamp).label("latest_at"),
        )
        .group_by(RunLogORM.run_id)
    )
    run_times_cte = run_times.cte("run_times")

    # Subquery to get the latest timestamp for each run
    latest_timestamp_per_run = (
        select(
            RunLogORM.run_id,
            func.max(RunLogORM.timestamp).label("max_timestamp")
        )
        .group_by(RunLogORM.run_id)
        .subquery()
    )

    # CTE to get the latest log for each run
    latest_run_log = (
        select(RunLogORM)
        .join(
            latest_timestamp_per_run,
            (RunLogORM.run_id == latest_timestamp_per_run.c.run_id) &
            (RunLogORM.timestamp == latest_timestamp_per_run.c.max_timestamp)
        )
    )
    latest_run_log_cte = latest_run_log.cte("latest_run_log")

    latest_run_by_flow_name = (
        latest_run_log
        .join(RunORM, RunORM.run_id == RunLogORM.run_id)
        .where(RunORM.name == bindparam("name"))
    )

    # CTE to rank flow runs by recency within each flow name
    ranked_flow_runs = (
        select(
            RunORM.run_id,
            RunORM.name,
            run_times_cte.c.earliest_at,
            run_times_cte.c.latest_at,
            latest_run_log_cte.c.status,
            latest_run_log_cte.c.log,
            func.row_number()
            .over(
                partition_by=RunORM.name,
                order_by=run_times_cte.c.earliest_at.desc(),
            )
            .label("rnk"),
        )
        .select_from(RunORM)
        .where(RunORM.run_type == "flow")
        .join(run_times_cte, RunORM.run_id == run_times_cte.c.run_id)
        .join(latest_run_log_cte, RunORM.run_id == latest_run_log_cte.c.run_id)
        .cte("ranked_flow_runs")
    )

    list_flows = select(ranked_flow_runs).where(ranked_flow_runs.c.rnk <= bindparam("n_last"))

    # # CTE to rank tasks by recency within each task name
    # task_runs_by_flow_id = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name,
    #         run_times.c.earliest_at,
    #         run_times.c.latest_at,
    #     )
    #     .select_from(RunORM)
    #     .where(RunORM.run_type == "Task")
    #     .join(RunLinkORM, RunLinkORM.child_run_id == RunORM.run_id)
    #     .where(RunLinkORM.parent_run_id == bindparam("flow_id"))
    #     .join(run_times, run_times.c.run_id == RunORM.run_id)
    # )

    # task_runs = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name,
    #         RunLinkORM.parent_run_id.label("flow_run_id"),
    #         latest_run_log.c.status,
    #         func.row_number()
    #         .over(
    #             partition_by=RunORM.name,
    #             order_by=run_times.c.created_at.desc(),
    #         )
    #         .label("rnk"),
    #     )
    #     .where(RunORM.run_type == "task")
    #     .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
    #     .join(run_times, RunORM.run_id == run_times.c.run_id)
    #     .join(latest_run_log, RunORM.run_id == latest_run_log.c.run_id)
    #     .cte("ranked_task_runs")
    # )

    # # Flow queries
    # get_flow_run_by_id = select(ranked_flow_runs).where(RunORM.run_id == bindparam("run_id"))

    # read_flow_run_many = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name.label("flow_name"),
    #         run_times.c.created_at.label("started_at"),
    #         run_times.c.latest_at.label("finished_at"),
    #         latest_run_log.c.status,
    #         latest_run_log.c.log,
    #     )
    #     .where(RunORM.run_type == "flow")
    #     .join(run_times, RunORM.run_id == run_times.c.run_id)
    #     .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
    #     .select_from(ranked_flows)
    #     .where(RunORM.run_id == ranked_flows.c.run_id)
    #     .order_by(run_times.c.created_at.desc())
    #     .limit(bindparam("limit"))
    #     .offset(bindparam("offset"))
    # )

    # # Task queries
    # FlowRunAlias = aliased(RunORM)
    # read_flow_task_run_many = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name.label("task_name"),
    #         RunLinkORM.parent_run_id.label("flow_run_id"),
    #         FlowRunAlias.name.label("flow_name"),
    #         run_times.c.created_at.label("started_at"),
    #         run_times.c.latest_at.label("finished_at"),
    #         latest_run_status.c.status,
    #         latest_run_status.c.log,
    #     )
    #     .where(RunORM.run_type == "task")
    #     .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
    #     .join(FlowRunAlias, RunLinkORM.parent_run_id == FlowRunAlias.run_id)
    #     .join(run_times, RunORM.run_id == run_times.c.run_id)
    #     .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
    #     .where(RunLinkORM.parent_run_id == bindparam("run_id"))
    # )

    # read_task_many = select(ranked_tasks).where(ranked_tasks.c.rnk <= bindparam("n_last"))

    # read_task_run = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name.label("task_name"),
    #         RunLinkORM.parent_run_id.label("flow_run_id"),
    #         run_times.c.created_at.label("started_at"),
    #         run_times.c.latest_at.label("finished_at"),
    #         latest_run_status.c.status,
    #         latest_run_status.c.log,
    #     )
    #     .where(RunORM.run_type == "task")
    #     .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
    #     .join(run_times, RunORM.run_id == run_times.c.run_id)
    #     .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
    #     .where(RunORM.run_id == bindparam("run_id"))
    # )

    # read_task_run_many = (
    #     select(
    #         RunORM.run_id,
    #         RunORM.name.label("task_name"),
    #         RunLinkORM.parent_run_id.label("flow_run_id"),
    #         run_times.c.created_at.label("started_at"),
    #         run_times.c.latest_at.label("finished_at"),
    #         latest_run_status.c.status,
    #         latest_run_status.c.log,
    #     )
    #     .where(RunORM.run_type == "task")
    #     .join(RunLinkORM, RunORM.run_id == RunLinkORM.child_run_id)
    #     .join(run_times, RunORM.run_id == run_times.c.run_id)
    #     .join(latest_run_status, RunORM.run_id == latest_run_status.c.run_id)
    #     .select_from(ranked_tasks)
    #     .where(RunORM.run_id == ranked_tasks.c.run_id)
    #     .order_by(run_times.c.created_at.desc())
    #     .limit(bindparam("limit"))
    #     .offset(bindparam("offset"))
    # )


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

    def list_flows(self, n_last_runs: int=1, db: Session | None=None) -> list[models.FlowSummary]:
        if db is None:
            with self.db_session_factory() as db:
                results = self.list_flows(n_last_runs=n_last_runs, db=db)
            return results
        results = []
        runs = db.execute(SQL.list_flows, {"n_last": n_last_runs}).all()
        runs_idx = {}
        for row in runs:
            name = row.flow_name
            runs_idx[name] = runs_idx.get(name, [])
            runs_idx[name].append(row)

        for name in self.register.list_flows():
            last_runs = runs_idx.get(name)
            res = models.FlowSummary(name=name)
            if last_runs:
                run = last_runs[0]
                res.status = run.status
                res.started_at = run.earliest_at
                res.ended_at = run.latest_at
            results.append(res)
        return results

    def list_runs(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List runs summaries."""
        if db is None:
            with self.db_session_factory() as db:
                runs = self.list_runs(offset=offset, limit=limit, db=db)
            return runs
        rows = db.scalars(SQL.latest_run_log.offset(offset).limit(limit)).all()
        results = []
        for row in rows:
            results.append(models.FlowRunSummary(
                name=row.run.name,
                run_id=row.run_id,
                status=row.status,
                ended_at=row.timestamp,
            ))
        return results

    def list_runs_by_flow_name(self, name: str, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List runs summaries for a given flow `name`."""
        if db is None:
            with self.db_session_factory() as db:
                runs = self.list_runs_by_flow_name(name=name, db=db)
            return runs
        rows = db.scalars(SQL.latest_run_by_flow_name, {"name": name}).all()
        results = []
        for row in rows:
            results.append(models.FlowRunSummary(
                name=row.run.name,
                run_id=row.run_id,
                status=row.status,
                ended_at=row.timestamp,
            ))
        return results

    def get_run_by_id(self, run_id: UUID, db: Session | None=None) -> models.RunModel | None:
        if db is None:
            with self.db_session_factory() as db:
                run = self.get_run_by_id(run_id=run_id, db=db)
            return run
        row = db.scalar(SQL.get_run_by_id, {"run_id": run_id})
        if row is None:
            return None
        # Build 
        run_attr = models.RunAttrModel(
            name=row.name,
            run_type=row.run_type,
            run_id=row.run_id,
        )
        logs = []
        for log in row.logs:
            logs.append(models.RunLogAttrModel(**log.to_dict()))
        if row.parent:
            parent = models.RunAttrModel(
                name=row.parent.name,
                run_type=row.parent.run_type,
                run_id=row.parent.run_id,
            )
        else:
            parent = None
        children = []
        for child in row.children:
            child_attr = models.RunAttrModel(
                name=child.name,
                run_type=child.run_type,
                run_id=child.run_id,
            )
            child_logs = []
            for child_log in child.logs:
                child_logs.append(models.RunLogAttrModel(**child_log.to_dict()))
            children.append(models.RunModel(
                run=child_attr,
                logs=child_logs,
            ))
        return models.RunModel(run=run_attr, logs=logs, parent=parent, children=children)
