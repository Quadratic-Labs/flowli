from datetime import UTC, datetime
from uuid import UUID
from sqlalchemy.orm import Session, aliased
from sqlalchemy import bindparam, func, select

from .database import (FlowRun, TaskRun)
from .register import FlowRegister


class SQL:
    read_ranked_flow = (
        select(
            FlowRun,
            func.row_number()
            .over(
                partition_by=FlowRun.flow_name,
                order_by=FlowRun.started_at.desc(),
            )
            .label("rnk"),
        )
    )
    _ranked = read_ranked_flow.subquery()
    _ranked_alias = aliased(FlowRun, _ranked)
    read_flow_many = (
        select(_ranked_alias)
        .where(_ranked.c.rnk <= bindparam("limit"))
        .order_by(_ranked.c.flow_name, _ranked.c.started_at.desc())
    )
    read_flow_run = (
        select(FlowRun)
        .where(FlowRun.run_id == bindparam("run_id"))
    )
    read_flow_run_many = (
        select(FlowRun)
        .order_by(FlowRun.started_at.desc())
        .limit(bindparam("limit"))
        .offset(bindparam("offset"))
    )
    read_run_task_many = (
        select(TaskRun)
        .where(TaskRun.flow_run_id == bindparam("run_id"))
    )


class FlowRepository:
    def __init__(self, *, register: FlowRegister, **_):
        self.register = register
        self.db_session_factory = register.db_session_factory
        self.db = None

    def __enter__(self):
        # Create database session
        self.db = self.db_session_factory()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Complete flow execution tracking and cleanup."""
        if self.db:
            self.db.close()
        return False
    
    def read_flow_many(self, limit: int=1):
        results = []
        runs = self.db.scalars(SQL.read_flow_many, {"limit": limit}).all()
        runs_idx = {}
        for r in runs:
            name = r.flow_name
            runs_idx[name] = runs_idx.get(name, [])
            runs_idx[name].append(r)
        now = datetime.now(UTC)

        for name in self.register.list_flows():
            last_runs = runs_idx.get(name)
            res = {
                "name": name,
                "last_status": None,
                "duration": None,
                "finished_ago": None,
            }
            if last_runs:
                last_run = last_runs[0]
                res["last_status"] = last_run.status
                if last_run.finished_at and last_run.started_at:
                    res["duration"] = (last_run.finished_at - last_run.started_at).total_seconds()
                if last_run.finished_at:
                    res["finished_ago"] = now - last_run.finished_at
            results.append(res)
        return results

    def read_flow_run(self, run_id: UUID):
        run = self.db.scalar_or_none(SQL.read_flow_run, {"run_id": run_id})
        if run is not None:
            run = run.to_dict()
        return run

    def read_flow_run_many(self, offset: int = 0, limit: int = 10):
        params = {"offset": offset, "limit": limit}
        runs = self.db.scalars(SQL.read_flow_run_many, params).all()
        return [run.to_dict() for run in runs]

    def read_run_task_many(self, run_id: UUID):
        tasks = self.db.scalars(SQL.read_run_task_many, {"run_id": run_id}).all()
        return [task.to_dict() for task in tasks]
