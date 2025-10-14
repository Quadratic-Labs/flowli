from datetime import UTC, datetime
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import bindparam, select
from .database import (FlowRun, LastFlowRun, TaskRun)


class SQL:
    read_flow_many = (
        select(LastFlowRun)
        .join(FlowRun, FlowRun.flow_name == LastFlowRun.flow_name)
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


class FlowsRepository:
    def __init__(self, flowlet, db: Session):
        self.flowlet = flowlet
        self.db = db

    def read_flow_many(self):
        results = []
        runs = self.db.scalars(SQL.read_flow_many).all()
        runs_idx = {r.flow_name: r for r in runs}
        now = datetime.now(UTC)

        for name, fn in self.flowlet._FLOWS.items():
            last_run = runs_idx.get(name)
            res = {
                "name": name,
                "last_status": None,
                "duration": None,
                "finished_ago": None,
            }
            if last_run:
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
        return [run.to_dict()
                 for run in self.db.scalars(SQL.read_flow_run_many, params)]

    def read_run_task_many(self, run_id: UUID):
        return [task.to_dict()
                 for task in self.db.scalars(SQL.read_run_task_many, {"run_id": run_id})]