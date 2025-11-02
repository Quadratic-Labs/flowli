from typing import Protocol, Self
from uuid import UUID

from sqlalchemy.orm import Session

from . import models


class FlowQueryRepositoryProtocol(Protocol):
    db_session_factory: Session
    def read_flow_many(self, limit: int=1, db: Session | None=None) -> list[models.FlowSummary]:
        """Read flow summaries."""
        ...
    def read_flow_run(self, run_id: UUID, db: Session | None=None) -> models.FlowRun | None:
        """Read information on a flow run given by run_id"""
        ...
    def read_flow_run_many(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRun]:
        """Read in bulk flow runs information"""
        ...
    def read_run_task_many(self, run_id: UUID, db: Session | None=None) -> list[models.TaskRun]:
        """Read task runs for a run given by run_id"""
        ...


class FlowTrackerProtocol(Protocol):
    def log_flow_run(self, data: models.FlowRunLog, db: Session | None = None) -> models.FlowRun: ...
    def log_task_run(self, data: models.TaskRunLog, db: Session | None = None) -> models.FlowRun: ...