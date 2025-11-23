from typing import Protocol, Self
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from . import models


class FlowQueryRepositoryProtocol(Protocol):
    db_session_factory: sessionmaker

    def list_flows(self, n_last_runs: int=1, db: Session | None=None) -> list[models.FlowSummary]:
        """List flow summaries."""
        ...

    def list_runs(self, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List runs summaries."""
        ...

    def list_runs_by_flow_name(self, name: str, offset: int = 0, limit: int = 10, db: Session | None=None) -> list[models.FlowRunSummary]:
        """List runs summaries for the flow given by its `name`."""
        ...

    def get_run_by_id(self, run_id: UUID, db: Session | None=None) -> models.RunModel | None:
        """Get run data for the run given by its `run_id`."""
        ...


class FlowTrackerProtocol(Protocol):
    db_session_factory: sessionmaker

    def create_run(self, data: models.RunAttrModel, db: Session | None = None) -> models.RunAttrModel:
        """Create a new run record (flow or task) with fresh IDs."""
        ...

    def link_runs(self, parent: models.RunAttrModel | None, child: models.RunAttrModel | None, db: Session | None = None) -> None:
        """Link a child run to a parent run."""
        ...

    def log(self, data: models.RunLogAttrModel, db: Session | None = None) -> models.RunLogAttrModel:
        """Append a log entry to a run."""
        ...