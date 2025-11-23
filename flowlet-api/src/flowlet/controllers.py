"""
FastAPI router for Flowlet endpoints.

Provides a factory function to create routers with custom or default FlowManager instances.
"""
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Self

from fastapi import HTTPException
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, computed_field

from .repositories.query import FlowQueryRepository


# region Controllers' Models
# ============================================================================
def humanize_timedelta(td: timedelta) -> str:
    """Convert a timedelta into a friendly 'x m ago' string."""
    seconds = int(td.total_seconds())
    if seconds < 60:
        return f"{seconds}s ago"
    elif seconds < 3600:
        return f"{seconds // 60}m ago"
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m ago"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h ago"

HumanDuration = Annotated[timedelta, AfterValidator(humanize_timedelta)]


class Base(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class FlowInputModel(Base):
    # arguments are simply JSON serializable and passed as positional/keyword args.
    # For simplicity we accept an object of kwargs only.
    kwargs: dict[str, Any] = Field(default_factory=dict)


class FlowSummaryModel(Base):
    name: str
    status: str | None = Field(default=None)
    started_at: datetime | None = Field(default=None)
    last_at: datetime | None = Field(default=None)
    doc: str | None = Field(default=None)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def finished_ago(self) -> str | None:
        """Compute time since last_at relative to now."""
        if self.last_at is None:
            return None
        delta = datetime.now(UTC) - self.last_at
        return humanize_timedelta(delta)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def duration(self) -> str | None:
        """Compute duration from started_at to last_at."""
        if self.started_at is None or self.last_at is None:
            return None
        delta = self.last_at - self.started_at
        return humanize_timedelta(delta)


class FlowRunModel(Base):
    name: str
    run_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    status: str | None = Field(default=None)
    ended_at: datetime | None = Field(default=None)


class RunAttrModel(Base):
    name: str
    run_type: str
    run_id: uuid.UUID


class RunLogModel(Base):
    status: str
    log: str = Field(default="")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    log_id: uuid.UUID = Field(default_factory=uuid.uuid4)


class RunModel(Base):
    run: RunAttrModel
    logs: list[RunLogModel] = Field(default_factory=list)
    parent: RunAttrModel | None = Field(default=None)
    children: list[Self] = Field(default_factory=list)

# ============================================================================
# endregion

# region Controller
# ============================================================================
class FlowController:
    """
    Create a FastAPI router for Flowlet endpoints.

    This factory function allows you to either use the global FlowManager
    or provide a custom instance for dependency injection.

    Args:
        flowlet: Optional FlowManager instance. If None, uses the global instance.

    Returns:
        Configured APIRouter with all Flowlet endpoints

    Example:
        >>> from flowlet import create_router
        >>> from fastapi import FastAPI
        >>>
        >>> app = FastAPI()
        >>> # Use default global manager
        >>> app.include_router(create_router())
        >>>
        >>> # Or use custom manager
        >>> from flowlet import FlowManager
        >>> custom_flowlet = FlowManager(my_session_factory)
        >>> app.include_router(create_router(flowlet=custom_flowlet))
    """
    def __init__(self, *, query_repository: FlowQueryRepository, **_):
        self.query_repository = query_repository
        self.register = query_repository.register

    def run_flow(self, flow_name: str, payload: FlowInputModel) -> None:
        if flow_name not in self.register.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")
        fn = self.register.flows[flow_name]
        # For simplicity, accept only kwargs
        kwargs = payload.kwargs or {}
        # TODO: schedule background task, get immediate status
        # TODO: add azure job to execution and PubSub sockets
        _ = fn(**kwargs)

    def list_flows(self) -> list[FlowSummaryModel]:
        flows = self.query_repository.list_flows()
        result = []
        for flow in flows:
            result.append(FlowSummaryModel.model_validate(flow))
        return result

    def list_runs(self, offset: int = 0, limit: int = 50) -> list[FlowRunModel]:
        rows = self.query_repository.list_runs(offset=offset, limit=limit)
        return [FlowRunModel.model_validate(row) for row in rows]

    def get_run(self, run_id: uuid.UUID) -> RunModel:
        run = self.query_repository.get_run_by_id(run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return RunModel.model_validate(run)

    def list_flow_runs(self, name: str) -> list[FlowRunModel]:
        runs = self.query_repository.list_runs_by_flow_name(name=name)
        result = []
        for run in runs:
            result.append(FlowRunModel.model_validate(run))
        return result

# ============================================================================
# endregion