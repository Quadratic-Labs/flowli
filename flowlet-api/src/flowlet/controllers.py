"""
FastAPI router for Flowlet endpoints.

Provides a factory function to create routers with custom or default FlowManager instances.
"""
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import HTTPException
from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from .repository import FlowRepository


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


class FlowSummaryModel(BaseModel):
    name: str
    last_status: str | None = Field(default=None)
    finished_ago: HumanDuration | None = Field(default=None)
    duration: HumanDuration | None = Field(default=None)
    doc: str | None = Field(default=None)


class FlowInputModel(BaseModel):
    # arguments are simply JSON serializable and passed as positional/keyword args.
    # For simplicity we accept an object of kwargs only.
    kwargs: dict[str, Any] = Field(default_factory=dict)


class FlowRunModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flow_name: str
    run_id: uuid.UUID
    started_at: datetime
    finished_at: datetime | None
    status: str
    error: str | None = None


class TaskRunModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    task_name: str
    flow_run_id: uuid.UUID
    flow_name: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    result: str | None = None
    error: str | None = None

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
    def __init__(self, *, repository: FlowRepository, **_):
        self.repository = repository
        self.register = repository.register

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
        result = self.repository.read_flow_many()
        return [FlowSummaryModel.model_validate(res) for res in result]

    def list_runs(self, offset: int = 0, limit: int = 50) -> list[FlowRunModel]:
        rows = self.repository.read_flow_run_many(offset=offset, limit=limit)
        return [FlowRunModel.model_validate(row) for row in rows]

    def get_run(self, run_id: uuid.UUID) -> FlowRunModel:
        run = self.repository.read_flow_run(run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return FlowRunModel.model_validate(run)

    def get_run_tasks(self, run_id: uuid.UUID) -> list[TaskRunModel]:
        tasks = self.repository.read_run_task_many(run_id=run_id)
        return [TaskRunModel.model_validate(task) for task in tasks]

# ============================================================================
# endregion