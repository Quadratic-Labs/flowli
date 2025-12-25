"""FastAPI controllers and API models for Flowlet.

Provides controller classes for handling flow execution and querying,
along with Pydantic models for API request/response serialization.
"""
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import HTTPException
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, computed_field

from .repositories.query import FlowQueryRepository


def humanize_timedelta(td: timedelta) -> str:
    """Convert a timedelta into a human-friendly relative time string.

    Args:
        td: Timedelta to humanize.

    Returns:
        str: Human-readable string like "5s ago", "3m ago", "2h 15m ago", or "1d 3h ago".

    Example:
        >>> from datetime import timedelta
        >>> humanize_timedelta(timedelta(seconds=45))
        '45s ago'
        >>> humanize_timedelta(timedelta(minutes=5, seconds=30))
        '5m ago'
        >>> humanize_timedelta(timedelta(hours=2, minutes=15))
        '2h 15m ago'
    """
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
"""Type alias for timedelta that is automatically humanized when validated."""


class Base(BaseModel):
    """Base Pydantic model with common configuration.

    Enables automatic conversion from SQLAlchemy ORM objects.
    """
    model_config = ConfigDict(from_attributes=True)


class FlowInputModel(Base):
    """API model for flow execution request.

    Accepts keyword arguments to pass to the flow function.

    Attributes:
        kwargs: Dictionary of keyword arguments for flow execution.

    Example:
        >>> flow_input = FlowInputModel(kwargs={"user_id": 123, "mode": "test"})
    """
    kwargs: dict[str, Any] = Field(default_factory=dict)


class FlowSummaryModel(Base):
    """API model for flow summary with execution statistics.

    Includes computed fields for human-readable durations.

    Attributes:
        name: Flow name.
        status: Latest execution status ("running", "success", "failed", etc.).
        started_at: Timestamp when the latest run started.
        last_at: Timestamp of the latest status update.
        doc: Flow docstring or description.
        finished_ago: Computed field showing time since completion.
        duration: Computed field showing execution duration.
    """
    name: str
    status: str | None = Field(default=None)
    started_at: datetime | None = Field(default=None)
    last_at: datetime | None = Field(default=None)
    doc: str | None = Field(default=None)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def finished_ago(self) -> str | None:
        """Compute time since last_at relative to now.

        Returns:
            str | None: Human-readable string like "5m ago", or None if not finished.
        """
        if self.last_at is None:
            return None
        delta = datetime.now(UTC) - self.last_at
        return humanize_timedelta(delta)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def duration(self) -> str | None:
        """Compute execution duration from started_at to last_at.

        Returns:
            str | None: Human-readable duration like "2h 15m ago", or None if incomplete.
        """
        if self.started_at is None or self.last_at is None:
            return None
        delta = self.last_at - self.started_at
        return humanize_timedelta(delta)


class FlowRunModel(Base):
    """API model for a single flow run summary.

    Attributes:
        name: Flow name.
        run_id: Unique run identifier.
        status: Run status ("running", "success", "failed", etc.).
        ended_at: Timestamp when the run ended.
    """
    name: str
    run_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    status: str | None = Field(default=None)
    ended_at: datetime | None = Field(default=None)


class RunAttrModel(Base):
    """API model for run attributes.

    Attributes:
        name: Name of the flow or task.
        run_type: Type of run ("flow" or "task").
        run_id: Unique run identifier.
    """
    name: str
    run_type: str
    run_id: uuid.UUID


class RunLogModel(Base):
    """API model for a run log entry.

    Attributes:
        status: Status at this log point ("running", "success", "failed").
        log: Optional log message or error details.
        timestamp: When this log entry was created.
        log_id: Unique log entry identifier.
    """
    status: str
    log: str = Field(default="")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    log_id: uuid.UUID = Field(default_factory=uuid.uuid4)


class RunModel(Base):
    """API model for detailed run information with hierarchy.

    Includes the run attributes, all log entries, parent run, and child runs.

    Attributes:
        run: Run attributes (name, type, ID).
        logs: All log entries for this run.
        parent: Parent run attributes if this is a task.
        children: Child runs (tasks) if this is a flow.
    """
    run: RunAttrModel
    logs: list[RunLogModel] = Field(default_factory=list)
    parent: RunAttrModel | None = Field(default=None)
    children: list["RunModel"] = Field(default_factory=list)


class FlowController:
    """FastAPI controller for flow execution and query endpoints.

    Provides handler methods for all Flowlet REST API operations including
    flow execution, listing flows, querying runs, and retrieving run details.

    Attributes:
        query_repository: Repository for querying flow execution data.
        register: Flow and task register for accessing registered flows.

    Example:
        >>> controller = FlowController(query_repository=repo)
        >>> flows = controller.list_flows()
        >>> controller.run_flow("my_flow", FlowInputModel(kwargs={"param": "value"}))
    """
    def __init__(self, *, query_repository: FlowQueryRepository, **_):
        """Initialize the flow controller.

        Args:
            query_repository: Repository for querying flow execution history.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.query_repository = query_repository
        self.register = query_repository.register

    def run_flow(self, flow_name: str, payload: FlowInputModel) -> None:
        """Execute a registered flow with provided arguments.

        Args:
            flow_name: Name of the flow to execute.
            payload: Input model containing kwargs for the flow function.

        Raises:
            HTTPException: 404 if the flow is not registered.

        Note:
            Currently executes flows synchronously. Future versions will
            support background task scheduling.
        """
        if flow_name not in self.register.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")
        fn = self.register.flows[flow_name]
        # For simplicity, accept only kwargs
        kwargs = payload.kwargs or {}
        # TODO: schedule background task, get immediate status
        # TODO: add azure job to execution and PubSub sockets
        _ = fn(**kwargs)

    def list_flows(self) -> list[FlowSummaryModel]:
        """List all registered flows with execution summaries.

        Returns:
            list[FlowSummaryModel]: List of flow summaries including latest run status.
        """
        flows = self.query_repository.list_flows()
        result = []
        for flow in flows:
            result.append(FlowSummaryModel.model_validate(flow))
        return result

    def list_runs(self, offset: int = 0, limit: int = 50) -> list[FlowRunModel]:
        """List recent flow runs with pagination.

        Args:
            offset: Number of runs to skip. Defaults to 0.
            limit: Maximum number of runs to return. Defaults to 50.

        Returns:
            list[FlowRunModel]: List of flow run summaries.
        """
        rows = self.query_repository.list_runs(offset=offset, limit=limit)
        return [FlowRunModel.model_validate(row) for row in rows]

    def get_run(self, run_id: uuid.UUID) -> RunModel:
        """Get detailed information for a specific run.

        Includes full run hierarchy with parent and child runs, and all log entries.

        Args:
            run_id: Unique identifier of the run.

        Returns:
            RunModel: Detailed run information with logs and hierarchy.

        Raises:
            HTTPException: 404 if the run is not found.
        """
        run = self.query_repository.get_run_by_id(run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return RunModel.model_validate(run)

    def list_flow_runs(self, name: str) -> list[FlowRunModel]:
        """List all runs for a specific flow.

        Args:
            name: Name of the flow.

        Returns:
            list[FlowRunModel]: List of run summaries for the specified flow.
        """
        runs = self.query_repository.list_runs_by_flow_name(name=name)
        result = []
        for run in runs:
            result.append(FlowRunModel.model_validate(run))
        return result