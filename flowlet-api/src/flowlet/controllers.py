"""FastAPI controllers and API models for Flowlet.

Provides controller classes for handling flow execution and querying,
along with Pydantic models for API request/response serialization.
"""
from datetime import datetime, timedelta
from inspect import Parameter
from typing import Annotated, Any
from uuid import UUID

from fastapi import HTTPException
from jsonry.serdes.from_dict import from_dict
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError

from .interfaces.query import RunQueryProtocol
from .interfaces.tracker import TrackerProtocol
from .models import RunSummary, SpanLog
from .types import RunStatus, SpanType


def humanize_timedelta(td: timedelta) -> str:
    """Convert a timedelta into a human-friendly relative time string.

    Args:
        td: Timedelta to humanize.

    Returns:
        str: Human-readable string like "5s", "3m", "2h 15m", or "1d 3h".

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
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h"

HumanDuration = Annotated[timedelta, AfterValidator(humanize_timedelta)]
"""Type alias for timedelta that is automatically humanized when validated."""


class Base(BaseModel):
    """Base Pydantic model with common configuration.

    Enables automatic conversion from SQLAlchemy ORM objects.
    """
    model_config = ConfigDict(from_attributes=True)


class FlowArguments(Base):
    """API model for flow execution request.

    Accepts keyword arguments to pass to the flow function.

    Attributes:
        kwargs: Dictionary of keyword arguments for flow execution.

    Example:
        >>> flow_input = FlowInputModel(kwargs={"user_id": 123, "mode": "test"})
    """
    kwargs: dict[str, Any] = Field(default_factory=dict)


class RunQueryRequest(Base):
    """API model for querying runs with jsonry.

    Attributes:
        names: Optional list of flow names to filter by.
        query: Optional jsonry query as a JSON object. See jsonry documentation for syntax.
            Validation happens during deserialization.

    Example:
        >>> # Filter runs by status
        >>> request = RunQueryRequest(
        ...     query={"type": "Filter", "predicate": {"type": "Equal", "left": {"type": "Get", "keys": ["status"]}, "right": "success"}}
        ... )
    """
    names: list[str] | None = Field(
        None,
        description="Optional list of flow names to filter by"
    )
    query: dict[str, Any] | None = Field(
        None,
        description="jsonry Query specification as JSON. See jsonry documentation for query syntax."
    )


class LogQueryRequest(Base):
    """API model for querying logs with jsonry.

    Attributes:
        runs: Optional list of run UUIDs to filter by.
        query: Optional jsonry query as a JSON object. See jsonry documentation for syntax.
            Validation happens during deserialization.

    Example:
        >>> # Get error logs only
        >>> request = LogQueryRequest(
        ...     query={"type": "Filter", "predicate": {"type": "Equal", "left": {"type": "Get", "keys": ["level"]}, "right": "error"}}
        ... )
    """
    runs: list[str] | None = Field(
        None,
        description="Optional list of run UUIDs (as strings) to filter by"
    )
    query: dict[str, Any] | None = Field(
        None,
        description="jsonry Query specification as JSON. See jsonry documentation for query syntax."
    )


class SpanLogDTO(Base):
    """API model for a run log entry.

    Attributes:
        TODO
    """
    flow_name: str | None = None
    run_id: str | None = None
    span_type: SpanType | None = None
    span_name: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    ts: datetime | None = None
    message: str | None = None
    level: str | None = None


class RunSummaryDTO(Base):
    """API model for a single flow run summary.

    Attributes:
        span_id: span's identifier.
        span_name: span's name.
        status: span's most recent status.
        start_ts: span's starting time.
        end_ts: span's ending time.
        children: span's children span's summaries.
    """
    span_id: str
    span_name: str
    status: RunStatus
    start_ts: datetime
    end_ts: datetime
    duration: HumanDuration | None = Field(default=None)
    children: list[RunSummaryDTO]


class RunDTO(RunSummaryDTO):
    logs: list[SpanLogDTO]


class FlowController:
    """FastAPI controller for flow execution and query endpoints.

    Provides handler methods for all Flowlet REST API operations including
    flow execution, listing flows, querying runs, and retrieving run details.

    Attributes:
        query_querier: querier for querying flow execution data.
        register: Flow and task register for accessing registered flows.
        tracker: Tracker for computing run summaries from logs.

    Example:
        >>> controller = FlowController(query_querier=repo, tracker=tracker)
        >>> flows = controller.list_flows()
        >>> controller.run_flow("my_flow", FlowInputModel(kwargs={"param": "value"}))
    """
    def __init__(self, *, querier: RunQueryProtocol, tracker: TrackerProtocol, **_):
        """Initialize the flow controller.

        Args:
            querier: querier for querying flow execution history.
            tracker: Tracker for computing run summaries from logs.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.querier = querier
        self.registry = querier.registry
        self.tracker = tracker

    def run_flow(self, flow_name: str, payload: FlowArguments) -> None:
        """Execute a registered flow with provided arguments.

        Args:
            flow_name: Name of the flow to execute.
            payload: Input model containing kwargs for the flow function.

        Raises:
            HTTPException: 404 if the flow is not registered.
            HTTPException: 422 if the provided arguments fail schema validation.

        Note:
            If the flow has type hints, arguments are validated against the generated schema.
            Untyped flows execute without validation (backwards compatible).
            Currently executes flows synchronously. Future versions will
            support background task scheduling.
        """
        if flow_name not in self.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")

        # Get schema for validation
        schema = self.registry.get_flow_schema(flow_name)

        if schema is not None:
            # Validate kwargs against Pydantic model
            try:
                validated_data = schema.pydantic_model(**payload.kwargs)
                kwargs = validated_data.model_dump()
            except ValidationError as e:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "message": "Invalid flow arguments",
                        "flow": flow_name,
                        "errors": e.errors()
                    }
                )
        else:
            # Graceful degradation: no schema, use kwargs as-is
            kwargs = payload.kwargs or {}

        fn = self.registry.get_flow(flow_name)
        # TODO: schedule background task, get immediate status
        # TODO: add azure job to execution and PubSub sockets
        _ = fn(**kwargs)


    def query_runs(self, request: RunQueryRequest) -> list[RunSummaryDTO]:
        """Query run summaries with flexible jsonry queries.

        Supports filtering, projection, aggregation, and transformation of run summary data
        using jsonry query language.

        Args:
            request: Query request containing optional flow names filter and jsonry query dict.

        Returns:
            list[Any]: Query results. Type depends on the query transformation applied.
                      Returns RunSummary models when full schema, or dicts when projected.

        Raises:
            HTTPException: 400 if the query is invalid or cannot be executed.

        Example:
            >>> # Filter run summaries by status
            >>> request = RunQueryRequest(
            ...     query={"type": "Filter", "predicate": {"type": "Equal", "left": {"type": "Get", "keys": ["status"]}, "right": "success"}}
            ... )
            >>> results = controller.query_runs(request)
        """
        try:
            # Deserialize dict to Query object if provided, with validation
            query = from_dict(request.query) if request.query else None

            # Execute query through the repository layer
            results = self.querier.list_summaries(
                names=request.names,
                query=query
            )

            # Convert iterator to list and return
            return [RunSummaryDTO.model_validate(res) for res in results]
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query specification: {str(e)}"
            )

    def query_logs(self, request: LogQueryRequest) -> list[RunDTO]:
        """Query runs (logs + summaries) with flexible jsonry queries.

        Supports filtering, projection, aggregation, and transformation of run data
        using jsonry query language.

        Behavior:
        - If full schema is available: returns Run models (with logs and computed summary)
        - If projection/transformation is used: returns the projected results directly

        Args:
            request: Query request containing optional run UUIDs filter and jsonry query dict.

        Returns:
            list[Any]: Query results. Type depends on the query transformation applied.
                      Returns Run models when full schema, or dicts when projected.

        Raises:
            HTTPException: 400 if the query is invalid or cannot be executed.

        Example:
            >>> # Get full runs with logs and summaries (no projection)
            >>> request = LogQueryRequest()
            >>> results = controller.query_logs(request)

            >>> # Get only specific fields (projection query)
            >>> request = LogQueryRequest(
            ...     query={"type": "Map", "transform": {"type": "Pick", "keys": ["summary", "status"]}}
            ... )
            >>> results = controller.query_logs(request)
        """
        breakpoint()
        try:
            # Deserialize dict to Query object if provided, with validation
            query = from_dict(request.query) if request.query else None

            # Convert string UUIDs to UUID objects if provided
            run_uuids = None
            if request.runs is not None:
                run_uuids = [UUID(run_id) for run_id in request.runs]

            # Execute query through the repository layer
            # list_runs returns Run models (logs + summary) or dicts if projected
            results = self.querier.list_runs(
                runs=run_uuids,
                query=query
            )

            # Convert iterator to list and return
            return [RunDTO.model_validate(res) for res in results]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid UUID format: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query specification: {str(e)}"
            )

    def get_flow_schema(self, flow_name: str) -> dict[str, Any]:
        """Get parameter schema for a specific flow.

        Args:
            flow_name: Name of the flow.

        Returns:
            Dictionary containing flow schema information including:
            - flow_name: Name of the flow
            - has_schema: Whether type hints are available
            - docstring: Flow's docstring (if available)
            - parameters: List of parameter metadata
            - json_schema: JSON Schema representation (if available)

        Raises:
            HTTPException: 404 if flow not found.

        Example:
            >>> schema = controller.get_flow_schema("my_flow")
            >>> print(schema["parameters"])
            [{"name": "x", "type": "int", "required": true}, ...]
        """
        if flow_name not in self.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")

        schema = self.registry.get_flow_schema(flow_name)

        if schema is None:
            return {
                "flow_name": flow_name,
                "has_schema": False,
                "message": "No type hints available for this flow"
            }

        return {
            "flow_name": flow_name,
            "docstring": schema.docstring,
            "has_schema": True,
            "parameters": [
                {
                    "name": p.name,
                    "type": str(p.type_annotation),
                    "required": p.required,
                    "default": p.default if p.default != Parameter.empty else None,
                    "description": p.description
                }
                for p in schema.parameters
            ],
            "json_schema": schema.pydantic_model.model_json_schema()
        }

    def list_flows_with_schemas(self) -> list[dict[str, Any]]:
        """List all flows with their parameter schemas.

        Returns:
            List of flow metadata dictionaries, each containing:
            - name: Flow name
            - docstring: Flow's docstring (if available)
            - has_schema: Whether type hints are available
            - parameters: List of parameter info (if schema available)

        Example:
            >>> flows = controller.list_flows_with_schemas()
            >>> for flow in flows:
            ...     print(f"{flow['name']}: {flow['has_schema']}")
            my_flow: True
            legacy_flow: False
        """
        flows = []
        for flow_name in self.registry.list_flows():
            fn = self.registry.get_flow(flow_name)
            schema = self.registry.get_flow_schema(flow_name)

            flow_info: dict[str, Any] = {
                "name": flow_name,
                "docstring": fn.__doc__,
                "has_schema": schema is not None,
            }

            if schema:
                flow_info["parameters"] = [
                    {
                        "name": p.name,
                        "type": str(p.type_annotation),
                        "required": p.required,
                    }
                    for p in schema.parameters
                ]

            flows.append(flow_info)

        return flows
