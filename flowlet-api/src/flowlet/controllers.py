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
from .models import RunSummary, SpanLog
from .types import RunStatus, SpanType


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
    flow_name: str
    run_id: str
    span_type: SpanType
    span_name: str
    span_id: str
    parent_span_id: str | None
    ts: datetime
    message: str
    level: str


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


def span_log_to_dto(data: SpanLog):
    return SpanLogDTO(
        flow_name = data.flow_name, 
        run_id = str(data.run_id),
        span_type = data.span_type,
        span_name = data.span_name,
        span_id = str(data.span_id),
        parent_span_id = str(data.parent_span_id) if data.parent_span_id else None,
        ts = data.ts,
        message = data.message,
        level = data.level,
    )


def run_summary_to_dto(data: RunSummary):
    return RunSummaryDTO(
        span_id = str(data.span_id),
        span_name = data.span_name,
        status = data.status,
        start_ts = data.start_ts,
        end_ts = data.end_ts,
        duration = data.duration if data.duration is not None else None,
        children = [run_summary_to_dto(c) for c in data.children],
    )


class FlowController:
    """FastAPI controller for flow execution and query endpoints.

    Provides handler methods for all Flowlet REST API operations including
    flow execution, listing flows, querying runs, and retrieving run details.

    Attributes:
        query_querier: querier for querying flow execution data.
        register: Flow and task register for accessing registered flows.

    Example:
        >>> controller = FlowController(query_querier=repo)
        >>> flows = controller.list_flows()
        >>> controller.run_flow("my_flow", FlowInputModel(kwargs={"param": "value"}))
    """
    def __init__(self, *, querier: RunQueryProtocol, **_):
        """Initialize the flow controller.

        Args:
            querier: querier for querying flow execution history.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.querier = querier
        self.registry = querier.registry

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


    def query_runs(self, request: RunQueryRequest) -> list[Any]:
        """Query runs with flexible jsonry queries.

        Supports filtering, projection, aggregation, and transformation of run data
        using jsonry query language.

        Args:
            request: Query request containing optional flow names filter and jsonry query dict.

        Returns:
            list[Any]: Query results. Type depends on the query transformation applied.

        Raises:
            HTTPException: 400 if the query is invalid or cannot be executed.

        Example:
            >>> # Filter runs by status
            >>> request = RunQueryRequest(
            ...     query={"type": "Filter", "predicate": {"type": "Equal", "left": {"type": "Get", "keys": ["status"]}, "right": "success"}}
            ... )
            >>> results = controller.query_runs(request)
        """
        try:
            # Deserialize dict to Query object if provided, with validation
            query = from_dict(request.query) if request.query else None

            # Execute query through the repository layer
            results = self.querier.list_runs(
                names=request.names,
                query=query
            )

            # Convert iterator to list and return
            return list(results)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query specification: {str(e)}"
            )

    def query_logs(self, request: LogQueryRequest) -> list[Any]:
        """Query logs with flexible jsonry queries.

        Supports filtering, projection, aggregation, and transformation of log data
        using jsonry query language.

        Args:
            request: Query request containing optional run UUIDs filter and jsonry query dict.

        Returns:
            list[Any]: Query results. Type depends on the query transformation applied.

        Raises:
            HTTPException: 400 if the query is invalid or cannot be executed.

        Example:
            >>> # Get error-level logs only
            >>> request = LogQueryRequest(
            ...     query={"type": "Filter", "predicate": {"type": "Equal", "left": {"type": "Get", "keys": ["level"]}, "right": "error"}}
            ... )
            >>> results = controller.query_logs(request)
        """
        try:
            # Deserialize dict to Query object if provided, with validation
            query = from_dict(request.query) if request.query else None

            # Convert string UUIDs to UUID objects if provided
            run_uuids = None
            if request.runs is not None:
                run_uuids = [UUID(run_id) for run_id in request.runs]

            # Execute query through the repository layer
            results = self.querier.list_logs(
                runs=run_uuids,
                query=query
            )

            # Convert iterator to list and return
            return list(results)
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