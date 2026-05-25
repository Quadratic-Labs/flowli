"""
FastAPI controllers for Flowlet.

Provides the FlowController class for handling flow execution and query endpoints.
"""
import logging
from inspect import Parameter
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid7

logger = logging.getLogger(__name__)

from fastapi import HTTPException, WebSocketDisconnect, WebSocket
from pydantic import ValidationError

from ..models import FlowJob
from ..pubsub import PubSubProtocol
from ..queue import JobQueueProtocol
from .repository import SnapshotRepository
from .models import (
    FlowArguments,
    FlowSubmissionResponse,
    LogQueryRequest,
    RunDTO,
    RunQueryRequest,
    RunStateDTO,
)
from .query import RunQuery

if TYPE_CHECKING:
    from ..registry import Registry
    from .ws import ConnectionManager


# region @controller
# ---
# role: api
# intent: handle flow execution and run-query HTTP endpoints
# description: >
#   FlowController is the thin FastAPI handler layer.  It validates incoming
#   requests, delegates to the domain layer (RunQuery, Registry, JobQueue),
#   and maps domain exceptions to HTTP status codes.
#   Execution endpoints: run_flow (synchronous) and submit_flow (async queue).
#   Query endpoints: query_runs (recent RunState rows) and query_logs (full run
#   with log detail).
#   Introspection endpoints: get_flow_schema and list_flows_with_schemas read
#   parameter metadata straight from the Registry.
# rules:
#   - MUST NOT contain business logic; delegate everything to the domain layer.
#   - MUST map domain exceptions to appropriate HTTP status codes.
#   - query_runs MUST be async (delegates to RunQuery async methods).
# dependencies:
#   - query
#   - registry.registry
#   - models.job
# aliases:
#   - flow-controller
# triggers:
#   - where are HTTP handlers defined
#   - how does the API layer work
# ---


class FlowController:
    """FastAPI controller for flow execution and query endpoints.

    Provides handler methods for all Flowlet REST API operations including
    flow execution, listing flows, querying run states, and retrieving run
    details.

    Attributes:
        querier: RunQuery instance for reading run states and log details.
        registry: Flow registry, accessed via querier.
        queue: Optional job queue for asynchronous execution.
        pubsub: Optional pubsub for publishing state events.
        snapshot_repo: Optional snapshot repository for persisting state.

    Example:
        >>> controller = FlowController(querier=run_query)
        >>> flows = controller.list_flows_with_schemas()
        >>> await controller.submit_flow("my_flow", FlowArguments(kwargs={"x": 1}))
    """

    def __init__(
        self,
        *,
        registry: Registry,
        querier: RunQuery | None = None,
        queue: JobQueueProtocol | None = None,
        connection_manager: ConnectionManager | None = None,
        pubsub: PubSubProtocol[RunStateDTO] | None = None,
        snapshot_repo: SnapshotRepository | None = None,
        **_,
    ):
        """Initialise the flow controller.

        Args:
            registry: Flow registry for resolving registered flows.
            querier: RunQuery providing read access to run states and logs.
                When None the query endpoints return 503.
            queue: Optional job queue for asynchronous flow submission.
            connection_manager: ConnectionManager providing WebSocket broadcast.
                When None the ``/ws/runs`` endpoint is unavailable.
            pubsub: Optional pubsub for publishing state events.
            snapshot_repo: Optional snapshot repository for persisting state.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.registry = registry
        self.querier = querier
        self.queue = queue
        self.connection_manager = connection_manager
        self.pubsub = pubsub
        self.snapshot_repo = snapshot_repo

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_kwargs(self, flow_name: str, payload: FlowArguments) -> dict[str, Any]:
        schema = self.registry.get_flow_schema(flow_name)
        if schema is not None:
            try:
                validated = schema.pydantic_model(**payload.kwargs)
                return validated.model_dump()
            except ValidationError as e:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "message": "Invalid flow arguments",
                        "flow": flow_name,
                        "errors": e.errors(),
                    },
                )
        return payload.kwargs or {}

    # ------------------------------------------------------------------
    # Execution endpoints
    # ------------------------------------------------------------------

    def run_flow(self, flow_name: str, payload: FlowArguments) -> None:
        """Execute a registered flow synchronously.

        Args:
            flow_name: Name of the flow to execute.
            payload: Keyword arguments for the flow function.

        Raises:
            HTTPException: 404 if the flow is not registered.
            HTTPException: 422 if the arguments fail schema validation.
        """
        if flow_name not in self.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")
        fn = self.registry.get_flow(flow_name)
        kwargs = self._parse_kwargs(flow_name, payload)

        run_id = uuid7()
        from ..pubsub import PubSubProtocol
        from ..models import RunState, RunStatus, RunType, Timestamp
        from ..context import ContextManager

        logger.info(
            "flow_run_started",
            extra={"flow_name": flow_name, "run_id": str(run_id)},
        )

        pubsub: PubSubProtocol | None = getattr(self, "pubsub", None)
        snapshot_repo = getattr(self, "snapshot_repo", None)

        state = RunState(
            run_id=run_id,
            flow_name=flow_name,
            status=RunStatus.running,
            worker_id="sync-worker",
            started_at=Timestamp.now(),
            heartbeat_at=Timestamp.now(),
        )

        if snapshot_repo is not None:
            snapshot_repo.update(state)

        if pubsub is not None:
            try:
                pubsub.publish(f"state/{flow_name}", state)
            except Exception:
                pass

        try:
            with ContextManager.begin_span(flow_name, RunType.flow, span_id=run_id):
                fn(**kwargs)

            state.status = RunStatus.completed
            state.ended_at = Timestamp.now()
            state.heartbeat_at = Timestamp.now()

            if snapshot_repo is not None:
                snapshot_repo.update(state)

            if pubsub is not None:
                try:
                    pubsub.publish(f"state/{flow_name}", state)
                except Exception:
                    pass

            logger.info(
                "flow_run_completed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )

        except Exception:
            logger.exception(
                "flow_run_failed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )
            state.status = RunStatus.failed
            state.ended_at = Timestamp.now()
            state.heartbeat_at = Timestamp.now()

            if snapshot_repo is not None:
                snapshot_repo.update(state)

            if pubsub is not None:
                try:
                    pubsub.publish(f"state/{flow_name}", state)
                except Exception:
                    pass

            raise

    def submit_flow(self, flow_name: str, payload: FlowArguments) -> FlowSubmissionResponse:
        """Submit a flow for asynchronous execution via the job queue.

        Validates arguments and enqueues the job, returning immediately with
        tracking information.

        Args:
            flow_name: Name of the flow to submit.
            payload: Keyword arguments for the flow function.

        Returns:
            FlowSubmissionResponse with the job_id and submission timestamp.

        Raises:
            HTTPException: 503 if no queue is configured.
            HTTPException: 404 if the flow is not registered.
            HTTPException: 422 if the arguments fail schema validation.
            HTTPException: 500 if enqueueing fails.
        """
        if self.queue is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Queue not configured. Asynchronous execution is not available. "
                    "Use POST /execute/{flow_name} for synchronous execution."
                ),
            )
        if flow_name not in self.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")

        kwargs = self._parse_kwargs(flow_name, payload)
        job = FlowJob(flow_name=flow_name, kwargs=kwargs)

        try:
            self.queue.enqueue(job)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to enqueue job: {e}",
            )

        logger.info(
            "flow_submitted",
            extra={
                "flow_name": flow_name,
                "job_id": str(job.job_id),
                "run_id": str(job.run_id),
            },
        )
        return FlowSubmissionResponse(
            job_id=job.job_id,
            submitted_at=job.submitted_at,
        )

    # ------------------------------------------------------------------
    # Query endpoints
    # ------------------------------------------------------------------

    async def query_runs(self, request: RunQueryRequest) -> list[RunStateDTO]:
        """Return the most recent run states per flow.

        Args:
            request: Filter parameters — optional flow name allow-list and
                per-flow result limit.

        Returns:
            List of RunStateDTO objects ordered newest-first within each flow.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 400 on any query failure.
        """
        if self.querier is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        try:
            rows = await self.querier.list_recent_states(
                flow_names=request.names,
                last_n=request.last_n,
            )
            return [RunStateDTO.model_validate(row) for row in rows]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")

    async def get_run_by_run_id(self, run_id: UUID, with_logs: bool = True) -> RunDTO:
        """Fetch a run by its ID alone, looking up flow_name from the database.

        Convenience wrapper around ``query_logs`` that first queries the
        database to discover which flow owns the run.

        Args:
            run_id: UUID of the run to fetch.
            with_logs: Include log entries in the response (default: True).

        Returns:
            RunDTO containing the summary tree and (optionally) all log entries.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 if the run cannot be found.
            HTTPException: 400 on any other failure.
        """
        if self.querier is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        try:
            result = await self.querier.get_run_by_run_id(run_id, with_logs)
            return RunDTO.model_validate(result)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    def query_logs(self, request: LogQueryRequest) -> RunDTO:
        """Fetch a single run with its full log detail.

        Loads log entries recursively from storage (subflows and subtasks
        included) and derives a hierarchical RunSummary.

        Args:
            request: Identifies the run by flow_name and run_id; optionally
                suppresses log entries via with_logs=False.

        Returns:
            RunDTO containing the summary tree and (optionally) all log entries.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 if the run cannot be found.
            HTTPException: 400 on any other failure.
        """
        if self.querier is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        try:
            result = self.querier.get_run(
                request.flow_name,
                request.run_id,
                request.with_logs,
            )
            return RunDTO.model_validate(result)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    # ------------------------------------------------------------------
    # Introspection endpoints
    # ------------------------------------------------------------------

    def get_flow_schema(self, flow_name: str) -> dict[str, Any]:
        """Return the parameter schema for a specific flow.

        Args:
            flow_name: Name of the flow.

        Returns:
            Dictionary with flow metadata including JSON Schema and parameter
            descriptions.

        Raises:
            HTTPException: 404 if the flow is not registered.
        """
        if flow_name not in self.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")

        schema = self.registry.get_flow_schema(flow_name)
        if schema is None:
            return {
                "flow_name": flow_name,
                "has_schema": False,
                "message": "No type hints available for this flow",
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
                    "description": p.description,
                }
                for p in schema.parameters
            ],
            "json_schema": schema.pydantic_model.model_json_schema(),
        }

    def list_flows_with_schemas(self) -> list[dict[str, Any]]:
        """List all registered flows with their parameter schemas.

        Returns:
            List of flow metadata dictionaries.
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

    # ------------------------------------------------------------------
    # WebSocket endpoint
    # ------------------------------------------------------------------

    async def ws_runs(self, websocket: WebSocket) -> None:
        """WebSocket endpoint that streams RunState events in real-time.

        Accepts the connection and keeps it open until the client disconnects.
        All state broadcasts are pushed by ConnectionManager independently.

        Args:
            websocket: Incoming WebSocket connection.

        Raises:
            WebSocketDisconnect: handled internally; connection is removed cleanly.
        """
        assert self.connection_manager is not None
        await self.connection_manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            self.connection_manager.disconnect(websocket)

# ---
# endregion
