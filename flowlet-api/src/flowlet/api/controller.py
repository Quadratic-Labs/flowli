"""
FastAPI controllers for Flowlet.

Provides the FlowController class for handling flow execution and query endpoints.
"""
import hashlib
import json
import logging
from inspect import Parameter
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid7

logger = logging.getLogger(__name__)

from fastapi import HTTPException, Request, Response
from pydantic import ValidationError

from ..models import FlowJob
from ..queue import JobQueueProtocol
from .models import (
    CancelRunResponse,
    FlowArguments,
    FlowSubmissionResponse,
    LogQueryRequest,
    RunDTO,
    RunQueryRequest,
    RunStateDTO,
)
from .query import RunQuery

if TYPE_CHECKING:
    from ..events import RunEventLog
    from ..registry import Registry
    from ..repository.dispatch import DispatchKeyRepository
    from ..repository.state import StateRepository


def _etag_json_response(request: Request, body: str) -> Response:
    """Serve a JSON body with a content-hash ETag, honouring If-None-Match.

    Polling clients send the ETag of their last seen version back via
    ``If-None-Match``; when the content is unchanged they get an empty 304
    and keep their cached copy, so an idle poll costs neither bandwidth nor
    client-side re-parsing.

    Args:
        request: Incoming request (read for the If-None-Match header).
        body: The serialized JSON payload.

    Returns:
        A 304 response when the client already holds this version, else a
        200 JSON response stamped with the ETag.
    """
    etag = f'"{hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]}"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})
    return Response(content=body, media_type="application/json", headers={"ETag": etag})


# region @controller
# ---
# role: api
# intent: handle flow execution and run-query HTTP endpoints
# description: >
#   FlowController is the thin FastAPI handler layer.  It validates incoming
#   requests, delegates to the domain layer (RunQuery, Registry, JobQueue),
#   and maps domain exceptions to HTTP status codes.
#   Execution endpoints: run_flow (synchronous), submit_flow (async queue,
#   idempotent via optional dispatch_key), and cancel_run (cooperative
#   cancellation through the state store).
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
        state_repo: Optional state repository; when present, synchronous
            run_flow executions record their state like a worker would.

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
        state_repo: "StateRepository | None" = None,
        dispatch_repo: "DispatchKeyRepository | None" = None,
        events: "RunEventLog | None" = None,
        **_,
    ):
        """Initialise the flow controller.

        Args:
            registry: Flow registry for resolving registered flows.
            querier: RunQuery providing read access to run states and logs.
                When None the query endpoints return 503.
            queue: Optional job queue for asynchronous flow submission.
            state_repo: Optional state repository so synchronous executions
                leave the same durable record a worker would; also required
                by the cancel endpoint.
            dispatch_repo: Optional dispatch-key repository enabling
                idempotent submissions; when None, dispatch_key submissions
                return 503.
            events: Optional run event log receiving lifecycle events.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.registry = registry
        self.querier = querier
        self.queue = queue
        self.state_repo = state_repo
        self.dispatch_repo = dispatch_repo
        self.events = events

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
        from .. import tracing
        from ..models import RunState, RunStatus, Timestamp

        logger.info(
            "flow_run_started",
            extra={"flow_name": flow_name, "run_id": str(run_id)},
        )

        # Record state like a worker would (best-effort; sync runs have no
        # lease because there is no takeover story for an in-process call).
        state = None
        etag = None
        if self.state_repo is not None:
            state = RunState(
                run_id=run_id,
                flow_name=flow_name,
                status=RunStatus.running,
                worker_id="sync-worker",
                started_at=Timestamp.now(),
                kwargs=kwargs,
            )
            ok, etag = self.state_repo.write(state, None)
            if not ok:
                state = None

        try:
            try:
                with tracing.run_root(run_id, flow_name):
                    fn(**kwargs)
            finally:
                tracing.force_flush()

            if state is not None:
                state.status = RunStatus.completed
                state.ended_at = Timestamp.now()
                self.state_repo.write(state, etag)

            logger.info(
                "flow_run_completed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )

        except Exception:
            logger.exception(
                "flow_run_failed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )
            if state is not None:
                state.status = RunStatus.failed
                state.ended_at = Timestamp.now()
                self.state_repo.write(state, etag)
            raise

    def submit_flow(self, flow_name: str, payload: FlowArguments) -> FlowSubmissionResponse:
        """Submit a flow for asynchronous execution via the job queue.

        Validates arguments and enqueues the job, returning immediately with
        tracking information.  When the payload carries a ``dispatch_key``,
        the submission is idempotent: the first submission with a given key
        creates the run, and every later one resolves to the same run_id
        (``deduplicated=True``).  The wake-up message is enqueued either way
        — duplicates are dropped by the worker state machine, and re-sending
        protects against a lost original message.

        Args:
            flow_name: Name of the flow to submit.
            payload: Keyword arguments for the flow function, plus the
                optional dispatch_key.

        Returns:
            FlowSubmissionResponse with the job_id, run_id, submission
            timestamp, and deduplication outcome.

        Raises:
            HTTPException: 503 if no queue is configured, or a dispatch_key
                was given without storage configured.
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
        options = self.registry.get_flow_options(flow_name)
        job = FlowJob(
            flow_name=flow_name,
            kwargs=kwargs,
            timeout_seconds=options.timeout_seconds,
            max_retries=options.max_retries,
        )

        deduplicated = False
        if payload.dispatch_key is not None:
            if self.dispatch_repo is None:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "dispatch_key requires a storage backend; configure "
                        "storage or submit without a dispatch_key."
                    ),
                )
            run_id, created = self.dispatch_repo.resolve_or_create(
                flow_name, payload.dispatch_key, job.run_id
            )
            deduplicated = not created
            job.run_id = run_id

        try:
            self.queue.enqueue(job)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to enqueue job: {e}",
            )

        if self.events is not None:
            details: dict[str, Any] = {"job_id": str(job.job_id)}
            if payload.dispatch_key is not None:
                details["dispatch_key"] = payload.dispatch_key
                details["deduplicated"] = deduplicated
            self.events.append(
                flow_name=flow_name,
                run_id=job.run_id,
                event="submitted",
                actor="api",
                details=details,
            )

        logger.info(
            "flow_submitted",
            extra={
                "flow_name": flow_name,
                "job_id": str(job.job_id),
                "run_id": str(job.run_id),
                "deduplicated": deduplicated,
            },
        )
        return FlowSubmissionResponse(
            job_id=job.job_id,
            run_id=job.run_id,
            submitted_at=job.submitted_at,
            deduplicated=deduplicated,
        )

    def cancel_run(self, run_id: UUID) -> CancelRunResponse:
        """Request cancellation of an active run.

        A ``pending``/``retry`` run is closed as ``canceled`` directly (no
        worker owns it).  A ``running`` run gets its ``cancel_requested``
        flag set; the owning worker observes it at its next heartbeat and
        finalizes the run as ``canceled``.  Already-closed runs are left
        untouched and reported as-is.

        Args:
            run_id: UUID of the run to cancel.

        Returns:
            CancelRunResponse describing the state after the request.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 when the run is not among the active states.
            HTTPException: 409 when concurrent writes prevented the update.
        """
        if self.state_repo is None:
            raise HTTPException(status_code=503, detail="Storage not configured")

        match = next(
            (s for s in self.state_repo.list_states() if s.run_id == run_id), None
        )
        if match is None:
            raise HTTPException(
                status_code=404,
                detail="Run not found among active runs (it may already be archived)",
            )

        from ..models import RunStatus
        from ..types import Timestamp

        for _ in range(3):
            read = self.state_repo.read(match.flow_name, run_id)
            if read is None:
                raise HTTPException(status_code=404, detail="Run state disappeared")
            state, etag = read

            if state.status.is_closed():
                return CancelRunResponse(
                    run_id=run_id,
                    status=state.status,
                    cancel_requested=state.cancel_requested,
                )

            if state.status == RunStatus.running:
                event = "cancel_requested"
                state.cancel_requested = True
            else:  # pending / retry — nobody owns it, close it here
                event = "canceled"
                state.status = RunStatus.canceled
                state.cancel_requested = True
                state.ended_at = Timestamp.now()

            ok, _ = self.state_repo.write(state, etag)
            if ok:
                if self.events is not None:
                    self.events.append(
                        flow_name=state.flow_name,
                        run_id=run_id,
                        event=event,
                        actor="api",
                        attempt=state.attempt,
                        to_status=str(state.status),
                    )
                logger.info(
                    "run_cancel_requested",
                    extra={"run_id": str(run_id), "status": str(state.status)},
                )
                return CancelRunResponse(
                    run_id=run_id,
                    status=state.status,
                    cancel_requested=state.cancel_requested,
                )

        raise HTTPException(
            status_code=409,
            detail="Concurrent state writes prevented cancellation; retry",
        )

    # ------------------------------------------------------------------
    # Query endpoints
    # ------------------------------------------------------------------

    async def get_run_events(self, request: Request, run_id: UUID) -> Response:
        """Return a run's lifecycle events (audit timeline) in append order.

        The owning flow is resolved from the active state directory first
        (covers live runs the read cache has not seen yet), then from the
        querier's cache (covers archived runs).  Responses carry a
        content-hash ETag; a matching ``If-None-Match`` yields a 304 so
        polling clients only pay for actual changes.

        Args:
            request: Incoming request (If-None-Match handling).
            run_id: UUID of the run.

        Returns:
            JSON list of event records (empty when the run has no events
            file), or 304 when unchanged.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 when the run cannot be resolved to a flow.
        """
        if self.events is None:
            raise HTTPException(status_code=503, detail="Storage not configured")

        flow_name: str | None = None
        if self.state_repo is not None:
            match = next(
                (s for s in self.state_repo.list_states() if s.run_id == run_id),
                None,
            )
            if match is not None:
                flow_name = match.flow_name
        if flow_name is None and self.querier is not None:
            flow_name = await self.querier.find_flow_name(run_id)
        if flow_name is None:
            raise HTTPException(status_code=404, detail="Run not found")

        records = self.events.read(flow_name, run_id)
        return _etag_json_response(request, json.dumps(records, default=str))

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

    async def get_run_by_run_id(
        self, request: Request, run_id: UUID, with_logs: bool = True
    ) -> Response:
        """Fetch a run by its ID alone, looking up flow_name from the database.

        Convenience wrapper around ``query_logs`` that first queries the
        database to discover which flow owns the run.  Responses carry a
        content-hash ETag; a matching ``If-None-Match`` yields a 304 so
        polling clients only pay for actual changes.

        Args:
            request: Incoming request (If-None-Match handling).
            run_id: UUID of the run to fetch.
            with_logs: Include log entries in the response (default: True).

        Returns:
            JSON RunDTO with the summary tree and (optionally) all log
            entries, or 304 when unchanged.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 if the run cannot be found.
            HTTPException: 400 on any other failure.
        """
        if self.querier is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        try:
            result = await self.querier.get_run_by_run_id(run_id, with_logs)
            dto = RunDTO.model_validate(result)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        return _etag_json_response(request, dto.model_dump_json())

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

# ---
# endregion
