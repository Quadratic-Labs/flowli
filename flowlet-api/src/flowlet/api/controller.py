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
    AdjudicationRequest,
    AdjudicationResponse,
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
    from ..repository.signals import SignalRepository
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
        signals: "SignalRepository | None" = None,
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
            signals: Optional signal repository; required by the cancel
                endpoint to reach an actively-owned run.
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
        self.signals = signals
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
        from ..models import (
            AttemptOutcome,
            Obligation,
            ObligationRecord,
            Timestamp,
            Verdict,
            VerdictDecision,
        )
        from ..worker import DEFAULT_TIMEOUT

        logger.info(
            "flow_run_started",
            extra={"flow_name": flow_name, "run_id": str(run_id)},
        )

        # Account the run like a worker would: acquire the obligation's
        # lease so the record follows the same fenced discipline
        # (best-effort — a failed acquisition never blocks the call).
        lease = None
        if self.state_repo is not None:
            def _initial(_existing) -> ObligationRecord:
                record = ObligationRecord(
                    obligation=Obligation(
                        id=run_id,
                        flow_name=flow_name,
                        kwargs=kwargs,
                        max_retries=1,  # synchronous calls are never retried
                        created_at=Timestamp.now(),
                    )
                )
                record.begin_attempt("sync-worker")
                return record

            try:
                lease = self.state_repo.acquire(
                    flow_name,
                    run_id,
                    ttl=DEFAULT_TIMEOUT,
                    holder="sync-worker",
                    state_fn=_initial,
                )
            except Exception:
                logger.exception(
                    "sync_run_state_acquire_failed", extra={"run_id": str(run_id)}
                )

        def _finalize(exc: Exception | None) -> None:
            if lease is None:
                return
            record = lease.record
            now = Timestamp.now()
            if exc is None:
                record.record_outcome(
                    AttemptOutcome.returned,
                    verdict=Verdict(
                        decision=VerdictDecision.accepted, rendered_at=now
                    ),
                )
                record.discharge()
            else:
                record.record_outcome(
                    AttemptOutcome.raised,
                    error=type(exc).__name__,
                    verdict=Verdict(
                        decision=VerdictDecision.rejected,
                        rendered_at=now,
                        reason=type(exc).__name__,
                    ),
                )
                record.abandon("max_retries_exceeded")
            try:
                lease.release(record)
            except Exception:
                logger.exception(
                    "sync_run_state_release_failed", extra={"run_id": str(run_id)}
                )

        try:
            try:
                with tracing.run_root(run_id, flow_name):
                    fn(**kwargs)
            finally:
                tracing.force_flush()

            _finalize(None)
            logger.info(
                "flow_run_completed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )

        except Exception as exc:
            logger.exception(
                "flow_run_failed",
                extra={"flow_name": flow_name, "run_id": str(run_id)},
            )
            _finalize(exc)
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

        parent_id = payload.parent_run_id
        root_id = None
        if parent_id is not None:
            # The child's root is the parent's root, or the parent itself.
            root_id = parent_id
            if self.state_repo is not None:
                parent_view = next(
                    (
                        v
                        for v in self.state_repo.list_views()
                        if v.record.obligation.id == parent_id
                    ),
                    None,
                )
                if (
                    parent_view is not None
                    and parent_view.record.obligation.root_id is not None
                ):
                    root_id = parent_view.record.obligation.root_id

        job = FlowJob(
            flow_name=flow_name,
            kwargs=kwargs,
            timeout_seconds=options.timeout_seconds,
            max_retries=options.max_retries,
            parent_id=parent_id,
            root_id=root_id,
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

        An unowned run (released ``pending``, or ``running`` with an expired
        lease) is closed as ``canceled`` directly through a fenced lease
        steal.  An actively-owned run gets its durable ``cancel`` signal
        set; the owning worker observes it at its next heartbeat and
        finalizes the run as ``canceled``.  Already-closed runs are left
        untouched and reported as-is.

        Args:
            run_id: UUID of the run to cancel.

        Returns:
            CancelRunResponse describing the state after the request.

        Raises:
            HTTPException: 503 when no storage backend is configured.
            HTTPException: 404 when the run is not among the active states.
        """
        if self.state_repo is None or self.signals is None:
            raise HTTPException(status_code=503, detail="Storage not configured")

        match = next(
            (
                v
                for v in self.state_repo.list_views()
                if v.record.obligation.id == run_id
            ),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=404,
                detail="Run not found among active runs (it may already be archived)",
            )
        flow_name = match.record.obligation.flow_name

        from ..models import (
            AttemptOutcome,
            ObligationRecord,
            Verdict,
            VerdictDecision,
        )
        from ..repository import AlreadyClosed
        from ..repository.signals import CANCEL
        from ..types import Timestamp

        def _respond(record: ObligationRecord, event: str | None) -> CancelRunResponse:
            state = record.summary()
            if event is not None and self.events is not None:
                self.events.append(
                    flow_name=flow_name,
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
                cancel_requested=state.cancel_requested or event is not None,
            )

        view = self.state_repo.read(flow_name, run_id)
        if view is None:
            raise HTTPException(status_code=404, detail="Run state disappeared")
        if view.record.obligation.status.is_closed():
            return _respond(view.record, None)

        if view.held():
            # Actively owned: set the durable cancel signal; the worker's
            # next heartbeat observes it and finalizes as canceled.
            self.signals.send(flow_name, run_id, CANCEL, actor="api")
            return _respond(view.record, "cancel_requested")

        # Unowned (parked, or crashed in flight): close it here through a
        # fenced steal — the same discipline every actor uses.  A dead
        # in-flight attempt gets its crash accounted before the abandon.
        def transition(existing: ObligationRecord | None) -> ObligationRecord:
            if existing is None:
                raise LookupError(f"account for run {run_id} disappeared")
            if existing.obligation.status.is_closed():
                raise AlreadyClosed(existing)
            if existing.open_attempt is not None:
                existing.record_outcome(
                    AttemptOutcome.crashed,
                    verdict=Verdict(
                        decision=VerdictDecision.rejected,
                        rendered_at=Timestamp.now(),
                        reason="lease_expired",
                    ),
                )
            existing.obligation.cancel_requested = True
            existing.abandon("canceled")
            return existing

        try:
            lease = self.state_repo.acquire(
                flow_name, run_id, ttl=60, holder="api", state_fn=transition
            )
        except AlreadyClosed as closed:
            return _respond(closed.record, None)
        except LookupError:
            raise HTTPException(status_code=404, detail="Run state disappeared")

        if lease is None:
            # A worker claimed it between the read and the steal — fall back
            # to the signal so the new owner cancels cooperatively.
            self.signals.send(flow_name, run_id, CANCEL, actor="api")
            return _respond(view.record, "cancel_requested")

        record = lease.record
        lease.release()
        return _respond(record, "canceled")

    def adjudicate_run(
        self, run_id: UUID, payload: AdjudicationRequest
    ) -> AdjudicationResponse:
        """Resolve a gated obligation with an authorized verdict.

        The obligation must be ``awaiting_adjudication``.  The flow's gate
        policy (if declared) decides whether the actor may adjudicate; only
        the actor and decision are recorded in the account, never the
        policy.  ``accepted`` discharges the obligation; ``rejected``
        reopens it (with a wake-up message when a queue is configured, else
        the sweeper re-enqueues the parked obligation) or abandons it when
        the attempt budget is spent.

        Args:
            run_id: UUID of the gated obligation.
            payload: Decision, actor, and optional reason.

        Returns:
            AdjudicationResponse with the resulting projection status.

        Raises:
            HTTPException: 503 when storage is not configured; 404 when the
                run is unknown; 409 when it is not awaiting adjudication;
                403 when the gate policy refuses the actor.
        """
        if self.state_repo is None:
            raise HTTPException(status_code=503, detail="Storage not configured")

        match = next(
            (
                v
                for v in self.state_repo.list_views()
                if v.record.obligation.id == run_id
            ),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=404,
                detail="Run not found among active runs (it may already be archived)",
            )
        record = match.record
        flow_name = record.obligation.flow_name

        from ..models import (
            ObligationRecord,
            ObligationStatus,
            Verdict,
            VerdictDecision,
        )
        from ..repository import AlreadyClosed
        from ..types import Timestamp

        if record.obligation.status != ObligationStatus.awaiting_adjudication:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Run is not awaiting adjudication "
                    f"(status: {record.obligation.status})"
                ),
            )

        options = self.registry.get_flow_options(flow_name)
        gate = getattr(options, "gate", None)
        if gate is not None and not gate(payload.actor, record):
            raise HTTPException(
                status_code=403,
                detail=f"Actor {payload.actor!r} is not eligible to adjudicate",
            )

        verdict = Verdict(
            decision=VerdictDecision(payload.decision),
            by=payload.actor,
            rendered_at=Timestamp.now(),
            reason=payload.reason,
        )

        def transition(existing: ObligationRecord | None) -> ObligationRecord:
            if existing is None:
                raise LookupError(f"account for run {run_id} disappeared")
            if existing.obligation.status.is_closed():
                raise AlreadyClosed(existing)
            existing.adjudicate(verdict)
            return existing

        try:
            lease = self.state_repo.acquire(
                flow_name, run_id, ttl=60, holder="api", state_fn=transition
            )
        except AlreadyClosed as closed:
            return AdjudicationResponse(
                run_id=run_id,
                status=str(closed.record.summary().status),
                decision=payload.decision,
            )
        except LookupError:
            raise HTTPException(status_code=404, detail="Run state disappeared")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        if lease is None:
            raise HTTPException(
                status_code=409,
                detail="Run is actively owned; retry the adjudication",
            )

        resolved = lease.record
        lease.release()

        if self.events is not None:
            self.events.append(
                flow_name=flow_name,
                run_id=run_id,
                event="adjudicated",
                actor=payload.actor,
                attempt=len(resolved.attempts),
                to_status=str(resolved.summary().status),
                details={"decision": payload.decision, "reason": payload.reason},
            )

        # A rejected verdict with budget left reopens the obligation — wake
        # a worker when we can; the sweeper's parked-requeue is the fallback.
        if (
            resolved.obligation.status == ObligationStatus.open
            and self.queue is not None
        ):
            from ..models import FlowJob

            try:
                self.queue.enqueue(
                    FlowJob(
                        run_id=run_id,
                        flow_name=flow_name,
                        kwargs=resolved.obligation.kwargs,
                        max_retries=resolved.obligation.max_retries,
                        parent_id=resolved.obligation.parent_id,
                        root_id=resolved.obligation.root_id,
                        caused_by=f"adjudication:rejected_by:{payload.actor}",
                    )
                )
            except Exception:
                logger.warning(
                    "adjudication_requeue_failed",
                    extra={"run_id": str(run_id)},
                    exc_info=True,
                )

        logger.info(
            "run_adjudicated",
            extra={
                "run_id": str(run_id),
                "decision": payload.decision,
                "actor": payload.actor,
            },
        )
        return AdjudicationResponse(
            run_id=run_id,
            status=str(resolved.summary().status),
            decision=payload.decision,
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
