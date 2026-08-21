"""
Flowlet FastAPI router assembly.

Provides build_router() to wire a FlowController onto an APIRouter.
Route HTTP metadata (path, method, summary, tags, responses) is defined
separately in @router.contracts so the controller layer stays HTTP-unaware.
"""
from collections.abc import Callable
from typing import Any

from attrs import Factory, define
from fastapi import APIRouter

from .controller import FlowController
from .models import (
    AdjudicationResponse,
    CancelRunResponse,
    ExecutorClaimResponse,
    ExecutorEffectResponse,
    ExecutorOutcomeResponse,
    ExecutorRenewResponse,
    FlowSubmissionResponse,
    RunDTO,
    RunStateDTO,
)

# region @router.contracts
# ---
# role: api
# intent: declare HTTP route metadata independently of the controller implementation
# description: >
#   RouteSpec carries every piece of FastAPI route metadata except the endpoint
#   callable.  All route definitions live here so the HTTP contract (URLs,
#   methods, summaries, tags, status codes) can be read and changed in one
#   place without touching any controller logic.
#   build_router (see @router.routes) consumes these specs and attaches
#   controller methods as callbacks via router.add_api_route.
# rules:
#   - RouteSpec MUST NOT reference FlowController or any domain type.
#   - Conditional routes (requires_queue=True) MUST be skipped silently
#     by build_router when no queue is available.
# ---


@define(slots=True, kw_only=True)
class RouteSpec:
    """HTTP route contract — metadata only, no endpoint reference.

    Attributes:
        path: URL path, may contain FastAPI path parameters.
        method: HTTP method string (``"GET"``, ``"POST"``, …).
        summary: Short OpenAPI summary line.
        tags: OpenAPI tag groups.
        description: Longer OpenAPI description (optional).
        response_model: Pydantic model for the success response (optional).
        responses: Extra OpenAPI response descriptions keyed by status code.
        requires_queue: When ``True`` the route is omitted from the router if
            no job queue is configured on the controller.
        requires_querier: When ``True`` the route is omitted from the router if
            no storage / querier is configured on the controller.
        requires_state: When ``True`` the route is omitted from the router if
            no state repository is configured on the controller.
        requires_events: When ``True`` the route is omitted from the router if
            no run event log is configured on the controller.
    """
    path: str
    method: str
    summary: str
    tags: list[str]
    description: str = ""
    response_model: Any = None
    responses: dict[int, dict[str, str]] = Factory(dict)
    requires_queue: bool = False
    requires_querier: bool = False
    requires_state: bool = False
    requires_events: bool = False


_SUBMIT_FLOW = RouteSpec(
    path="/submit/{flow_name}",
    method="POST",
    summary="Submit a flow for asynchronous execution",
    description=(
        "Enqueue a flow job and return immediately with tracking info.\n\n"
        "Track execution status: POST /runs/query\n"
        "Inspect the parameter schema first: GET /flows/{flow_name}/schema"
    ),
    tags=["Execution"],
    response_model=FlowSubmissionResponse,
    responses={
        200: {"description": "Flow submitted successfully"},
        404: {"description": "Flow not found"},
        422: {"description": "Invalid flow arguments — see error details"},
        503: {"description": "Queue not configured"},
    },
    requires_queue=True,
)

_CANCEL_RUN = RouteSpec(
    path="/runs/{run_id}/cancel",
    method="POST",
    summary="Request cancellation of an active run",
    description=(
        "Close a pending run as canceled, or flag a running run for "
        "cooperative cancellation — the owning worker observes the flag at "
        "its next heartbeat.  Already-closed runs are reported unchanged."
    ),
    tags=["Execution"],
    response_model=CancelRunResponse,
    responses={
        200: {"description": "Cancellation applied or already effective"},
        404: {"description": "Run not found among active runs"},
        409: {"description": "Concurrent writes prevented cancellation"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_ADJUDICATE_RUN = RouteSpec(
    path="/runs/{run_id}/adjudicate",
    method="POST",
    summary="Resolve a gated obligation with a verdict",
    description=(
        "Record an authorized verdict on a run awaiting adjudication: "
        "'accepted' discharges the obligation, 'rejected' reopens it for "
        "another attempt (or abandons it when the budget is spent).  The "
        "flow's gate policy decides actor eligibility; actor and decision "
        "are recorded in the account."
    ),
    tags=["Execution"],
    response_model=AdjudicationResponse,
    responses={
        200: {"description": "Verdict recorded"},
        403: {"description": "Actor not eligible under the gate policy"},
        404: {"description": "Run not found among active runs"},
        409: {"description": "Run is not awaiting adjudication"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_CLAIM_RUN = RouteSpec(
    path="/runs/{run_id}/claim",
    method="POST",
    summary="Claim an obligation for a detached executor",
    description=(
        "Fenced acquisition of an existing obligation's lease for an "
        "external executor (harness). The claim transition — crash "
        "accounting for a dead predecessor, appending this executor's "
        "attempt — is atomic with the acquisition. Returns the epoch fence "
        "token required by every subsequent renew/effect/outcome call."
    ),
    tags=["Executor"],
    response_model=ExecutorClaimResponse,
    responses={
        200: {"description": "Claimed; epoch is the fence token"},
        404: {"description": "Unknown run — submit first"},
        409: {"description": "Not claimable (closed, gated, busy, paused, or budget spent)"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_RENEW_RUN = RouteSpec(
    path="/runs/{run_id}/renew",
    method="POST",
    summary="Heartbeat a held lease and observe signals",
    tags=["Executor"],
    response_model=ExecutorRenewResponse,
    responses={
        200: {"description": "Renewed; pending signals included"},
        409: {"description": "Fenced — discard the outcome"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_RUN_EFFECT = RouteSpec(
    path="/runs/{run_id}/effects",
    method="POST",
    summary="Record a side-effect exactly once per occurrence",
    tags=["Executor"],
    response_model=ExecutorEffectResponse,
    responses={
        200: {"description": "Effect recorded (or converged on a prior record)"},
        409: {"description": "Fenced — discard the outcome"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_RUN_OUTCOME = RouteSpec(
    path="/runs/{run_id}/outcome",
    method="POST",
    summary="Conclude the attempt and route the obligation",
    tags=["Executor"],
    response_model=ExecutorOutcomeResponse,
    responses={
        200: {"description": "Outcome recorded; status is the route taken"},
        409: {"description": "Fenced — the outcome was discarded"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_RUNS_QUERY = RouteSpec(
    path="/runs/query",
    method="POST",
    summary="List recent run states",
    description="Return the most recent run states per flow with optional filtering by flow name and result limit.",
    tags=["Query"],
    response_model=list[RunStateDTO],
    responses={503: {"description": "Storage not configured"}},
    requires_querier=True,
)

_LOGS_QUERY = RouteSpec(
    path="/logs/query",
    method="POST",
    summary="Fetch a single run with log detail",
    tags=["Query"],
    response_model=RunDTO,
    responses={503: {"description": "Storage not configured"}},
    requires_querier=True,
)

_RUN_BY_ID = RouteSpec(
    path="/runs/{run_id}",
    method="GET",
    summary="Fetch a run by its ID",
    description="Look up a run using only its run_id without needing flow_name.",
    tags=["Query"],
    response_model=RunDTO,
    responses={
        503: {"description": "Storage not configured"},
        404: {"description": "Run not found"},
    },
    requires_querier=True,
)

_RUN_EVENTS = RouteSpec(
    path="/runs/{run_id}/events",
    method="GET",
    summary="Fetch a run's lifecycle event timeline",
    description=(
        "Return the audit trail of state transitions (submitted, claimed, "
        "completed, retries, cancellation, sweeper recoveries) recorded in "
        "the run's append-only events file."
    ),
    tags=["Query"],
    response_model=list[dict],
    responses={
        404: {"description": "Run not found"},
        503: {"description": "Storage not configured"},
    },
    requires_events=True,
)

# ---
# endregion


# region @router.routes
# ---
# role: api
# intent: wire RouteSpec contracts to FlowController methods via add_api_route
# description: >
#   build_router iterates over the route registry, skipping requires_queue
#   entries when no queue is present, and calls router.add_api_route with
#   the bound controller method as the endpoint callable.
#   FastAPI introspects the bound method signature directly — no closure
#   wrappers are needed and the controller stays free of HTTP concerns.
# rules:
#   - MUST use add_api_route; MUST NOT introduce closure wrappers.
#   - Conditional routes MUST be skipped when controller.queue is None.
# dependencies:
#   - controller
# aliases:
#   - flowlet-router
# triggers:
#   - where are routes defined
#   - how to add a new endpoint
# ---


def _wire(router: APIRouter, spec: RouteSpec, endpoint: Callable) -> None:
    """Register one RouteSpec onto *router* with *endpoint* as the handler."""
    router.add_api_route(
        spec.path,
        endpoint=endpoint,
        methods=[spec.method],
        summary=spec.summary,
        description=spec.description or None,
        tags=spec.tags,  # type: ignore[arg-type]  # list[str] satisfies list[str|Enum] at runtime
        response_model=spec.response_model,
        responses=spec.responses,  # type: ignore[arg-type]  # int keys satisfy int|str at runtime
    )


def build_router(controller: FlowController, **_) -> APIRouter:
    """Assemble and return the Flowlet APIRouter.

    Attaches each RouteSpec to the matching FlowController method.
    Routes marked ``requires_queue=True`` are omitted when no queue is
    configured so the OpenAPI schema does not advertise unavailable endpoints.

    Args:
        controller: Fully-initialised FlowController to handle requests.

    Returns:
        APIRouter ready to be included in a FastAPI application.

    Example:
        >>> app = FastAPI()
        >>> app.include_router(build_router(controller), prefix="/flowlet")
    """
    router = APIRouter()

    if not _SUBMIT_FLOW.requires_queue or controller.queue is not None:
        _wire(router, _SUBMIT_FLOW, controller.submit_flow)
    if not _CANCEL_RUN.requires_state or controller.state_repo is not None:
        _wire(router, _CANCEL_RUN, controller.cancel_run)
        _wire(router, _ADJUDICATE_RUN, controller.adjudicate_run)
        _wire(router, _CLAIM_RUN, controller.claim_run)
        _wire(router, _RENEW_RUN, controller.renew_run)
        _wire(router, _RUN_EFFECT, controller.record_run_effect)
        _wire(router, _RUN_OUTCOME, controller.record_run_outcome)
    if not _RUNS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _RUNS_QUERY, controller.query_runs)
    if not _LOGS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _LOGS_QUERY, controller.query_logs)
    if not _RUN_BY_ID.requires_querier or controller.querier is not None:
        _wire(router, _RUN_BY_ID, controller.get_run_by_run_id)
    if not _RUN_EVENTS.requires_events or controller.events is not None:
        _wire(router, _RUN_EVENTS, controller.get_run_events)


    return router

# ---
# endregion
