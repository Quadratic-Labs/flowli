"""
Flowlet FastAPI router assembly.

Provides build_router() to wire a FlowController onto an APIRouter.
Route HTTP metadata (path, method, summary, tags, responses) is defined
separately in @router.contracts so the controller layer stays HTTP-unaware.
"""
from typing import Any, Callable

from attrs import Factory, define
from fastapi import APIRouter

from .controller import FlowController
from .models import FlowSubmissionResponse, RunDTO, RunStateDTO


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


_EXECUTE_FLOW = RouteSpec(
    path="/execute/{flow_name}",
    method="POST",
    summary="Execute a registered flow synchronously",
    description=(
        "Execute a flow with validated arguments and block until it completes.\n\n"
        "Inspect the parameter schema first: GET /flows/{flow_name}/schema"
    ),
    tags=["Execution"],
    responses={
        200: {"description": "Flow executed successfully"},
        404: {"description": "Flow not found"},
        422: {"description": "Invalid flow arguments — see error details"},
    },
)

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

_RUNS_QUERY = RouteSpec(
    path="/runs/query",
    method="POST",
    summary="List recent run states",
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

_LIST_FLOWS = RouteSpec(
    path="/flows",
    method="GET",
    summary="List all registered flows with their schemas",
    description="Returns metadata for all flows including parameter information.",
    tags=["Introspection"],
    response_model=list[dict],
)

_FLOW_SCHEMA = RouteSpec(
    path="/flows/{flow_name}/schema",
    method="GET",
    summary="Get parameter schema for a specific flow",
    description="Returns detailed schema including JSON Schema for client generation.",
    tags=["Introspection"],
    response_model=dict,
    responses={404: {"description": "Flow not found"}},
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

    _wire(router, _EXECUTE_FLOW, controller.run_flow)
    if not _SUBMIT_FLOW.requires_queue or controller.queue is not None:
        _wire(router, _SUBMIT_FLOW, controller.submit_flow)
    if not _RUNS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _RUNS_QUERY, controller.query_runs)
    if not _LOGS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _LOGS_QUERY, controller.query_logs)
    _wire(router, _LIST_FLOWS, controller.list_flows_with_schemas)
    _wire(router, _FLOW_SCHEMA, controller.get_flow_schema)

    return router

# ---
# endregion
