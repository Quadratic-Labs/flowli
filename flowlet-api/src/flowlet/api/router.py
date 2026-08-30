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

from flowlet.api.controller import FlowController
from flowlet.api.models import (
    AdmissionResponse,
    CancelObligationResponse,
    ExecutorClaimResponse,
    ExecutorEffectResponse,
    ExecutorOutcomeResponse,
    ExecutorRecvResponse,
    ExecutorRenewResponse,
    FlowSubmissionResponse,
    MessageResponse,
    ObligationSummaryDTO,
    ReviewResponse,
    TraceDTO,
    TransitionPageResponse,
)


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
            no obligation event log is configured on the controller.
        requires_transitions: When ``True`` the route is omitted from the
            router if no transition feed is configured on the controller.
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
    requires_transitions: bool = False


_SUBMIT_FLOW = RouteSpec(
    path="/submit/{flow_name}",
    method="POST",
    summary="Submit a flow for asynchronous execution",
    description=(
        "Enqueue a flow job and return immediately with tracking info.\n\n"
        "Track execution status: POST /obligations/query\n"
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

_CANCEL_OBLIGATION = RouteSpec(
    path="/obligations/{obligation_id}/cancel",
    method="POST",
    summary="Request cancellation of an active obligation",
    description=(
        "Close a pending obligation as canceled, or flag a running obligation for "
        "cooperative cancellation — the owning worker observes the flag at "
        "its next heartbeat.  Already-closed obligations are reported unchanged."
    ),
    tags=["Execution"],
    response_model=CancelObligationResponse,
    responses={
        200: {"description": "Cancellation applied or already effective"},
        404: {"description": "Obligation not found among active obligations"},
        409: {"description": "Concurrent writes prevented cancellation"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_REVIEW_OBLIGATION = RouteSpec(
    path="/obligations/{obligation_id}/review",
    method="POST",
    summary="Resolve a gated obligation with a review",
    description=(
        "Record an authorized review on an obligation awaiting review: "
        "'approved' discharges the obligation, 'rejected' reopens it for "
        "another attempt (or abandons it when the budget is spent).  The "
        "flow's gate policy decides actor eligibility; actor and decision "
        "are recorded in the account."
    ),
    tags=["Execution"],
    response_model=ReviewResponse,
    responses={
        200: {"description": "Review recorded"},
        403: {"description": "Actor not eligible under the gate policy"},
        404: {"description": "Obligation not found among active obligations"},
        409: {"description": "Obligation is not awaiting review"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_ADMIT_OBLIGATION = RouteSpec(
    path="/obligations/{obligation_id}/admit",
    method="POST",
    summary="Release a held obligation's admission",
    description=(
        "The entry-gate mirror of review: a gated-admission "
        "obligation is born held — durably on the books, not claimable. "
        "This fenced transition opens it (admitted_by/admitted_at recorded "
        "in the account) and wakes a worker. When to admit is the caller's "
        "knowledge — a dependency discharged, a human approved."
    ),
    tags=["Execution"],
    response_model=AdmissionResponse,
    responses={
        200: {"description": "Admission released"},
        404: {"description": "Obligation not found among active obligations"},
        409: {"description": "Obligation is not held"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_CLAIM_OBLIGATION = RouteSpec(
    path="/obligations/{obligation_id}/claim",
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
        404: {"description": "Unknown obligation — submit first"},
        409: {"description": "Not claimable (closed, gated, busy, paused, or budget spent)"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_RENEW_OBLIGATION = RouteSpec(
    path="/obligations/{obligation_id}/renew",
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

_OBLIGATION_EFFECT = RouteSpec(
    path="/obligations/{obligation_id}/effects",
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

_SEND_MESSAGE = RouteSpec(
    path="/obligations/{obligation_id}/messages/{topic}",
    method="POST",
    summary="Send an ordered message to an obligation's topic",
    description=(
        "Append one message to the obligation's message channel. Unlike a signal "
        "(a single-shot latch), messages queue in send order and the obligation's "
        "executor consumes them exactly once via recv. A dedup_key makes "
        "the send idempotent. Senders never touch the obligation's lease."
    ),
    tags=["Execution"],
    response_model=MessageResponse,
    responses={
        200: {"description": "Message appended (or converged via dedup_key)"},
        404: {"description": "Obligation not found among active obligations"},
        409: {"description": "Obligation is closed — nothing will consume"},
        422: {"description": "Unsafe topic name"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_OBLIGATION_RECV = RouteSpec(
    path="/obligations/{obligation_id}/recv",
    method="POST",
    summary="Consume one message from a topic, checkpointed",
    description=(
        "Fenced consumption from the obligation's message channel: seq within the "
        "recorded consumptions replays the identical message (retry "
        "determinism), exactly one past them consumes the next fresh "
        "message and records it in the account under the lease fence."
    ),
    tags=["Executor"],
    response_model=ExecutorRecvResponse,
    responses={
        200: {"description": "Message consumed, replayed, or none pending"},
        409: {"description": "Fenced, seq skipped ahead, or record unreadable"},
        422: {"description": "Unsafe topic name"},
        503: {"description": "Storage not configured"},
    },
    requires_state=True,
)

_OBLIGATION_OUTCOME = RouteSpec(
    path="/obligations/{obligation_id}/outcome",
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

_OBLIGATIONS_QUERY = RouteSpec(
    path="/obligations/query",
    method="POST",
    summary="List recent obligation summaries",
    description="Return the most recent obligation summaries per flow with optional filtering by flow name and result limit.",
    tags=["Query"],
    response_model=list[ObligationSummaryDTO],
    responses={503: {"description": "Storage not configured"}},
    requires_querier=True,
)

_LOGS_QUERY = RouteSpec(
    path="/logs/query",
    method="POST",
    summary="Fetch a single obligation with log detail",
    tags=["Query"],
    response_model=TraceDTO,
    responses={503: {"description": "Storage not configured"}},
    requires_querier=True,
)

_OBLIGATION_BY_ID = RouteSpec(
    path="/obligations/{obligation_id}",
    method="GET",
    summary="Fetch an obligation by its ID",
    description="Look up an obligation using only its obligation_id without needing flow_name.",
    tags=["Query"],
    response_model=TraceDTO,
    responses={
        503: {"description": "Storage not configured"},
        404: {"description": "Obligation not found"},
    },
    requires_querier=True,
)

_TRANSITIONS = RouteSpec(
    path="/transitions",
    method="GET",
    summary="Tail the account-transition feed",
    description=(
        "Ordered lifecycle events for reconcilers, with a resume cursor: "
        "pass the returned cursor back as ?after= to continue. A wake-up "
        "channel, never authority — consumers confirm what they read "
        "against the account and keep a reconciliation poll as fallback."
    ),
    tags=["Query"],
    response_model=TransitionPageResponse,
    responses={
        503: {"description": "Transition feed not configured"},
    },
    requires_transitions=True,
)

_OBLIGATION_EVENTS = RouteSpec(
    path="/obligations/{obligation_id}/events",
    method="GET",
    summary="Fetch an obligation's lifecycle event timeline",
    description=(
        "Return the audit trail of state transitions (submitted, claimed, "
        "completed, retries, cancellation, sweeper recoveries) recorded in "
        "the obligation's append-only events file."
    ),
    tags=["Query"],
    response_model=list[dict],
    responses={
        404: {"description": "Obligation not found"},
        503: {"description": "Storage not configured"},
    },
    requires_events=True,
)


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
    if not _CANCEL_OBLIGATION.requires_state or controller.state_repo is not None:
        _wire(router, _CANCEL_OBLIGATION, controller.cancel_obligation)
        _wire(router, _REVIEW_OBLIGATION, controller.review_obligation)
        _wire(router, _ADMIT_OBLIGATION, controller.admit_obligation)
        _wire(router, _CLAIM_OBLIGATION, controller.claim_obligation)
        _wire(router, _RENEW_OBLIGATION, controller.renew_obligation)
        _wire(router, _OBLIGATION_EFFECT, controller.record_obligation_effect)
        _wire(router, _SEND_MESSAGE, controller.send_obligation_message)
        _wire(router, _OBLIGATION_RECV, controller.recv_obligation_message)
        _wire(router, _OBLIGATION_OUTCOME, controller.record_obligation_outcome)
    if not _OBLIGATIONS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _OBLIGATIONS_QUERY, controller.query_obligations)
    if not _LOGS_QUERY.requires_querier or controller.querier is not None:
        _wire(router, _LOGS_QUERY, controller.query_logs)
    if not _OBLIGATION_BY_ID.requires_querier or controller.querier is not None:
        _wire(router, _OBLIGATION_BY_ID, controller.get_obligation)
    if not _OBLIGATION_EVENTS.requires_events or controller.events is not None:
        _wire(router, _OBLIGATION_EVENTS, controller.get_obligation_events)
    if not _TRANSITIONS.requires_transitions or controller.transitions is not None:
        _wire(router, _TRANSITIONS, controller.list_transitions)


    return router
