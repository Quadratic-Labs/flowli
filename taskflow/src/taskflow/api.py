"""Taskflow's authoring surface: synchronous execution and schema introspection.

Mounted beside the kernel's account router.  Synchronous execution accounts
the obligation exactly like a worker would — acquire the obligation's lease, run
the callable through the RegistryExecutor, conclude — so ``POST /execute``
executions leave the same durable record as queued ones.
"""
import logging
from inspect import Parameter
from typing import TYPE_CHECKING, Any
from uuid import uuid7

from fastapi import APIRouter, HTTPException
from flowlet.api.models import FlowArguments
from flowlet.lease import ObligationCancelled, ObligationLease, bind_lease, unbind_lease
from flowlet.models import AttemptOutcome, Obligation, ObligationRecord
from flowlet.repository import EffectRepository, MessageRepository
from flowlet.types import Timestamp
from flowlet.worker import DEFAULT_TIMEOUT, conclude_attempt

from taskflow.executor import RegistryExecutor

if TYPE_CHECKING:
    from taskflow.app import Taskflow

logger = logging.getLogger(__name__)


def _run_flow_sync(tf: Taskflow, flow_name: str, payload: FlowArguments) -> None:
    """Execute a registered flow synchronously with full account discipline."""
    registry = tf.registry
    if flow_name not in registry.list_flows():
        raise HTTPException(status_code=404, detail="Flow not found")
    prepared = tf.kernel.controller.prepare_submission
    if prepared is not None:
        payload = prepared(flow_name, payload)
    kwargs = payload.kwargs or {}

    state_repo = tf.state_repo
    obligation_id = uuid7()
    executor = RegistryExecutor(registry)

    if state_repo is None:
        # No persistence configured: plain call, no account.
        registry.get_flow(flow_name)(**kwargs)
        return

    def _initial(_existing) -> ObligationRecord:
        record = ObligationRecord(
            obligation=Obligation(
                id=obligation_id,
                flow_name=flow_name,
                kwargs=kwargs,
                max_retries=1,  # synchronous calls are never retried
                review_policy=tf.review_policy_for(flow_name),
                caused_by="sync_execute",
                created_at=Timestamp.now(),
            )
        )
        record.begin_attempt("sync-worker")
        return record

    # configure() wires signals whenever storage is configured.
    assert tf.signals is not None

    lease = state_repo.acquire(
        flow_name, obligation_id, ttl=DEFAULT_TIMEOUT, holder="sync-worker",
        state_fn=_initial,
    )
    assert lease is not None  # fresh uuid7 — cannot be held

    obligation_lease = ObligationLease(
        lease=lease,
        signals=tf.signals,
        effects=EffectRepository(store=state_repo.store),
        messages=MessageRepository(store=state_repo.store),
    )
    token = bind_lease(obligation_lease)
    outcome, error = AttemptOutcome.returned, None
    try:
        executor.execute(lease.record.obligation, 1)
    except ObligationCancelled:
        outcome = AttemptOutcome.interrupted
    except Exception as exc:
        outcome, error = AttemptOutcome.raised, type(exc).__name__
        conclude_attempt(
            lease, outcome=outcome, error=error,
            events=tf.events, actor="sync-worker",
        )
        unbind_lease(token)
        raise
    finally:
        if outcome != AttemptOutcome.raised:
            unbind_lease(token)
            conclude_attempt(
                lease, outcome=outcome, error=error,
                events=tf.events, actor="sync-worker",
            )


def build_taskflow_router(tf: Taskflow) -> APIRouter:
    """The authoring routes: execute, flow listing, schemas."""
    router = APIRouter()

    @router.post(
        "/execute/{flow_name}",
        summary="Execute a registered flow synchronously",
        tags=["Authoring"],
        responses={
            404: {"description": "Flow not found"},
            422: {"description": "Invalid flow arguments"},
        },
    )
    def run_flow(flow_name: str, payload: FlowArguments) -> None:
        _run_flow_sync(tf, flow_name, payload)

    @router.get(
        "/flows",
        summary="List registered flows with their schemas",
        tags=["Authoring"],
    )
    def list_flows() -> list[dict[str, Any]]:
        flows = []
        for flow_name in tf.registry.list_flows():
            fn = tf.registry.get_flow(flow_name)
            schema = tf.registry.get_flow_schema(flow_name)
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

    @router.get(
        "/flows/{flow_name}/schema",
        summary="Parameter schema of one registered flow",
        tags=["Authoring"],
        responses={404: {"description": "Flow not found"}},
    )
    def flow_schema(flow_name: str) -> dict[str, Any]:
        if flow_name not in tf.registry.list_flows():
            raise HTTPException(status_code=404, detail="Flow not found")
        schema = tf.registry.get_flow_schema(flow_name)
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

    return router
