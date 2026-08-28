"""
FastAPI controllers for the Flowlet account surface.

FlowController handles the kernel's HTTP endpoints: submission, cancel,
adjudication, the fenced executor claim lifecycle, and the run/log query
projections.  Authoring surfaces (synchronous execution of registered
callables, schema introspection) belong to layer-2 controllers — taskflow
mounts them beside this router.
"""
import hashlib
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID

logger = logging.getLogger(__name__)

from fastapi import HTTPException, Request, Response
from pydantic import ValidationError

from flowlet.models import FlowJob
from flowlet.queue import JobQueueProtocol
from flowlet.api.models import (
    AdjudicationRequest,
    AdjudicationResponse,
    AdmissionRequest,
    AdmissionResponse,
    CancelRunResponse,
    ExecutorClaimRequest,
    ExecutorClaimResponse,
    ExecutorEffectRequest,
    ExecutorEffectResponse,
    ExecutorOutcomeRequest,
    ExecutorOutcomeResponse,
    ExecutorRenewRequest,
    ExecutorRenewResponse,
    FlowArguments,
    FlowSubmissionResponse,
    LogQueryRequest,
    RunDTO,
    RunQueryRequest,
    RunStateDTO,
    TransitionPageResponse,
)
from flowlet.api.query import RunQuery

if TYPE_CHECKING:
    from flowlet.events import RunEventLog
    from flowlet.repository.dispatch import DispatchKeyRepository
    from flowlet.repository.signals import SignalRepository
    from flowlet.repository.state import StateRepository
    from flowlet.transitions import TransitionFeed


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


class FlowController:
    """FastAPI controller for the kernel's account surface.

    Attributes:
        querier: RunQuery instance for reading run states and log details.
        queue: Optional job queue for asynchronous execution.
        state_repo: Optional state repository (cancel/adjudicate/claim).
        signals: Optional signal repository (cancel signal, pause scopes).
        prepare_submission: Optional hook ``(flow_name, payload) → payload``
            applied on submission — layer-2 controllers inject schema
            validation and per-flow defaults (timeout, budget) here; the
            kernel accepts anything by default.
        gate_policy: Optional hook ``flow_name → ((actor, record) → bool) |
            None`` consulted by adjudication — eligibility policy stays in
            controllers, the kernel records only actor and decision.

    Example:
        >>> controller = FlowController(querier=run_query)
        >>> await controller.submit_flow("my_flow", FlowArguments(kwargs={"x": 1}))
    """

    # Declared attribute types — the class-level annotations are the single
    # source of truth for type checkers; __init__ assigns against them.
    querier: RunQuery | None
    queue: JobQueueProtocol | None
    state_repo: "StateRepository | None"
    signals: "SignalRepository | None"
    dispatch_repo: "DispatchKeyRepository | None"
    events: "RunEventLog | None"
    transitions: "TransitionFeed | None"
    prepare_submission: "Callable[[str, FlowArguments], FlowArguments] | None"
    gate_policy: "Callable[[str], Callable | None] | None"

    def __init__(
        self,
        *,
        querier: RunQuery | None = None,
        queue: JobQueueProtocol | None = None,
        state_repo: "StateRepository | None" = None,
        signals: "SignalRepository | None" = None,
        dispatch_repo: "DispatchKeyRepository | None" = None,
        events: "RunEventLog | None" = None,
        transitions: "TransitionFeed | None" = None,
        prepare_submission: "Callable[[str, FlowArguments], FlowArguments] | None" = None,
        gate_policy: "Callable[[str], Callable | None] | None" = None,
        adjudication_for: "Callable[[str], str] | None" = None,
        **_,
    ):
        """Initialise the flow controller.

        Args:
            querier: RunQuery providing read access to run states and logs.
                When None the query endpoints return 503.
            queue: Optional job queue for asynchronous flow submission.
            state_repo: Optional state repository; required by the cancel,
                adjudication, and executor endpoints.
            signals: Optional signal repository; required by the cancel
                endpoint to reach an actively-owned run.
            dispatch_repo: Optional dispatch-key repository enabling
                idempotent submissions; when None, dispatch_key submissions
                return 503.
            events: Optional run event log receiving lifecycle events.
            transitions: Optional account-transition feed; when present the
                GET /transitions endpoint serves ordered lifecycle events
                with a cursor for reconcilers.
            prepare_submission: Optional submission hook (layer 2).
            gate_policy: Optional adjudication-eligibility provider (layer 2).
            adjudication_for: Optional per-flow adjudication policy, used
                when a gated-admission submission creates the obligation
                eagerly (queue mode otherwise stamps it at first claim).
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.querier = querier
        self.queue = queue
        self.state_repo = state_repo
        self.signals = signals
        self.dispatch_repo = dispatch_repo
        self.events = events
        self.transitions = transitions
        self.prepare_submission = prepare_submission
        self.gate_policy = gate_policy
        self.adjudication_for = adjudication_for

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
                / admission: gated / adjudicates was given without storage
                configured.
            HTTPException: 404 if the flow is not registered, or the
                adjudicated run is unknown.
            HTTPException: 409 if the adjudicated run is not awaiting
                adjudication.
            HTTPException: 422 if the arguments fail schema validation.
            HTTPException: 500 if enqueueing fails.
        """
        gated_admission = payload.admission == "gated"
        if self.queue is None and not gated_admission:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Queue not configured. Asynchronous execution is not available. "
                    "Use POST /execute/{flow_name} for synchronous execution."
                ),
            )
        if gated_admission and self.state_repo is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "admission: gated requires a storage backend — the held "
                    "obligation is the durable record."
                ),
            )
        if self.prepare_submission is not None:
            payload = self.prepare_submission(flow_name, payload)
        kwargs = payload.kwargs or {}

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
            timeout_seconds=payload.timeout_seconds,
            max_retries=payload.max_retries if payload.max_retries is not None else 3,
            parent_id=parent_id,
            root_id=root_id,
        )

        adjudicates = payload.adjudicates
        target_view = None
        if adjudicates is not None:
            # This submission mints the adjudicator of a parked run: the
            # lock (awaiting_adjudication) is on the target; this obligation
            # is the job that will render the verdict.
            if self.state_repo is None:
                raise HTTPException(
                    status_code=503,
                    detail="adjudicates requires a storage backend",
                )
            from flowlet.models import ObligationStatus

            target_view = next(
                (
                    v
                    for v in self.state_repo.list_views()
                    if v.record.obligation.id == adjudicates
                ),
                None,
            )
            if target_view is None:
                raise HTTPException(
                    status_code=404,
                    detail="Adjudicated run not found among active runs",
                )
            target_status = target_view.record.obligation.status
            if target_status != ObligationStatus.awaiting_adjudication:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Adjudicated run is not awaiting adjudication "
                        f"(status: {target_status})"
                    ),
                )
            job.caused_by = f"adjudicate:{adjudicates}"

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

        if gated_admission:
            # Born held: the durable obligation *is* the submission — no
            # wake-up is enqueued, nothing could execute it.  Release comes
            # through POST /runs/{run_id}/admit (the entry gate).
            from flowlet.models import Obligation, ObligationRecord, ObligationStatus
            from flowlet.types import Timestamp

            adjudication = (
                self.adjudication_for(flow_name)
                if self.adjudication_for is not None
                else "auto"
            )
            record = ObligationRecord(
                obligation=Obligation(
                    id=job.run_id,
                    flow_name=flow_name,
                    kwargs=kwargs,
                    parent_id=parent_id,
                    root_id=root_id,
                    max_retries=job.max_retries,
                    adjudication=adjudication,
                    admission="gated",
                    caused_by=job.caused_by,
                    created_at=Timestamp.now(),
                    status=ObligationStatus.held,
                )
            )
            # Put-if-absent: a duplicate submission converges on the winner.
            self.state_repo.create(flow_name, job.run_id, record)
        else:
            try:
                self.queue.enqueue(job)
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to enqueue job: {e}",
                )

        if adjudicates is not None and target_view is not None:
            # Record the debt on the parked target: its account now answers
            # "who owes me the verdict".  Best-effort after the submission —
            # if the target moved on in between, the adjudicator discovers
            # it at adjudication time (409) and its flow handles it.
            from flowlet.models import ObligationRecord
            target_flow = target_view.record.obligation.flow_name

            def link(existing: "ObligationRecord | None") -> "ObligationRecord":
                if existing is None:
                    raise LookupError(
                        f"account for run {adjudicates} disappeared"
                    )
                existing.assign_adjudicator(job.run_id)
                return existing

            lease = None
            try:
                lease = self.state_repo.acquire(
                    target_flow, adjudicates, ttl=60, holder="api",
                    state_fn=link,
                )
            except (LookupError, ValueError):
                logger.warning(
                    "adjudicator_link_failed",
                    extra={
                        "run_id": str(adjudicates),
                        "adjudicator_id": str(job.run_id),
                    },
                    exc_info=True,
                )
            if lease is not None:
                lease.release()
                if self.events is not None:
                    self.events.append(
                        flow_name=target_flow,
                        run_id=adjudicates,
                        event="adjudicator_assigned",
                        actor="api",
                        details={"adjudicator_id": str(job.run_id)},
                    )

        if self.events is not None:
            details: dict[str, Any] = {"job_id": str(job.job_id)}
            if payload.dispatch_key is not None:
                details["dispatch_key"] = payload.dispatch_key
                details["deduplicated"] = deduplicated
            if gated_admission:
                details["admission"] = "gated"
            if adjudicates is not None:
                details["adjudicates"] = str(adjudicates)
            self.events.append(
                flow_name=flow_name,
                run_id=job.run_id,
                event="submitted",
                actor="api",
                to_status="held" if gated_admission else None,
                details=details,
            )

        logger.info(
            "flow_submitted",
            extra={
                "flow_name": flow_name,
                "job_id": str(job.job_id),
                "run_id": str(job.run_id),
                "deduplicated": deduplicated,
                "admission": payload.admission or "auto",
            },
        )
        if gated_admission:
            from flowlet.models import RunStatus

            return FlowSubmissionResponse(
                job_id=job.job_id,
                run_id=job.run_id,
                status=RunStatus.held,
                submitted_at=job.submitted_at,
                deduplicated=deduplicated,
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

        from flowlet.models import (
            AttemptOutcome,
            ObligationRecord,
            Verdict,
            VerdictDecision,
        )
        from flowlet.repository import AlreadyClosed
        from flowlet.repository.signals import CANCEL
        from flowlet.types import Timestamp

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
        first grants ``extend_budget`` extra attempts (the resume path for
        an obligation gated on exhaustion), then reopens it (with a wake-up
        message when a queue is configured, else the sweeper re-enqueues
        the parked obligation) or abandons it when the attempt budget is
        spent.

        Args:
            run_id: UUID of the gated obligation.
            payload: Decision, actor, optional reason and evidence ref.

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

        from flowlet.models import (
            ObligationRecord,
            ObligationStatus,
            Verdict,
            VerdictDecision,
        )
        from flowlet.repository import AlreadyClosed
        from flowlet.types import Timestamp

        if record.obligation.status != ObligationStatus.awaiting_adjudication:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Run is not awaiting adjudication "
                    f"(status: {record.obligation.status})"
                ),
            )

        gate = self.gate_policy(flow_name) if self.gate_policy is not None else None
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
            evidence_ref=payload.evidence_ref,
        )

        def transition(existing: ObligationRecord | None) -> ObligationRecord:
            if existing is None:
                raise LookupError(f"account for run {run_id} disappeared")
            if existing.obligation.status.is_closed():
                raise AlreadyClosed(existing)
            existing.adjudicate(verdict, extend_budget=payload.extend_budget)
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
            from flowlet.models import FlowJob

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

    def admit_run(
        self, run_id: UUID, payload: AdmissionRequest
    ) -> AdmissionResponse:
        """Release a held obligation's admission — the entry-gate mirror of
        :meth:`adjudicate_run`.

        The obligation must be ``held``.  The release is a fenced
        transition (``held → open``, ``admitted_by``/``admitted_at``
        recorded in the account); a wake-up is enqueued when a queue is
        configured — with the account-backed job source the now-open
        obligation is picked up by polling instead.  *When* to admit is the
        caller's knowledge (a dependency discharged, a human approved); the
        kernel only records the state change and the actor.

        Args:
            run_id: UUID of the held obligation.
            payload: Actor and optional reason.

        Returns:
            AdmissionResponse with the resulting projection status.

        Raises:
            HTTPException: 503 when storage is not configured; 404 when the
                run is unknown; 409 when it is not held.
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

        from flowlet.models import ObligationRecord, ObligationStatus
        from flowlet.repository import AlreadyClosed

        if record.obligation.status != ObligationStatus.held:
            raise HTTPException(
                status_code=409,
                detail=f"Run is not held (status: {record.obligation.status})",
            )

        def transition(existing: ObligationRecord | None) -> ObligationRecord:
            if existing is None:
                raise LookupError(f"account for run {run_id} disappeared")
            if existing.obligation.status.is_closed():
                raise AlreadyClosed(existing)
            existing.admit(by=payload.actor)
            return existing

        try:
            lease = self.state_repo.acquire(
                flow_name, run_id, ttl=60, holder="api", state_fn=transition
            )
        except AlreadyClosed as closed:
            return AdmissionResponse(
                run_id=run_id, status=str(closed.record.summary().status)
            )
        except LookupError:
            raise HTTPException(status_code=404, detail="Run state disappeared")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        if lease is None:
            raise HTTPException(
                status_code=409, detail="Run is busy; retry the admission"
            )

        resolved = lease.record
        lease.release()

        if self.events is not None:
            self.events.append(
                flow_name=flow_name,
                run_id=run_id,
                event="admitted",
                actor=payload.actor,
                from_status="held",
                to_status=str(resolved.summary().status),
                cause=payload.reason,
            )

        # Wake a worker when we can; with the account-backed source the
        # enqueue converges on the existing record and polling takes over.
        if self.queue is not None:
            try:
                self.queue.enqueue(
                    FlowJob(
                        run_id=run_id,
                        flow_name=flow_name,
                        kwargs=resolved.obligation.kwargs,
                        max_retries=resolved.obligation.max_retries,
                        parent_id=resolved.obligation.parent_id,
                        root_id=resolved.obligation.root_id,
                        caused_by=f"admission:released_by:{payload.actor}",
                    )
                )
            except Exception:
                logger.warning(
                    "admission_wakeup_failed",
                    extra={"run_id": str(run_id)},
                    exc_info=True,
                )

        logger.info(
            "run_admitted",
            extra={"run_id": str(run_id), "actor": payload.actor},
        )
        return AdmissionResponse(
            run_id=run_id, status=str(resolved.summary().status)
        )

    # ------------------------------------------------------------------
    # External-executor surface — the claim lifecycle over HTTP
    # ------------------------------------------------------------------

    def _resume_or_409(self, flow_name, run_id, *, executor, epoch, ttl):
        """Reattach to a held lease or raise the fencing 409."""
        assert self.state_repo is not None
        lease = self.state_repo.resume(
            flow_name, run_id, holder=executor, epoch=epoch, ttl=ttl
        )
        if lease is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Lease not held under this (executor, epoch) — it was "
                    "released, stolen after expiry, or never claimed. "
                    "Discard the outcome."
                ),
            )
        return lease

    def claim_run(
        self, run_id: UUID, payload: ExecutorClaimRequest
    ) -> ExecutorClaimResponse:
        """Claim an existing obligation for a detached executor.

        The claim transition (crash accounting for a dead predecessor,
        appending this executor's attempt) is written atomically with the
        fenced acquisition — identical semantics to the in-process worker.
        Obligations are created by submission, never by claim: an unknown
        run is 404.

        Raises:
            HTTPException: 503 without storage; 404 unknown run; 409 when
                the obligation is closed, gated, busy, cancel-pending,
                its attempt budget is spent (a gated obligation then parks
                awaiting adjudication instead of closing), or
                ``resumed_from`` does not reference a recorded attempt.
        """
        if self.state_repo is None or self.signals is None:
            raise HTTPException(status_code=503, detail="Storage not configured")

        from flowlet.models import FlowJob
        from flowlet.repository import AlreadyClosed, Unclaimable
        from flowlet.repository.signals import CANCEL
        from flowlet.worker import DEFAULT_TIMEOUT, _claim_transition

        flow_name = payload.flow_name
        view = self.state_repo.read(flow_name, run_id)
        if view is None:
            raise HTTPException(
                status_code=404,
                detail="Unknown run — obligations are created by submission",
            )

        paused = self.signals.paused_scopes(
            flow_name=flow_name,
            run_id=run_id,
            parent_id=view.record.obligation.parent_id,
            root_id=view.record.obligation.root_id,
        )
        if paused:
            raise HTTPException(
                status_code=409, detail=f"Admission paused at scopes {paused}"
            )

        cancel_pending = (
            self.signals.get(flow_name, run_id, CANCEL) is not None
        )
        ttl = payload.ttl_seconds or DEFAULT_TIMEOUT
        decision: dict = {}
        synthetic = FlowJob(
            run_id=run_id,
            flow_name=flow_name,
            kwargs=view.record.obligation.kwargs,
            max_retries=view.record.obligation.max_retries,
        )
        try:
            lease = self.state_repo.acquire(
                flow_name,
                run_id,
                ttl=ttl,
                holder=payload.executor,
                state_fn=lambda existing: _claim_transition(
                    existing,
                    job=synthetic,
                    worker_id=payload.executor,
                    cancel_pending=cancel_pending,
                    # Creation is forbidden here (404 above), and existing
                    # obligations keep the adjudication fixed at creation.
                    gated=False,
                    decision=decision,
                    resumed_from=payload.resumed_from,
                ),
            )
        except (AlreadyClosed, Unclaimable) as refused:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Run is not claimable "
                    f"(status: {refused.record.obligation.status})"
                ),
            )
        except ValueError as exc:
            # resumed_from validated against the account the CAS observed.
            raise HTTPException(status_code=409, detail=str(exc))
        if lease is None:
            raise HTTPException(
                status_code=409, detail="Run is actively owned by another executor"
            )

        from flowlet.worker import _ClaimCase, _release_terminal

        if decision["case"] == _ClaimCase.gated:
            # Budget spent on a gated obligation: park it for human
            # judgment (resumable via adjudication) and refuse the claim.
            record = lease.record
            _release_terminal(
                lease, self.events, record, "awaiting_adjudication",
                payload.executor, cause="max_retries_exceeded",
            )
            raise HTTPException(
                status_code=409,
                detail="Run gated at claim (awaiting_adjudication)",
            )

        if decision["case"] != _ClaimCase.execute:
            # Cancel honoured or budget spent at claim: close and refuse.
            record = lease.record
            _release_terminal(
                lease, self.events, record,
                "canceled" if decision["case"] == _ClaimCase.canceled else "failed",
                payload.executor,
                cause=(
                    "cancel_requested_before_claim"
                    if decision["case"] == _ClaimCase.canceled
                    else "max_retries_exceeded"
                ),
            )
            raise HTTPException(
                status_code=409,
                detail=f"Run closed at claim ({record.summary().status})",
            )

        record = lease.record
        logger.info(
            "run_claimed_externally",
            extra={"run_id": str(run_id), "executor": payload.executor},
        )
        return ExecutorClaimResponse(
            run_id=run_id,
            epoch=lease.epoch,
            deadline_at=lease.deadline_at,
            attempt=len(record.attempts),
            kwargs=record.obligation.kwargs,
            adjudication=record.obligation.adjudication,
            signals=self.signals.list(flow_name, run_id),
        )

    def renew_run(
        self, run_id: UUID, payload: ExecutorRenewRequest
    ) -> ExecutorRenewResponse:
        """Heartbeat: renew the fenced lease and observe pending signals.

        Raises:
            HTTPException: 503 without storage; 409 when fenced.
        """
        if self.state_repo is None or self.signals is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        from flowlet.lease import LeaseLost
        from flowlet.worker import DEFAULT_TIMEOUT

        lease = self._resume_or_409(
            payload.flow_name, run_id,
            executor=payload.executor, epoch=payload.epoch,
            ttl=payload.ttl_seconds or DEFAULT_TIMEOUT,
        )
        try:
            lease.renew()
        except LeaseLost:
            raise HTTPException(status_code=409, detail="Lease fenced during renewal")
        return ExecutorRenewResponse(
            run_id=run_id,
            deadline_at=lease.deadline_at,
            signals=self.signals.list(payload.flow_name, run_id),
        )

    def record_run_effect(
        self, run_id: UUID, payload: ExecutorEffectRequest
    ) -> ExecutorEffectResponse:
        """Record a side-effect exactly once per occurrence, fenced.

        The executor performs the side-effect and reports its result; the
        occurrence claim makes duplicate reports (retries, races) converge
        on the first recorded result.

        Raises:
            HTTPException: 503 without storage; 409 when fenced.
        """
        if self.state_repo is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        from flowlet.lease import LeaseLost
        from flowlet.models import Effect
        from flowlet.repository import EffectRepository
        from flowlet.types import Timestamp
        from flowlet.worker import DEFAULT_TIMEOUT

        lease = self._resume_or_409(
            payload.flow_name, run_id,
            executor=payload.executor, epoch=payload.epoch,
            ttl=DEFAULT_TIMEOUT,
        )
        effects = EffectRepository(store=self.state_repo.store)
        result, produced = effects.memoize(
            payload.flow_name,
            run_id,
            payload.name,
            lambda: payload.result,
            occurrence=payload.occurrence,
            executor=payload.executor,
        )
        if produced:
            record = lease.record
            record.record_effect(
                Effect(
                    name=payload.name,
                    occurrence=payload.occurrence,
                    attempt_n=len(record.attempts),
                    produced_at=Timestamp.now(),
                    result_ref=result,
                )
            )
            try:
                lease.write(record)
            except LeaseLost:
                raise HTTPException(
                    status_code=409, detail="Lease fenced while recording the effect"
                )
        return ExecutorEffectResponse(
            run_id=run_id,
            name=payload.name,
            occurrence=payload.occurrence,
            result=result,
            produced=produced,
        )

    def record_run_outcome(
        self, run_id: UUID, payload: ExecutorOutcomeRequest
    ) -> ExecutorOutcomeResponse:
        """Conclude the attempt: record the outcome and route the obligation.

        Applies the same shared epilogue as the in-process worker —
        auto-verdict or gated suspension on ``returned``, retry budget on
        ``raised`` (with a wake-up when a queue is configured), abandonment
        on ``interrupted``.

        Raises:
            HTTPException: 503 without storage; 409 when fenced.
        """
        if self.state_repo is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        from flowlet.models import AttemptOutcome
        from flowlet.worker import DEFAULT_TIMEOUT, conclude_attempt

        lease = self._resume_or_409(
            payload.flow_name, run_id,
            executor=payload.executor, epoch=payload.epoch,
            ttl=DEFAULT_TIMEOUT,
        )
        route = conclude_attempt(
            lease,
            outcome=AttemptOutcome(payload.outcome),
            error=payload.error,
            queue=self.queue,
            events=self.events,
            actor=payload.executor,
        )
        if route == "lost":
            raise HTTPException(
                status_code=409,
                detail="Lease fenced at conclusion — the outcome was discarded",
            )
        logger.info(
            "run_concluded_externally",
            extra={
                "run_id": str(run_id),
                "executor": payload.executor,
                "route": route,
            },
        )
        return ExecutorOutcomeResponse(run_id=run_id, status=route)

    # ------------------------------------------------------------------
    # Query endpoints
    # ------------------------------------------------------------------

    def list_transitions(
        self, after: int = 0, limit: int = 256
    ) -> TransitionPageResponse:
        """Read the account-transition feed with a resume cursor.

        Ordered lifecycle events for reconcilers (dependency controllers,
        adjudicator dispatch): pass the returned ``cursor`` back as
        ``after`` to continue from where the last page ended.  The feed is
        a wake-up channel, never authority — a crash can drop an event and
        a re-driven transition can duplicate one, so consumers confirm
        against the account and keep a reconciliation poll as fallback.

        Args:
            after: Cursor from a previous page (0 reads from the start).
            limit: Soft page cap (clamped to 1..1000); pages end on commit
                boundaries so the cursor never splits a commit.

        Returns:
            TransitionPageResponse with the entries and the next cursor.

        Raises:
            HTTPException: 503 when the transition feed is not configured.
        """
        if self.transitions is None:
            raise HTTPException(
                status_code=503, detail="Transition feed not configured"
            )
        page = self.transitions.read(
            after=max(after, 0), limit=min(max(limit, 1), 1000)
        )
        return TransitionPageResponse(entries=page.entries, cursor=page.cursor)

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
        except ValidationError as e:
            logger.exception("run_state_dto_validation_failed")
            raise HTTPException(status_code=500, detail=f"Response validation failed: {e}")
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
            HTTPException: 500 when the run loads but violates the response
                contract (server bug, never "not found").
            HTTPException: 404 if the run cannot be found.
            HTTPException: 400 on any other failure.
        """
        if self.querier is None:
            raise HTTPException(status_code=503, detail="Storage not configured")
        try:
            result = await self.querier.get_run_by_run_id(run_id, with_logs)
            dto = RunDTO.model_validate(result)
        # ValidationError extends ValueError: without its own clause a broken
        # response contract would masquerade as a 404 (run not found).
        except ValidationError as e:
            logger.exception("run_dto_validation_failed", extra={"run_id": str(run_id)})
            raise HTTPException(status_code=500, detail=f"Response validation failed: {e}")
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
            HTTPException: 500 when the run loads but violates the response
                contract (server bug, never "not found").
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
        # ValidationError extends ValueError: without its own clause a broken
        # response contract would masquerade as a 404 (run not found).
        except ValidationError as e:
            logger.exception(
                "run_dto_validation_failed", extra={"run_id": str(request.run_id)}
            )
            raise HTTPException(status_code=500, detail=f"Response validation failed: {e}")
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
