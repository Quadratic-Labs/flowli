"""Unit tests for gated obligations: suspension, adjudication, and effects.

Covers the gated flow path (returned attempt with pending verdict,
obligation awaiting_adjudication), the adjudication endpoint (guarded
verdicts, accept/reject routing), the sweeper's passivity toward gated
runs, exactly-once effects across retries, and caused_by provenance.
"""

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

import flowlet as flowlet_pkg
from flowlet.api.controller import FlowController
from flowlet.api.models import AdjudicationRequest
from flowlet.models import (
    AttemptOutcome,
    ObligationStatus,
    RunStatus,
    VerdictDecision,
)
from flowlet.registry import FlowOptions
from flowlet.repository import SignalRepository, StateRepository
from flowlet.sweeper import sweep
from flowlet.worker import execute_job

from .test_worker_layer import FakeQueue, FakeRegistry


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store):
    return StateRepository(store=store)


@pytest.fixture
def signals(store):
    return SignalRepository(store=store)


def _gated_registry(flow_name, fn, gate=None):
    return FakeRegistry(
        {flow_name: fn},
        options={flow_name: FlowOptions(gated=True, gate=gate)},
    )


def _run_gated(state_repo, signals, job, fn=lambda **kw: None, gate=None):
    """Execute a gated job once and return the worker's exit code."""
    registry = _gated_registry(job.flow_name, fn, gate)
    return execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")


class _Controller:
    """Build a controller wired to the same stores as the worker."""

    def __new__(cls, state_repo, signals, registry, queue=None):
        return FlowController(
            registry=registry,
            state_repo=state_repo,
            signals=signals,
            queue=queue,
        )


# ============================================================================
# Suspension
# ============================================================================


@pytest.mark.unit
class TestGatedSuspension:
    def test_returned_attempt_suspends_without_verdict(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        rc = _run_gated(state_repo, signals, job)

        assert rc == 0
        view = state_repo.read(job.flow_name, job.run_id)
        record = view.record
        assert record.obligation.status == ObligationStatus.awaiting_adjudication
        assert record.last_attempt.outcome == AttemptOutcome.returned
        assert record.last_attempt.verdict is None  # judgment pending
        assert record.pending_verdict_attempt is not None
        assert view.holder is None  # parked, passive
        assert view.state.status == RunStatus.gated

    def test_wakeup_for_gated_run_is_dropped(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        epoch_before = state_repo.read(job.flow_name, job.run_id).epoch

        executed = []
        registry = _gated_registry(job.flow_name, lambda **kw: executed.append(1))
        rc = execute_job(FakeQueue([job]), registry, state_repo, signals, "w2")

        assert rc == 2
        assert executed == []
        assert state_repo.read(job.flow_name, job.run_id).epoch == epoch_before

    def test_sweeper_leaves_gated_runs_alone(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals, pending_grace=0)

        assert stats.requeued == 0
        assert queue.enqueued == []


# ============================================================================
# Adjudication endpoint
# ============================================================================


@pytest.mark.unit
class TestAdjudication:
    def test_accepted_discharges_and_records_the_actor(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        controller = _Controller(
            state_repo, signals, _gated_registry(job.flow_name, lambda **kw: None)
        )

        resp = controller.adjudicate_run(
            job.run_id,
            AdjudicationRequest(
                decision="accepted", actor="reviewer-7", reason="looks good"
            ),
        )

        assert resp.status == "completed"
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.discharged
        verdict = record.last_attempt.verdict
        assert verdict.decision == VerdictDecision.accepted
        assert verdict.by == "reviewer-7"
        assert verdict.reason == "looks good"

    def test_rejected_with_budget_reopens_and_wakes_a_worker(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=3)
        _run_gated(state_repo, signals, job)
        queue = FakeQueue([])
        registry = _gated_registry(job.flow_name, lambda **kw: None)
        controller = _Controller(state_repo, signals, registry, queue=queue)

        resp = controller.adjudicate_run(
            job.run_id,
            AdjudicationRequest(decision="rejected", actor="reviewer-7"),
        )

        assert resp.status == "pending"
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.open
        assert record.last_attempt.verdict.decision == VerdictDecision.rejected
        # A wake-up carrying provenance was enqueued …
        wake, _ = queue.enqueued[0]
        assert wake.run_id == job.run_id
        assert wake.caused_by == "adjudication:rejected_by:reviewer-7"
        # … and a worker executes attempt 2, suspending again for judgment.
        queue.jobs = [wake]
        rc = execute_job(queue, registry, state_repo, signals, "w2")
        assert rc == 0
        record = state_repo.read(job.flow_name, job.run_id).record
        assert len(record.attempts) == 2
        assert record.obligation.status == ObligationStatus.awaiting_adjudication

    def test_rejected_without_budget_abandons(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=1)
        _run_gated(state_repo, signals, job)
        controller = _Controller(
            state_repo, signals, _gated_registry(job.flow_name, lambda **kw: None)
        )

        resp = controller.adjudicate_run(
            job.run_id,
            AdjudicationRequest(decision="rejected", actor="reviewer-7"),
        )

        assert resp.status == "failed"
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.obligation.cause == "rejected"

    def test_gate_policy_refuses_ineligible_actor(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        gate = lambda actor, record: actor.startswith("admin")  # noqa: E731
        _run_gated(state_repo, signals, job, gate=gate)
        registry = _gated_registry(job.flow_name, lambda **kw: None, gate=gate)
        controller = _Controller(state_repo, signals, registry)

        with pytest.raises(HTTPException) as exc:
            controller.adjudicate_run(
                job.run_id,
                AdjudicationRequest(decision="accepted", actor="intern-1"),
            )
        assert exc.value.status_code == 403
        # The account is untouched — the refusal never reached the record.
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.awaiting_adjudication

        resp = controller.adjudicate_run(
            job.run_id,
            AdjudicationRequest(decision="accepted", actor="admin-2"),
        )
        assert resp.status == "completed"

    def test_adjudicating_a_non_gated_run_is_409(
        self, state_repo, signals, make_flow_job, make_record, seed_lease, store
    ):
        record = make_record(status=RunStatus.pending)
        seed_lease(store, record)
        controller = _Controller(
            state_repo, signals,
            _gated_registry(record.obligation.flow_name, lambda **kw: None),
        )

        with pytest.raises(HTTPException) as exc:
            controller.adjudicate_run(
                record.obligation.id,
                AdjudicationRequest(decision="accepted", actor="reviewer"),
            )
        assert exc.value.status_code == 409

    def test_gated_run_can_still_be_canceled(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        controller = _Controller(
            state_repo, signals, _gated_registry(job.flow_name, lambda **kw: None)
        )

        resp = controller.cancel_run(job.run_id)

        assert resp.status == RunStatus.canceled
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.obligation.cause == "canceled"


# ============================================================================
# Effects — exactly-once by occurrence
# ============================================================================


@pytest.mark.unit
class TestEffects:
    def test_effect_runs_once_and_lands_in_the_account(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        fired = []

        def flow(**kw):
            result = flowlet_pkg.effect("send_email", lambda: fired.append(1) or "sent")
            assert result == "sent"

        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        assert rc == 0
        assert fired == [1]
        record = state_repo.read(job.flow_name, job.run_id).record
        assert len(record.effects) == 1
        effect = record.effects[0]
        assert effect.name == "send_email"
        assert effect.result_ref == "sent"
        assert effect.attempt_n == 1

    def test_retry_converges_on_the_recorded_effect(
        self, state_repo, signals, make_flow_job
    ):
        """The occurrence key excludes the attempt: a retry replays the
        effect's result instead of re-firing the side effect."""
        job = make_flow_job(max_retries=3)
        fired = []
        calls = {"n": 0}

        def flow(**kw):
            flowlet_pkg.effect("charge_card", lambda: fired.append(1) or "charged")
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("flaky after the effect")

        registry = FakeRegistry({job.flow_name: flow})
        execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")
        rc = execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        assert rc == 0
        assert fired == [1]  # the side effect fired exactly once
        record = state_repo.read(job.flow_name, job.run_id).record
        assert len(record.attempts) == 2
        assert len(record.effects) == 1  # recorded once, on the producing attempt
        assert record.effects[0].attempt_n == 1

    def test_new_occurrence_deliberately_repeats(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        fired = []

        def flow(**kw):
            flowlet_pkg.effect("notify", lambda: fired.append(1), occurrence="1")
            flowlet_pkg.effect("notify", lambda: fired.append(1), occurrence="manual-2")

        registry = FakeRegistry({job.flow_name: flow})
        execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        assert fired == [1, 1]
        record = state_repo.read(job.flow_name, job.run_id).record
        assert [e.occurrence for e in record.effects] == ["1", "manual-2"]

    def test_effect_outside_run_context_is_a_plain_call(self):
        assert flowlet_pkg.effect("adhoc", lambda: 42) == 42


# ============================================================================
# Provenance
# ============================================================================


@pytest.mark.unit
class TestProvenance:
    def test_job_caused_by_lands_on_the_obligation(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        job.caused_by = "dispatch:webhook-42"
        registry = FakeRegistry({job.flow_name: lambda **kw: None})
        execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.caused_by == "dispatch:webhook-42"

    def test_retry_wakeup_carries_attempt_provenance(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=3)

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        execute_job(queue, FakeRegistry({job.flow_name: boom}), state_repo, signals, "w1")

        retry_job, _ = queue.enqueued[0]
        assert retry_job.caused_by == "retry_of_attempt:1"
