"""Unit tests for gated admission — the entry gate (abstractions v0.3, delta 4).

Covers the held lifecycle state (born held, admit → open, projection),
claim refusal and wake-up dropping for held obligations, the account-backed
job source skipping held work, sweeper passivity, the submission and
admission endpoints, and the full held → admitted → executed loop.
"""
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.api.models import AdmissionRequest, FlowArguments
from flowlet.models import (
    Obligation,
    ObligationRecord,
    ObligationStatus,
    RunStatus,
)
from flowlet.queue.account import AccountJobSource
from flowlet.repository import SignalRepository, StateRepository
from flowlet.sweeper import sweep
from flowlet.types import Timestamp
from flowlet.worker import execute_job

from unit.test_worker_layer import FakeExecutor, FakeQueue


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store):
    return StateRepository(store=store)


@pytest.fixture
def signals(store):
    return SignalRepository(store=store)


def _held_record(run_id=None, flow_name="test_flow", **obligation_kwargs):
    return ObligationRecord(
        obligation=Obligation(
            id=run_id or uuid7(),
            flow_name=flow_name,
            admission="gated",
            status=ObligationStatus.held,
            created_at=Timestamp.now(),
            **obligation_kwargs,
        )
    )


# ============================================================================
# Account model
# ============================================================================


@pytest.mark.unit
class TestHeldObligation:
    def test_admit_opens_and_records_the_actor(self):
        record = _held_record()
        record.admit(by="dep-controller")
        assert record.obligation.status == ObligationStatus.open
        assert record.obligation.admitted_by == "dep-controller"
        assert record.obligation.admitted_at is not None

    def test_admit_on_non_held_raises(self):
        record = ObligationRecord(
            obligation=Obligation(
                id=uuid7(), flow_name="f", created_at=Timestamp.now()
            )
        )
        with pytest.raises(ValueError):
            record.admit(by="anyone")

    def test_held_projects_as_held(self):
        state = _held_record().summary()
        assert state.status == RunStatus.held
        assert not state.status.is_closed()


# ============================================================================
# Claimability
# ============================================================================


@pytest.mark.unit
class TestHeldIsNotClaimable:
    def test_wakeup_for_held_run_is_dropped(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        state_repo.create(
            job.flow_name, job.run_id, _held_record(run_id=job.run_id)
        )

        executed = []
        rc = execute_job(
            FakeQueue([job]),
            FakeExecutor({job.flow_name: lambda **kw: executed.append(1)}),
            state_repo, signals, "w1",
        )

        assert rc == 2
        assert executed == []
        record = state_repo.read(job.flow_name, job.run_id).record
        assert record.obligation.status == ObligationStatus.held
        assert record.attempts == []  # nothing was claimed

    def test_account_source_skips_held_until_admitted(self, state_repo):
        record = _held_record()
        obligation = record.obligation
        state_repo.create(obligation.flow_name, obligation.id, record)
        source = AccountJobSource(state_repo=state_repo)

        assert source.dequeue() is None
        assert source.get_queue_size() == 0

        # A fenced admit makes it claimable.
        lease = state_repo.acquire(
            obligation.flow_name, obligation.id, ttl=60, holder="api",
            state_fn=lambda existing: (existing.admit(by="dep"), existing)[1],
        )
        lease.release(lease.record)

        wake = source.dequeue()
        assert wake is not None
        assert wake.run_id == obligation.id

    def test_sweeper_leaves_held_runs_alone(self, state_repo, signals):
        record = _held_record()
        obligation = record.obligation
        state_repo.create(obligation.flow_name, obligation.id, record)

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals)

        assert stats.requeued == 0
        assert stats.failed == 0
        assert queue.enqueued == []
        after = state_repo.read(obligation.flow_name, obligation.id).record
        assert after.obligation.status == ObligationStatus.held


# ============================================================================
# Endpoints: gated submission and admission release
# ============================================================================


@pytest.mark.unit
class TestAdmissionEndpoints:
    def _controller(self, state_repo, signals, queue=None, **kwargs):
        return FlowController(
            state_repo=state_repo, signals=signals, queue=queue, **kwargs
        )

    def test_gated_submission_is_born_held_and_not_enqueued(
        self, state_repo, signals
    ):
        queue = FakeQueue([])
        controller = self._controller(state_repo, signals, queue=queue)

        resp = controller.submit_flow(
            "test_flow", FlowArguments(kwargs={"x": 1}, admission="gated")
        )

        assert resp.status == RunStatus.held
        assert queue.enqueued == []  # nothing could execute it
        record = state_repo.read("test_flow", resp.run_id).record
        assert record.obligation.status == ObligationStatus.held
        assert record.obligation.admission == "gated"
        assert record.obligation.kwargs == {"x": 1}

    def test_gated_submission_stamps_the_adjudication_policy(
        self, state_repo, signals
    ):
        controller = self._controller(
            state_repo, signals, adjudication_for=lambda _f: "gated"
        )
        resp = controller.submit_flow(
            "test_flow", FlowArguments(admission="gated")
        )
        record = state_repo.read("test_flow", resp.run_id).record
        assert record.obligation.adjudication == "gated"

    def test_gated_submission_needs_no_queue(self, state_repo, signals):
        controller = self._controller(state_repo, signals, queue=None)
        resp = controller.submit_flow(
            "test_flow", FlowArguments(admission="gated")
        )
        assert resp.status == RunStatus.held

    def test_admit_opens_and_wakes_a_worker(self, state_repo, signals):
        queue = FakeQueue([])
        controller = self._controller(state_repo, signals, queue=queue)
        resp = controller.submit_flow(
            "test_flow", FlowArguments(kwargs={"x": 1}, admission="gated")
        )

        admitted = controller.admit_run(
            resp.run_id,
            AdmissionRequest(actor="dep-controller", reason="dep_discharged"),
        )

        assert admitted.status == "pending"
        record = state_repo.read("test_flow", resp.run_id).record
        assert record.obligation.status == ObligationStatus.open
        assert record.obligation.admitted_by == "dep-controller"
        wake, _ = queue.enqueued[0]
        assert wake.run_id == resp.run_id
        assert wake.caused_by == "admission:released_by:dep-controller"

    def test_admit_on_non_held_run_is_409(self, state_repo, signals):
        queue = FakeQueue([])
        controller = self._controller(state_repo, signals, queue=queue)
        resp = controller.submit_flow(
            "test_flow", FlowArguments(admission="gated")
        )
        controller.admit_run(resp.run_id, AdmissionRequest(actor="dep"))

        with pytest.raises(HTTPException) as exc:
            controller.admit_run(resp.run_id, AdmissionRequest(actor="dep"))
        assert exc.value.status_code == 409

    def test_full_loop_held_admitted_executed(
        self, state_repo, signals, make_flow_job
    ):
        """The static-graph story: plan durable as obligations, released on
        a completion event, executed by an ordinary worker."""
        queue = FakeQueue([])
        controller = self._controller(state_repo, signals, queue=queue)
        resp = controller.submit_flow(
            "test_flow", FlowArguments(kwargs={"x": 1}, admission="gated")
        )
        controller.admit_run(resp.run_id, AdmissionRequest(actor="dep"))

        seen = []
        wake, _ = queue.enqueued[0]
        queue.jobs = [wake]
        rc = execute_job(
            queue,
            FakeExecutor({"test_flow": lambda **kw: seen.append(kw)}),
            state_repo, signals, "w1",
        )

        assert rc == 0
        assert seen == [{"x": 1}]
        record = state_repo.read("test_flow", resp.run_id).record
        assert record.obligation.status == ObligationStatus.discharged
        assert record.obligation.admitted_by == "dep"
