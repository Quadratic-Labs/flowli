"""Unit tests for the kernel extensions pulled by CodeFlow.

Covers scoped pause admission (worker + sweeper), generalized heartbeat
signal observation, generic resource leases, durable timers fired by the
sweeper, and the external-executor claim lifecycle over the controller.
"""
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

import flowlet as flowlet_pkg
from flowlet.api.controller import FlowController
from flowlet.api.models import (
    AdjudicationRequest,
    ExecutorClaimRequest,
    ExecutorEffectRequest,
    ExecutorOutcomeRequest,
    ExecutorRenewRequest,
)
from flowlet.models import (
    AttemptOutcome,
    ObligationStatus,
    RunStatus,
    VerdictDecision,
)
from flowlet.registry import FlowOptions
from flowlet.repository import (
    ResourceLeaseRepository,
    SignalRepository,
    StateRepository,
    TimerRepository,
)
from flowlet.repository.signals import CANCEL, PAUSE
from flowlet.sweeper import sweep
from flowlet.types import Timestamp
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


@pytest.fixture
def timers(store):
    return TimerRepository(store=store)


def _controller(state_repo, signals, registry=None, queue=None):
    return FlowController(
        registry=registry or FakeRegistry({}),
        state_repo=state_repo,
        signals=signals,
        queue=queue,
    )


# ============================================================================
# Pause admission
# ============================================================================


@pytest.mark.unit
class TestPauseAdmission:
    def test_global_pause_blocks_new_claims(
        self, state_repo, signals, make_flow_job
    ):
        signals.send_scoped("global", PAUSE, actor="operator")
        job = make_flow_job()
        executed = []
        registry = FakeRegistry({job.flow_name: lambda **kw: executed.append(1)})

        rc = execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        assert rc == 2
        assert executed == []
        assert state_repo.read(job.flow_name, job.run_id) is None  # nothing claimed

    def test_revoked_pause_reopens_admission(
        self, state_repo, signals, make_flow_job
    ):
        signals.send_scoped("flow:test_flow", PAUSE, actor="operator")
        job = make_flow_job(flow_name="test_flow")
        registry = FakeRegistry({job.flow_name: lambda **kw: None})

        assert execute_job(FakeQueue([job]), registry, state_repo, signals, "w1") == 2

        signals.revoke_scoped("flow:test_flow", PAUSE)
        assert execute_job(FakeQueue([job]), registry, state_repo, signals, "w1") == 0
        assert state_repo.read(job.flow_name, job.run_id).state.status == RunStatus.completed

    def test_ancestor_pause_covers_children(
        self, state_repo, signals, make_flow_job
    ):
        parent = uuid7()
        signals.send_scoped(f"run:{parent}", PAUSE, actor="operator")
        job = make_flow_job()
        job.parent_id = parent
        registry = FakeRegistry({job.flow_name: lambda **kw: None})

        assert execute_job(FakeQueue([job]), registry, state_repo, signals, "w1") == 2

    def test_sweeper_keeps_paused_runs_parked(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.pending)
        seed_lease(store, state, deadline=datetime.now(UTC) - timedelta(seconds=700))
        signals.send_scoped(f"run:{state.run_id}", PAUSE, actor="operator")

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals, pending_grace=0)
        assert stats.requeued == 0

        signals.revoke_scoped(f"run:{state.run_id}", PAUSE)
        stats = sweep(state_repo, queue, signals=signals, pending_grace=0)
        assert stats.requeued == 1


# ============================================================================
# Generalized heartbeat observation
# ============================================================================


@pytest.mark.unit
class TestHeartbeatSignals:
    def test_flow_observes_custom_signals(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        observed = []

        def flow(**kw):
            lease = flowlet_pkg.lease.current_lease()
            lease.min_interval = 0.0
            signals.send(job.flow_name, job.run_id, "interrupt", actor="ui",
                         details={"reason": "steer"})
            observed.append(flowlet_pkg.heartbeat())

        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        assert rc == 0
        pending = observed[0]
        assert "interrupt" in pending
        assert pending["interrupt"]["details"] == {"reason": "steer"}
        assert CANCEL not in pending


# ============================================================================
# Resource leases
# ============================================================================


@pytest.mark.unit
class TestResourceLeases:
    def test_acquire_and_mutual_exclusion(self, store):
        resources = ResourceLeaseRepository(store=store)
        lock = resources.acquire("merge-queue", holder="w1", ttl=60)
        assert lock is not None
        assert lock.epoch == 1
        assert resources.acquire("merge-queue", holder="w2", ttl=60) is None

        lock.release()
        again = resources.acquire("merge-queue", holder="w2", ttl=60)
        assert again is not None
        assert again.epoch == 2

    def test_expired_lease_is_stolen_and_fences(self, store):
        from cairndb.core.exceptions import LeaseLost

        resources = ResourceLeaseRepository(store=store)
        lock = resources.acquire("scope:src/billing/**", holder="w1", ttl=60)

        # Simulate expiry by rewriting the deadline into the past.
        import json

        key = resources._key("scope:src/billing/**")
        obj = store.get_object_sync(key)
        doc = json.loads(obj.data)
        doc["deadline_at"] = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
        store.put_object_sync(key, json.dumps(doc).encode(), if_match=obj.etag)

        thief = resources.acquire("scope:src/billing/**", holder="w2", ttl=60)
        assert thief is not None
        with pytest.raises(LeaseLost):
            lock.renew()


# ============================================================================
# Timers
# ============================================================================


@pytest.mark.unit
class TestTimers:
    def test_due_timer_fires_signal_and_wakeup_then_clears(
        self, state_repo, signals, timers, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.pending)
        seed_lease(store, state)
        timers.set(
            state.flow_name, state.run_id, "gate_timeout",
            due_at=Timestamp(datetime.now(UTC) - timedelta(seconds=1)),
            signal="interrupt", wakeup=True, details={"grace": "expired"},
        )

        queue = FakeQueue([])
        stats = sweep(
            state_repo, queue, signals=signals, timers=timers,
            pending_grace=3600,
        )

        assert stats.timers_fired == 1
        sent = signals.get(state.flow_name, state.run_id, "interrupt")
        assert sent is not None
        assert sent["details"]["timer"] == "gate_timeout"
        wake, _ = queue.enqueued[0]
        assert wake.run_id == state.run_id
        assert wake.caused_by == "timer:gate_timeout"
        assert timers.due(Timestamp(datetime.now(UTC) + timedelta(days=1))) == []

    def test_undue_timer_is_left_alone(
        self, state_repo, signals, timers, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.pending)
        seed_lease(store, state)
        timers.set(
            state.flow_name, state.run_id, "later",
            due_at=Timestamp(datetime.now(UTC) + timedelta(hours=1)),
            wakeup=True,
        )

        stats = sweep(
            state_repo, FakeQueue([]), signals=signals, timers=timers,
            pending_grace=3600,
        )
        assert stats.timers_fired == 0
        assert len(timers.due(Timestamp(datetime.now(UTC) + timedelta(days=1)))) == 1

    def test_timer_for_closed_run_clears_without_firing(
        self, state_repo, signals, timers, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.completed, ended_at=Timestamp.now())
        seed_lease(store, state)
        timers.set(
            state.flow_name, state.run_id, "stale",
            due_at=Timestamp(datetime.now(UTC) - timedelta(seconds=1)),
            signal="interrupt",
        )

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals, timers=timers)

        assert stats.timers_fired == 0
        assert signals.get(state.flow_name, state.run_id, "interrupt") is None
        assert timers.due(Timestamp(datetime.now(UTC) + timedelta(days=1))) == []


# ============================================================================
# External-executor surface
# ============================================================================


@pytest.mark.unit
class TestExternalExecutor:
    def _seed_ready(self, make_run_state, seed_lease, store, **overrides):
        state = make_run_state(status=RunStatus.pending, **overrides)
        seed_lease(store, state)
        return state

    def test_full_lifecycle_claim_renew_effect_outcome(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store, attempt=1)
        controller = _controller(state_repo, signals)

        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(
                flow_name=state.flow_name, executor="omnigent-session-1"
            ),
        )
        assert claim.attempt == 2  # a fresh attempt on top of the failed one
        assert claim.adjudication == "auto"
        epoch = claim.epoch

        renewed = controller.renew_run(
            state.run_id,
            ExecutorRenewRequest(
                flow_name=state.flow_name, executor="omnigent-session-1",
                epoch=epoch,
            ),
        )
        assert renewed.signals == {}

        effect = controller.record_run_effect(
            state.run_id,
            ExecutorEffectRequest(
                flow_name=state.flow_name, executor="omnigent-session-1",
                epoch=epoch, name="implement", occurrence="spec-abc",
                result="commit:a1b2c3",
            ),
        )
        assert effect.produced is True
        assert effect.result == "commit:a1b2c3"

        outcome = controller.record_run_outcome(
            state.run_id,
            ExecutorOutcomeRequest(
                flow_name=state.flow_name, executor="omnigent-session-1",
                epoch=epoch, outcome="returned",
            ),
        )
        assert outcome.status == "completed"

        record = state_repo.read(state.flow_name, state.run_id).record
        assert record.obligation.status == ObligationStatus.discharged
        last = record.last_attempt
        assert last.executor == "omnigent-session-1"
        assert last.outcome == AttemptOutcome.returned
        assert last.verdict.decision == VerdictDecision.accepted
        assert record.effects[0].result_ref == "commit:a1b2c3"

    def test_duplicate_effect_report_converges(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )

        first = controller.record_run_effect(
            state.run_id,
            ExecutorEffectRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                name="send", result="v1",
            ),
        )
        second = controller.record_run_effect(
            state.run_id,
            ExecutorEffectRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                name="send", result="v2-should-be-ignored",
            ),
        )
        assert first.produced is True
        assert second.produced is False
        assert second.result == "v1"
        record = state_repo.read(state.flow_name, state.run_id).record
        assert len(record.effects) == 1

    def test_claim_unknown_run_is_404(self, state_repo, signals):
        controller = _controller(state_repo, signals)
        with pytest.raises(HTTPException) as exc:
            controller.claim_run(
                uuid7(),
                ExecutorClaimRequest(flow_name="ghost", executor="s1"),
            )
        assert exc.value.status_code == 404

    def test_claim_of_held_run_is_409(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.running)
        seed_lease(
            store, state, holder="other",
            deadline=datetime.now(UTC) + timedelta(seconds=300),
        )
        controller = _controller(state_repo, signals)
        with pytest.raises(HTTPException) as exc:
            controller.claim_run(
                state.run_id,
                ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
            )
        assert exc.value.status_code == 409

    def test_stale_epoch_is_fenced_with_409(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )
        with pytest.raises(HTTPException) as exc:
            controller.renew_run(
                state.run_id,
                ExecutorRenewRequest(
                    flow_name=state.flow_name, executor="s1",
                    epoch=claim.epoch + 1,
                ),
            )
        assert exc.value.status_code == 409

    def test_raised_outcome_parks_and_wakes_a_retry(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store, max_retries=3)
        queue = FakeQueue([])
        controller = _controller(state_repo, signals, queue=queue)
        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )

        outcome = controller.record_run_outcome(
            state.run_id,
            ExecutorOutcomeRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                outcome="raised", error="ToolFailure",
            ),
        )

        assert outcome.status == "pending"
        record = state_repo.read(state.flow_name, state.run_id).record
        assert record.last_attempt.error == "ToolFailure"
        wake, _ = queue.enqueued[0]
        assert wake.run_id == state.run_id

    def test_gated_flow_suspends_then_adjudicates(
        self, state_repo, signals, make_record, seed_lease, store
    ):
        # The adjudication policy is part of the contract, fixed at the
        # obligation's creation — seed a gated one directly.
        record = make_record(status=RunStatus.pending)
        record.obligation.adjudication = "gated"
        seed_lease(store, record)
        state = record.obligation
        registry = FakeRegistry(
            {state.flow_name: lambda **kw: None},
            options={state.flow_name: FlowOptions(gated=True)},
        )
        controller = _controller(state_repo, signals, registry=registry)
        claim = controller.claim_run(
            state.id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )
        assert claim.adjudication == "gated"

        outcome = controller.record_run_outcome(
            state.id,
            ExecutorOutcomeRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                outcome="returned",
            ),
        )
        assert outcome.status == "gated"

        resp = controller.adjudicate_run(
            state.id,
            AdjudicationRequest(decision="accepted", actor="reviewer-1"),
        )
        assert resp.status == "completed"

    def test_external_claim_accounts_a_crashed_predecessor(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=RunStatus.running, worker_id="dead-session",
            attempt=1, max_retries=3,
        )
        seed_lease(
            store, state, holder="dead-session",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )
        controller = _controller(state_repo, signals)

        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s2"),
        )
        assert claim.attempt == 2
        record = state_repo.read(state.flow_name, state.run_id).record
        assert record.attempts[0].outcome == AttemptOutcome.crashed
        assert record.attempts[0].executor == "dead-session"
        assert record.open_attempt.executor == "s2"
