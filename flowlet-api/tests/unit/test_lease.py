"""Unit tests for lease renewal (heartbeat) and cooperative cancellation.

Covers RunLease.beat() mechanics (fenced renewal, throttling, cancel-signal
observation), the module-level heartbeat() ergonomics, the worker's
cancel/finalize paths, the controller cancel endpoint, and RunState serdes
compatibility for the cancel_requested record field.
"""
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.lease import (
    LeaseLost,
    RunCancelled,
    RunLease,
    bind_lease,
    current_lease,
    heartbeat,
    unbind_lease,
)
from flowlet.models import RunStatus
from flowlet.repository import SignalRepository, StateRepository
from flowlet.repository.signals import CANCEL
from flowlet.serdes import from_json, to_json
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


def _claimed_lease(state_repo, make_run_state, holder="w1", **overrides):
    """Acquire a running run's lease as a worker claim would."""
    state = make_run_state(status=RunStatus.running, **overrides)
    lease = state_repo.acquire(
        state.flow_name, state.run_id, ttl=600, holder=holder,
        state_fn=lambda _: state,
    )
    assert lease is not None
    return lease


# ============================================================================
# RunLease.beat
# ============================================================================


class TestRunLeaseBeat:
    def test_beat_renews_envelope_deadline(
        self, state_repo, signals, make_run_state
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        before = state_repo.read(state.flow_name, state.run_id).deadline_at

        run_lease = RunLease(lease=lease, signals=signals, min_interval=0.0)
        assert run_lease.beat() is False

        after = state_repo.read(state.flow_name, state.run_id).deadline_at
        assert after.value >= before.value

    def test_beat_is_throttled_within_min_interval(
        self, state_repo, signals, make_run_state, store
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        key = f"state/{state.flow_name}/{state.run_id}.json"
        etag_before = store.get_object_sync(key).etag

        run_lease = RunLease(lease=lease, signals=signals, min_interval=3600.0)
        # Interval starts at claim time, so this beat is a free no-op.
        assert run_lease.beat() is False
        assert store.get_object_sync(key).etag == etag_before  # no write

    def test_beat_observes_cancel_signal(
        self, state_repo, signals, make_run_state
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        signals.send(state.flow_name, state.run_id, CANCEL, actor="api")

        run_lease = RunLease(lease=lease, signals=signals, min_interval=0.0)
        assert run_lease.beat() is True

    def test_beat_raises_lease_lost_when_fenced(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        # The lease expires and another worker steals it (epoch bump).
        seed_lease(
            store, state, holder="w1",
            deadline=datetime.now(UTC) - timedelta(seconds=5),
        )
        thief = state_repo.acquire(
            state.flow_name, state.run_id, ttl=600, holder="other-worker",
            state_fn=lambda s: s,
        )
        assert thief is not None

        run_lease = RunLease(lease=lease, signals=signals, min_interval=0.0)
        with pytest.raises(LeaseLost):
            run_lease.beat()


# ============================================================================
# heartbeat() — module-level ergonomics
# ============================================================================


class TestHeartbeat:
    def test_noop_outside_worker_context(self):
        assert heartbeat() is False

    def test_raises_run_cancelled_by_default(
        self, state_repo, signals, make_run_state
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        signals.send(state.flow_name, state.run_id, CANCEL, actor="api")
        run_lease = RunLease(lease=lease, signals=signals, min_interval=0.0)

        token = bind_lease(run_lease)
        try:
            with pytest.raises(RunCancelled):
                heartbeat()
        finally:
            unbind_lease(token)

    def test_returns_flag_when_raise_disabled(
        self, state_repo, signals, make_run_state
    ):
        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        signals.send(state.flow_name, state.run_id, CANCEL, actor="api")
        run_lease = RunLease(lease=lease, signals=signals, min_interval=0.0)

        token = bind_lease(run_lease)
        try:
            assert heartbeat(raise_on_cancel=False) is True
        finally:
            unbind_lease(token)


# ============================================================================
# Worker — cancellation and finalize paths
# ============================================================================


class TestWorkerCancellation:
    def test_cancelled_flow_finalizes_as_canceled(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()

        def flow(**kw):
            lease = current_lease()
            lease.min_interval = 0.0
            state = lease.lease.state
            signals.send(state.flow_name, state.run_id, CANCEL, actor="api")
            heartbeat()

        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(queue, registry, state_repo, signals, "w1")

        assert rc == 0
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.canceled
        assert view.state.cancel_requested is True
        assert view.state.ended_at is not None
        assert view.holder is None  # terminal states are released
        assert queue.enqueued == []  # cancellation never schedules a retry

    def test_signalled_run_is_canceled_without_executing(
        self, state_repo, signals, make_flow_job, make_run_state, seed_lease, store
    ):
        job = make_flow_job()
        # A released pending run whose cancel arrived while off-lease.
        state = make_run_state(
            run_id=job.run_id, flow_name=job.flow_name, status=RunStatus.pending
        )
        seed_lease(store, state)
        signals.send(job.flow_name, job.run_id, CANCEL, actor="api")

        executed = []
        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: lambda **kw: executed.append(1)})
        rc = execute_job(queue, registry, state_repo, signals, "w1")

        assert rc == 0
        assert executed == []
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.canceled
        assert view.state.cancel_requested is True

    def test_heartbeat_inside_flow_is_harmless_when_not_cancelled(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()

        def flow(**kw):
            lease = current_lease()
            lease.min_interval = 0.0
            heartbeat()

        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(queue, registry, state_repo, signals, "w1")

        assert rc == 0
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.completed


class TestRelease:
    def test_cancel_signal_never_disturbs_the_release(
        self, state_repo, signals, make_run_state
    ):
        """A cancel that lands after the flow finished does not corrupt the
        terminal write — the signal is out-of-band and simply arrives too
        late to be honoured."""
        from attrs import evolve

        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        signals.send(state.flow_name, state.run_id, CANCEL, actor="api")

        lease.release(
            evolve(state, status=RunStatus.completed)
        )
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == RunStatus.completed
        assert view.holder is None

    def test_release_after_fencing_raises_lease_lost(
        self, state_repo, make_run_state, seed_lease, store
    ):
        from attrs import evolve

        lease = _claimed_lease(state_repo, make_run_state)
        state = lease.state
        seed_lease(
            store, state, holder="w1",
            deadline=datetime.now(UTC) - timedelta(seconds=5),
        )
        thief = state_repo.acquire(
            state.flow_name, state.run_id, ttl=600, holder="other",
            state_fn=lambda s: s,
        )
        assert thief is not None

        with pytest.raises(LeaseLost):
            lease.release(evolve(state, status=RunStatus.completed))


# ============================================================================
# Controller — cancel endpoint
# ============================================================================


class TestCancelRun:
    @pytest.fixture
    def controller(self, registry, state_repo, signals):
        return FlowController(
            registry=registry, state_repo=state_repo, signals=signals
        )

    def test_pending_run_is_closed_directly(
        self, controller, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.pending)
        seed_lease(store, state)  # released — nobody owns it

        resp = controller.cancel_run(state.run_id)

        assert resp.status == RunStatus.canceled
        assert resp.cancel_requested is True
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == RunStatus.canceled
        assert view.state.ended_at is not None
        assert view.holder is None

    def test_running_run_is_signalled_not_closed(
        self, controller, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.running)
        seed_lease(
            store, state, holder="w1",
            deadline=datetime.now(UTC) + timedelta(seconds=300),
        )

        resp = controller.cancel_run(state.run_id)

        assert resp.status == RunStatus.running
        assert resp.cancel_requested is True
        # The lease document is untouched; the signal carries the request.
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == RunStatus.running
        assert signals.get(state.flow_name, state.run_id, CANCEL) is not None

    def test_closed_run_is_reported_unchanged(
        self, controller, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=RunStatus.completed)
        seed_lease(store, state)

        resp = controller.cancel_run(state.run_id)

        assert resp.status == RunStatus.completed
        assert resp.cancel_requested is False

    def test_unknown_run_is_404(self, controller):
        with pytest.raises(HTTPException) as exc:
            controller.cancel_run(uuid7())
        assert exc.value.status_code == 404

    def test_no_storage_is_503(self, registry):
        controller = FlowController(registry=registry)
        with pytest.raises(HTTPException) as exc:
            controller.cancel_run(uuid7())
        assert exc.value.status_code == 503


# ============================================================================
# Serdes — cancel_requested wire compatibility
# ============================================================================


class TestCancelRequestedSerdes:
    def test_roundtrip_preserves_flag(self, make_run_state):
        from flowlet.models import RunState

        state = make_run_state()
        state.cancel_requested = True
        restored = from_json(RunState)(to_json(state))
        assert restored.cancel_requested is True

    def test_legacy_state_without_flag_defaults_false(self, make_run_state):
        import json

        from flowlet.models import RunState

        state = make_run_state()
        raw = json.loads(to_json(state))
        del raw["cancel_requested"]  # simulate a pre-upgrade state file
        restored = from_json(RunState)(json.dumps(raw))
        assert restored.cancel_requested is False
