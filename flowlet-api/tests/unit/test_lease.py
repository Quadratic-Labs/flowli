"""Unit tests for lease renewal (heartbeat) and cooperative cancellation.

Covers RunLease.beat() mechanics (renewal, throttling, conflict
reinterpretation), the module-level heartbeat() ergonomics, the worker's
cancel/finalize paths, the controller cancel endpoint, and RunState
serdes compatibility for the new cancel_requested flag.
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
from flowlet.repository import StateRepository
from flowlet.serdes import from_json, to_json
from flowlet.types import Timestamp
from flowlet.worker import _finalize, execute_job

from .test_worker_layer import FakeQueue, FakeRegistry


@pytest.fixture
def state_repo(tmp_path):
    return StateRepository(store=FilesystemStorage(tmp_path))


def _claimed_state(state_repo, make_run_state, **overrides):
    """Write a running state as a worker claim would and return (state, etag)."""
    state = make_run_state(status=RunStatus.running, **overrides)
    ok, etag = state_repo.write(state, None)
    assert ok
    return state, etag


def _flag_cancel(state_repo, state):
    """Set cancel_requested through a concurrent read-modify-write (API-style)."""
    current, etag = state_repo.read(state.flow_name, state.run_id)
    current.cancel_requested = True
    ok, _ = state_repo.write(current, etag)
    assert ok


# ============================================================================
# RunLease.beat
# ============================================================================


class TestRunLeaseBeat:
    def test_beat_renews_deadline_and_advances_etag(
        self, state_repo, make_run_state
    ):
        old_deadline = Timestamp(datetime.now(UTC) + timedelta(seconds=1))
        state, etag = _claimed_state(
            state_repo, make_run_state, deadline_at=old_deadline
        )
        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=0.0,
        )

        assert lease.beat() is False

        stored, new_etag = state_repo.read(state.flow_name, state.run_id)
        assert stored.deadline_at.value > old_deadline.value
        assert new_etag == lease.etag != etag

    def test_beat_is_throttled_within_min_interval(
        self, state_repo, make_run_state
    ):
        state, etag = _claimed_state(state_repo, make_run_state)
        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=3600.0,
        )
        # Interval starts at claim time, so this beat is a free no-op.
        assert lease.beat() is False
        _, stored_etag = state_repo.read(state.flow_name, state.run_id)
        assert stored_etag == etag  # no write happened

    def test_beat_detects_cancel_via_cas_conflict(
        self, state_repo, make_run_state
    ):
        state, etag = _claimed_state(state_repo, make_run_state)
        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=0.0,
        )
        _flag_cancel(state_repo, state)

        assert lease.beat() is True
        assert lease.state.cancel_requested is True

    def test_beat_raises_lease_lost_when_reclaimed(
        self, state_repo, make_run_state
    ):
        state, etag = _claimed_state(state_repo, make_run_state)
        # Another worker reclaims the run (new attempt, new owner).
        current, cur_etag = state_repo.read(state.flow_name, state.run_id)
        current.worker_id = "other-worker"
        current.attempt += 1
        state_repo.write(current, cur_etag)

        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=0.0,
        )
        with pytest.raises(LeaseLost):
            lease.beat()


# ============================================================================
# heartbeat() — module-level ergonomics
# ============================================================================


class TestHeartbeat:
    def test_noop_outside_worker_context(self):
        assert heartbeat() is False

    def test_raises_run_cancelled_by_default(self, state_repo, make_run_state):
        state, etag = _claimed_state(state_repo, make_run_state)
        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=0.0,
        )
        _flag_cancel(state_repo, state)

        token = bind_lease(lease)
        try:
            with pytest.raises(RunCancelled):
                heartbeat()
        finally:
            unbind_lease(token)

    def test_returns_flag_when_raise_disabled(self, state_repo, make_run_state):
        state, etag = _claimed_state(state_repo, make_run_state)
        lease = RunLease(
            state=state, etag=etag, state_repo=state_repo,
            timeout_seconds=600, min_interval=0.0,
        )
        _flag_cancel(state_repo, state)

        token = bind_lease(lease)
        try:
            assert heartbeat(raise_on_cancel=False) is True
        finally:
            unbind_lease(token)


# ============================================================================
# Worker — cancellation and finalize paths
# ============================================================================


class TestWorkerCancellation:
    def test_cancelled_flow_finalizes_as_canceled(
        self, state_repo, make_flow_job
    ):
        job = make_flow_job()

        def flow(**kw):
            lease = current_lease()
            lease.min_interval = 0.0
            _flag_cancel(state_repo, lease.state)
            heartbeat()

        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(queue, registry, state_repo, "w1")

        assert rc == 0
        stored, _ = state_repo.read(job.flow_name, job.run_id)
        assert stored.status == RunStatus.canceled
        assert stored.cancel_requested is True
        assert stored.ended_at is not None
        assert queue.enqueued == []  # cancellation never schedules a retry

    def test_flagged_state_is_canceled_without_executing(
        self, state_repo, make_flow_job, make_run_state
    ):
        job = make_flow_job()
        # A pending run flagged while off-lease (e.g. sweeper-requeued after
        # the owner died mid-cancel).
        state = make_run_state(
            run_id=job.run_id, flow_name=job.flow_name, status=RunStatus.pending
        )
        state.cancel_requested = True
        state_repo.write(state, None)

        executed = []
        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: lambda **kw: executed.append(1)})
        rc = execute_job(queue, registry, state_repo, "w1")

        assert rc == 0
        assert executed == []
        stored, _ = state_repo.read(job.flow_name, job.run_id)
        assert stored.status == RunStatus.canceled

    def test_heartbeat_inside_flow_is_harmless_when_not_cancelled(
        self, state_repo, make_flow_job
    ):
        job = make_flow_job()

        def flow(**kw):
            lease = current_lease()
            lease.min_interval = 0.0
            heartbeat()

        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: flow})
        rc = execute_job(queue, registry, state_repo, "w1")

        assert rc == 0
        stored, _ = state_repo.read(job.flow_name, job.run_id)
        assert stored.status == RunStatus.completed


class TestFinalize:
    def test_finalize_survives_concurrent_cancel_flag_write(
        self, state_repo, make_run_state
    ):
        state, etag = _claimed_state(state_repo, make_run_state)
        _flag_cancel(state_repo, state)  # bumps the version under us

        state.status = RunStatus.completed
        state.ended_at = Timestamp.now()
        assert _finalize(state_repo, state, etag) is True

        stored, _ = state_repo.read(state.flow_name, state.run_id)
        assert stored.status == RunStatus.completed
        assert stored.cancel_requested is True  # flag merged, not lost

    def test_finalize_gives_up_when_reclaimed(self, state_repo, make_run_state):
        state, etag = _claimed_state(state_repo, make_run_state)
        current, cur_etag = state_repo.read(state.flow_name, state.run_id)
        current.worker_id = "other-worker"
        current.attempt += 1
        state_repo.write(current, cur_etag)

        state.status = RunStatus.completed
        assert _finalize(state_repo, state, etag) is False


# ============================================================================
# Controller — cancel endpoint
# ============================================================================


class TestCancelRun:
    @pytest.fixture
    def controller(self, registry, state_repo):
        return FlowController(registry=registry, state_repo=state_repo)

    def test_pending_run_is_closed_directly(
        self, controller, state_repo, make_run_state
    ):
        state = make_run_state(status=RunStatus.pending)
        state_repo.write(state, None)

        resp = controller.cancel_run(state.run_id)

        assert resp.status == RunStatus.canceled
        assert resp.cancel_requested is True
        stored, _ = state_repo.read(state.flow_name, state.run_id)
        assert stored.status == RunStatus.canceled
        assert stored.ended_at is not None

    def test_running_run_is_flagged_not_closed(
        self, controller, state_repo, make_run_state
    ):
        state = make_run_state(status=RunStatus.running)
        state_repo.write(state, None)

        resp = controller.cancel_run(state.run_id)

        assert resp.status == RunStatus.running
        assert resp.cancel_requested is True
        stored, _ = state_repo.read(state.flow_name, state.run_id)
        assert stored.status == RunStatus.running
        assert stored.cancel_requested is True

    def test_closed_run_is_reported_unchanged(
        self, controller, state_repo, make_run_state
    ):
        state = make_run_state(status=RunStatus.completed)
        state_repo.write(state, None)

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
