"""Unit tests for the sweeper: lease recovery, stuck-pending requeue, archiving."""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import ReportedStatus
from flowlet.repository import SignalRepository, StateRepository
from flowlet.repository.signals import CANCEL
from flowlet.sweeper import sweep
from flowlet.types import Timestamp


class FakeQueue:
    def __init__(self):
        self.enqueued = []  # (job, delay)

    def enqueue(self, job, delay=0):
        self.enqueued.append((job, delay))
        return job.job_id

    def dequeue(self, timeout=None):  # pragma: no cover - sweeper never dequeues
        return None

    def ack(self, job_id):  # pragma: no cover
        pass

    def get_queue_size(self):
        return len(self.enqueued)


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
def queue():
    return FakeQueue()


def _at(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


class TestLeaseRecovery:
    def test_live_lease_left_alone(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=ReportedStatus.running)
        seed_lease(store, state, holder="w1", deadline=_at(300))

        stats = sweep(state_repo, queue)

        assert stats.requeued == 0
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == ReportedStatus.running
        assert view.epoch == 1  # never touched

    def test_expired_lease_requeued_as_pending(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=ReportedStatus.running,
            attempt=1,
            max_retries=3,
            kwargs={"x": 1},
        )
        seed_lease(store, state, holder="dead-worker", deadline=_at(-10))

        stats = sweep(state_repo, queue)

        assert stats.requeued == 1
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == ReportedStatus.pending
        assert view.state.attempt == 1  # attempt increments at claim, not at sweep
        assert view.holder is None  # recovered runs are parked, released
        assert view.epoch == 2  # the recovery was a fenced steal
        job, _ = queue.enqueued[0]
        assert job.run_id == state.run_id
        assert job.kwargs == {"x": 1}  # rebuilt from the state payload

    def test_expired_lease_with_exhausted_retries_failed(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        # attempt=4: the budget is spent by three consuming attempts; the
        # dead in-flight attempt is a free crash (v0.3 delta 1).
        state = make_run_state(
            status=ReportedStatus.running,
            attempt=4,
            max_retries=3,
        )
        seed_lease(store, state, holder="dead-worker", deadline=_at(-10))

        stats = sweep(state_repo, queue)

        assert stats.failed == 1
        assert queue.enqueued == []
        view = state_repo.read(state.flow_name, state.run_id)
        assert view.state.status == ReportedStatus.failed
        assert view.state.ended_at is not None
        assert view.holder is None

    def test_sweep_is_idempotent(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=ReportedStatus.running, attempt=1)
        seed_lease(store, state, holder="dead-worker", deadline=_at(-10))

        sweep(state_repo, queue)
        second = sweep(state_repo, queue, pending_grace=600)

        # The run is now pending and well within its grace window: no-op.
        assert second.requeued == 0
        assert second.failed == 0


class TestStuckPending:
    def test_old_pending_requeued(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=ReportedStatus.pending, kwargs={"y": 2})
        # Released long ago: the release time (envelope deadline) is stale.
        seed_lease(store, state, deadline=_at(-700))

        stats = sweep(state_repo, queue, pending_grace=600)

        assert stats.requeued == 1
        job, _ = queue.enqueued[0]
        assert job.run_id == state.run_id
        assert job.kwargs == {"y": 2}

    def test_recent_pending_left_alone(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=ReportedStatus.pending)
        seed_lease(store, state, deadline=_at(-10))

        stats = sweep(state_repo, queue, pending_grace=600)

        assert stats.requeued == 0


class TestArchiving:
    def test_old_closed_run_archived(
        self, state_repo, queue, make_run_state, seed_lease, store, tmp_path
    ):
        state = make_run_state(
            status=ReportedStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)

        stats = sweep(state_repo, queue, archive_grace=3600)

        assert stats.archived == 1
        assert state_repo.read(state.flow_name, state.run_id) is None
        assert list((tmp_path / "runs").rglob("state.json"))

    def test_recent_closed_run_kept(
        self, state_repo, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=ReportedStatus.completed, ended_at=Timestamp.now()
        )
        seed_lease(store, state)

        stats = sweep(state_repo, queue, archive_grace=3600)

        assert stats.archived == 0
        assert state_repo.read(state.flow_name, state.run_id) is not None

    def test_archive_clears_signals(
        self, state_repo, signals, queue, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=ReportedStatus.canceled,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        seed_lease(store, state)
        signals.send(state.flow_name, state.run_id, CANCEL, actor="api")

        stats = sweep(state_repo, queue, signals=signals, archive_grace=3600)

        assert stats.archived == 1
        assert signals.get(state.flow_name, state.run_id, CANCEL) is None
