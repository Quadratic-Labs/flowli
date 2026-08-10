"""Unit tests for the sweeper: lease recovery, stuck-pending requeue, archiving."""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import RunStatus
from flowlet.repository import StateRepository
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
def state_repo(tmp_path):
    return StateRepository(store=FilesystemStorage(tmp_path))


@pytest.fixture
def queue():
    return FakeQueue()


def _ts_in(seconds: float) -> Timestamp:
    return Timestamp(datetime.now(UTC) + timedelta(seconds=seconds))


class TestLeaseRecovery:
    def test_live_lease_left_alone(self, state_repo, queue, make_run_state):
        state = make_run_state(status=RunStatus.running, deadline_at=_ts_in(300))
        state_repo.write(state, None)

        stats = sweep(state_repo, queue)

        assert stats.requeued == 0
        current, _ = state_repo.read(state.flow_name, state.run_id)
        assert current.status == RunStatus.running

    def test_expired_lease_requeued_as_pending(self, state_repo, queue, make_run_state):
        state = make_run_state(
            status=RunStatus.running,
            deadline_at=_ts_in(-10),
            attempt=1,
            max_retries=3,
            kwargs={"x": 1},
        )
        state_repo.write(state, None)

        stats = sweep(state_repo, queue)

        assert stats.requeued == 1
        current, _ = state_repo.read(state.flow_name, state.run_id)
        assert current.status == RunStatus.pending
        assert current.attempt == 1  # attempt increments at claim, not at sweep
        job, _ = queue.enqueued[0]
        assert job.run_id == state.run_id
        assert job.kwargs == {"x": 1}  # rebuilt from the state file

    def test_expired_lease_with_exhausted_retries_failed(
        self, state_repo, queue, make_run_state
    ):
        state = make_run_state(
            status=RunStatus.running,
            deadline_at=_ts_in(-10),
            attempt=3,
            max_retries=3,
        )
        state_repo.write(state, None)

        stats = sweep(state_repo, queue)

        assert stats.failed == 1
        assert queue.enqueued == []
        current, _ = state_repo.read(state.flow_name, state.run_id)
        assert current.status == RunStatus.failed
        assert current.ended_at is not None

    def test_sweep_is_idempotent(self, state_repo, queue, make_run_state):
        state = make_run_state(
            status=RunStatus.running, deadline_at=_ts_in(-10), attempt=1
        )
        state_repo.write(state, None)

        sweep(state_repo, queue)
        second = sweep(state_repo, queue, pending_grace=600)

        # The run is now pending and well within its grace window: no-op.
        assert second.requeued == 0
        assert second.failed == 0


class TestStuckPending:
    def test_old_pending_requeued(self, state_repo, queue, make_run_state):
        state = make_run_state(
            status=RunStatus.pending,
            deadline_at=_ts_in(-700),  # failed attempt long past its lease
            kwargs={"y": 2},
        )
        state_repo.write(state, None)

        stats = sweep(state_repo, queue, pending_grace=600)

        assert stats.requeued == 1
        job, _ = queue.enqueued[0]
        assert job.run_id == state.run_id
        assert job.kwargs == {"y": 2}

    def test_recent_pending_left_alone(self, state_repo, queue, make_run_state):
        state = make_run_state(status=RunStatus.pending, deadline_at=_ts_in(-10))
        state_repo.write(state, None)

        stats = sweep(state_repo, queue, pending_grace=600)

        assert stats.requeued == 0


class TestArchiving:
    def test_old_closed_run_archived(self, state_repo, queue, make_run_state, tmp_path):
        state = make_run_state(
            status=RunStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=2)),
        )
        state_repo.write(state, None)

        stats = sweep(state_repo, queue, archive_grace=3600)

        assert stats.archived == 1
        assert state_repo.read(state.flow_name, state.run_id) is None
        assert list((tmp_path / "runs").rglob("state.json"))

    def test_recent_closed_run_kept(self, state_repo, queue, make_run_state):
        state = make_run_state(
            status=RunStatus.completed, ended_at=Timestamp.now()
        )
        state_repo.write(state, None)

        stats = sweep(state_repo, queue, archive_grace=3600)

        assert stats.archived == 0
        assert state_repo.read(state.flow_name, state.run_id) is not None
