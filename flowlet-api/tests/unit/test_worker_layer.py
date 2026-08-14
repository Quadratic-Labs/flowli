"""Unit tests for the lease-based worker state machine and execute_job.

Covers JobState resolution (existing_state_case over StateView) and the
end-to-end execute_job paths: atomic claim + immediate ack, success, retry
accounting via self-enqueue, terminal failure, busy/closed message dropping,
and expired lease takeover with epoch fencing.
"""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import RunStatus
from flowlet.repository import SignalRepository, StateRepository, StateView
from flowlet.types import Timestamp
from flowlet.worker import JobState, execute_job, existing_state_case


# ============================================================================
# Fakes
# ============================================================================


class FakeQueue:
    """Minimal JobQueueProtocol implementation recording every interaction."""

    def __init__(self, jobs=()):
        self.jobs = list(jobs)
        self.acked = []
        self.enqueued = []  # (job, delay)

    def enqueue(self, job, delay=0):
        self.enqueued.append((job, delay))
        return job.job_id

    def dequeue(self, timeout=None):
        return self.jobs.pop(0) if self.jobs else None

    def ack(self, job_id):
        self.acked.append(job_id)

    def get_queue_size(self):
        return len(self.jobs)


class FakeRegistry:
    """Maps flow names to callables."""

    def __init__(self, flows):
        self.flows = flows

    def get_flow(self, name):
        return self.flows[name]

    def list_flows(self):
        return list(self.flows)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store):
    return StateRepository(store=store)


@pytest.fixture
def signals(store):
    return SignalRepository(store=store)


def _ts_in(seconds: float) -> Timestamp:
    return Timestamp(datetime.now(UTC) + timedelta(seconds=seconds))


def _view(state, *, holder=None, deadline: Timestamp | None = None, epoch=1) -> StateView:
    """Build a StateView with an explicit envelope for classification tests."""
    return StateView(
        state=state,
        holder=holder,
        deadline_at=deadline if deadline is not None else Timestamp.now(),
        epoch=epoch,
    )


# ============================================================================
# existing_state_case — envelope-based liveness
# ============================================================================


class TestExistingStateCase:
    def test_no_state_is_new(self):
        assert existing_state_case(None) == JobState.new

    def test_closed_statuses_are_closed(self, make_run_state):
        for status in (RunStatus.completed, RunStatus.failed, RunStatus.canceled):
            view = _view(make_run_state(status=status))
            assert existing_state_case(view) == JobState.closed

    def test_running_with_live_lease_is_busy(self, make_run_state):
        view = _view(
            make_run_state(status=RunStatus.running),
            holder="other-worker", deadline=_ts_in(300),
        )
        assert existing_state_case(view) == JobState.busy

    def test_released_pending_is_ready(self, make_run_state):
        view = _view(make_run_state(status=RunStatus.pending))
        assert existing_state_case(view) == JobState.ready

    def test_running_past_deadline_is_expired(self, make_run_state):
        view = _view(
            make_run_state(status=RunStatus.running),
            holder="dead-worker", deadline=_ts_in(-1),
        )
        assert existing_state_case(view) == JobState.expired

    def test_expired_with_exhausted_retries_is_failed(self, make_run_state):
        view = _view(
            make_run_state(status=RunStatus.running, attempt=3, max_retries=3),
            holder="dead-worker", deadline=_ts_in(-1),
        )
        assert existing_state_case(view) == JobState.failed


# ============================================================================
# execute_job
# ============================================================================


class TestExecuteJobSuccess:
    def test_new_job_completes(self, state_repo, signals, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})
        seen = []
        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: lambda **kw: seen.append(kw)})

        rc = execute_job(queue, registry, state_repo, signals, "w1")

        assert rc == 0
        assert seen == [{"x": 1}]
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.completed
        assert view.state.attempt == 1
        assert view.state.ended_at is not None
        assert view.state.kwargs == {"x": 1}  # persisted for sweeper re-enqueue
        assert view.holder is None  # terminal payloads are released

    def test_message_acked_immediately_after_claim(
        self, state_repo, signals, make_flow_job
    ):
        """The ack must not depend on the flow outcome."""
        job = make_flow_job()

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        execute_job(
            queue, FakeRegistry({job.flow_name: boom}), state_repo, signals, "w1"
        )
        assert queue.acked == [job.job_id]

    def test_lease_ttl_honours_job_timeout(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(timeout_seconds=1234)
        observed = []

        def flow(**kw):
            view = state_repo.read(job.flow_name, job.run_id)
            observed.append(view)

        queue = FakeQueue([job])
        execute_job(
            queue, FakeRegistry({job.flow_name: flow}), state_repo, signals, "w1"
        )

        view = observed[0]
        assert view.holder == "w1"
        remaining = (view.deadline_at.value - datetime.now(UTC)).total_seconds()
        assert 1200 < remaining <= 1234

    def test_empty_queue_returns_2(self, state_repo, signals):
        rc = execute_job(FakeQueue(), FakeRegistry({}), state_repo, signals, "w1")
        assert rc == 2


class TestExecuteJobRetries:
    def test_failure_with_retries_left_self_enqueues(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=3, kwargs={"a": 1})

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        rc = execute_job(
            queue, FakeRegistry({job.flow_name: boom}), state_repo, signals, "w1"
        )

        assert rc == 1
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.pending
        assert view.state.attempt == 1
        assert view.holder is None  # parked, reclaimable
        # A fresh wake-up message with backoff, carrying the same run_id and kwargs
        assert len(queue.enqueued) == 1
        retry_job, delay = queue.enqueued[0]
        assert retry_job.run_id == job.run_id
        assert retry_job.job_id != job.job_id
        assert retry_job.kwargs == {"a": 1}
        assert delay > 0

    def test_retry_increments_attempt(self, state_repo, signals, make_flow_job):
        job = make_flow_job(max_retries=3)

        def boom(**kw):
            raise ValueError("boom")

        registry = FakeRegistry({job.flow_name: boom})
        execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")
        execute_job(FakeQueue([job]), registry, state_repo, signals, "w1")

        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.attempt == 2
        assert view.state.status == RunStatus.pending
        assert view.epoch == 2  # one epoch bump per (re)acquisition

    def test_exhausted_retries_marks_failed_terminal(
        self, state_repo, signals, make_flow_job
    ):
        """The state machine — not the queue — decides permanent failure."""
        job = make_flow_job(max_retries=2)

        def boom(**kw):
            raise ValueError("boom")

        registry = FakeRegistry({job.flow_name: boom})
        assert execute_job(FakeQueue([job]), registry, state_repo, signals, "w1") == 1
        q2 = FakeQueue([job])
        assert execute_job(q2, registry, state_repo, signals, "w1") == 1

        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.failed
        assert view.state.attempt == 2
        assert view.state.ended_at is not None
        assert q2.enqueued == []  # no further retries


class TestExecuteJobSkips:
    def test_closed_state_acks_without_executing(
        self, state_repo, signals, make_flow_job, make_run_state, seed_lease, store
    ):
        job = make_flow_job()
        done = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.completed,
        )
        seed_lease(store, done)

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            signals,
            "w1",
        )

        assert rc == 0
        assert calls == []
        assert queue.acked == [job.job_id]
        # The closed document was never rewritten (epoch untouched).
        assert state_repo.read(job.flow_name, job.run_id).epoch == 1

    def test_busy_state_acks_duplicate_wakeup(
        self, state_repo, signals, make_flow_job, make_run_state, seed_lease, store
    ):
        job = make_flow_job()
        busy = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="other-worker",
        )
        seed_lease(
            store, busy, holder="other-worker",
            deadline=datetime.now(UTC) + timedelta(seconds=300),
        )

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            signals,
            "w1",
        )

        assert rc == 2
        assert calls == []
        assert queue.acked == [job.job_id]
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.holder == "other-worker"  # untouched


class TestExecuteJobTakeover:
    def test_expired_lease_takeover_increments_attempt(
        self, state_repo, signals, make_flow_job, make_run_state, seed_lease, store
    ):
        job = make_flow_job()
        expired = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="dead-worker",
            attempt=1,
            max_retries=3,
            kwargs={"orig": True},
        )
        seed_lease(
            store, expired, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        seen = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: seen.append(kw)}),
            state_repo,
            signals,
            "w2",
        )

        assert rc == 0
        # Takeover re-executes with the kwargs persisted in the state payload.
        assert seen == [{"orig": True}]
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.completed
        assert view.state.attempt == 2
        assert view.state.worker_id == "w2"
        assert view.epoch == 2  # the takeover was a fenced steal

    def test_exhausted_expired_state_marked_failed(
        self, state_repo, signals, make_flow_job, make_run_state, seed_lease, store
    ):
        job = make_flow_job()
        expired = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="dead-worker",
            attempt=3,
            max_retries=3,
        )
        seed_lease(
            store, expired, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            signals,
            "w2",
        )

        assert rc == 1
        assert calls == []
        view = state_repo.read(job.flow_name, job.run_id)
        assert view.state.status == RunStatus.failed
        assert view.state.worker_id == "dead-worker"  # the attempt's executor
        assert queue.acked == [job.job_id]


class TestStateRepositoryFiles:
    def test_claim_lists_exactly_the_state_object(
        self, state_repo, make_run_state
    ):
        state = make_run_state()
        state_repo.acquire(
            state.flow_name, state.run_id, ttl=60, holder="w1",
            state_fn=lambda _: state,
        )

        # The store's lock/temp machinery never leaks into listings.
        keys = state_repo.store.list_objects_sync("state/")
        assert keys == [f"state/{state.flow_name}/{state.run_id}.json"]

    def test_delete_removes_state_object(
        self, state_repo, make_run_state, seed_lease, store
    ):
        state = make_run_state()
        seed_lease(store, state)
        state_repo.delete(state.flow_name, state.run_id)

        assert state_repo.store.list_objects_sync("state/") == []

    def test_archive_moves_state_out_of_active_dir(
        self, state_repo, make_run_state, seed_lease, store, tmp_path
    ):
        state = make_run_state(status=RunStatus.completed, ended_at=Timestamp.now())
        seed_lease(store, state)
        state_repo.archive(state.flow_name, state.run_id)

        assert state_repo.read(state.flow_name, state.run_id) is None
        archived = list((tmp_path / "runs").rglob("state.json"))
        assert len(archived) == 1
        assert str(state.run_id) in str(archived[0].parent)
