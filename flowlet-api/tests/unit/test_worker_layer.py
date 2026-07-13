"""Unit tests for the lease-based worker state machine and execute_job.

Covers JobState resolution (existing_state_case) and the end-to-end
execute_job paths: claim + immediate ack, success, retry accounting via
self-enqueue, terminal failure, busy/closed message dropping, and expired
lease takeover.
"""
from datetime import UTC, datetime, timedelta

import pytest

from flowlet.models import RunStatus
from flowlet.repository import StateRepository
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
def state_repo(tmp_path):
    return StateRepository(root=tmp_path)


def _ts_in(seconds: float) -> Timestamp:
    return Timestamp(datetime.now(UTC) + timedelta(seconds=seconds))


# ============================================================================
# existing_state_case — lease-only liveness
# ============================================================================


class TestExistingStateCase:
    def test_no_state_is_new(self):
        assert existing_state_case(None) == JobState.new

    def test_closed_statuses_are_closed(self, make_run_state):
        for status in (RunStatus.completed, RunStatus.failed, RunStatus.canceled):
            state = make_run_state(status=status)
            assert existing_state_case(state) == JobState.closed

    def test_running_with_live_lease_is_busy(self, make_run_state):
        state = make_run_state(status=RunStatus.running, deadline_at=_ts_in(300))
        assert existing_state_case(state) == JobState.busy

    def test_pending_is_ready(self, make_run_state):
        state = make_run_state(status=RunStatus.pending)
        assert existing_state_case(state) == JobState.ready

    def test_running_past_deadline_is_expired(self, make_run_state):
        state = make_run_state(status=RunStatus.running, deadline_at=_ts_in(-1))
        assert existing_state_case(state) == JobState.expired

    def test_expired_with_exhausted_retries_is_failed(self, make_run_state):
        state = make_run_state(
            status=RunStatus.running,
            deadline_at=_ts_in(-1),
            attempt=3,
            max_retries=3,
        )
        assert existing_state_case(state) == JobState.failed


# ============================================================================
# execute_job
# ============================================================================


class TestExecuteJobSuccess:
    def test_new_job_completes(self, state_repo, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})
        seen = []
        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: lambda **kw: seen.append(kw)})

        rc = execute_job(queue, registry, state_repo, "w1")

        assert rc == 0
        assert seen == [{"x": 1}]
        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.status == RunStatus.completed
        assert state.attempt == 1
        assert state.ended_at is not None
        assert state.kwargs == {"x": 1}  # persisted for sweeper re-enqueue

    def test_message_acked_immediately_after_claim(
        self, state_repo, make_flow_job
    ):
        """The ack must not depend on the flow outcome."""
        job = make_flow_job()

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        execute_job(queue, FakeRegistry({job.flow_name: boom}), state_repo, "w1")
        assert queue.acked == [job.job_id]

    def test_lease_deadline_is_set_on_claim(self, state_repo, make_flow_job):
        job = make_flow_job(timeout_seconds=1234)
        queue = FakeQueue([job])
        registry = FakeRegistry({job.flow_name: lambda **kw: None})

        execute_job(queue, registry, state_repo, "w1")

        state, _ = state_repo.read(job.flow_name, job.run_id)
        remaining = (state.deadline_at.value - datetime.now(UTC)).total_seconds()
        # Terminal state keeps the claim-time deadline: now + 1234s (minus test time)
        assert 1200 < remaining <= 1234

    def test_empty_queue_returns_2(self, state_repo):
        rc = execute_job(FakeQueue(), FakeRegistry({}), state_repo, "w1")
        assert rc == 2


class TestExecuteJobRetries:
    def test_failure_with_retries_left_self_enqueues(
        self, state_repo, make_flow_job
    ):
        job = make_flow_job(max_retries=3, kwargs={"a": 1})

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        rc = execute_job(queue, FakeRegistry({job.flow_name: boom}), state_repo, "w1")

        assert rc == 1
        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.status == RunStatus.pending
        assert state.attempt == 1
        # A fresh wake-up message with backoff, carrying the same run_id and kwargs
        assert len(queue.enqueued) == 1
        retry_job, delay = queue.enqueued[0]
        assert retry_job.run_id == job.run_id
        assert retry_job.job_id != job.job_id
        assert retry_job.kwargs == {"a": 1}
        assert delay > 0

    def test_retry_increments_attempt(self, state_repo, make_flow_job):
        job = make_flow_job(max_retries=3)

        def boom(**kw):
            raise ValueError("boom")

        registry = FakeRegistry({job.flow_name: boom})
        execute_job(FakeQueue([job]), registry, state_repo, "w1")
        execute_job(FakeQueue([job]), registry, state_repo, "w1")

        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.attempt == 2
        assert state.status == RunStatus.pending

    def test_exhausted_retries_marks_failed_terminal(
        self, state_repo, make_flow_job
    ):
        """The state machine — not the queue — decides permanent failure."""
        job = make_flow_job(max_retries=2)

        def boom(**kw):
            raise ValueError("boom")

        registry = FakeRegistry({job.flow_name: boom})
        assert execute_job(FakeQueue([job]), registry, state_repo, "w1") == 1
        q2 = FakeQueue([job])
        assert execute_job(q2, registry, state_repo, "w1") == 1

        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.status == RunStatus.failed
        assert state.attempt == 2
        assert state.ended_at is not None
        assert q2.enqueued == []  # no further retries


class TestExecuteJobSkips:
    def test_closed_state_acks_without_executing(
        self, state_repo, make_flow_job, make_run_state
    ):
        job = make_flow_job()
        done = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.completed,
        )
        state_repo.write(done, None)

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            "w1",
        )

        assert rc == 0
        assert calls == []
        assert queue.acked == [job.job_id]

    def test_busy_state_acks_duplicate_wakeup(
        self, state_repo, make_flow_job, make_run_state
    ):
        job = make_flow_job()
        busy = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="other-worker",
            deadline_at=_ts_in(300),
        )
        state_repo.write(busy, None)

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            "w1",
        )

        assert rc == 2
        assert calls == []
        assert queue.acked == [job.job_id]
        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.worker_id == "other-worker"  # untouched


class TestExecuteJobTakeover:
    def test_expired_lease_takeover_increments_attempt(
        self, state_repo, make_flow_job, make_run_state
    ):
        job = make_flow_job()
        expired = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="dead-worker",
            deadline_at=_ts_in(-10),
            attempt=1,
            max_retries=3,
            kwargs={"orig": True},
        )
        state_repo.write(expired, None)

        seen = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: seen.append(kw)}),
            state_repo,
            "w2",
        )

        assert rc == 0
        # Takeover re-executes with the kwargs persisted in the state file.
        assert seen == [{"orig": True}]
        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.status == RunStatus.completed
        assert state.attempt == 2
        assert state.worker_id == "w2"

    def test_exhausted_expired_state_marked_failed(
        self, state_repo, make_flow_job, make_run_state
    ):
        job = make_flow_job()
        expired = make_run_state(
            run_id=job.run_id,
            flow_name=job.flow_name,
            status=RunStatus.running,
            worker_id="dead-worker",
            deadline_at=_ts_in(-10),
            attempt=3,
            max_retries=3,
        )
        state_repo.write(expired, None)

        calls = []
        queue = FakeQueue([job])
        rc = execute_job(
            queue,
            FakeRegistry({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo,
            "w2",
        )

        assert rc == 1
        assert calls == []
        state, _ = state_repo.read(job.flow_name, job.run_id)
        assert state.status == RunStatus.failed
        assert queue.acked == [job.job_id]


class TestStateRepositoryFiles:
    def test_write_leaves_no_tmp_file_and_keeps_lock(self, state_repo, make_run_state, tmp_path):
        state = make_run_state()
        state_repo.write(state, None)

        state_dir = tmp_path / "state" / state.flow_name
        names = {p.name for p in state_dir.iterdir()}
        assert f"{state.run_id}.json" in names
        assert f"{state.run_id}.lock" in names
        assert not any(n.endswith(".tmp") for n in names)

    def test_delete_removes_state_and_lock(self, state_repo, make_run_state, tmp_path):
        state = make_run_state()
        state_repo.write(state, None)
        state_repo.delete(state.flow_name, state.run_id)

        state_dir = tmp_path / "state" / state.flow_name
        assert list(state_dir.iterdir()) == []

    def test_archive_moves_state_out_of_active_dir(self, state_repo, make_run_state, tmp_path):
        state = make_run_state(status=RunStatus.completed, ended_at=Timestamp.now())
        state_repo.write(state, None)
        state_repo.archive(state.flow_name, state.run_id)

        assert state_repo.read(state.flow_name, state.run_id) is None
        archived = list((tmp_path / "runs").rglob("state.json"))
        assert len(archived) == 1
        assert str(state.run_id) in str(archived[0].parent)
