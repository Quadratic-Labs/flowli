"""Unit tests for the account-backed job source (queue-less mode).

Covers the JobQueueProtocol surface over the account store: enqueue as
obligation creation (put-if-absent, duplicate submissions converge),
dequeue as a claimability scan (readiness, backoff gating, crash takeover,
busy/closed/gated exclusion, chronological ordering, paused scopes), and
the end-to-end execute_job loop with no queue infrastructure at all.
"""
from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import AttemptOutcome, ObligationStatus, ReportedStatus
from flowlet.queue.account import AccountJobSource
from flowlet.repository import SignalRepository, StateRepository
from flowlet.worker import execute_job

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


@pytest.fixture
def source(state_repo):
    return AccountJobSource(state_repo=state_repo)


class FakeExecutor:
    """Kernel executor double: maps flow names to callables."""

    def __init__(self, flows):
        self.flows = flows

    def execute(self, obligation, attempt):
        self.flows[obligation.flow_name](**obligation.kwargs)


# ============================================================================
# enqueue — submitting is creating the obligation
# ============================================================================


class TestEnqueue:
    def test_creates_open_unheld_obligation(self, source, state_repo, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})

        assert source.enqueue(job) == job.job_id

        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view is not None
        assert view.holder is None
        assert view.record.obligation.status == ObligationStatus.open
        assert view.record.obligation.kwargs == {"x": 1}
        assert view.record.attempts == []

    def test_duplicate_submission_converges(self, source, state_repo, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})
        source.enqueue(job)

        duplicate = make_flow_job(
            flow_name=job.flow_name, kwargs={"x": 999}
        )
        duplicate.obligation_id = job.obligation_id
        source.enqueue(duplicate)

        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view.record.obligation.kwargs == {"x": 1}  # first claim won

    def test_review_policy_stamped_at_creation(
        self, state_repo, make_flow_job
    ):
        source = AccountJobSource(
            state_repo=state_repo, review_policy_for=lambda flow: "gated"
        )
        job = make_flow_job()
        source.enqueue(job)

        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view.record.obligation.review_policy == "gated"

    def test_delay_is_ignored(self, source, state_repo, make_flow_job):
        job = make_flow_job()
        source.enqueue(job, delay=300)

        assert source.dequeue() is not None  # claimable immediately


# ============================================================================
# dequeue — the claimability scan
# ============================================================================


class TestDequeue:
    def test_empty_store_returns_none(self, source):
        assert source.dequeue() is None
        assert source.get_queue_size() == 0

    def test_fresh_obligation_is_handed_out(self, source, make_flow_job):
        job = make_flow_job(kwargs={"x": 1})
        source.enqueue(job)

        wakeup = source.dequeue()
        assert wakeup is not None
        assert wakeup.obligation_id == job.obligation_id
        assert wakeup.flow_name == job.flow_name
        assert wakeup.kwargs == {"x": 1}
        assert wakeup.caused_by == "account_poll"
        assert source.get_queue_size() == 1

    def test_oldest_submission_first(self, source, make_flow_job):
        first = make_flow_job()
        second = make_flow_job()
        source.enqueue(second)
        source.enqueue(first)

        # uuid7 order = creation order, regardless of enqueue order
        expected = min(first.obligation_id, second.obligation_id)
        assert source.dequeue().obligation_id == expected

    def test_held_lease_is_skipped(self, source, store, make_record, seed_lease):
        seed_lease(
            store,
            make_record(status=ReportedStatus.running),
            holder="other-worker",
            deadline=datetime.now(UTC) + timedelta(seconds=300),
        )
        assert source.dequeue() is None

    def test_closed_and_gated_are_skipped(
        self, source, store, make_record, seed_lease
    ):
        seed_lease(store, make_record(status=ReportedStatus.completed))
        gated = make_record(status=ReportedStatus.running)
        gated.record_outcome(AttemptOutcome.returned)
        gated.suspend_for_review()
        seed_lease(store, gated)

        assert source.dequeue() is None

    def test_expired_holder_is_claimable_for_takeover(
        self, source, store, make_record, seed_lease
    ):
        seed_lease(
            store,
            make_record(status=ReportedStatus.running),
            holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=1),
        )
        assert source.dequeue() is not None

    def test_parked_retry_waits_for_backoff(
        self, source, store, make_record, seed_lease
    ):
        record = make_record(status=ReportedStatus.pending, attempt=1)

        seed_lease(store, record, deadline=datetime.now(UTC))
        assert source.dequeue() is None  # inside the 2s backoff window

        seed_lease(
            store, record,
            deadline=datetime.now(UTC) - timedelta(seconds=record.backoff_seconds() + 1),
        )
        assert source.dequeue() is not None  # backoff elapsed

    def test_paused_scope_is_not_handed_out(
        self, state_repo, signals, make_flow_job
    ):
        source = AccountJobSource(state_repo=state_repo, signals=signals)
        job = make_flow_job()
        source.enqueue(job)
        scope = f"flow:{job.flow_name}"
        signals.send_scoped(scope, "pause", actor="test")

        assert source.dequeue() is None

        signals.revoke_scoped(scope, "pause")
        assert source.dequeue() is not None

    def test_ack_is_a_noop(self, source, make_flow_job):
        job = make_flow_job()
        source.enqueue(job)
        source.ack(job.job_id)
        assert source.dequeue() is not None  # still on the books


# ============================================================================
# end-to-end — execute_job with no queue infrastructure
# ============================================================================


class TestExecuteJobOverAccount:
    def test_flow_runs_to_discharged(
        self, source, state_repo, signals, make_flow_job
    ):
        ran = []
        executor = FakeExecutor({"test_flow": lambda **kw: ran.append(kw)})
        job = make_flow_job(kwargs={"x": 1})
        source.enqueue(job)

        rc = execute_job(source, executor, state_repo, signals, "w1")

        assert rc == 0
        assert ran == [{"x": 1}]
        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view.record.obligation.status == ObligationStatus.discharged

    def test_failed_flow_parks_and_retries_after_backoff(
        self, source, state_repo, signals, store, make_flow_job, seed_lease
    ):
        def boom(**kw):
            raise RuntimeError("boom")

        executor = FakeExecutor({"test_flow": boom})
        job = make_flow_job(max_retries=2)
        source.enqueue(job)

        assert execute_job(source, executor, state_repo, signals, "w1") == 1
        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view.record.obligation.status == ObligationStatus.open
        assert len(view.record.attempts) == 1

        # Parked inside the backoff window: nothing to hand out.
        assert source.dequeue() is None

        # Age the release beyond the backoff window, then retry to exhaustion.
        seed_lease(
            store, view.record,
            deadline=datetime.now(UTC) - timedelta(seconds=600),
        )
        assert execute_job(source, executor, state_repo, signals, "w1") == 1
        view = state_repo.read(job.flow_name, job.obligation_id)
        assert view.record.obligation.status == ObligationStatus.abandoned
        assert view.record.obligation.cause == "max_retries_exceeded"
