"""Unit tests for the account model: Obligation, Attempt, Review.

Covers the ObligationRecord transitions (begin_attempt, record_outcome,
discharge, abandon), the ObligationSummary projection (summary), sub-obligation
links, crash accounting through the worker's claim transition, and the
account wire format round trip.
"""
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import (
    AttemptOutcome,
    Obligation,
    ObligationRecord,
    ObligationStatus,
    ReportedStatus,
    Review,
    Decision,
)
from flowlet.repository import SignalRepository, StateRepository
from flowlet.serdes import from_json, from_payload, to_json, to_payload
from flowlet.types import Timestamp
from flowlet.worker import execute_job

from unit.test_worker_layer import FakeExecutor, FakeQueue


def _record(**obligation_kwargs) -> ObligationRecord:
    defaults = dict(
        id=uuid7(),
        flow_name="test_flow",
        created_at=Timestamp.now(),
    )
    defaults.update(obligation_kwargs)
    return ObligationRecord(obligation=Obligation(**defaults))


# ============================================================================
# Record transitions
# ============================================================================


@pytest.mark.unit
class TestObligationRecord:
    def test_begin_attempt_appends_in_flight(self):
        record = _record()
        attempt = record.begin_attempt("w1")
        assert attempt.n == 1
        assert record.open_attempt is attempt
        assert attempt.outcome is None

    def test_record_outcome_closes_the_open_attempt(self):
        record = _record()
        record.begin_attempt("w1")
        record.record_outcome(
            AttemptOutcome.returned,
            review=Review(
                decision=Decision.approved, decided_at=Timestamp.now()
            ),
        )
        assert record.open_attempt is None
        last = record.last_attempt
        assert last.outcome == AttemptOutcome.returned
        assert last.ended_at is not None
        assert last.review.decision == Decision.approved

    def test_record_outcome_without_open_attempt_is_noop(self):
        record = _record()
        record.record_outcome(AttemptOutcome.crashed)
        assert record.attempts == []

    def test_retries_left_counts_attempts_against_budget(self):
        record = _record(max_retries=2)
        assert record.retries_left() is True
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.raised)
        assert record.retries_left() is True
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.raised)
        assert record.retries_left() is False

    def test_budget_counts_consuming_attempts_only(self):
        """v0.3 delta 1: crashed/interrupted attempts are free; raised
        outcomes and rejected reviews on returned attempts consume."""
        record = _record(max_retries=2)
        record.begin_attempt("w1")
        record.record_outcome(
            AttemptOutcome.crashed,
            review=Review(
                decision=Decision.rejected,
                decided_at=Timestamp.now(),
                reason="lease_expired",
            ),
        )
        record.begin_attempt("w2")
        record.record_outcome(AttemptOutcome.interrupted)
        assert record.consumed_attempts() == 0
        assert record.retries_left() is True

        record.begin_attempt("w3")
        record.record_outcome(AttemptOutcome.raised)
        assert record.consumed_attempts() == 1
        assert record.retries_left() is True

        record.begin_attempt("w4")
        record.record_outcome(
            AttemptOutcome.returned,
            review=Review(
                decision=Decision.rejected, decided_at=Timestamp.now()
            ),
        )
        assert record.consumed_attempts() == 2
        assert record.retries_left() is False

    def test_returned_attempt_pending_or_accepted_is_free(self):
        record = _record(max_retries=1)
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.returned)  # review pending
        assert record.consumed_attempts() == 0
        assert record.retries_left() is True
        record.last_attempt.review = Review(
            decision=Decision.approved, decided_at=Timestamp.now()
        )
        assert record.consumed_attempts() == 0

    def test_begin_attempt_records_resumption(self):
        """v0.3 delta 2: continuation is account data — attempt N records
        which attempt's substrate it continued."""
        record = _record()
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.interrupted)
        resumed = record.begin_attempt("w1", resumed_from=1)
        assert resumed.resumed_from == 1
        assert record.attempts[0].resumed_from is None

    def test_begin_attempt_rejects_unknown_resumption(self):
        record = _record()
        with pytest.raises(ValueError):
            record.begin_attempt("w1", resumed_from=1)
        assert record.attempts == []

    def test_backoff_still_paces_free_attempts(self):
        """Crashed attempts don't bill the budget but must still back off,
        or a crash loop spins at full speed."""
        record = _record(max_retries=3)
        before = record.backoff_seconds()
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.crashed)
        assert record.consumed_attempts() == 0
        assert record.backoff_seconds() > before

    def test_discharge_and_abandon_close_with_metadata(self):
        record = _record()
        record.discharge()
        assert record.obligation.status == ObligationStatus.discharged
        assert record.obligation.closed_at is not None

        other = _record()
        other.abandon("max_retries_exceeded")
        assert other.obligation.status == ObligationStatus.abandoned
        assert other.obligation.cause == "max_retries_exceeded"
        assert other.obligation.status.is_closed()


# ============================================================================
# Projection (summary)
# ============================================================================


@pytest.mark.unit
class TestSummaryProjection:
    def test_open_with_in_flight_attempt_projects_running(self):
        record = _record()
        record.begin_attempt("w7")
        state = record.summary()
        assert state.status == ReportedStatus.running
        assert state.worker_id == "w7"
        assert state.attempt == 1

    def test_open_parked_projects_pending(self):
        record = _record()
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.raised)
        assert record.summary().status == ReportedStatus.pending

    def test_discharged_projects_completed(self):
        record = _record()
        record.begin_attempt("w1")
        record.record_outcome(AttemptOutcome.returned)
        record.discharge()
        assert record.summary().status == ReportedStatus.completed

    def test_abandoned_cause_splits_failed_and_canceled(self):
        failed = _record()
        failed.abandon("max_retries_exceeded")
        assert failed.summary().status == ReportedStatus.failed

        canceled = _record()
        canceled.abandon("canceled")
        assert canceled.summary().status == ReportedStatus.canceled

    def test_summary_carries_identity_and_contract(self):
        record = _record(kwargs={"x": 1}, max_retries=5)
        state = record.summary()
        assert state.obligation_id == record.obligation.id
        assert state.kwargs == {"x": 1}
        assert state.max_retries == 5


# ============================================================================
# Wire format
# ============================================================================


@pytest.mark.unit
class TestAccountSerdes:
    def test_round_trip_preserves_the_account(self):
        record = _record(kwargs={"a": 1}, parent_id=uuid7(), root_id=uuid7())
        record.begin_attempt("w1")
        record.record_outcome(
            AttemptOutcome.raised,
            error="ValueError",
            review=Review(
                decision=Decision.rejected,
                decided_at=Timestamp.now(),
                reason="ValueError",
                evidence_ref={"report": "sha256:abc123"},
            ),
        )
        record.begin_attempt("w2", resumed_from=1)

        restored = from_json(ObligationRecord)(to_json(record))
        assert restored.obligation.id == record.obligation.id
        assert restored.obligation.parent_id == record.obligation.parent_id
        assert restored.obligation.root_id == record.obligation.root_id
        assert len(restored.attempts) == 2
        assert restored.attempts[0].outcome == AttemptOutcome.raised
        assert restored.attempts[0].review.decision == Decision.rejected
        assert restored.attempts[0].review.reason == "ValueError"
        assert restored.attempts[0].review.evidence_ref == {
            "report": "sha256:abc123"
        }
        assert restored.open_attempt is not None
        assert restored.open_attempt.executor == "w2"
        assert restored.open_attempt.resumed_from == 1

    def test_pre_delta2_wire_still_parses(self):
        """Payloads written before evidence_ref/resumed_from default to None."""
        record = _record()
        record.begin_attempt("w1")
        record.record_outcome(
            AttemptOutcome.returned,
            review=Review(
                decision=Decision.approved, decided_at=Timestamp.now()
            ),
        )
        tree = to_payload(record)
        del tree["attempts"][0]["resumed_from"]
        del tree["attempts"][0]["review"]["evidence_ref"]

        restored = from_payload(ObligationRecord)(tree)
        assert restored.last_attempt.resumed_from is None
        assert restored.last_attempt.review.evidence_ref is None

    def test_payload_round_trip_matches_json_wire(self):
        record = _record()
        record.begin_attempt("w1")
        assert from_payload(ObligationRecord)(to_payload(record)).obligation.id \
            == record.obligation.id


# ============================================================================
# Sub-obligations and crash accounting through the worker
# ============================================================================


@pytest.mark.unit
class TestAccountThroughWorker:
    @pytest.fixture
    def store(self, tmp_path):
        return FilesystemStorage(tmp_path)

    @pytest.fixture
    def state_repo(self, store):
        return StateRepository(store=store)

    @pytest.fixture
    def signals(self, store):
        return SignalRepository(store=store)

    def test_job_parent_refs_land_on_the_obligation(
        self, state_repo, signals, make_flow_job
    ):
        parent, root = uuid7(), uuid7()
        job = make_flow_job()
        job.parent_id = parent
        job.root_id = root

        queue = FakeQueue([job])
        executor = FakeExecutor({job.flow_name: lambda **kw: None})
        execute_job(queue, executor, state_repo, signals, "w1")

        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.parent_id == parent
        assert record.obligation.root_id == root

    def test_takeover_accounts_the_crashed_attempt_explicitly(
        self, state_repo, signals, make_flow_job, make_record, seed_lease, store
    ):
        job = make_flow_job()
        crashed = make_record(
            obligation_id=job.obligation_id,
            flow_name=job.flow_name,
            status=ReportedStatus.running,
            worker_id="dead-worker",
            attempt=1,
            max_retries=3,
        )
        seed_lease(
            store, crashed, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        queue = FakeQueue([job])
        executor = FakeExecutor({job.flow_name: lambda **kw: None})
        rc = execute_job(queue, executor, state_repo, signals, "w2")

        assert rc == 0
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert len(record.attempts) == 2
        first, second = record.attempts
        # The dead attempt was closed by whoever discovered the crash …
        assert first.executor == "dead-worker"
        assert first.outcome == AttemptOutcome.crashed
        assert first.review.decision == Decision.rejected
        assert first.review.reason == "lease_expired"
        # … and the takeover attempt carries its own outcome and review.
        assert second.executor == "w2"
        assert second.outcome == AttemptOutcome.returned
        assert second.review.decision == Decision.approved
        assert record.obligation.status == ObligationStatus.discharged

    def test_every_attempt_ends_with_an_explicit_outcome(
        self, state_repo, signals, make_flow_job
    ):
        """Two failures then success: the account shows the full history."""
        job = make_flow_job(max_retries=3)
        calls = {"n": 0}

        def flaky(**kw):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("flaky")

        executor = FakeExecutor({job.flow_name: flaky})
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        record = state_repo.read(job.flow_name, job.obligation_id).record
        outcomes = [a.outcome for a in record.attempts]
        decisions = [a.review.decision for a in record.attempts]
        assert outcomes == [
            AttemptOutcome.raised,
            AttemptOutcome.raised,
            AttemptOutcome.returned,
        ]
        assert decisions == [
            Decision.rejected,
            Decision.rejected,
            Decision.approved,
        ]
        assert record.attempts[0].error == "ValueError"
        assert record.obligation.status == ObligationStatus.discharged
