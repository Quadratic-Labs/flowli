"""Unit tests for gated obligations: suspension, review, and effects.

Covers the gated flow path (returned attempt with pending review,
obligation awaiting_review), the review endpoint (guarded
reviews, accept/reject routing), the sweeper's passivity toward gated
runs, exactly-once effects across retries, and caused_by provenance.
"""

from datetime import UTC, datetime, timedelta

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

import flowlet as flowlet_pkg
from flowlet.api.controller import FlowController
from flowlet.api.models import ReviewRequest, FlowArguments
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


GATED = "gated"


def _run_gated(state_repo, signals, job, fn=lambda **kw: None):
    """Execute a gated job once and return the worker's exit code."""
    executor = FakeExecutor({job.flow_name: fn})
    return execute_job(
        FakeQueue([job]), executor, state_repo, signals, "w1",
        review_policy_for=lambda _f: GATED,
    )


def _exhausted_gated_record(job) -> ObligationRecord:
    """A gated obligation whose budget was spent by a raised attempt.

    Only consuming attempts (raised outcomes, rejected reviews) bill the
    budget, so exhaustion scenarios must seed a real failure — a crashed
    in-flight attempt no longer spends anything.
    """
    record = ObligationRecord(
        obligation=Obligation(
            id=job.obligation_id, flow_name=job.flow_name, max_retries=1,
            review_policy=GATED, created_at=Timestamp.now(),
        )
    )
    record.begin_attempt("w0")
    record.record_outcome(
        AttemptOutcome.raised,
        error="ValueError",
        review=Review(
            decision=Decision.rejected, by="auto",
            decided_at=Timestamp.now(), reason="ValueError",
        ),
    )
    return record


class _Controller:
    """Build a controller wired to the same stores as the worker."""

    def __new__(cls, state_repo, signals, gate=None, queue=None):
        return FlowController(
            state_repo=state_repo,
            signals=signals,
            queue=queue,
            gate_policy=(lambda _f: gate) if gate is not None else None,
        )


# ============================================================================
# Suspension
# ============================================================================


@pytest.mark.unit
class TestGatedSuspension:
    def test_returned_attempt_suspends_without_review(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        rc = _run_gated(state_repo, signals, job)

        assert rc == 0
        view = state_repo.read(job.flow_name, job.obligation_id)
        record = view.record
        assert record.obligation.status == ObligationStatus.awaiting_review
        assert record.last_attempt.outcome == AttemptOutcome.returned
        assert record.last_attempt.review is None  # judgment pending
        assert record.pending_review_attempt is not None
        assert view.holder is None  # parked, passive
        assert view.state.status == ReportedStatus.gated

    def test_wakeup_for_gated_obligation_is_dropped(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        epoch_before = state_repo.read(job.flow_name, job.obligation_id).epoch

        executed = []
        executor = FakeExecutor({job.flow_name: lambda **kw: executed.append(1)})
        rc = execute_job(
            FakeQueue([job]), executor, state_repo, signals, "w2",
            review_policy_for=lambda _f: GATED,
        )

        assert rc == 2
        assert executed == []
        assert state_repo.read(job.flow_name, job.obligation_id).epoch == epoch_before

    def test_sweeper_leaves_gated_obligations_alone(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals, pending_grace=0)

        assert stats.requeued == 0
        assert queue.enqueued == []


# ============================================================================
# Review endpoint
# ============================================================================


@pytest.mark.unit
class TestReview:
    def test_accepted_discharges_and_records_the_actor(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        controller = _Controller(state_repo, signals)

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(
                decision="approved", actor="reviewer-7", reason="looks good"
            ),
        )

        assert resp.status == "completed"
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.discharged
        review = record.last_attempt.review
        assert review.decision == Decision.approved
        assert review.by == "reviewer-7"
        assert review.reason == "looks good"

    def test_rejected_with_budget_reopens_and_wakes_a_worker(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=3)
        _run_gated(state_repo, signals, job)
        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="rejected", actor="reviewer-7"),
        )

        assert resp.status == "pending"
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.open
        assert record.last_attempt.review.decision == Decision.rejected
        # A wake-up carrying provenance was enqueued …
        wake, _ = queue.enqueued[0]
        assert wake.obligation_id == job.obligation_id
        assert wake.caused_by == "review:rejected_by:reviewer-7"
        # … and a worker executes attempt 2, suspending again for judgment
        # (the obligation was created gated; the policy is fixed at creation).
        queue.jobs = [wake]
        rc = execute_job(
            queue, FakeExecutor({job.flow_name: lambda **kw: None}),
            state_repo, signals, "w2",
        )
        assert rc == 0
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert len(record.attempts) == 2
        assert record.obligation.status == ObligationStatus.awaiting_review

    def test_review_evidence_ref_lands_in_the_account(
        self, state_repo, signals, make_flow_job
    ):
        """v0.3 delta 2: the review records the grounds of the decision as
        a ref — the rejecting evidence is what seeds the next attempt."""
        job = make_flow_job(max_retries=3)
        _run_gated(state_repo, signals, job)
        controller = _Controller(state_repo, signals)

        controller.review_obligation(
            job.obligation_id,
            ReviewRequest(
                decision="rejected", actor="reviewer-7",
                reason="needs_rework",
                evidence_ref={"report": "sha256:abc123"},
            ),
        )

        record = state_repo.read(job.flow_name, job.obligation_id).record
        review = record.last_attempt.review
        assert review.decision == Decision.rejected
        assert review.evidence_ref == {"report": "sha256:abc123"}

    def test_rejected_without_budget_abandons(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=1)
        _run_gated(state_repo, signals, job)
        controller = _Controller(state_repo, signals)

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="rejected", actor="reviewer-7"),
        )

        assert resp.status == "failed"
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.obligation.cause == "rejected"

    def test_gate_policy_refuses_ineligible_actor(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        gate = lambda actor, record: actor.startswith("admin")  # noqa: E731
        _run_gated(state_repo, signals, job)
        controller = _Controller(state_repo, signals, gate=gate)

        with pytest.raises(HTTPException) as exc:
            controller.review_obligation(
                job.obligation_id,
                ReviewRequest(decision="approved", actor="intern-1"),
            )
        assert exc.value.status_code == 403
        # The account is untouched — the refusal never reached the record.
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.awaiting_review

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="approved", actor="admin-2"),
        )
        assert resp.status == "completed"

    def test_reviewing_a_non_gated_obligation_is_409(
        self, state_repo, signals, make_flow_job, make_record, seed_lease, store
    ):
        record = make_record(status=ReportedStatus.pending)
        seed_lease(store, record)
        controller = _Controller(state_repo, signals)

        with pytest.raises(HTTPException) as exc:
            controller.review_obligation(
                record.obligation.id,
                ReviewRequest(decision="approved", actor="reviewer"),
            )
        assert exc.value.status_code == 409

    def test_gated_obligation_can_still_be_canceled(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        controller = _Controller(state_repo, signals)

        resp = controller.cancel_obligation(job.obligation_id)

        assert resp.status == ReportedStatus.canceled
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.obligation.cause == "canceled"


# ============================================================================
# Exhaustion at the gate — a spent budget parks for judgment, never closes
# ============================================================================


@pytest.mark.unit
class TestGatedExhaustion:
    def test_raised_exhaustion_parks_awaiting_review(
        self, state_repo, signals, make_flow_job
    ):
        """The last failed attempt suspends the gated obligation with its
        review pending — exhaustion is a judgment point, not an auto-review."""
        job = make_flow_job(max_retries=1)

        def boom(**kw):
            raise ValueError("boom")

        rc = _run_gated(state_repo, signals, job, fn=boom)

        assert rc == 1
        view = state_repo.read(job.flow_name, job.obligation_id)
        record = view.record
        assert record.obligation.status == ObligationStatus.awaiting_review
        assert record.last_attempt.outcome == AttemptOutcome.raised
        assert record.last_attempt.error == "ValueError"
        assert record.last_attempt.review is None  # judgment pending
        assert record.pending_review_attempt is not None
        assert view.holder is None  # parked, passive
        assert view.state.status == ReportedStatus.gated

    def test_raised_with_budget_left_still_retries(
        self, state_repo, signals, make_flow_job
    ):
        """The gate only enters at exhaustion — mid-budget failures keep the
        auto-rejected review and the retry loop."""
        job = make_flow_job(max_retries=2)

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        rc = execute_job(
            queue, FakeExecutor({job.flow_name: boom}), state_repo, signals, "w1",
            review_policy_for=lambda _f: GATED,
        )

        assert rc == 1
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.open
        assert record.last_attempt.review.decision == Decision.rejected
        assert record.last_attempt.review.by == "auto"
        assert len(queue.enqueued) == 1  # retry wake-up

    def test_crashed_holder_on_exhausted_budget_parks_at_claim(
        self, state_repo, signals, make_flow_job, seed_lease, store
    ):
        """A dead holder on an already-exhausted gated obligation parks it;
        the crashed attempt is what the gate reviews.  The budget was
        spent by the earlier raised attempt — the crash itself is free."""
        job = make_flow_job(max_retries=1)
        record = _exhausted_gated_record(job)
        record.begin_attempt("dead-worker")
        seed_lease(
            store, record, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        calls = []
        rc = execute_job(
            FakeQueue([job]),
            FakeExecutor({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo, signals, "w2",
        )

        assert rc == 2
        assert calls == []  # nothing executed — parked, not re-attempted
        recovered = state_repo.read(job.flow_name, job.obligation_id).record
        assert recovered.obligation.status == ObligationStatus.awaiting_review
        assert recovered.last_attempt.outcome == AttemptOutcome.crashed
        assert recovered.last_attempt.review is None  # judgment pending

    def test_crashed_holder_with_budget_untouched_re_attempts(
        self, state_repo, signals, make_flow_job, seed_lease, store
    ):
        """Crashed attempts are free (v0.3 delta 1): a dead holder never
        spends the budget, so the claim accounts the crash and re-runs."""
        job = make_flow_job(max_retries=1)
        obligation = Obligation(
            id=job.obligation_id, flow_name=job.flow_name, max_retries=1,
            review_policy=GATED, created_at=Timestamp.now(),
        )
        record = ObligationRecord(obligation=obligation)
        record.begin_attempt("dead-worker")
        seed_lease(
            store, record, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        calls = []
        rc = execute_job(
            FakeQueue([job]),
            FakeExecutor({job.flow_name: lambda **kw: calls.append(1)}),
            state_repo, signals, "w2",
        )

        assert rc == 0
        assert calls == [1]  # the work re-ran
        recovered = state_repo.read(job.flow_name, job.obligation_id).record
        crashed = recovered.attempts[0]
        assert crashed.outcome == AttemptOutcome.crashed
        assert crashed.review.decision == Decision.rejected
        assert crashed.review.reason == "lease_expired"
        assert crashed.consumes_budget() is False
        # The fresh attempt returned; the gated obligation parks as usual.
        assert recovered.last_attempt.outcome == AttemptOutcome.returned
        assert recovered.obligation.status == ObligationStatus.awaiting_review

    def test_sweeper_parks_exhausted_gated_crash(
        self, state_repo, signals, make_flow_job, seed_lease, store
    ):
        job = make_flow_job(max_retries=1)
        record = _exhausted_gated_record(job)
        record.begin_attempt("dead-worker")
        seed_lease(
            store, record, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )

        queue = FakeQueue([])
        stats = sweep(state_repo, queue, signals=signals)

        assert stats.failed == 0
        assert stats.requeued == 0
        assert queue.enqueued == []  # a wake-up could not execute anything
        recovered = state_repo.read(job.flow_name, job.obligation_id).record
        assert recovered.obligation.status == ObligationStatus.awaiting_review
        assert recovered.last_attempt.outcome == AttemptOutcome.crashed
        assert recovered.last_attempt.review is None

    def test_extend_budget_resumes_and_completes(
        self, state_repo, signals, make_flow_job
    ):
        """The scenario-5 story: the environment broke, the budget spent,
        a human fixes the environment and resumes with extra attempts."""
        job = make_flow_job(max_retries=1)
        calls = {"n": 0}

        def flaky(**kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("env_broken")

        executor = FakeExecutor({job.flow_name: flaky})
        rc = execute_job(
            FakeQueue([job]), executor, state_repo, signals, "w1",
            review_policy_for=lambda _f: GATED,
        )
        assert rc == 1
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.awaiting_review

        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)
        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(
                decision="rejected", actor="human:tz",
                reason="ssh key fixed", extend_budget=1,
            ),
        )

        assert resp.status == "pending"
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.open
        assert record.obligation.max_retries == 2  # budget extended
        assert record.last_attempt.review.decision == Decision.rejected
        assert record.last_attempt.review.by == "human:tz"

        # The wake-up re-executes; the fixed environment succeeds and the
        # obligation gates again on the returned outcome.
        wake, _ = queue.enqueued[0]
        queue.jobs = [wake]
        rc = execute_job(queue, executor, state_repo, signals, "w2")
        assert rc == 0
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert len(record.attempts) == 2
        assert record.obligation.status == ObligationStatus.awaiting_review

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="approved", actor="human:tz"),
        )
        assert resp.status == "completed"

    def test_rejected_without_extension_closes(
        self, state_repo, signals, make_flow_job
    ):
        """A plain rejected review on an exhausted obligation is the
        explicit human close."""
        job = make_flow_job(max_retries=1)

        def boom(**kw):
            raise ValueError("boom")

        _run_gated(state_repo, signals, job, fn=boom)
        controller = _Controller(state_repo, signals)

        resp = controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="rejected", actor="human:tz"),
        )

        assert resp.status == "failed"
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.obligation.cause == "rejected"


# ============================================================================
# Effects — exactly-once by occurrence
# ============================================================================


@pytest.mark.unit
class TestEffects:
    def test_effect_runs_once_and_lands_in_the_account(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        fired = []

        def flow(**kw):
            result = flowlet_pkg.effect("send_email", lambda: fired.append(1) or "sent")
            assert result == "sent"

        executor = FakeExecutor({job.flow_name: flow})
        rc = execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        assert rc == 0
        assert fired == [1]
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert len(record.effects) == 1
        effect = record.effects[0]
        assert effect.name == "send_email"
        assert effect.result_ref == "sent"
        assert effect.attempt_n == 1

    def test_retry_converges_on_the_recorded_effect(
        self, state_repo, signals, make_flow_job
    ):
        """The occurrence key excludes the attempt: a retry replays the
        effect's result instead of re-firing the side effect."""
        job = make_flow_job(max_retries=3)
        fired = []
        calls = {"n": 0}

        def flow(**kw):
            flowlet_pkg.effect("charge_card", lambda: fired.append(1) or "charged")
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("flaky after the effect")

        executor = FakeExecutor({job.flow_name: flow})
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")
        rc = execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        assert rc == 0
        assert fired == [1]  # the side effect fired exactly once
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert len(record.attempts) == 2
        assert len(record.effects) == 1  # recorded once, on the producing attempt
        assert record.effects[0].attempt_n == 1

    def test_new_occurrence_deliberately_repeats(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        fired = []

        def flow(**kw):
            flowlet_pkg.effect("notify", lambda: fired.append(1), occurrence="1")
            flowlet_pkg.effect("notify", lambda: fired.append(1), occurrence="manual-2")

        executor = FakeExecutor({job.flow_name: flow})
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        assert fired == [1, 1]
        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert [e.occurrence for e in record.effects] == ["1", "manual-2"]

    def test_effect_outside_obligation_context_is_a_plain_call(self):
        assert flowlet_pkg.effect("adhoc", lambda: 42) == 42


# ============================================================================
# Provenance
# ============================================================================


@pytest.mark.unit
class TestProvenance:
    def test_job_caused_by_lands_on_the_obligation(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        job.caused_by = "dispatch:webhook-42"
        executor = FakeExecutor({job.flow_name: lambda **kw: None})
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        record = state_repo.read(job.flow_name, job.obligation_id).record
        assert record.obligation.caused_by == "dispatch:webhook-42"

    def test_retry_wakeup_carries_attempt_provenance(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job(max_retries=3)

        def boom(**kw):
            raise ValueError("boom")

        queue = FakeQueue([job])
        execute_job(queue, FakeExecutor({job.flow_name: boom}), state_repo, signals, "w1")

        retry_job, _ = queue.enqueued[0]
        assert retry_job.caused_by == "retry_of_attempt:1"


# ============================================================================
# Review as work — the linkage (abstractions v0.3, delta 5)
# ============================================================================


@pytest.mark.unit
class TestReviewAsWork:
    def test_assign_reviewer_requires_parked(self):
        from uuid import uuid7

        record = ObligationRecord(
            obligation=Obligation(
                id=uuid7(), flow_name="f", created_at=Timestamp.now()
            )
        )
        with pytest.raises(ValueError):
            record.assign_reviewer(uuid7())

        record.suspend_for_review()
        reviewer = uuid7()
        record.assign_reviewer(reviewer)
        assert record.obligation.reviewer_id == reviewer

    def test_review_settles_the_debt(self, state_repo, signals, make_flow_job):
        """reviewer_id is 'who currently owes me the review' — cleared
        when any review lands."""
        from uuid import uuid7

        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)

        review = controller.submit_flow(
            "review_flow", FlowArguments(reviews=job.obligation_id)
        )
        parked = state_repo.read(job.flow_name, job.obligation_id).record
        assert parked.obligation.reviewer_id == review.obligation_id

        controller.review_obligation(
            job.obligation_id,
            ReviewRequest(decision="approved", actor="reviewer-flow"),
        )
        settled = state_repo.read(job.flow_name, job.obligation_id).record
        assert settled.obligation.status == ObligationStatus.discharged
        assert settled.obligation.reviewer_id is None

    def test_reviewer_submission_stamps_provenance_and_links(
        self, state_repo, signals, make_flow_job
    ):
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)

        review = controller.submit_flow(
            "review_flow",
            FlowArguments(kwargs={"report": "sha256:abc"}, reviews=job.obligation_id),
        )

        # The wake-up carries the convention; the claim will land it on the
        # reviewer obligation's account.
        wake, _ = queue.enqueued[0]
        assert wake.obligation_id == review.obligation_id
        assert wake.caused_by == f"review:{job.obligation_id}"
        # The parked parent records who owes it the review.
        parent = state_repo.read(job.flow_name, job.obligation_id).record
        assert parent.obligation.reviewer_id == review.obligation_id

    def test_reviews_requires_a_parked_target(
        self, state_repo, signals, make_flow_job
    ):
        from uuid import uuid7

        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)

        with pytest.raises(HTTPException) as exc:
            controller.submit_flow(
                "review_flow", FlowArguments(reviews=uuid7())
            )
        assert exc.value.status_code == 404

        # An open (not parked) target is refused.
        job = make_flow_job()
        executor = FakeExecutor({job.flow_name: lambda **kw: None})

        def boom(**kw):
            raise ValueError("boom")

        failing = make_flow_job(max_retries=3)
        execute_job(
            FakeQueue([failing]),
            FakeExecutor({failing.flow_name: boom}),
            state_repo, signals, "w1",
        )  # open, parked for retry — not awaiting review
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow(
                "review_flow", FlowArguments(reviews=failing.obligation_id)
            )
        assert exc.value.status_code == 409
        assert queue.enqueued == []  # neither submission was enqueued

    def test_full_loop_review_as_effect_of_review_obligation(
        self, state_repo, signals, make_flow_job
    ):
        """The delta-5 story end to end: the parent parks, a review
        obligation is minted against it, a worker runs the review flow whose
        terminal act is the review, and both accounts close."""
        job = make_flow_job()
        _run_gated(state_repo, signals, job)
        queue = FakeQueue([])
        controller = _Controller(state_repo, signals, queue=queue)

        review = controller.submit_flow(
            "review_flow", FlowArguments(reviews=job.obligation_id)
        )

        def review_flow(**kw):
            controller.review_obligation(
                job.obligation_id,
                ReviewRequest(
                    decision="approved", actor="reviewer-flow",
                    reason="claims verified",
                ),
            )

        wake, _ = queue.enqueued[0]
        queue.jobs = [wake]
        rc = execute_job(
            queue, FakeExecutor({"review_flow": review_flow}),
            state_repo, signals, "w-review",
        )

        assert rc == 0
        parent = state_repo.read(job.flow_name, job.obligation_id).record
        assert parent.obligation.status == ObligationStatus.discharged
        assert parent.obligation.reviewer_id is None
        assert parent.last_attempt.review.by == "reviewer-flow"
        reviewer = state_repo.read("review_flow", review.obligation_id).record
        assert reviewer.obligation.status == ObligationStatus.discharged
        assert reviewer.obligation.caused_by == f"review:{job.obligation_id}"
