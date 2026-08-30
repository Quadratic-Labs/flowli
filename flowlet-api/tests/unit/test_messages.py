"""Unit tests for the ordered message channel and the recv checkpoint.

Covers the MessageRepository (ordering, dedup convergence, counts, clear),
the in-process ``flowlet.recv`` discipline (consume in order, fenced
checkpoint, deterministic replay across attempts), the controller's
send/recv endpoints over the executor claim lifecycle, flow_version
provenance stamping, and sweeper archive cleanup of a run's messages.
"""
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

import flowlet as flowlet_pkg
from flowlet.api.controller import FlowController
from flowlet.api.models import (
    ExecutorClaimRequest,
    ExecutorRecvRequest,
    ExecutorRenewRequest,
    FlowArguments,
    RunMessageRequest,
)
from flowlet.lease import RunLease, bind_lease, recv, unbind_lease
from flowlet.models import FlowJob, ReportedStatus
from flowlet.repository import (
    MessageRepository,
    SignalRepository,
    StateRepository,
)
from flowlet.sweeper import sweep
from flowlet.types import Timestamp
from flowlet.worker import _claim_transition, execute_job

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


@pytest.fixture
def messages(store):
    return MessageRepository(store=store)


def _controller(state_repo, signals, queue=None):
    return FlowController(state_repo=state_repo, signals=signals, queue=queue)


def _claimed_run_lease(state_repo, signals, messages, make_record, holder="w1"):
    """Acquire a running obligation's lease with the channel wired in."""
    record = make_record(status=ReportedStatus.running)
    obligation = record.obligation
    lease = state_repo.acquire(
        obligation.flow_name, obligation.id, ttl=600, holder=holder,
        state_fn=lambda _: record,
    )
    assert lease is not None
    return RunLease(
        lease=lease, signals=signals, messages=messages, min_interval=0.0
    )


# ============================================================================
# MessageRepository
# ============================================================================


@pytest.mark.unit
class TestMessageRepository:
    def test_messages_list_in_send_order(self, messages):
        run_id = uuid7()
        ids = [
            messages.send("f", run_id, "steer", {"n": n}, actor="a")[0]
            for n in range(3)
        ]

        listed = messages.list_topic("f", run_id, "steer")

        assert [doc.id for doc in listed] == ids
        assert [doc.body["n"] for doc in listed] == [0, 1, 2]

    def test_list_after_cursor_excludes_consumed(self, messages):
        run_id = uuid7()
        first, _ = messages.send("f", run_id, "steer", "one", actor="a")
        messages.send("f", run_id, "steer", "two", actor="a")

        listed = messages.list_topic("f", run_id, "steer", after=first)

        assert [doc.body for doc in listed] == ["two"]

    def test_dedup_key_converges_on_one_message(self, messages):
        run_id = uuid7()
        first_id, created = messages.send(
            "f", run_id, "hook", "payload-1", actor="a", dedup_key="delivery-42"
        )
        second_id, again = messages.send(
            "f", run_id, "hook", "payload-2", actor="a", dedup_key="delivery-42"
        )

        assert created is True
        assert again is False
        assert second_id == first_id
        listed = messages.list_topic("f", run_id, "hook")
        assert len(listed) == 1
        assert listed[0].body == "payload-1"  # first write wins

    def test_counts_per_topic_exclude_dedup_claims(self, messages):
        run_id = uuid7()
        messages.send("f", run_id, "steer", 1, actor="a")
        messages.send("f", run_id, "steer", 2, actor="a")
        messages.send("f", run_id, "hook", 3, actor="a", dedup_key="k")

        assert messages.counts("f", run_id) == {"steer": 2, "hook": 1}

    def test_clear_removes_all_topics(self, messages):
        run_id = uuid7()
        messages.send("f", run_id, "steer", 1, actor="a")
        messages.send("f", run_id, "hook", 2, actor="a", dedup_key="k")

        messages.clear("f", run_id)

        assert messages.counts("f", run_id) == {}
        assert messages.list_topic("f", run_id, "steer") == []

    def test_unsafe_topic_is_refused(self, messages):
        with pytest.raises(ValueError):
            messages.send("f", uuid7(), "_dedup", 1, actor="a")
        with pytest.raises(ValueError):
            messages.send("f", uuid7(), "a/b", 1, actor="a")


# ============================================================================
# flowlet.recv — in-process consumption and replay
# ============================================================================


@pytest.mark.unit
class TestRecv:
    def test_noop_outside_worker_context(self):
        assert recv("steer") is None

    def test_consumes_in_order_then_none(
        self, state_repo, signals, messages, make_record
    ):
        run_lease = _claimed_run_lease(state_repo, signals, messages, make_record)
        obligation = run_lease.lease.record.obligation
        messages.send(
            obligation.flow_name, obligation.id, "steer", "one", actor="api"
        )
        messages.send(
            obligation.flow_name, obligation.id, "steer", "two", actor="api"
        )

        token = bind_lease(run_lease)
        try:
            assert recv("steer")["body"] == "one"
            assert recv("steer")["body"] == "two"
            assert recv("steer") is None
        finally:
            unbind_lease(token)

    def test_consumption_is_checkpointed_in_the_account(
        self, state_repo, signals, messages, make_record
    ):
        run_lease = _claimed_run_lease(state_repo, signals, messages, make_record)
        obligation = run_lease.lease.record.obligation
        sent_id, _ = messages.send(
            obligation.flow_name, obligation.id, "steer", "go", actor="api"
        )

        token = bind_lease(run_lease)
        try:
            recv("steer")
        finally:
            unbind_lease(token)

        view = state_repo.read(obligation.flow_name, obligation.id)
        consumed = view.record.consumptions_for("steer")
        assert [c.message_id for c in consumed] == [sent_id]

    def test_next_attempt_replays_recorded_consumptions_first(
        self, state_repo, signals, messages, make_record
    ):
        """The recv checkpoint: attempt 2's first recvs return exactly the
        messages attempt 1 consumed, in order, before any fresh one."""
        run_lease = _claimed_run_lease(state_repo, signals, messages, make_record)
        obligation = run_lease.lease.record.obligation
        messages.send(
            obligation.flow_name, obligation.id, "steer", "one", actor="api"
        )
        token = bind_lease(run_lease)
        try:
            assert recv("steer")["body"] == "one"
        finally:
            unbind_lease(token)
        # A third message lands; attempt 1 "crashes" (lease released here
        # for simplicity — the account keeps the consumption either way).
        run_lease.lease.release(run_lease.lease.record)
        messages.send(
            obligation.flow_name, obligation.id, "steer", "two", actor="api"
        )

        # Attempt 2 claims and re-runs the flow from the top.
        lease2 = state_repo.acquire(
            obligation.flow_name, obligation.id, ttl=600, holder="w2",
            state_fn=lambda existing: existing,
        )
        run_lease2 = RunLease(
            lease=lease2, signals=signals, messages=messages, min_interval=0.0
        )
        token = bind_lease(run_lease2)
        try:
            assert recv("steer")["body"] == "one"   # replayed, identical
            assert recv("steer")["body"] == "two"   # fresh consume
            assert recv("steer") is None
        finally:
            unbind_lease(token)

        view = state_repo.read(obligation.flow_name, obligation.id)
        assert len(view.record.consumptions_for("steer")) == 2

    def test_flow_receives_messages_through_worker(
        self, state_repo, signals, messages, make_flow_job
    ):
        """End to end through execute_job: the ambient lease carries the
        channel, so plain flows call flowlet.recv()."""
        job = make_flow_job()
        messages.send(job.flow_name, job.run_id, "steer", "left", actor="api")
        received = []

        def flow(**kwargs):
            while (msg := flowlet_pkg.recv("steer")) is not None:
                received.append(msg["body"])

        executor = FakeExecutor({job.flow_name: flow})
        rc = execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        assert rc == 0
        assert received == ["left"]


# ============================================================================
# Controller — send and recv endpoints
# ============================================================================


@pytest.mark.unit
class TestMessageEndpoints:
    def _seed_ready(self, make_run_state, seed_lease, store, **overrides):
        state = make_run_state(status=ReportedStatus.pending, **overrides)
        seed_lease(store, state)
        return state

    def test_send_then_recv_lifecycle(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)

        sent = controller.send_run_message(
            state.run_id, "steer",
            RunMessageRequest(
                flow_name=state.flow_name, actor="operator", body={"go": 1}
            ),
        )
        assert sent.deduplicated is False

        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )
        assert claim.messages == {"steer": 1}

        first = controller.recv_run_message(
            state.run_id,
            ExecutorRecvRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                topic="steer", seq=1,
            ),
        )
        assert first.message["body"] == {"go": 1}
        assert first.replayed is False
        assert first.pending == 0

        empty = controller.recv_run_message(
            state.run_id,
            ExecutorRecvRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch,
                topic="steer", seq=2,
            ),
        )
        assert empty.message is None

    def test_recv_replays_recorded_seq_and_refuses_gaps(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        controller.send_run_message(
            state.run_id, "steer",
            RunMessageRequest(
                flow_name=state.flow_name, actor="operator", body="one"
            ),
        )
        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )

        def _recv(seq):
            return controller.recv_run_message(
                state.run_id,
                ExecutorRecvRequest(
                    flow_name=state.flow_name, executor="s1",
                    epoch=claim.epoch, topic="steer", seq=seq,
                ),
            )

        first = _recv(1)
        replay = _recv(1)  # a retried report converges on the same message
        assert replay.replayed is True
        assert replay.message == first.message

        with pytest.raises(HTTPException) as exc:
            _recv(5)
        assert exc.value.status_code == 409

    def test_renew_reports_pending_message_counts(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        claim = controller.claim_run(
            state.run_id,
            ExecutorClaimRequest(flow_name=state.flow_name, executor="s1"),
        )
        controller.send_run_message(
            state.run_id, "steer",
            RunMessageRequest(
                flow_name=state.flow_name, actor="operator", body="hello"
            ),
        )

        renewed = controller.renew_run(
            state.run_id,
            ExecutorRenewRequest(
                flow_name=state.flow_name, executor="s1", epoch=claim.epoch
            ),
        )

        assert renewed.messages == {"steer": 1}

    def test_send_with_dedup_key_converges(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        req = RunMessageRequest(
            flow_name=state.flow_name, actor="hook", body="x",
            dedup_key="delivery-1",
        )

        first = controller.send_run_message(state.run_id, "hook", req)
        second = controller.send_run_message(state.run_id, "hook", req)

        assert second.deduplicated is True
        assert second.message_id == first.message_id

    def test_send_to_unknown_run_is_404(self, state_repo, signals):
        controller = _controller(state_repo, signals)
        with pytest.raises(HTTPException) as exc:
            controller.send_run_message(
                uuid7(), "steer",
                RunMessageRequest(flow_name="f", actor="a", body=1),
            )
        assert exc.value.status_code == 404

    def test_send_to_closed_run_is_409(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = make_run_state(status=ReportedStatus.completed)
        seed_lease(store, state)
        controller = _controller(state_repo, signals)

        with pytest.raises(HTTPException) as exc:
            controller.send_run_message(
                state.run_id, "steer",
                RunMessageRequest(
                    flow_name=state.flow_name, actor="a", body=1
                ),
            )
        assert exc.value.status_code == 409

    def test_unsafe_topic_is_422(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        state = self._seed_ready(make_run_state, seed_lease, store)
        controller = _controller(state_repo, signals)
        with pytest.raises(HTTPException) as exc:
            controller.send_run_message(
                state.run_id, "_dedup",
                RunMessageRequest(
                    flow_name=state.flow_name, actor="a", body=1
                ),
            )
        assert exc.value.status_code == 422


# ============================================================================
# flow_version provenance
# ============================================================================


@pytest.mark.unit
class TestFlowVersion:
    def test_claim_transition_stamps_version_at_creation(self):
        job = FlowJob(flow_name="f", flow_version="git:abc123")
        decision = {}

        record = _claim_transition(
            None, job=job, worker_id="w1", cancel_pending=False,
            gated=False, decision=decision,
        )

        assert record.obligation.flow_version == "git:abc123"

    def test_submission_carries_version_to_the_account(
        self, state_repo, signals, make_run_state, seed_lease, store
    ):
        """Queue-mode submission stamps the job; the first claim stamps the
        obligation; the executor claim response surfaces it."""
        queue = FakeQueue([])
        controller = _controller(state_repo, signals, queue=queue)
        response = controller.submit_flow(
            "f", FlowArguments(kwargs={}, flow_version="rel-2026.08")
        )
        job, _delay = queue.enqueued[0]
        assert job.flow_version == "rel-2026.08"

        executor = FakeExecutor({"f": lambda **kw: None})
        execute_job(FakeQueue([job]), executor, state_repo, signals, "w1")

        view = state_repo.read("f", response.run_id)
        assert view.record.obligation.flow_version == "rel-2026.08"


# ============================================================================
# Sweeper — archive clears the channel
# ============================================================================


@pytest.mark.unit
class TestArchiveClearsMessages:
    def test_archived_run_loses_its_messages(
        self, state_repo, signals, messages, make_run_state, seed_lease, store
    ):
        state = make_run_state(
            status=ReportedStatus.completed,
            ended_at=Timestamp(datetime.now(UTC) - timedelta(hours=3)),
        )
        seed_lease(
            store, state, deadline=datetime.now(UTC) - timedelta(hours=3)
        )
        messages.send(state.flow_name, state.run_id, "steer", "x", actor="a")

        stats = sweep(
            state_repo, FakeQueue([]), signals=signals, archive_grace=3600
        )

        assert stats.archived == 1
        assert messages.counts(state.flow_name, state.run_id) == {}
