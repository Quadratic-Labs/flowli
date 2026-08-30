"""
Unit tests for the storage repository layer.

Covers:
- StateRepository: lease acquisition with atomic state transitions, fenced
  writes, and StateView reads on the cairndb store (the filesystem backend
  implements the same conditional-write contract as the cloud backends, so
  these exercise genuine CAS and epoch fencing without mocks)
- SignalRepository: idempotent obligation-scoped signals
- LogRepository: span reading over the obligations/<flow>/<date>/<obligation_id>/ layout
"""
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid7

import pytest
from cairndb.core.exceptions import LeaseLost
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.models import ReportedStatus, SpanRecord
from flowlet.repository.log import LogRepository
from flowlet.repository.signals import CANCEL, SignalRepository
from flowlet.repository.state import AlreadyClosed, StateRepository
from flowlet.serdes import to_json
from flowlet.storage import obligation_prefix


@pytest.fixture
def store(tmp_path: Path) -> FilesystemStorage:
    return FilesystemStorage(tmp_path)


@pytest.fixture
def state_repo(store) -> StateRepository:
    return StateRepository(store=store)


@pytest.fixture
def signals(store) -> SignalRepository:
    return SignalRepository(store=store)


# ============================================================================
# StateRepository
# ============================================================================


@pytest.mark.unit
class TestStateRepository:
    def test_acquire_and_read_round_trip(self, state_repo, make_record):
        record = make_record(flow_name="my_flow")
        obligation = record.obligation
        lease = state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w1", state_fn=lambda _: record,
        )
        assert lease is not None
        assert lease.holder == "w1"
        assert lease.epoch == 1

        view = state_repo.read(obligation.flow_name, obligation.id)
        assert view is not None
        assert view.record.obligation.id == obligation.id
        assert view.state.status == record.summary().status
        assert view.holder == "w1"
        assert view.held() is True

    def test_read_missing_returns_none(self, state_repo):
        assert state_repo.read("no_such_flow", uuid7()) is None

    def test_acquire_held_returns_none(self, state_repo, make_record):
        record = make_record()
        obligation = record.obligation
        assert state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w1", state_fn=lambda _: record,
        ) is not None
        assert state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w2", state_fn=lambda _: record,
        ) is None

    def test_release_then_reacquire_applies_transition(
        self, state_repo, make_record
    ):
        from flowlet.models import AttemptOutcome

        record = make_record(status=ReportedStatus.running, attempt=1)
        obligation = record.obligation
        lease = state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w1", state_fn=lambda _: record,
        )
        record.record_outcome(AttemptOutcome.raised, error="ValueError")
        lease.release(record)

        def next_attempt(current):
            current.begin_attempt("w2")
            return current

        second = state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w2", state_fn=next_attempt,
        )
        assert second is not None
        assert second.epoch == 2
        assert len(second.record.attempts) == 2
        assert second.record.open_attempt is not None
        assert second.record.open_attempt.executor == "w2"

    def test_fenced_holder_gets_lease_lost(
        self, state_repo, make_obligation_summary, seed_lease, store
    ):
        """A holder whose lease expired and was stolen cannot write."""
        state = make_obligation_summary(status=ReportedStatus.running)
        # (seed_lease expands the ObligationSummary spec into an account)
        # Seed an expired held lease, then steal it.
        seed_lease(
            store, state, holder="w1",
            deadline=datetime.now(UTC) - timedelta(seconds=5),
        )
        thief = state_repo.acquire(
            state.flow_name, state.obligation_id,
            ttl=60, holder="w2", state_fn=lambda s: s,
        )
        assert thief is not None
        assert thief.epoch == 2

        # Reconstruct the old holder's lease via a fresh steal race: the
        # thief's own handle must keep working, while writes through a
        # stale-epoch handle raise LeaseLost.  Simulate the stale holder by
        # stealing with an expired deadline again.
        seed_lease(
            store, state, holder="w2",
            deadline=datetime.now(UTC) - timedelta(seconds=5), epoch=2,
        )
        stale = thief  # epoch 2 handle, but the document was rewritten
        third = state_repo.acquire(
            state.flow_name, state.obligation_id,
            ttl=60, holder="w3", state_fn=lambda s: s,
        )
        assert third is not None
        with pytest.raises(LeaseLost):
            stale.renew()

    def test_state_fn_exception_aborts_acquisition(
        self, state_repo, make_obligation_summary, seed_lease, store
    ):
        state = make_obligation_summary(status=ReportedStatus.completed)
        seed_lease(store, state)  # released, closed

        def refuse(existing):
            if existing.obligation.status.is_closed():
                raise AlreadyClosed(existing)
            return existing

        with pytest.raises(AlreadyClosed):
            state_repo.acquire(
                state.flow_name, state.obligation_id,
                ttl=60, holder="w1", state_fn=refuse,
            )
        # Nothing was written: still released at epoch 1.
        view = state_repo.read(state.flow_name, state.obligation_id)
        assert view.epoch == 1
        assert view.holder is None

    def test_write_through_lease_updates_payload(self, state_repo, make_record):
        record = make_record(status=ReportedStatus.running)
        obligation = record.obligation
        lease = state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w1", state_fn=lambda _: record,
        )
        record.obligation.cancel_requested = True
        lease.write(record)
        view = state_repo.read(obligation.flow_name, obligation.id)
        assert view.record.obligation.cancel_requested is True
        assert view.holder == "w1"

    def test_delete_removes_state(self, state_repo, make_obligation_summary, seed_lease, store):
        state = make_obligation_summary(flow_name="temp_flow")
        seed_lease(store, state)
        state_repo.delete(state.flow_name, state.obligation_id)
        assert state_repo.read(state.flow_name, state.obligation_id) is None

    def test_list_views_returns_all(self, state_repo, make_obligation_summary, seed_lease, store):
        states = [make_obligation_summary(flow_name="batch_flow") for _ in range(3)]
        for s in states:
            seed_lease(store, s)

        listed = state_repo.list_views(flow_name="batch_flow")
        assert {v.state.obligation_id for v in listed} == {s.obligation_id for s in states}

    def test_list_states_filtered_by_flow(
        self, state_repo, make_obligation_summary, seed_lease, store
    ):
        seed_lease(store, make_obligation_summary(flow_name="flow_a"))
        seed_lease(store, make_obligation_summary(flow_name="flow_b"))

        only_a = state_repo.list_states(flow_name="flow_a")
        assert len(only_a) == 1
        assert only_a[0].flow_name == "flow_a"

    def test_list_states_empty_when_no_state_prefix(self, state_repo):
        assert state_repo.list_states() == []

    def test_state_key_layout(self, state_repo, store, make_record):
        """Lease documents live at state/<flow>/<obligation_id>.json."""
        record = make_record(flow_name="my_flow")
        obligation = record.obligation
        state_repo.acquire(
            obligation.flow_name, obligation.id,
            ttl=60, holder="w1", state_fn=lambda _: record,
        )

        keys = store.list_objects_sync("state/")
        assert keys == [f"state/my_flow/{obligation.id}.json"]

    def test_archive_moves_payload_into_obligation_folder(
        self, state_repo, store, make_obligation_summary, seed_lease
    ):
        state = make_obligation_summary(flow_name="my_flow", status=ReportedStatus.completed)
        seed_lease(store, state)

        state_repo.archive(state.flow_name, state.obligation_id)

        assert state_repo.read(state.flow_name, state.obligation_id) is None
        archived = store.get_object_sync(
            f"{obligation_prefix(state.flow_name, state.obligation_id)}/state.json"
        )
        assert archived is not None
        # The archived object is the bare account wire format (no envelope).
        from flowlet.models import ObligationRecord, ObligationStatus
        from flowlet.serdes import from_json

        restored = from_json(ObligationRecord)(archived.data.decode())
        assert restored.obligation.id == state.obligation_id
        assert restored.obligation.status == ObligationStatus.discharged
        assert restored.summary().status == ReportedStatus.completed


# ============================================================================
# SignalRepository
# ============================================================================


@pytest.mark.unit
class TestSignalRepository:
    def test_send_and_get_round_trip(self, signals):
        obligation_id = uuid7()
        assert signals.send("my_flow", obligation_id, CANCEL, actor="api") is True
        payload = signals.get("my_flow", obligation_id, CANCEL)
        assert payload is not None
        assert payload["actor"] == "api"

    def test_send_is_idempotent(self, signals):
        obligation_id = uuid7()
        assert signals.send("my_flow", obligation_id, CANCEL, actor="api") is True
        assert signals.send("my_flow", obligation_id, CANCEL, actor="other") is False
        # First sender's payload wins.
        assert signals.get("my_flow", obligation_id, CANCEL)["actor"] == "api"

    def test_get_missing_returns_none(self, signals):
        assert signals.get("my_flow", uuid7(), CANCEL) is None

    def test_clear_removes_all_signals(self, signals):
        obligation_id = uuid7()
        signals.send("my_flow", obligation_id, CANCEL, actor="api")
        signals.clear("my_flow", obligation_id)
        assert signals.get("my_flow", obligation_id, CANCEL) is None


# ============================================================================
# LogRepository helpers
# ============================================================================


def _write_span_file(store, flow_name: str, obligation_id, spans: list[SpanRecord], attempt: int = 1) -> None:
    """Write a spans-<attempt>.jsonl object in the obligations/<flow>/<date>/<obligation_id>/ layout."""
    key = f"{obligation_prefix(flow_name, obligation_id)}/spans-{attempt}.jsonl"
    store.put_object_sync(
        key, "\n".join(to_json(span) for span in spans).encode("utf-8")
    )


@pytest.fixture
def log_repo(store) -> LogRepository:
    return LogRepository(store=store)


@pytest.mark.unit
class TestLogRepository:
    def test_get_spans_missing_folder_returns_empty(self, log_repo):
        assert log_repo.get_spans("ghost_flow", uuid7()) == []

    def test_get_spans_returns_written_records(self, log_repo, store, make_span_record):
        obligation_id = uuid7()
        spans = [
            make_span_record(obligation_id=obligation_id, span_id="a" * 16),
            make_span_record(obligation_id=obligation_id, span_id="b" * 16, parent_span_id="a" * 16),
        ]
        _write_span_file(store, "my_flow", obligation_id, spans)

        assert len(log_repo.get_spans("my_flow", obligation_id)) == 2

    def test_get_spans_merges_attempts(self, log_repo, store, make_span_record):
        obligation_id = uuid7()
        _write_span_file(
            store, "my_flow", obligation_id,
            [make_span_record(obligation_id=obligation_id, attempt=1)], attempt=1,
        )
        _write_span_file(
            store, "my_flow", obligation_id,
            [make_span_record(obligation_id=obligation_id, attempt=2)], attempt=2,
        )

        spans = log_repo.get_spans("my_flow", obligation_id)
        assert sorted(s.attempt for s in spans) == [1, 2]

    def test_list_obligation_ids_for_flow(self, log_repo, store, make_span_record):
        obligation_id = uuid7()
        _write_span_file(store, "my_flow", obligation_id, [make_span_record(obligation_id=obligation_id)])

        results = log_repo.list_obligation_ids(flow_name="my_flow")
        assert results == [("my_flow", obligation_id)]

    def test_list_obligation_ids_empty_when_no_obligations_prefix(self, log_repo):
        assert log_repo.list_obligation_ids() == []

    def test_list_obligation_ids_sorted_chronologically(self, log_repo, store, make_span_record):
        first, second = uuid7(), uuid7()
        _write_span_file(store, "my_flow", second, [make_span_record(obligation_id=second)])
        _write_span_file(store, "my_flow", first, [make_span_record(obligation_id=first)])

        results = log_repo.list_obligation_ids(flow_name="my_flow")
        assert [r for _, r in results] == sorted([first, second], key=str)

    def test_malformed_lines_are_skipped(self, log_repo, store, make_span_record):
        obligation_id = uuid7()
        span = make_span_record(obligation_id=obligation_id)
        key = f"{obligation_prefix('my_flow', obligation_id)}/spans-1.jsonl"
        store.put_object_sync(
            key, (to_json(span) + "\n{not json}\n").encode("utf-8")
        )

        assert len(log_repo.get_spans("my_flow", obligation_id)) == 1
