"""Unit tests for the append-only obligation event log.

Covers file placement in the obligation folder, JSONL record shape, append-only
accumulation, the non-throwing guarantee, and emission from the worker and
sweeper lifecycles.
"""
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.events import EventLog
from flowlet.models import ReportedStatus
from flowlet.repository import StateRepository
from flowlet.storage import obligation_prefix
from flowlet.sweeper import sweep
from flowlet.worker import execute_job

from unit.test_worker_layer import FakeExecutor, FakeQueue


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def event_log(store):
    return EventLog(store=store)


def _read_events(store, flow_name, obligation_id):
    obj = store.get_object_sync(f"{obligation_prefix(flow_name, obligation_id)}/events.jsonl")
    if obj is None:
        return []
    return [json.loads(line) for line in obj.data.decode().splitlines()]


# ============================================================================
# EventLog.append
# ============================================================================


class TestAppend:
    def test_event_lands_in_obligation_folder(self, event_log, store):
        obligation_id = uuid7()
        event_log.append(
            flow_name="flow", obligation_id=obligation_id, event="submitted", actor="api"
        )
        events = _read_events(store, "flow", obligation_id)
        assert len(events) == 1
        assert events[0]["event"] == "submitted"
        assert events[0]["actor"] == "api"
        assert events[0]["obligation_id"] == str(obligation_id)
        assert events[0]["flow_name"] == "flow"
        assert "ts" in events[0]

    def test_appends_accumulate_in_order(self, event_log, store):
        obligation_id = uuid7()
        for name in ("submitted", "claimed", "completed"):
            event_log.append(
                flow_name="flow", obligation_id=obligation_id, event=name, actor="w1"
            )
        events = _read_events(store, "flow", obligation_id)
        assert [e["event"] for e in events] == ["submitted", "claimed", "completed"]

    def test_optional_fields_are_omitted_when_absent(self, event_log, store):
        obligation_id = uuid7()
        event_log.append(
            flow_name="flow", obligation_id=obligation_id, event="submitted", actor="api"
        )
        record = _read_events(store, "flow", obligation_id)[0]
        for key in ("attempt", "from", "to", "cause", "details"):
            assert key not in record

    def test_transition_fields_are_recorded(self, event_log, store):
        obligation_id = uuid7()
        event_log.append(
            flow_name="flow",
            obligation_id=obligation_id,
            event="claimed",
            actor="w1",
            attempt=2,
            from_status="pending",
            to_status="running",
            cause="retry",
            details={"case": "ready"},
        )
        record = _read_events(store, "flow", obligation_id)[0]
        assert record["attempt"] == 2
        assert record["from"] == "pending"
        assert record["to"] == "running"
        assert record["cause"] == "retry"
        assert record["details"] == {"case": "ready"}

    def test_append_never_raises(self, tmp_path, store):
        # Make the obligations/ segment a plain file, so the store cannot create
        # keys under it — the append must swallow the resulting error.
        (tmp_path / "obligations").write_text("not a directory")
        log = EventLog(store=store)
        log.append(flow_name="flow", obligation_id=uuid7(), event="submitted", actor="api")


# ============================================================================
# EventLog.read
# ============================================================================


class TestRead:
    def test_read_returns_appended_events(self, event_log):
        obligation_id = uuid7()
        event_log.append(flow_name="flow", obligation_id=obligation_id, event="submitted", actor="api")
        event_log.append(flow_name="flow", obligation_id=obligation_id, event="claimed", actor="w1")
        records = event_log.read("flow", obligation_id)
        assert [r["event"] for r in records] == ["submitted", "claimed"]

    def test_read_missing_file_returns_empty(self, event_log):
        assert event_log.read("flow", uuid7()) == []

    def test_read_skips_malformed_lines(self, event_log, store):
        obligation_id = uuid7()
        event_log.append(flow_name="flow", obligation_id=obligation_id, event="submitted", actor="api")
        key = f"{obligation_prefix('flow', obligation_id)}/events.jsonl"
        obj = store.get_object_sync(key)
        store.put_object_sync(key, obj.data + b"not json\n", if_match=obj.etag)
        event_log.append(flow_name="flow", obligation_id=obligation_id, event="claimed", actor="w1")
        records = event_log.read("flow", obligation_id)
        assert [r["event"] for r in records] == ["submitted", "claimed"]


# ============================================================================
# Controller — GET /obligations/{obligation_id}/events
# ============================================================================


def _request(headers: dict[str, str] | None = None):
    """Build a minimal FastAPI Request for direct controller calls."""
    from fastapi import Request

    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request(scope={"type": "http", "headers": raw})


def _body_events(response) -> list[dict]:
    return json.loads(bytes(response.body))


class TestObligationEventsEndpoint:
    @pytest.fixture
    def state_repo(self, store):
        return StateRepository(store=store)

    @pytest.fixture
    def controller(self, state_repo, event_log):
        from flowlet.api.controller import FlowController

        return FlowController(state_repo=state_repo, events=event_log)

    @pytest.mark.asyncio
    async def test_events_of_active_obligation(
        self, controller, state_repo, event_log, make_obligation_summary, seed_lease, store
    ):
        state = make_obligation_summary(status=ReportedStatus.running)
        seed_lease(store, state)
        event_log.append(
            flow_name=state.flow_name, obligation_id=state.obligation_id,
            event="claimed", actor="w1",
        )

        response = await controller.get_obligation_events(_request(), state.obligation_id)
        assert [r["event"] for r in _body_events(response)] == ["claimed"]

    @pytest.mark.asyncio
    async def test_archived_obligation_resolved_via_querier(
        self, event_log, state_repo
    ):
        from unittest.mock import AsyncMock

        from flowlet.api.controller import FlowController

        obligation_id = uuid7()
        event_log.append(
            flow_name="flow", obligation_id=obligation_id, event="completed", actor="w1"
        )
        querier = AsyncMock()
        querier.find_flow_name = AsyncMock(return_value="flow")
        controller = FlowController(
            state_repo=state_repo, events=event_log, querier=querier,
        )

        response = await controller.get_obligation_events(_request(), obligation_id)
        assert [r["event"] for r in _body_events(response)] == ["completed"]

    @pytest.mark.asyncio
    async def test_unknown_obligation_is_404(self, controller):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await controller.get_obligation_events(_request(), uuid7())
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_no_storage_is_503(self):
        from fastapi import HTTPException

        from flowlet.api.controller import FlowController

        controller = FlowController()
        with pytest.raises(HTTPException) as exc:
            await controller.get_obligation_events(_request(), uuid7())
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_etag_roundtrip_yields_304(
        self, controller, state_repo, event_log, make_obligation_summary, seed_lease, store
    ):
        state = make_obligation_summary(status=ReportedStatus.running)
        seed_lease(store, state)
        event_log.append(
            flow_name=state.flow_name, obligation_id=state.obligation_id,
            event="claimed", actor="w1",
        )

        first = await controller.get_obligation_events(_request(), state.obligation_id)
        etag = first.headers["etag"]
        second = await controller.get_obligation_events(
            _request({"If-None-Match": etag}), state.obligation_id
        )
        assert second.status_code == 304
        assert second.headers["etag"] == etag

        # New content invalidates the tag — full body again.
        event_log.append(
            flow_name=state.flow_name, obligation_id=state.obligation_id,
            event="completed", actor="w1",
        )
        third = await controller.get_obligation_events(
            _request({"If-None-Match": etag}), state.obligation_id
        )
        assert third.status_code == 200
        assert third.headers["etag"] != etag
        assert [r["event"] for r in _body_events(third)] == ["claimed", "completed"]


# ============================================================================
# Worker emission
# ============================================================================


class TestWorkerEmission:
    def test_success_emits_claimed_then_completed(
        self, store, event_log, make_flow_job
    ):
        from flowlet.repository import SignalRepository

        state_repo = StateRepository(store=store)
        signals = SignalRepository(store=store)
        job = make_flow_job()
        queue = FakeQueue([job])
        executor = FakeExecutor({job.flow_name: lambda **kw: None})

        rc = execute_job(queue, executor, state_repo, signals, "w1", events=event_log)

        assert rc == 0
        names = [e["event"] for e in _read_events(store, job.flow_name, job.obligation_id)]
        assert names == ["claimed", "completed"]

    def test_failure_emits_retry_then_final_failure(
        self, store, event_log, make_flow_job
    ):
        from flowlet.repository import SignalRepository

        state_repo = StateRepository(store=store)
        signals = SignalRepository(store=store)

        def boom(**kw):
            raise ValueError("nope")

        job = make_flow_job(max_retries=2)
        queue = FakeQueue([job])
        executor = FakeExecutor({job.flow_name: boom})

        execute_job(queue, executor, state_repo, signals, "w1", events=event_log)  # attempt 1
        retry_job = queue.enqueued[0][0]
        queue.jobs = [retry_job]
        execute_job(queue, executor, state_repo, signals, "w1", events=event_log)  # attempt 2

        events = _read_events(store, job.flow_name, job.obligation_id)
        names = [e["event"] for e in events]
        assert names == ["claimed", "retry_scheduled", "claimed", "failed"]
        assert events[1]["cause"] == "ValueError"


# ============================================================================
# Sweeper emission
# ============================================================================


class TestSweeperEmission:
    def test_expired_lease_recovery_emits_requeued(
        self, store, event_log, make_obligation_summary, seed_lease
    ):
        state_repo = StateRepository(store=store)
        state = make_obligation_summary(status=ReportedStatus.running)
        seed_lease(
            store, state, holder="dead-worker",
            deadline=datetime.now(UTC) - timedelta(seconds=5),
        )
        queue = FakeQueue()

        stats = sweep(state_repo, queue, events=event_log)

        assert stats.requeued == 1
        events = _read_events(store, state.flow_name, state.obligation_id)
        assert [e["event"] for e in events] == ["requeued"]
        assert events[0]["actor"] == "sweeper"
        assert events[0]["cause"] == "lease_expired"
