"""Unit tests for the account-transition feed (abstractions v0.3, delta 3).

Covers the ordered named-log feed behind RunEventLog: append-through from
lifecycle events, ordered reads with a resume cursor, commit-boundary
paging, the strictly non-throwing writer contract, the worker lifecycle
landing on the feed, and the controller read endpoint.
"""
from uuid import uuid7

import pytest
from cairndb.storage.filesystem import FilesystemStorage
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.events import RunEventLog
from flowlet.repository import SignalRepository, StateRepository
from flowlet.transitions import TransitionFeed

from unit.test_worker_layer import FakeExecutor, FakeQueue


@pytest.fixture
def store(tmp_path):
    return FilesystemStorage(tmp_path)


@pytest.fixture
def feed(store):
    return TransitionFeed(store=store)


@pytest.fixture
def events(store, feed):
    return RunEventLog(store=store, feed=feed)


def _append(events, run_id, event, **kwargs):
    events.append(
        flow_name="test_flow", run_id=run_id, event=event, actor="w1", **kwargs
    )


@pytest.mark.unit
class TestTransitionFeed:
    def test_events_land_on_the_feed_in_order(self, events, feed):
        run_id = uuid7()
        _append(events, run_id, "claimed", attempt=1)
        _append(events, run_id, "completed", attempt=1, to_status="completed")

        page = feed.read()
        assert [e["event"] for e in page.entries] == ["claimed", "completed"]
        assert page.entries[0]["run_id"] == str(run_id)
        assert page.entries[1]["to"] == "completed"
        assert all("seq" in e for e in page.entries)
        assert page.cursor > 0

    def test_cursor_resumes_without_skip_or_replay(self, events, feed):
        run_id = uuid7()
        _append(events, run_id, "claimed")
        first = feed.read()
        assert [e["event"] for e in first.entries] == ["claimed"]

        _append(events, run_id, "completed")
        second = feed.read(after=first.cursor)
        assert [e["event"] for e in second.entries] == ["completed"]

        # At the tail: no entries, cursor unchanged.
        tail = feed.read(after=second.cursor)
        assert tail.entries == []
        assert tail.cursor == second.cursor

    def test_limit_pages_on_commit_boundaries(self, events, feed):
        run_id = uuid7()
        for name in ("a", "b", "c"):
            _append(events, run_id, name)

        collected = []
        cursor = 0
        while True:
            page = feed.read(after=cursor, limit=1)
            if not page.entries:
                break
            collected.extend(e["event"] for e in page.entries)
            cursor = page.cursor
        assert collected == ["a", "b", "c"]

    def test_empty_feed_reads_empty(self, feed):
        page = feed.read()
        assert page.entries == []
        assert page.cursor == 0

    def test_feed_failure_never_breaks_the_event_append(
        self, events, feed, monkeypatch
    ):
        """The feed is derived: its liveness must never gate a transition."""
        async def boom(self, record):
            raise RuntimeError("log unavailable")

        monkeypatch.setattr(TransitionFeed, "_append", boom)
        run_id = uuid7()
        _append(events, run_id, "claimed")  # must not raise

        # The per-run event log still has the event; the feed simply lost it.
        assert [e["event"] for e in events.read("test_flow", run_id)] == [
            "claimed"
        ]


@pytest.mark.unit
class TestWorkerLifecycleOnTheFeed:
    def test_completed_run_is_observable_with_a_cursor(
        self, store, feed, events, make_flow_job
    ):
        from flowlet.worker import execute_job

        state_repo = StateRepository(store=store)
        signals = SignalRepository(store=store)
        job = make_flow_job()

        rc = execute_job(
            FakeQueue([job]),
            FakeExecutor({job.flow_name: lambda **kw: None}),
            state_repo, signals, "w1",
            events=events,
        )

        assert rc == 0
        page = feed.read()
        assert any(
            e["event"] == "completed" and e["run_id"] == str(job.run_id)
            for e in page.entries
        )


@pytest.mark.unit
class TestTransitionsEndpoint:
    def test_list_transitions_serves_entries_and_cursor(self, events, feed):
        run_id = uuid7()
        _append(events, run_id, "claimed")
        controller = FlowController(transitions=feed)

        resp = controller.list_transitions()
        assert [e["event"] for e in resp.entries] == ["claimed"]
        assert resp.cursor > 0

        empty = controller.list_transitions(after=resp.cursor)
        assert empty.entries == []

    def test_unconfigured_feed_is_503(self):
        controller = FlowController()
        with pytest.raises(HTTPException) as exc:
            controller.list_transitions()
        assert exc.value.status_code == 503
