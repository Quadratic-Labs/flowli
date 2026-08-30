"""Taskflow tests: authoring surface + the Prefect-like loop over the kernel.

The end-to-end tests run the combined router (authoring + account surface)
against a real filesystem bucket, with the worker executing registered
callables through the kernel's executor seam.
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flowlet.models import ObligationStatus, ReportedStatus, Decision
from flowlet.worker import execute_job

from taskflow import RegistryExecutor, Taskflow


@pytest.fixture
def tf(tmp_path) -> Taskflow:
    return Taskflow.configure(
        {
            "storage": {"type": "filesystem", "path": str(tmp_path / "bucket")},
            "queue": {"type": "memory"},
        }
    )


@pytest.fixture
def http(tf) -> TestClient:
    app = FastAPI()
    app.include_router(tf.get_router())
    return TestClient(app)


def _work_once(tf, worker_id="w1") -> int:
    return execute_job(
        tf.queue,
        RegistryExecutor(tf.registry),
        tf.state_repo,
        tf.signals,
        worker_id,
        events=tf.events,
        review_policy_for=tf.review_policy_for,
    )


# ============================================================================
# Authoring surface
# ============================================================================


class TestSchemas:
    def test_registered_flows_appear_with_schemas(self, tf, http):
        @tf.flow()
        def my_flow(x: int, y: int) -> int:
            """Adds."""
            return x + y

        listed = http.get("/flows").json()
        assert [f["name"] for f in listed] == ["my_flow"]
        assert listed[0]["has_schema"] is True
        assert [p["name"] for p in listed[0]["parameters"]] == ["x", "y"]

        schema = http.get("/flows/my_flow/schema").json()
        assert schema["has_schema"] is True
        assert "json_schema" in schema

    def test_unknown_flow_schema_is_404(self, http):
        assert http.get("/flows/ghost/schema").status_code == 404


class TestSubmitValidation:
    def test_unknown_flow_is_404(self, http):
        resp = http.post("/submit/ghost", json={"kwargs": {}})
        assert resp.status_code == 404

    def test_invalid_kwargs_are_422(self, tf, http):
        @tf.flow()
        def typed(x: int) -> int:
            return x

        resp = http.post("/submit/typed", json={"kwargs": {"x": "not-an-int"}})
        assert resp.status_code == 422

    def test_flow_options_fill_the_job(self, tf, http):
        @tf.flow(timeout=1200, max_retries=5)
        def slow() -> None:
            pass

        resp = http.post("/submit/slow", json={"kwargs": {}})
        assert resp.status_code == 200
        job = tf.queue.dequeue()
        assert job.timeout_seconds == 1200
        assert job.max_retries == 5


class TestSyncExecute:
    def test_execute_accounts_the_obligation(self, tf, http):
        seen = []

        @tf.flow()
        def greet(name: str) -> None:
            seen.append(name)

        resp = http.post("/execute/greet", json={"kwargs": {"name": "ada"}})
        assert resp.status_code == 200
        assert seen == ["ada"]

        # The synchronous execution left a full account.
        views = tf.state_repo.list_views("greet")
        assert len(views) == 1
        record = views[0].record
        assert record.obligation.status == ObligationStatus.discharged
        assert record.obligation.caused_by == "sync_execute"
        assert record.last_attempt.executor == "sync-worker"

    def test_execute_failure_raises_and_accounts(self, tf, http):
        @tf.flow()
        def boom() -> None:
            raise ValueError("no")

        with pytest.raises(ValueError):
            http.post("/execute/boom", json={"kwargs": {}})

        record = tf.state_repo.list_views("boom")[0].record
        assert record.obligation.status == ObligationStatus.abandoned
        assert record.last_attempt.error == "ValueError"

    def test_execute_unknown_flow_is_404(self, http):
        assert http.post("/execute/ghost", json={"kwargs": {}}).status_code == 404


# ============================================================================
# The Prefect-like loop end to end
# ============================================================================


class TestWorkerLoop:
    def test_submit_then_work_completes(self, tf, http):
        results = []

        @tf.flow()
        def pipeline(x: int) -> None:
            results.append(x * 2)

        resp = http.post("/submit/pipeline", json={"kwargs": {"x": 21}})
        obligation_id = resp.json()["obligation_id"]

        assert _work_once(tf) == 0
        assert results == [42]
        state = http.post(
            "/obligations/query", json={"flow_name": "pipeline", "limit": 10}
        ).json()
        assert state[0]["obligation_id"] == obligation_id
        assert state[0]["status"] == "completed"

    def test_gated_flow_suspends_and_gate_policy_guards(self, tf, http):
        @tf.flow(gated=True, gate=lambda actor, record: actor == "lead")
        def risky() -> None:
            pass

        http.post("/submit/risky", json={"kwargs": {}})
        assert _work_once(tf) == 0

        view = tf.state_repo.list_views("risky")[0]
        assert view.record.obligation.status == ObligationStatus.awaiting_review
        obligation_id = str(view.record.obligation.id)

        refused = http.post(
            f"/obligations/{obligation_id}/review",
            json={"decision": "approved", "actor": "intern"},
        )
        assert refused.status_code == 403

        approved = http.post(
            f"/obligations/{obligation_id}/review",
            json={"decision": "approved", "actor": "lead"},
        )
        assert approved.status_code == 200
        record = tf.state_repo.list_views("risky")[0].record
        assert record.obligation.status == ObligationStatus.discharged
        assert record.last_attempt.review.decision == Decision.approved
        assert record.last_attempt.review.by == "lead"

    def test_failed_flow_parks_for_retry(self, tf, http):
        @tf.flow(max_retries=3)
        def flaky() -> None:
            raise RuntimeError("flaky")

        http.post("/submit/flaky", json={"kwargs": {}})
        assert _work_once(tf) == 1

        view = tf.state_repo.list_views("flaky")[0]
        assert view.state.status == ReportedStatus.pending
        assert view.record.last_attempt.error == "RuntimeError"
