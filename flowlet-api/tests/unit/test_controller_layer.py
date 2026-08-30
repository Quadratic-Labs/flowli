"""Unit tests for the kernel controller: submission and query surfaces.

Authoring endpoints (synchronous execution, schemas, submit validation)
belong to layer-2 controllers and are tested in the taskflow package.
"""
from unittest.mock import AsyncMock, Mock
from uuid import uuid7

import pytest
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.api.models import FlowArguments, LogQueryRequest, RunQueryRequest


@pytest.mark.unit
class TestSubmitFlow:
    def test_no_queue_raises_503(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow("any_flow", FlowArguments(kwargs={}))
        assert exc.value.status_code == 503

    def test_success_enqueues_and_returns_job(self, controller_with_queue, mock_queue):
        resp = controller_with_queue.submit_flow(
            "any_flow", FlowArguments(kwargs={"x": 1})
        )
        assert mock_queue.enqueue.called
        job = mock_queue.enqueue.call_args.args[0]
        assert job.flow_name == "any_flow"
        assert job.kwargs == {"x": 1}
        assert resp.obligation_id == job.obligation_id

    def test_kernel_accepts_unregistered_flows(self, controller_with_queue):
        """The kernel knows no registry: external flows (CodeFlow tasks)
        submit freely; validation is a layer-2 hook."""
        resp = controller_with_queue.submit_flow(
            "codeflow.task", FlowArguments(kwargs={"intent": "x"})
        )
        assert resp.obligation_id is not None

    def test_submission_params_reach_the_job(self, controller_with_queue, mock_queue):
        controller_with_queue.submit_flow(
            "any_flow",
            FlowArguments(kwargs={}, timeout_seconds=1200, max_retries=5),
        )
        job = mock_queue.enqueue.call_args.args[0]
        assert job.timeout_seconds == 1200
        assert job.max_retries == 5

    def test_prepare_submission_hook_is_applied(self, mock_queue):
        def prepare(flow_name, payload):
            return payload.model_copy(update={"kwargs": {"validated": True}})

        controller = FlowController(queue=mock_queue, prepare_submission=prepare)
        controller.submit_flow("any_flow", FlowArguments(kwargs={"raw": 1}))
        job = mock_queue.enqueue.call_args.args[0]
        assert job.kwargs == {"validated": True}

    def test_hook_exceptions_surface(self, mock_queue):
        def refuse(flow_name, payload):
            raise HTTPException(status_code=404, detail="Flow not found")

        controller = FlowController(queue=mock_queue, prepare_submission=refuse)
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow("ghost", FlowArguments(kwargs={}))
        assert exc.value.status_code == 404

    def test_enqueue_error_raises_500(self):
        queue = Mock()
        queue.enqueue = Mock(side_effect=RuntimeError("queue down"))
        controller = FlowController(queue=queue)
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow("any_flow", FlowArguments(kwargs={}))
        assert exc.value.status_code == 500


@pytest.mark.unit
class TestQueryRuns:
    async def test_no_querier_raises_503(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            await controller.query_runs(RunQueryRequest())
        assert exc.value.status_code == 503

    async def test_delegates_to_querier(self, controller_with_querier, mock_querier):
        mock_querier.list_recent_states = AsyncMock(return_value=[])
        result = await controller_with_querier.query_runs(RunQueryRequest())
        assert result == []
        assert mock_querier.list_recent_states.called

    async def test_querier_exception_raises_400(
        self, controller_with_querier, mock_querier
    ):
        mock_querier.list_recent_states = AsyncMock(side_effect=RuntimeError("db"))
        with pytest.raises(HTTPException) as exc:
            await controller_with_querier.query_runs(RunQueryRequest())
        assert exc.value.status_code == 400


@pytest.mark.unit
class TestQueryLogs:
    def test_no_querier_raises_503(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            controller.query_logs(LogQueryRequest(flow_name="f", obligation_id=uuid7()))
        assert exc.value.status_code == 503

    def test_value_error_raises_404(self, controller_with_querier, mock_querier):
        mock_querier.get_run = Mock(side_effect=ValueError("not found"))
        with pytest.raises(HTTPException) as exc:
            controller_with_querier.query_logs(
                LogQueryRequest(flow_name="f", obligation_id=uuid7())
            )
        assert exc.value.status_code == 404

    def test_generic_exception_raises_400(
        self, controller_with_querier, mock_querier
    ):
        mock_querier.get_run = Mock(side_effect=RuntimeError("boom"))
        with pytest.raises(HTTPException) as exc:
            controller_with_querier.query_logs(
                LogQueryRequest(flow_name="f", obligation_id=uuid7())
            )
        assert exc.value.status_code == 400

    def test_dto_validation_failure_raises_500_not_404(
        self, controller_with_querier, mock_querier
    ):
        # ValidationError extends ValueError: a response-contract violation
        # must surface as a server bug, never masquerade as "run not found".
        mock_querier.get_run = Mock(return_value={"span_id": "not-a-valid-run"})
        with pytest.raises(HTTPException) as exc:
            controller_with_querier.query_logs(
                LogQueryRequest(flow_name="f", obligation_id=uuid7())
            )
        assert exc.value.status_code == 500
