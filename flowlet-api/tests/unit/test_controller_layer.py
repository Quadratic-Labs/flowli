"""
Unit tests for api/controller.py — FlowController HTTP handlers.

Strategy:
- Execution endpoints (run_flow, submit_flow): use a real Registry to verify
  schema validation and 404/503 paths.
- Query endpoints (query_runs, query_logs): inject mock_querier / mock_queue
  via fixtures so the domain layer is fully isolated.
"""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.api.models import (
    FlowArguments,
    LogQueryRequest,
    RunQueryRequest,
)
from flowlet.models import RunStatus, RunType
from flowlet.registry import Registry
from uuid import uuid7


@pytest.mark.unit
class TestListFlowsWithSchemas:
    def test_empty_registry_returns_empty_list(self, controller: FlowController):
        assert controller.list_flows_with_schemas() == []

    def test_registered_flows_appear(self, registry_with_flows: Registry):
        ctrl  = FlowController(registry=registry_with_flows)
        names = [f["name"] for f in ctrl.list_flows_with_schemas()]
        assert "my_flow" in names
        assert "other_flow" in names

    def test_typed_flow_has_schema_with_params(self, registry_with_flows: Registry):
        ctrl      = FlowController(registry=registry_with_flows)
        my_flow   = next(f for f in ctrl.list_flows_with_schemas() if f["name"] == "my_flow")
        assert my_flow["has_schema"] is True
        param_names = [p["name"] for p in my_flow["parameters"]]
        assert "x" in param_names
        assert "y" in param_names


@pytest.mark.unit
class TestGetFlowSchema:
    def test_unknown_flow_raises_404(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            controller.get_flow_schema("no_such_flow")
        assert exc.value.status_code == 404

    def test_known_flow_returns_schema(self, registry_with_flows: Registry):
        ctrl   = FlowController(registry=registry_with_flows)
        result = ctrl.get_flow_schema("my_flow")
        assert result["has_schema"] is True
        assert result["flow_name"] == "my_flow"
        assert "json_schema" in result

    def test_schema_response_contains_required_keys(self, registry_with_flows: Registry):
        ctrl   = FlowController(registry=registry_with_flows)
        result = ctrl.get_flow_schema("my_flow")
        assert "flow_name" in result
        assert "has_schema" in result


@pytest.mark.unit
class TestRunFlow:
    def test_unknown_flow_raises_404(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            controller.run_flow("ghost_flow", FlowArguments())
        assert exc.value.status_code == 404

    def test_known_flow_called_with_kwargs(self, registry: Registry):
        results: list = []

        def my_flow(x: int, y: int) -> None:
            results.append((x, y))

        registry.register_flow(my_flow, name="my_flow")
        FlowController(registry=registry).run_flow(
            "my_flow", FlowArguments(kwargs={"x": 3, "y": 7})
        )
        assert results == [(3, 7)]

    def test_schema_validation_raises_422(self, registry: Registry):
        def typed_flow(x: int) -> None:
            pass

        registry.register_flow(typed_flow, name="typed_flow")
        with pytest.raises(HTTPException) as exc:
            FlowController(registry=registry).run_flow(
                "typed_flow", FlowArguments(kwargs={"x": "not_an_int"})
            )
        assert exc.value.status_code == 422


@pytest.mark.unit
class TestSubmitFlow:
    def test_no_queue_raises_503(self, controller: FlowController, registry: Registry):
        def my_flow() -> None:
            pass

        registry.register_flow(my_flow, name="my_flow")
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow("my_flow", FlowArguments())
        assert exc.value.status_code == 503

    def test_unknown_flow_raises_404(self, registry: Registry, mock_queue: Mock):
        ctrl = FlowController(registry=registry, queue=mock_queue)
        with pytest.raises(HTTPException) as exc:
            ctrl.submit_flow("ghost_flow", FlowArguments())
        assert exc.value.status_code == 404

    def test_success_enqueues_and_returns_job(self, registry: Registry, mock_queue: Mock):
        def my_flow(x: int) -> None:
            pass

        registry.register_flow(my_flow, name="my_flow")
        ctrl     = FlowController(registry=registry, queue=mock_queue)
        response = ctrl.submit_flow("my_flow", FlowArguments(kwargs={"x": 1}))

        mock_queue.enqueue.assert_called_once()
        assert response.status == RunStatus.pending
        assert isinstance(response.job_id, UUID)

    def test_enqueue_error_raises_500(self, registry: Registry):
        def my_flow() -> None:
            pass

        registry.register_flow(my_flow, name="my_flow")
        bad_queue = Mock()
        bad_queue.enqueue = Mock(side_effect=RuntimeError("broker down"))
        with pytest.raises(HTTPException) as exc:
            FlowController(registry=registry, queue=bad_queue).submit_flow(
                "my_flow", FlowArguments()
            )
        assert exc.value.status_code == 500


@pytest.mark.unit
class TestQueryRuns:
    async def test_no_querier_raises_503(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            await controller.query_runs(RunQueryRequest(names=None, last_n=5))
        assert exc.value.status_code == 503

    async def test_delegates_to_querier(
        self,
        controller_with_querier: FlowController,
        mock_querier: AsyncMock,
        make_run_state,
    ):
        state = make_run_state()
        mock_querier.list_recent_states.return_value = [state]

        result = await controller_with_querier.query_runs(
            RunQueryRequest(names=["test_flow"], last_n=3)
        )

        mock_querier.list_recent_states.assert_called_once_with(
            flow_names=["test_flow"], last_n=3
        )
        assert len(result) == 1

    async def test_querier_exception_raises_400(
        self, controller_with_querier: FlowController, mock_querier: AsyncMock
    ):
        mock_querier.list_recent_states.side_effect = RuntimeError("db unavailable")
        with pytest.raises(HTTPException) as exc:
            await controller_with_querier.query_runs(RunQueryRequest(names=None, last_n=5))
        assert exc.value.status_code == 400


@pytest.mark.unit
class TestQueryLogs:
    def test_no_querier_raises_503(self, controller: FlowController):
        with pytest.raises(HTTPException) as exc:
            controller.query_logs(
                LogQueryRequest(flow_name="my_flow", run_id=uuid7(), with_logs=True)
            )
        assert exc.value.status_code == 503

    def test_value_error_raises_404(
        self, controller_with_querier: FlowController, mock_querier: AsyncMock
    ):
        mock_querier.get_run.side_effect = ValueError("run not found")
        with pytest.raises(HTTPException) as exc:
            controller_with_querier.query_logs(
                LogQueryRequest(flow_name="my_flow", run_id=uuid7(), with_logs=True)
            )
        assert exc.value.status_code == 404

    def test_generic_exception_raises_400(
        self, controller_with_querier: FlowController, mock_querier: AsyncMock
    ):
        mock_querier.get_run.side_effect = RuntimeError("storage offline")
        with pytest.raises(HTTPException) as exc:
            controller_with_querier.query_logs(
                LogQueryRequest(flow_name="my_flow", run_id=uuid7(), with_logs=True)
            )
        assert exc.value.status_code == 400

    def test_success_returns_run_dto(
        self, controller_with_querier: FlowController, mock_querier: AsyncMock
    ):
        span_id = "00f067aa0ba902b7"
        run_id  = uuid7()
        now     = datetime.now(UTC)
        mock_querier.get_run.return_value = {
            "span_id":   span_id,
            "span_name": "test_flow",
            "span_type": RunType.flow,
            "status":    RunStatus.completed,
            "start_ts":  now,
            "end_ts":    now,
            "children":  [],
            "logs":      [],
        }

        result = controller_with_querier.query_logs(
            LogQueryRequest(flow_name="test_flow", run_id=run_id, with_logs=True)
        )

        assert result.span_id == span_id
        assert result.status == RunStatus.completed
