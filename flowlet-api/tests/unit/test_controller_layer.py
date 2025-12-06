"""
Unit tests for the controller/API layer (FlowController).

These tests verify:
- API endpoint logic
- Request/response handling
- Error handling
- Controller isolation from repository implementation

Best practices:
- Controllers are tested with mocked repositories
- Focus on controller behavior, not repository details
- Test error cases (404s, validation errors, etc.)
"""
import uuid
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from flowlet.controllers import FlowInputModel, FlowRunModel, FlowSummaryModel, RunModel
from flowlet.interfaces.repository.models import (
    FlowRunSummary,
    FlowSummary,
    RunAttrModel,
    RunLogAttrModel,
    RunModel as DomainRunModel,
)


@pytest.mark.unit
class TestFlowController:
    """Test FlowController API endpoints."""

    def test_list_flows_empty(self, flow_controller, mock_query_repository):
        """Test listing flows when none exist."""
        mock_query_repository.list_flows.return_value = []

        result = flow_controller.list_flows()

        assert result == []
        mock_query_repository.list_flows.assert_called_once()

    def test_list_flows_with_data(self, flow_controller, mock_query_repository):
        """Test listing flows with existing flows."""
        # Mock repository response
        mock_flows = [
            FlowSummary(
                name="flow_1",
                status="success",
                started_at=datetime.now(UTC),
                ended_at=datetime.now(UTC),
            ),
            FlowSummary(
                name="flow_2",
                status="running",
                started_at=datetime.now(UTC),
                ended_at=None,
            ),
        ]
        mock_query_repository.list_flows.return_value = mock_flows

        result = flow_controller.list_flows()

        assert len(result) == 2
        assert isinstance(result[0], FlowSummaryModel)
        assert result[0].name == "flow_1"
        assert result[0].status == "success"
        assert result[1].name == "flow_2"
        assert result[1].status == "running"

    def test_run_flow_success(self, flow_controller, mock_query_repository):
        """Test executing a flow successfully."""
        # Mock a registered flow
        mock_flow_fn = Mock(return_value={"result": "success"})
        mock_query_repository.register.flows = {"test_flow": mock_flow_fn}
        mock_query_repository.register.list_flows.return_value = ["test_flow"]

        payload = FlowInputModel(kwargs={"x": 1, "y": 2})
        result = flow_controller.run_flow("test_flow", payload)

        # Verify flow was called with correct arguments
        mock_flow_fn.assert_called_once_with(x=1, y=2)

    def test_run_flow_not_found(self, flow_controller, mock_query_repository):
        """Test running a non-existent flow raises 404."""
        mock_query_repository.register.list_flows.return_value = []

        payload = FlowInputModel(kwargs={})

        with pytest.raises(HTTPException) as exc_info:
            flow_controller.run_flow("non_existent_flow", payload)

        assert exc_info.value.status_code == 404
        assert "Flow not found" in exc_info.value.detail

    def test_run_flow_with_no_kwargs(self, flow_controller, mock_query_repository):
        """Test running a flow with no arguments."""
        mock_flow_fn = Mock(return_value=None)
        mock_query_repository.register.flows = {"test_flow": mock_flow_fn}
        mock_query_repository.register.list_flows.return_value = ["test_flow"]

        payload = FlowInputModel()  # No kwargs
        flow_controller.run_flow("test_flow", payload)

        # Should call with empty kwargs
        mock_flow_fn.assert_called_once_with()

    def test_list_runs(self, flow_controller, mock_query_repository):
        """Test listing all runs."""
        run_id_1 = uuid.uuid4()
        run_id_2 = uuid.uuid4()

        mock_runs = [
            FlowRunSummary(
                name="flow_1",
                run_id=run_id_1,
                status="success",
                ended_at=datetime.now(UTC),
            ),
            FlowRunSummary(
                name="flow_2",
                run_id=run_id_2,
                status="running",
                ended_at=None,
            ),
        ]
        mock_query_repository.list_runs.return_value = mock_runs

        result = flow_controller.list_runs(offset=0, limit=50)

        assert len(result) == 2
        assert isinstance(result[0], FlowRunModel)
        assert result[0].run_id == run_id_1
        assert result[1].run_id == run_id_2

    def test_list_runs_with_pagination(self, flow_controller, mock_query_repository):
        """Test listing runs with custom pagination."""
        mock_query_repository.list_runs.return_value = []

        flow_controller.list_runs(offset=10, limit=20)

        mock_query_repository.list_runs.assert_called_once_with(offset=10, limit=20)

    def test_get_run_success(self, flow_controller, mock_query_repository):
        """Test getting a specific run by ID."""
        run_id = uuid.uuid4()
        mock_run = DomainRunModel(
            run=RunAttrModel(run_id=run_id, run_type="flow", name="test_flow"),
            logs=[
                RunLogAttrModel(
                    run_id=run_id,
                    status="success",
                    log="Finished",
                    timestamp=datetime.now(UTC),
                )
            ],
            parent=None,
            children=[],
        )
        mock_query_repository.get_run_by_id.return_value = mock_run

        result = flow_controller.get_run(run_id)

        assert isinstance(result, RunModel)
        assert result.run.run_id == run_id
        assert len(result.logs) == 1
        mock_query_repository.get_run_by_id.assert_called_once_with(run_id=run_id)

    def test_get_run_not_found(self, flow_controller, mock_query_repository):
        """Test getting a non-existent run raises 404."""
        non_existent_id = uuid.uuid4()
        mock_query_repository.get_run_by_id.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            flow_controller.get_run(non_existent_id)

        assert exc_info.value.status_code == 404
        assert "Run not found" in exc_info.value.detail

    def test_list_flow_runs(self, flow_controller, mock_query_repository):
        """Test listing runs for a specific flow."""
        run_id_1 = uuid.uuid4()
        run_id_2 = uuid.uuid4()

        mock_runs = [
            FlowRunSummary(
                name="test_flow",
                run_id=run_id_1,
                status="success",
                ended_at=datetime.now(UTC),
            ),
            FlowRunSummary(
                name="test_flow",
                run_id=run_id_2,
                status="failed",
                ended_at=datetime.now(UTC),
            ),
        ]
        mock_query_repository.list_runs_by_flow_name.return_value = mock_runs

        result = flow_controller.list_flow_runs("test_flow")

        assert len(result) == 2
        assert all(isinstance(run, FlowRunModel) for run in result)
        assert all(run.name == "test_flow" for run in result)
        mock_query_repository.list_runs_by_flow_name.assert_called_once_with(
            name="test_flow"
        )

    def test_list_flow_runs_empty(self, flow_controller, mock_query_repository):
        """Test listing runs for a flow with no runs."""
        mock_query_repository.list_runs_by_flow_name.return_value = []

        result = flow_controller.list_flow_runs("empty_flow")

        assert result == []


@pytest.mark.unit
class TestControllerModels:
    """Test controller Pydantic models."""

    def test_flow_input_model_default(self):
        """Test FlowInputModel with default values."""
        model = FlowInputModel()
        assert model.kwargs == {}

    def test_flow_input_model_with_kwargs(self):
        """Test FlowInputModel with custom kwargs."""
        model = FlowInputModel(kwargs={"x": 1, "y": 2})
        assert model.kwargs == {"x": 1, "y": 2}

    def test_flow_summary_model_direct_creation(self):
        """Test creating FlowSummaryModel directly."""
        now = datetime.now(UTC)
        model = FlowSummaryModel(
            name="test_flow",
            status="success",
            started_at=now,
            last_at=now,
        )

        assert model.name == "test_flow"
        assert model.status == "success"
        assert model.started_at is not None
        assert model.last_at is not None

    def test_flow_summary_computed_fields(self):
        """Test FlowSummaryModel computed fields."""
        now = datetime.now(UTC)
        model = FlowSummaryModel(
            name="test_flow",
            status="success",
            started_at=now,
            last_at=now,
        )

        # These should be computed
        assert model.finished_ago is not None
        assert model.duration is not None

    def test_flow_run_model_from_attrs(self):
        """Test creating FlowRunModel from attrs object."""
        run_id = uuid.uuid4()
        flow_run = FlowRunSummary(
            name="test_flow",
            run_id=run_id,
            status="success",
            ended_at=datetime.now(UTC),
        )

        model = FlowRunModel.model_validate(flow_run)

        assert model.name == "test_flow"
        assert model.run_id == run_id
        assert model.status == "success"
