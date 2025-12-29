"""
Unit tests for the repository layer (FlowTracker and FlowQueryRepository).

These tests verify:
- Write operations (FlowTracker)
- Read operations (FlowQueryRepository)
- Repository isolation from domain logic
- Proper session management

Best practices:
- Repository tests use real database but mock domain dependencies
- Each test has an isolated database
- Focus on repository behavior, not ORM details
"""
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from flowlet.database import Run, RunLink, RunLog
from flowlet.models import RunAttrModel, RunLogAttrModel


@pytest.mark.unit
class TestFlowTracker:
    """Test FlowTracker write operations."""

    def test_create_run(self, flow_tracker, db_session):
        """Test creating a new run record."""
        run_id = uuid.uuid4()
        run_data = RunAttrModel(
            run_id=run_id,
            run_type="flow",
            name="test_flow"
        )

        result = flow_tracker.create_run(run_data, db=db_session)

        # Verify the result
        assert result.run_id == run_id
        assert result.run_type == "flow"
        assert result.name == "test_flow"

        # Verify database record
        db_run = db_session.query(Run).filter_by(run_id=run_id).first()
        assert db_run is not None
        assert db_run.run_id == run_id
        assert db_run.run_type == "flow"

    def test_create_run_without_session(self, flow_tracker):
        """Test creating a run without passing a session (uses session factory)."""
        run_id = uuid.uuid4()
        run_data = RunAttrModel(
            run_id=run_id,
            run_type="flow",
            name="test_flow"
        )

        # Should create its own session
        result = flow_tracker.create_run(run_data)

        assert result.run_id == run_id
        assert result.run_type == "flow"

    def test_link_runs(self, flow_tracker, db_session):
        """Test linking parent and child runs."""
        # Create parent and child runs
        parent_id = uuid.uuid4()
        child_id = uuid.uuid4()

        parent_data = RunAttrModel(run_id=parent_id, run_type="flow", name="parent")
        child_data = RunAttrModel(run_id=child_id, run_type="task", name="child")

        flow_tracker.create_run(parent_data, db=db_session)
        flow_tracker.create_run(child_data, db=db_session)

        # Link them
        flow_tracker.link_runs(parent_data, child_data, db=db_session)

        # Verify link exists
        link = db_session.query(RunLink).filter_by(
            parent_run_id=parent_id,
            child_run_id=child_id
        ).first()
        assert link is not None
        assert link.parent_run_id == parent_id
        assert link.child_run_id == child_id

    def test_link_runs_with_none_parent(self, flow_tracker, db_session):
        """Test that linking with None parent does nothing."""
        child_data = RunAttrModel(
            run_id=uuid.uuid4(),
            run_type="task",
            name="child"
        )

        # Should not raise an error
        flow_tracker.link_runs(None, child_data, db=db_session)

        # Verify no links created
        links = db_session.query(RunLink).all()
        assert len(links) == 0

    def test_link_runs_with_none_child(self, flow_tracker, db_session):
        """Test that linking with None child does nothing."""
        parent_data = RunAttrModel(
            run_id=uuid.uuid4(),
            run_type="flow",
            name="parent"
        )

        # Should not raise an error
        flow_tracker.link_runs(parent_data, None, db=db_session)

        # Verify no links created
        links = db_session.query(RunLink).all()
        assert len(links) == 0

    def test_log(self, flow_tracker, db_session):
        """Test adding a log entry to a run."""
        # Create a run
        run_id = uuid.uuid4()
        run_data = RunAttrModel(run_id=run_id, run_type="flow", name="test_flow")
        flow_tracker.create_run(run_data, db=db_session)

        # Add a log
        log_id = uuid.uuid4()
        timestamp = datetime.now(UTC)
        log_data = RunLogAttrModel(
            log_id=log_id,
            run_id=run_id,
            timestamp=timestamp,
            status="running",
            log="Flow started"
        )

        result = flow_tracker.log(log_data, db=db_session)

        # Verify result
        assert result.log_id == log_id
        assert result.run_id == run_id
        assert result.status == "running"

        # Verify database record
        db_log = db_session.query(RunLog).filter_by(log_id=log_id).first()
        assert db_log is not None
        assert db_log.run_id == run_id
        assert db_log.status == "running"
        assert db_log.log == "Flow started"

    def test_log_without_session(self, flow_tracker, db_session):
        """Test logging without passing a session."""
        # Create a run first
        run_id = uuid.uuid4()
        run_data = RunAttrModel(run_id=run_id, run_type="flow", name="test_flow")
        flow_tracker.create_run(run_data, db=db_session)

        # Add log without session
        log_data = RunLogAttrModel(
            run_id=run_id,
            status="running",
            log="Test log"
        )

        result = flow_tracker.log(log_data)
        assert result.run_id == run_id


@pytest.mark.unit
class TestFlowQueryRepository:
    """Test FlowQueryRepository read operations."""

    def test_list_flows_empty(self, flow_query_repository, mock_flow_register):
        """Test listing flows when none exist."""
        mock_flow_register.list_flows.return_value = []

        flows = flow_query_repository.list_flows()

        assert flows == []

    def test_list_flows_without_runs(self, flow_query_repository, mock_flow_register):
        """Test listing registered flows that have no runs."""
        mock_flow_register.list_flows.return_value = ["flow_1", "flow_2"]

        flows = flow_query_repository.list_flows()

        assert len(flows) == 2
        assert flows[0].name == "flow_1"
        assert flows[0].status is None
        assert flows[1].name == "flow_2"
        assert flows[1].status is None

    def test_list_flows_with_runs(
        self, flow_query_repository, mock_flow_register, db_session, flow_tracker
    ):
        """Test listing flows with execution history."""
        # Register a flow
        mock_flow_register.list_flows.return_value = ["test_flow"]

        # Create run and logs
        run_id = uuid.uuid4()
        run_data = RunAttrModel(run_id=run_id, run_type="flow", name="test_flow")
        flow_tracker.create_run(run_data, db=db_session)

        start_time = datetime.now(UTC)
        end_time = start_time + timedelta(seconds=5)

        log1 = RunLogAttrModel(
            run_id=run_id,
            timestamp=start_time,
            status="running",
            log="Started"
        )
        log2 = RunLogAttrModel(
            run_id=run_id,
            timestamp=end_time,
            status="success",
            log="Finished"
        )
        flow_tracker.log(log1, db=db_session)
        flow_tracker.log(log2, db=db_session)

        # Query flows
        flows = flow_query_repository.list_flows(db=db_session)

        assert len(flows) == 1
        assert flows[0].name == "test_flow"
        assert flows[0].status == "success"
        # Note: SQLite doesn't preserve timezone, so we compare without timezone
        assert flows[0].started_at.replace(tzinfo=None) == start_time.replace(tzinfo=None)
        assert flows[0].ended_at.replace(tzinfo=None) == end_time.replace(tzinfo=None)

    def test_list_runs(self, flow_query_repository, db_session, flow_tracker):
        """Test listing all runs."""
        # Create multiple runs
        for i in range(3):
            run_id = uuid.uuid4()
            run_data = RunAttrModel(
                run_id=run_id,
                run_type="flow",
                name=f"flow_{i}"
            )
            flow_tracker.create_run(run_data, db=db_session)

            log_data = RunLogAttrModel(
                run_id=run_id,
                status="success",
                log="Finished"
            )
            flow_tracker.log(log_data, db=db_session)

        # Query runs
        runs = flow_query_repository.list_runs(limit=10, db=db_session)

        assert len(runs) == 3

    def test_list_runs_pagination(self, flow_query_repository, db_session, flow_tracker):
        """Test pagination when listing runs."""
        # Create 5 runs
        for i in range(5):
            run_id = uuid.uuid4()
            run_data = RunAttrModel(
                run_id=run_id,
                run_type="flow",
                name=f"flow_{i}"
            )
            flow_tracker.create_run(run_data, db=db_session)

            log_data = RunLogAttrModel(run_id=run_id, status="success", log="Done")
            flow_tracker.log(log_data, db=db_session)

        # Get first page
        page1 = flow_query_repository.list_runs(offset=0, limit=2, db=db_session)
        assert len(page1) == 2

        # Get second page
        page2 = flow_query_repository.list_runs(offset=2, limit=2, db=db_session)
        assert len(page2) == 2

        # Verify different runs
        page1_ids = {run.run_id for run in page1}
        page2_ids = {run.run_id for run in page2}
        assert page1_ids.isdisjoint(page2_ids)

    def test_list_runs_by_flow_name(
        self, flow_query_repository, db_session, flow_tracker
    ):
        """Test listing runs for a specific flow."""
        # Create runs for different flows
        target_flow_runs = []
        for i in range(2):
            run_id = uuid.uuid4()
            run_data = RunAttrModel(
                run_id=run_id,
                run_type="flow",
                name="target_flow"
            )
            flow_tracker.create_run(run_data, db=db_session)
            log_data = RunLogAttrModel(run_id=run_id, status="success", log="Done")
            flow_tracker.log(log_data, db=db_session)
            target_flow_runs.append(run_id)

        # Create runs for other flow
        other_run_id = uuid.uuid4()
        other_data = RunAttrModel(
            run_id=other_run_id,
            run_type="flow",
            name="other_flow"
        )
        flow_tracker.create_run(other_data, db=db_session)
        log_data = RunLogAttrModel(run_id=other_run_id, status="success", log="Done")
        flow_tracker.log(log_data, db=db_session)

        # Query runs for target flow
        runs = flow_query_repository.list_runs_by_flow_name("target_flow", db=db_session)

        assert len(runs) == 2
        assert all(run.name == "target_flow" for run in runs)

    def test_get_run_by_id(self, flow_query_repository, db_session, flow_tracker):
        """Test getting a specific run by ID."""
        # Create a run
        run_id = uuid.uuid4()
        run_data = RunAttrModel(run_id=run_id, run_type="flow", name="test_flow")
        flow_tracker.create_run(run_data, db=db_session)

        # Add logs
        log_data = RunLogAttrModel(run_id=run_id, status="running", log="Started")
        flow_tracker.log(log_data, db=db_session)

        # Query by ID
        run = flow_query_repository.get_run_by_id(run_id, db=db_session)

        assert run is not None
        assert run.run.run_id == run_id
        assert run.run.name == "test_flow"
        assert len(run.logs) == 1

    def test_get_run_by_id_not_found(self, flow_query_repository, db_session):
        """Test getting a non-existent run."""
        non_existent_id = uuid.uuid4()
        run = flow_query_repository.get_run_by_id(non_existent_id, db=db_session)

        assert run is None

    def test_get_run_with_children(
        self, flow_query_repository, db_session, flow_tracker
    ):
        """Test getting a run with child tasks."""
        # Create parent flow
        parent_id = uuid.uuid4()
        parent_data = RunAttrModel(
            run_id=parent_id,
            run_type="flow",
            name="parent_flow"
        )
        flow_tracker.create_run(parent_data, db=db_session)

        # Create child tasks
        child_ids = []
        for i in range(2):
            child_id = uuid.uuid4()
            child_data = RunAttrModel(
                run_id=child_id,
                run_type="task",
                name=f"task_{i}"
            )
            flow_tracker.create_run(child_data, db=db_session)
            flow_tracker.link_runs(parent_data, child_data, db=db_session)
            child_ids.append(child_id)

        # Query parent run
        run = flow_query_repository.get_run_by_id(parent_id, db=db_session)

        assert run is not None
        assert len(run.children) == 2
        retrieved_child_ids = {child.run.run_id for child in run.children}
        assert retrieved_child_ids == set(child_ids)
