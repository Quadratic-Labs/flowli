"""
Unit tests for the database layer (ORM models).

These tests verify:
- ORM model behavior
- Database constraints
- Model methods and properties
- Relationships between models

Best practices:
- Each test uses an isolated in-memory database
- Tests are completely independent
- No mocking needed - we test actual database behavior
- Uses SQLAlchemy 2.1 unified query syntax
"""
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from flowlet.database import Run, RunLink, RunLog


@pytest.mark.unit
class TestRunModel:
    """Test the Run ORM model."""

    def test_create_flow_run(self, db_session):
        """Test creating a flow run record."""
        run_id = uuid.uuid4()
        run = Run(
            run_id=run_id,
            run_type="flow",
            name="test_flow"
        )
        db_session.add(run)
        db_session.commit()

        # Verify record was created
        stmt = select(Run).where(Run.run_id == run_id)
        retrieved = db_session.scalars(stmt).first()
        assert retrieved is not None
        assert retrieved.run_id == run_id
        assert retrieved.run_type == "flow"
        assert retrieved.name == "test_flow"

    def test_create_task_run(self, db_session):
        """Test creating a task run record."""
        run_id = uuid.uuid4()
        run = Run(
            run_id=run_id,
            run_type="task",
            name="test_task"
        )
        db_session.add(run)
        db_session.commit()

        # Verify record was created
        stmt = select(Run).where(Run.run_id == run_id)
        retrieved = db_session.scalars(stmt).first()
        assert retrieved is not None
        assert retrieved.run_type == "task"
        assert retrieved.name == "test_task"

    def test_run_to_dict(self, db_session):
        """Test Run.to_dict() method."""
        run_id = uuid.uuid4()
        run = Run(
            run_id=run_id,
            run_type="flow",
            name="test_flow"
        )
        db_session.add(run)
        db_session.commit()

        run_dict = run.to_dict()
        # Note: to_dict() returns the column keys as strings, but values keep their types
        assert run_dict["run_id"] == run_id  # UUID, not string
        assert run_dict["run_type"] == "flow"
        assert run_dict["name"] == "test_flow"

    def test_run_repr(self, db_session):
        """Test Run.__repr__() method."""
        run = Run(
            run_id=uuid.uuid4(),
            run_type="flow",
            name="test_flow"
        )
        db_session.add(run)
        db_session.commit()

        repr_str = repr(run)
        assert "Run(" in repr_str
        assert "run_id=" in repr_str
        assert "run_type=flow" in repr_str
        assert "name=test_flow" in repr_str


@pytest.mark.unit
class TestRunLogModel:
    """Test the RunLog ORM model."""

    def test_create_run_log(self, db_session):
        """Test creating a run log record."""
        # First create a run
        run_id = uuid.uuid4()
        run = Run(run_id=run_id, run_type="flow", name="test_flow")
        db_session.add(run)
        db_session.commit()

        # Create a log for the run
        log_id = uuid.uuid4()
        timestamp = datetime.now(UTC)
        log = RunLog(
            log_id=log_id,
            run_id=run_id,
            timestamp=timestamp,
            status="running",
            log="Flow started"
        )
        db_session.add(log)
        db_session.commit()

        # Verify log was created
        stmt = select(RunLog).where(RunLog.log_id == log_id)
        retrieved = db_session.scalars(stmt).first()
        assert retrieved is not None
        assert retrieved.run_id == run_id
        assert retrieved.status == "running"
        assert retrieved.log == "Flow started"

    def test_run_log_relationship(self, db_session):
        """Test the relationship between Run and RunLog."""
        # Create a run
        run_id = uuid.uuid4()
        run = Run(run_id=run_id, run_type="flow", name="test_flow")
        db_session.add(run)
        db_session.commit()

        # Create logs for the run
        log1 = RunLog(
            log_id=uuid.uuid4(),
            run_id=run_id,
            timestamp=datetime.now(UTC),
            status="running",
            log="Started"
        )
        log2 = RunLog(
            log_id=uuid.uuid4(),
            run_id=run_id,
            timestamp=datetime.now(UTC),
            status="success",
            log="Finished"
        )
        db_session.add_all([log1, log2])
        db_session.commit()

        # Verify relationship
        stmt = select(Run).where(Run.run_id == run_id)
        retrieved_run = db_session.scalars(stmt).first()
        assert len(retrieved_run.logs) == 2
        assert all(log.run_id == run_id for log in retrieved_run.logs)

    def test_run_log_to_dict(self, db_session):
        """Test RunLog.to_dict() method."""
        run_id = uuid.uuid4()
        run = Run(run_id=run_id, run_type="flow", name="test_flow")
        db_session.add(run)

        log_id = uuid.uuid4()
        timestamp = datetime.now(UTC)
        log = RunLog(
            log_id=log_id,
            run_id=run_id,
            timestamp=timestamp,
            status="running",
            log="Test log"
        )
        db_session.add(log)
        db_session.commit()

        log_dict = log.to_dict()
        # Note: to_dict() returns UUIDs, not strings
        assert log_dict["log_id"] == log_id
        assert log_dict["run_id"] == run_id
        assert log_dict["status"] == "running"
        assert log_dict["log"] == "Test log"


@pytest.mark.unit
class TestRunLinkModel:
    """Test the RunLink ORM model."""

    def test_create_run_link(self, db_session):
        """Test creating a link between parent and child runs."""
        # Create parent and child runs
        parent_id = uuid.uuid4()
        child_id = uuid.uuid4()

        parent = Run(run_id=parent_id, run_type="flow", name="parent_flow")
        child = Run(run_id=child_id, run_type="task", name="child_task")
        db_session.add_all([parent, child])
        db_session.commit()

        # Create link
        link_id = uuid.uuid4()
        link = RunLink(
            link_id=link_id,
            parent_run_id=parent_id,
            child_run_id=child_id
        )
        db_session.add(link)
        db_session.commit()

        # Verify link was created
        stmt = select(RunLink).where(RunLink.link_id == link_id)
        retrieved = db_session.scalars(stmt).first()
        assert retrieved is not None
        assert retrieved.parent_run_id == parent_id
        assert retrieved.child_run_id == child_id

    def test_run_link_relationships(self, db_session):
        """Test the relationships through RunLink."""
        # Create parent and child runs
        parent_id = uuid.uuid4()
        child_id = uuid.uuid4()

        parent = Run(run_id=parent_id, run_type="flow", name="parent_flow")
        child = Run(run_id=child_id, run_type="task", name="child_task")
        db_session.add_all([parent, child])
        db_session.commit()

        # Create link
        link = RunLink(
            link_id=uuid.uuid4(),
            parent_run_id=parent_id,
            child_run_id=child_id
        )
        db_session.add(link)
        db_session.commit()

        # Verify relationships through the link
        stmt = select(RunLink)
        retrieved_link = db_session.scalars(stmt).first()
        assert retrieved_link.parent_run.run_id == parent_id
        assert retrieved_link.child_run.run_id == child_id

    def test_multiple_children(self, db_session):
        """Test a parent run with multiple child tasks."""
        # Create parent flow
        parent_id = uuid.uuid4()
        parent = Run(run_id=parent_id, run_type="flow", name="parent_flow")
        db_session.add(parent)

        # Create multiple child tasks
        child_ids = [uuid.uuid4() for _ in range(3)]
        for idx, child_id in enumerate(child_ids):
            child = Run(run_id=child_id, run_type="task", name=f"task_{idx}")
            db_session.add(child)

            link = RunLink(
                link_id=uuid.uuid4(),
                parent_run_id=parent_id,
                child_run_id=child_id
            )
            db_session.add(link)

        db_session.commit()

        # Verify all links
        stmt = select(RunLink).where(RunLink.parent_run_id == parent_id)
        links = db_session.scalars(stmt).all()
        assert len(links) == 3
        assert all(link.parent_run_id == parent_id for link in links)


@pytest.mark.unit
class TestBaseModelHelpers:
    """Test Base model helper methods."""

    def test_from_attrs(self, db_session):
        """Test creating ORM instance from attrs object."""
        from flowlet.models import RunAttrModel

        # Create an attrs model
        attrs_run = RunAttrModel(
            run_id=uuid.uuid4(),
            run_type="flow",
            name="test_flow"
        )

        # Create ORM instance from attrs
        run = Run.from_attrs(attrs_run)
        db_session.add(run)
        db_session.commit()

        # Verify
        assert run.run_id == attrs_run.run_id
        assert run.run_type == attrs_run.run_type
        assert run.name == attrs_run.name

    def test_update_from_attrs(self, db_session):
        """Test updating ORM instance from attrs object."""
        from flowlet.models import RunAttrModel

        # Create initial run
        run_id = uuid.uuid4()
        run = Run(run_id=run_id, run_type="flow", name="original_name")
        db_session.add(run)
        db_session.commit()

        # Create attrs model with updates
        attrs_run = RunAttrModel(
            run_id=run_id,
            run_type="flow",
            name="updated_name"
        )

        # Update from attrs
        run.update_from_attrs(attrs_run)
        db_session.commit()

        # Verify update
        stmt = select(Run).where(Run.run_id == run_id)
        retrieved = db_session.scalars(stmt).first()
        assert retrieved.name == "updated_name"
