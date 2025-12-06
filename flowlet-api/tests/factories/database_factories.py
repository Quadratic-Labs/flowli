"""
Factory Boy factories for SQLAlchemy ORM models.

These factories generate database records (ORM objects) for testing
database layer operations.
"""
import uuid
from datetime import UTC, datetime

import factory
from factory.alchemy import SQLAlchemyModelFactory

from flowlet.database import Run, RunLink, RunLog


class RunFactory(SQLAlchemyModelFactory):
    """Factory for creating Run ORM instances."""

    class Meta:
        model = Run
        sqlalchemy_session = None  # Will be set by the fixture
        sqlalchemy_session_persistence = "commit"

    run_id = factory.LazyFunction(uuid.uuid4)
    run_type = "flow"
    name = factory.Sequence(lambda n: f"test_flow_{n}")


class RunLogFactory(SQLAlchemyModelFactory):
    """Factory for creating RunLog ORM instances."""

    class Meta:
        model = RunLog
        sqlalchemy_session = None  # Will be set by the fixture
        sqlalchemy_session_persistence = "commit"

    log_id = factory.LazyFunction(uuid.uuid4)
    run_id = factory.LazyFunction(uuid.uuid4)
    timestamp = factory.LazyFunction(lambda: datetime.now(UTC))
    status = "running"
    log = ""


class RunLinkFactory(SQLAlchemyModelFactory):
    """Factory for creating RunLink ORM instances."""

    class Meta:
        model = RunLink
        sqlalchemy_session = None  # Will be set by the fixture
        sqlalchemy_session_persistence = "commit"

    link_id = factory.LazyFunction(uuid.uuid4)
    parent_run_id = factory.LazyFunction(uuid.uuid4)
    child_run_id = factory.LazyFunction(uuid.uuid4)
