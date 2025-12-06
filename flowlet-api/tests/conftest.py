"""
Pytest configuration and shared fixtures for flowlet tests.

This module provides:
- Database isolation using SQLite in-memory databases
- Factory setup for generating test data
- Mocked dependencies for each layer
- Common test utilities
"""
import uuid
from typing import Generator
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from flowlet.config import FlowletConfig
from flowlet.controllers import FlowController
from flowlet.database import Base, DatabaseSettings
from flowlet.register import FlowRegister
from flowlet.repositories.query import FlowQueryRepository
from flowlet.repositories.tracker import FlowTracker


# ============================================================================
# Database Fixtures - Isolated per test
# ============================================================================


@pytest.fixture(scope="function")
def db_engine():
    """
    Create an isolated in-memory SQLite database engine for each test.

    This ensures complete isolation between tests - each test gets a fresh database.
    """
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session_factory(db_engine):
    """
    Create a session factory bound to the test database engine.

    Returns a sessionmaker that creates sessions for the test database.
    """
    return sessionmaker(bind=db_engine, autoflush=False, autocommit=False)


@pytest.fixture(scope="function")
def db_session(db_session_factory) -> Generator[Session, None, None]:
    """
    Create a database session for a test.

    The session is automatically rolled back after the test to ensure isolation.
    For true isolation, use db_session_factory to create multiple independent sessions.
    """
    session = db_session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


# ============================================================================
# Configuration Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def database_settings(db_engine):
    """Mock DatabaseSettings with test database."""
    settings = Mock(spec=DatabaseSettings)
    settings.url = "sqlite:///:memory:"
    settings.engine = db_engine
    settings.db_session_factory = sessionmaker(bind=db_engine, autoflush=False, autocommit=False)
    return settings


@pytest.fixture(scope="function")
def flowlet_config(database_settings):
    """FlowletConfig with test database settings."""
    config = Mock(spec=FlowletConfig)
    config.database = database_settings
    return config


# ============================================================================
# Repository Layer Fixtures - Isolated from Domain Layer
# ============================================================================


@pytest.fixture(scope="function")
def flow_tracker(db_session_factory):
    """
    FlowTracker instance for testing write operations.

    This is isolated - it only depends on the database session factory.
    """
    return FlowTracker(db_session_factory=db_session_factory)


@pytest.fixture(scope="function")
def mock_flow_register():
    """
    Mock FlowRegister for testing repository layer in isolation.

    Use this when testing repositories without needing actual flow registration logic.
    """
    register = Mock(spec=FlowRegister)
    register.list_flows.return_value = []
    register.list_tasks.return_value = []
    register.flows = {}
    register.tasks = {}
    return register


@pytest.fixture(scope="function")
def flow_query_repository(db_session_factory, mock_flow_register):
    """
    FlowQueryRepository instance for testing read operations.

    Uses a mock register to isolate database querying from flow registration.
    """
    return FlowQueryRepository(
        register=mock_flow_register,
        db_session_factory=db_session_factory
    )


# ============================================================================
# Domain Layer Fixtures - Isolated from API Layer
# ============================================================================


@pytest.fixture(scope="function")
def flow_register(db_session_factory, flow_tracker):
    """
    Real FlowRegister instance for testing flow/task registration.

    Use this for integration tests of the domain layer.
    """
    return FlowRegister(
        db_session_factory=db_session_factory,
        tracker=flow_tracker
    )


# ============================================================================
# API Layer Fixtures - Isolated from Implementation
# ============================================================================


@pytest.fixture(scope="function")
def mock_query_repository():
    """
    Mock FlowQueryRepository for testing controllers in isolation.

    Use this when testing API controllers without database dependencies.
    """
    repository = Mock(spec=FlowQueryRepository)
    repository.list_flows.return_value = []
    repository.list_runs.return_value = []
    repository.list_runs_by_flow_name.return_value = []
    repository.get_run_by_id.return_value = None
    return repository


@pytest.fixture(scope="function")
def flow_controller(mock_query_repository):
    """
    FlowController instance for testing API endpoints.

    Uses mocked dependencies to isolate controller logic.
    """
    mock_query_repository.register = Mock(spec=FlowRegister)
    mock_query_repository.register.list_flows.return_value = []
    mock_query_repository.register.flows = {}

    return FlowController(query_repository=mock_query_repository)


# ============================================================================
# Test Data Helpers
# ============================================================================


@pytest.fixture(scope="function")
def sample_run_id():
    """Generate a consistent run ID for tests."""
    return uuid.UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture(scope="function")
def sample_flow_name():
    """Sample flow name for tests."""
    return "test_flow"


@pytest.fixture(scope="function")
def sample_task_name():
    """Sample task name for tests."""
    return "test_task"
