# Flowlet Test Suite

This directory contains the comprehensive test suite for the flowlet-api package, following best practices for unit testing with proper layer isolation.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── factories/               # Factory Boy factories for test data
│   ├── __init__.py
│   ├── database_factories.py    # ORM model factories
│   └── model_factories.py       # Domain model factories
├── unit/                    # Unit tests
│   ├── test_database_layer.py       # Database/ORM tests
│   ├── test_repository_layer.py     # Repository layer tests
│   └── test_controller_layer.py     # API/Controller tests
└── integration/             # Integration tests (future)
```

## Architecture & Isolation

The tests follow the application's layered architecture with proper isolation:

### Database Layer Tests
- Test ORM models (Run, RunLog, RunLink)
- Use isolated in-memory SQLite databases
- No mocking needed - test actual database behavior
- Each test gets a fresh database

### Repository Layer Tests
- Test FlowTracker (write operations) and FlowQueryRepository (read operations)
- Use real database but mock domain layer dependencies (FlowRegister)
- Verify repository behavior in isolation from domain logic
- Focus on data access patterns

### Controller/API Layer Tests
- Test FlowController endpoint logic
- Mock all repository dependencies
- Test request/response handling and error cases
- Complete isolation from database

## Running Tests

### Run all tests
```bash
cd /home/tzamojski/Perso/Orchestration/Flowlet/flowlet/flowlet-api
pytest
```

### Run specific test categories
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only async tests
pytest -m asyncio
```

### Run with coverage
```bash
pytest --cov=flowlet --cov-report=html --cov-report=term
```

### Run specific test file
```bash
pytest tests/unit/test_database_layer.py
pytest tests/unit/test_repository_layer.py
pytest tests/unit/test_controller_layer.py
```

### Run specific test
```bash
pytest tests/unit/test_database_layer.py::TestRunModel::test_create_flow_run
```

### Run with verbose output
```bash
pytest -v
pytest -vv  # Extra verbose
```

## Test Fixtures

The test suite provides comprehensive fixtures for all layers:

### Database Fixtures
- `db_engine`: Isolated in-memory SQLite engine per test
- `db_session_factory`: Session factory for the test database
- `db_session`: Database session with automatic rollback

### Repository Fixtures
- `flow_tracker`: FlowTracker instance for testing write operations
- `flow_query_repository`: FlowQueryRepository for testing read operations
- `mock_flow_register`: Mocked FlowRegister for repository isolation

### Controller Fixtures
- `flow_controller`: FlowController for testing API endpoints
- `mock_query_repository`: Mocked query repository for controller isolation

### Configuration Fixtures
- `database_settings`: Mock DatabaseSettings with test database
- `flowlet_config`: FlowletConfig with test settings

## Factories

Factories provide clean test data generation using Factory Boy:

### Database ORM Factories
```python
from tests.factories import RunFactory, RunLogFactory, RunLinkFactory

# Create a run
run = RunFactory(name="my_flow", run_type="flow")

# Create with custom values
log = RunLogFactory(obligation_id=run.obligation_id, status="success")
```

### Domain Model Factories
```python
from tests.factories import RunAttrModelFactory, FlowSummaryFactory

# Create domain models
run_attr = RunAttrModelFactory(name="test_flow")
flow_summary = FlowSummaryFactory(status="success")
```

## Best Practices

### Test Isolation
- Each test is completely independent
- Use fresh database for each test
- No shared state between tests
- Tests can run in any order

### Mocking Strategy
- **Database Layer**: No mocking - test real DB behavior
- **Repository Layer**: Mock domain dependencies only
- **Controller Layer**: Mock all dependencies

### Test Naming
- Use descriptive test names: `test_<what>_<condition>_<expected_result>`
- Group related tests in classes
- Use pytest markers for categorization

### Assertions
- Be specific in assertions
- Test both positive and negative cases
- Verify side effects (DB changes, mock calls)

## Adding New Tests

### 1. Database Layer Test
```python
@pytest.mark.unit
class TestNewModel:
    def test_create_record(self, db_session):
        # Create and verify database record
        pass
```

### 2. Repository Layer Test
```python
@pytest.mark.unit
class TestNewRepository:
    def test_repository_method(self, repository_fixture, db_session):
        # Test repository with mocked domain dependencies
        pass
```

### 3. Controller Layer Test
```python
@pytest.mark.unit
class TestNewEndpoint:
    def test_endpoint(self, flow_controller, mock_query_repository):
        # Test controller with fully mocked dependencies
        pass
```

## Coverage Goals

- **Overall**: 80%+ coverage
- **Critical paths**: 100% coverage
- **Database models**: 90%+ coverage
- **Repositories**: 90%+ coverage
- **Controllers**: 95%+ coverage

## Continuous Integration

Tests are configured to run with:
- Coverage reporting
- Strict marker checking
- Async test support (via pytest-asyncio)

## Dependencies

All test dependencies are installed via:
```bash
pip install -e ".[dev]"
```

This includes:
- pytest
- pytest-asyncio
- pytest-cov
- pytest-mock
- factory-boy
- faker
- httpx (for FastAPI testing)

## Next Steps

- Add integration tests that test multiple layers together
- Add end-to-end tests using FastAPI TestClient
- Add performance/load tests
- Add property-based tests using Hypothesis
