# Test Suite Dependencies

This document describes the testing dependencies added to the `dev` optional dependencies in [pyproject.toml](../pyproject.toml).

## Testing Dependencies

All testing dependencies are installed via:
```bash
pip install -e ".[dev]"
```

### Core Testing Framework

#### pytest >= 7.4.0
The main testing framework for Python.
- **Purpose**: Test discovery, execution, and reporting
- **Usage**: Command-line test runner
- **Docs**: https://docs.pytest.org/

#### pytest-asyncio >= 0.21.0
Async/await support for pytest.
- **Purpose**: Test async functions and coroutines
- **Usage**: Automatic for async test functions
- **Configured**: `asyncio_mode = "auto"` in pyproject.toml
- **Docs**: https://pytest-asyncio.readthedocs.io/

### Code Coverage

#### pytest-cov >= 4.1.0
Code coverage plugin for pytest.
- **Purpose**: Measure test coverage
- **Usage**: `pytest --cov=flowlet --cov-report=html`
- **Configured**: Coverage settings in pyproject.toml
- **Output**: Terminal, HTML, and XML reports
- **Docs**: https://pytest-cov.readthedocs.io/

### Mocking and Test Doubles

#### pytest-mock >= 3.12.0
Pytest plugin for the mock library.
- **Purpose**: Simplified mocking interface
- **Usage**: Use `mocker` fixture in tests
- **Example**:
  ```python
  def test_something(mocker):
      mock = mocker.patch('module.function')
      mock.return_value = 'mocked'
  ```
- **Docs**: https://pytest-mock.readthedocs.io/

### Test Data Generation

#### factory-boy >= 3.3.0
Fixtures replacement for Python testing.
- **Purpose**: Generate test data objects with factories
- **Usage**: Define factories for ORM models and domain objects
- **Implementation**: See `tests/factories/`
- **Example**:
  ```python
  from tests.factories import RunFactory

  run = RunFactory(name="my_test_flow")
  ```
- **Docs**: https://factoryboy.readthedocs.io/

#### faker >= 20.0.0
Fake data generation library.
- **Purpose**: Generate realistic fake data (names, emails, dates, etc.)
- **Usage**: Integrated with factory-boy or standalone
- **Example**:
  ```python
  from faker import Faker

  fake = Faker()
  name = fake.name()
  email = fake.email()
  ```
- **Docs**: https://faker.readthedocs.io/

### Code Quality

#### ruff >= 0.1.0
Fast Python linter and formatter.
- **Purpose**: Linting and code formatting
- **Usage**: `ruff check .` or `ruff format .`
- **Configured**: Extensive settings in pyproject.toml
- **Docs**: https://docs.astral.sh/ruff/

#### mypy >= 1.6.0
Static type checker for Python.
- **Purpose**: Type checking and validation
- **Usage**: `mypy src/`
- **Configured**: Type checking settings in pyproject.toml
- **Docs**: https://mypy.readthedocs.io/

### HTTP Testing

#### httpx >= 0.25.0
HTTP client for testing FastAPI applications.
- **Purpose**: Test FastAPI endpoints with TestClient
- **Usage**: Used by FastAPI's TestClient
- **Example**:
  ```python
  from fastapi.testclient import TestClient

  client = TestClient(app)
  response = client.get("/flows")
  ```
- **Docs**: https://www.python-httpx.org/

## Dependency Versions

All dependencies specify minimum versions to ensure compatibility while allowing updates:

| Dependency | Minimum Version | Current Purpose |
|-----------|----------------|-----------------|
| pytest | 7.4.0 | Core test framework |
| pytest-asyncio | 0.21.0 | Async test support |
| pytest-cov | 4.1.0 | Coverage reporting |
| pytest-mock | 3.12.0 | Mocking utilities |
| factory-boy | 3.3.0 | Test data factories |
| faker | 20.0.0 | Fake data generation |
| ruff | 0.1.0 | Linting and formatting |
| mypy | 1.6.0 | Static type checking |
| httpx | 0.25.0 | HTTP client for testing |

## Installation

### Install All Dev Dependencies
```bash
cd flowlet-api
pip install -e ".[dev]"
```

### Install Specific Dependency Groups
```bash
# Development tools only
pip install pytest pytest-cov ruff mypy

# Test data generation only
pip install factory-boy faker

# Mocking utilities only
pip install pytest-mock
```

### Verify Installation
```bash
# Check pytest is installed
pytest --version

# Check all plugins are loaded
pytest --version --verbose

# List installed packages
pip list | grep -E "(pytest|factory|faker|ruff|mypy|httpx)"
```

## Usage Examples

### Running Tests with Coverage
```bash
# Basic coverage
pytest --cov=flowlet

# With HTML report
pytest --cov=flowlet --cov-report=html

# Open coverage report
open htmlcov/index.html
```

### Using Factories
```python
# tests/unit/test_example.py
from tests.factories import RunFactory, RunLogFactory

def test_with_factory(db_session):
    # Create test data easily
    run = RunFactory(name="test_flow", run_type="flow")
    log = RunLogFactory(run_id=run.run_id, status="success")

    # Use in tests
    assert run.name == "test_flow"
    assert log.status == "success"
```

### Using Mocks
```python
def test_with_mock(mocker):
    # Mock a function
    mock_db = mocker.patch('flowlet.database.get_session')
    mock_db.return_value = fake_session

    # Test code that uses the mocked function
    result = my_function()

    # Verify mock was called
    mock_db.assert_called_once()
```

### Type Checking
```bash
# Check all Python files
mypy src/

# Check specific file
mypy src/flowlet/database.py
```

### Linting and Formatting
```bash
# Check for issues
ruff check .

# Auto-fix issues
ruff check --fix .

# Format code
ruff format .
```

## Continuous Integration

These dependencies are used in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Install dependencies
  run: pip install -e ".[dev]"

- name: Run linter
  run: ruff check .

- name: Run type checker
  run: mypy src/

- name: Run tests with coverage
  run: pytest --cov=flowlet --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## Updating Dependencies

To update all dependencies to their latest versions:

```bash
# Update all packages
pip install --upgrade -e ".[dev]"

# Update specific package
pip install --upgrade pytest

# Check for outdated packages
pip list --outdated
```

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'pytest'`
- **Solution**: Run `pip install -e ".[dev]"`

**Issue**: Tests not discovered
- **Solution**: Ensure test files match `test_*.py` pattern

**Issue**: Coverage not working
- **Solution**: Ensure you're running from the package root directory

**Issue**: Factory imports failing
- **Solution**: Check `tests/factories/__init__.py` exports

## Additional Resources

- [Pytest Best Practices](https://docs.pytest.org/en/stable/goodpractices.html)
- [Factory Boy Best Practices](https://factoryboy.readthedocs.io/en/stable/recipes.html)
- [Testing FastAPI Applications](https://fastapi.tiangolo.com/tutorial/testing/)
- [Python Testing Guide](https://realpython.com/pytest-python-testing/)
