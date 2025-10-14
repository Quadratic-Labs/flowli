# Flowlet

A lightweight, thread-safe flow orchestration framework for Python with built-in execution tracking and observability.

## Features

- 🔄 **Flow Orchestration**: Define and execute complex workflows with tasks
- 🧵 **Thread-Safe**: Uses `contextvars` for safe execution in multithreaded and async environments
- 📊 **Execution Tracking**: Automatic tracking of flow and task execution with database persistence
- 🎯 **Simple API**: Clean decorator-based API (`@flow`, `@task`)
- 🔍 **Observability**: Built-in logging, metrics, and execution history
- 🌐 **REST API**: FastAPI-based API for managing and monitoring flows
- 📱 **Web UI**: Simple web interface for visualizing flows and runs

## Installation

```bash
# Basic installation
pip install flowlet

# With PostgreSQL support
pip install flowlet[postgres]

# With observability tools
pip install flowlet[observability]

# Development installation
pip install -e ".[dev]"
```

## Quick Start

### Define a Flow

```python
from flowlet import flow, task
import time

@task()
def fetch_data(source: str):
    print(f"Fetching data from {source}")
    time.sleep(0.5)
    return {"data": f"data from {source}"}

@task()
def process_data(data: dict):
    print(f"Processing {data}")
    time.sleep(0.3)
    return {"processed": True}

@task()
def save_results(results: dict):
    print(f"Saving {results}")
    time.sleep(0.2)
    return "saved"

@flow("data_pipeline")
def data_pipeline(source: str):
    """A simple data processing pipeline."""
    data = fetch_data(source)
    results = process_data(data)
    status = save_results(results)
    return {"status": status}
```

### Execute the Flow

```python
# Direct execution
result = data_pipeline("api")

# Or via the flow manager
from flowlet import FlowManager

manager = FlowManager(db_session_factory)
run_id = manager.run_flow("data_pipeline", source="api")
```

### Start the API Server

```bash
# Run the development server
python -m flowlet.app

# Or with uvicorn
uvicorn flowlet.app:app --reload
```

Then visit:
- API docs: http://localhost:8000/docs
- Web UI: http://localhost:8000/ui

## Architecture

### Context Management (Thread-Safe)

Flowlet uses Python's `contextvars` to safely track execution context across threads and async tasks:

```python
from flowlet.context_improved import (
    FlowContext,
    TaskContext,
    current_flow_name,
    current_flow_run_id
)

# Flow execution automatically sets context
with FlowContext("my_flow", db_session_factory):
    # All tasks here can access flow context
    flow_name = current_flow_name.get()      # "my_flow"
    flow_run_id = current_flow_run_id.get()  # unique UUID
```

### Executor Pattern

Execution logic is separated from task definition:

```python
from flowlet.executors import TaskThreadExecutor

# The decorator creates an executor
@task("my_task")
def my_task(x, y):
    return x + y

# Behind the scenes:
# executor = TaskThreadExecutor(
#     task_fn=my_task,
#     task_name="my_task",
#     db_session_factory=SessionLocal
# )
```

## API Endpoints

### Flows

- `GET /flows` - List all registered flows
- `POST /flows/{flow_name}/run` - Execute a flow

### Runs

- `GET /runs` - List recent flow runs
- `GET /runs/{run_id}` - Get specific run details
- `GET /runs/{run_id}/tasks` - Get tasks for a run

## Configuration

### Database

Configure the database via environment variables or code:

```python
from flowlet import database

# SQLite (default)
configs = database.DatabaseSettings(url="sqlite:///flows.db")

# PostgreSQL
configs = database.DatabaseSettings(
    url="postgresql://user:pass@localhost/flowlet"
)

# In-memory (testing)
configs = database.DatabaseSettings(url="sqlite:///:memory:")
```

### Logging

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flowlet")
```

## Examples

### Conditional Tasks

```python
@flow("conditional_flow")
def conditional_pipeline(env: str):
    data = fetch_data()

    if env == "production":
        validate_production(data)
    else:
        validate_test(data)

    return process(data)
```

### Error Handling

```python
@task()
def risky_operation():
    try:
        # Some operation that might fail
        result = dangerous_function()
        return result
    except Exception as e:
        # Error is automatically tracked in DB
        logger.error(f"Operation failed: {e}")
        raise  # Re-raise to mark task as failed
```

### Parallel Execution (Future)

```python
# Coming soon: parallel task execution
@flow("parallel_flow")
def parallel_pipeline():
    # These tasks will run in parallel
    results = await gather(
        task1.submit(),
        task2.submit(),
        task3.submit()
    )
    return combine_results(results)
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/flowlet.git
cd flowlet/flowlet-api

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in development mode
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=flowlet --cov-report=html

# Run specific test file
pytest tests/test_context_safety.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy src/flowlet

# All checks
ruff format . && ruff check . && mypy src/flowlet && pytest
```

## Documentation

- [Context Safety Guide](CONTEXT_SAFETY_GUIDE.md) - Thread and async safety with contextvars
- [Context Recommendation](CONTEXT_RECOMMENDATION.md) - Architecture decisions
- [Executor Pattern](src/flowlet/executors/README.md) - Separation of execution from definition

## Roadmap

- [ ] Async task execution support
- [ ] Distributed execution (Celery/RQ integration)
- [ ] Task retries and exponential backoff
- [ ] Task dependencies and DAG visualization
- [ ] Webhooks and event notifications
- [ ] Scheduled flows (cron-like)
- [ ] Flow versioning
- [ ] Parameter validation with Pydantic
- [ ] Result caching
- [ ] Rate limiting

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Acknowledgments

Inspired by workflow engines like:
- [Prefect](https://www.prefect.io/)
- [Apache Airflow](https://airflow.apache.org/)
- [Temporal](https://temporal.io/)

Built with:
- [FastAPI](https://fastapi.tiangolo.com/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [Pydantic](https://pydantic-docs.helpmanual.io/)
