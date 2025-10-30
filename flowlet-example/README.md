# Flowlet Example

A complete example application demonstrating how to use the **Flowlet** workflow orchestration framework.

## Overview

This example shows how to:
- Create a FlowManager instance
- Register flows and tasks using decorators
- Mount the Flowlet router in a FastAPI application
- Run flows through the API
- Track flow execution and task status

## Project Structure

```
flowlet-example/
├── src/
│   └── flowlet_example/
│       ├── __init__.py
│       ├── flows.py      # Flow and task definitions
│       └── main.py       # FastAPI application setup
├── pyproject.toml
└── README.md
```

## Installation

### Prerequisites

- Python 3.11 or higher
- The `flowlet` package installed

### Install from source

From the `flowlet-example` directory:

```bash
pip install -e .
```

Or install in development mode with dev dependencies:

```bash
pip install -e ".[dev]"
```

## Running the Application

### Option 1: Using Python module

```bash
python -m flowlet_example.main
```

### Option 2: Using uvicorn directly

```bash
uvicorn flowlet_example.main:app --reload --host 0.0.0.0 --port 8000
```

The application will start on `http://localhost:8000`

## Example Flows

This example includes several demonstration flows:

### 1. Hello World Flow (`hello_world`)
A simple flow to demonstrate basic functionality.

**Parameters:**
- `name` (str, default="World"): Name to greet

**Example:**
```bash
curl -X POST "http://localhost:8000/flows/hello_world/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"name": "Flowlet"}}'
```

### 2. Simple ETL Flow (`simple_etl`)
Demonstrates a basic Extract-Transform-Load pipeline:
1. Fetches data from a source
2. Transforms the data
3. Validates it
4. Saves to database

**Parameters:**
- `source` (str, default="api"): Data source identifier

**Example:**
```bash
curl -X POST "http://localhost:8000/flows/simple_etl/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"source": "database"}}'
```

### 3. Data Pipeline Flow (`data_pipeline`)
A more complex pipeline with statistics and notifications:
1. Fetches and transforms data
2. Calculates statistics
3. Saves results
4. Sends notifications (optional)

**Parameters:**
- `source` (str, default="database"): Data source
- `notify` (bool, default=true): Whether to send notifications

**Example:**
```bash
curl -X POST "http://localhost:8000/flows/data_pipeline/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"source": "api", "notify": true}}'
```

### 4. Parallel Tasks Flow (`parallel_tasks`)
Demonstrates running multiple independent tasks.

**Parameters:**
- `count` (int, default=3): Number of parallel tasks to run

**Example:**
```bash
curl -X POST "http://localhost:8000/flows/parallel_tasks/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"count": 5}}'
```

### 5. Error Handling Demo (`error_handling_demo`)
Shows how Flowlet handles task failures.

**Parameters:**
- `fail_chance` (float, default=0.5): Probability of failure (0.0 to 1.0)

**Example:**
```bash
curl -X POST "http://localhost:8000/flows/error_handling_demo/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"fail_chance": 0.3}}'
```

## API Endpoints

The application provides the following endpoints:

### Application Endpoints
- `GET /` - Root endpoint with application info and available flows
- `GET /health` - Health check endpoint
- `GET /docs` - Interactive API documentation (Swagger UI)

### Flowlet Endpoints
- `GET /flows` - List all registered flows with their status
- `POST /flows/{flow_name}/run` - Execute a specific flow
- `GET /runs` - List all flow runs (with pagination)
- `GET /runs/{run_id}` - Get details of a specific flow run
- `GET /runs/{run_id}/tasks` - Get all tasks for a specific flow run

## Example Usage

### List all flows
```bash
curl http://localhost:8000/flows
```

### Run a flow
```bash
curl -X POST "http://localhost:8000/flows/simple_etl/run" \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"source": "api"}}'
```

### List all runs
```bash
curl http://localhost:8000/runs
```

### Get run details
```bash
curl http://localhost:8000/runs/{run_id}
```

### Get tasks for a run
```bash
curl http://localhost:8000/runs/{run_id}/tasks
```

## Code Example: Creating Your Own Flow

Here's how to create a simple flow:

```python
from flowlet.database import DatabaseSettings, get_engine, get_db_session_factory, init_database
from flowlet.manager import FlowManager

# Initialize database and flow manager
configs = DatabaseSettings(url="sqlite:///my_flows.db")
engine = get_engine(configs)
init_database(engine)
db_session_factory = get_db_session_factory(engine)

flowlet = FlowManager(db_session_factory)

# Define tasks
@flowlet.task()
def my_task(x: int):
    return x * 2

# Define flow
@flowlet.flow("my_flow")
def my_flow(value: int):
    result = my_task(value)
    return {"result": result}
```

Then mount the Flowlet router in your FastAPI app:

```python
from fastapi import FastAPI
from flowlet.app import router as flowlet_router

app = FastAPI()
app.include_router(flowlet_router, prefix="", tags=["Flowlet"])
```

## Database

The example uses SQLite by default, with the database file created as `flowlet_example.db` in the current directory.

To use a different database, modify the `DatabaseSettings` in [flows.py](src/flowlet_example/flows.py):

```python
configs = DatabaseSettings(url="postgresql://user:pass@localhost/dbname")
```

## Development

### Running Tests
```bash
pytest
```

### Code Structure

- [flows.py](src/flowlet_example/flows.py) - Contains all flow and task definitions
- [main.py](src/flowlet_example/main.py) - FastAPI application setup and configuration

## Learn More

- Check the interactive API documentation at `http://localhost:8000/docs` after starting the server
- Explore the flow definitions in [flows.py](src/flowlet_example/flows.py) to understand different patterns
- View the Flowlet source code for advanced features

## License

MIT
