# region Imports, Dependencies and Configurations
# -----------------------------------------------------------------------------
import threading
import traceback
import uuid
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Callable, Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient
from pydantic import AfterValidator, BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import database
from .manager import FlowManager
from .repository import FlowsRepository


# Get package directory for static files and templates
# This works whether the package is installed or running from source
PACKAGE_DIR = Path(__file__).parent


configs = database.DatabaseSettings(url="sqlite:///flows.db")
engine = database.get_engine(configs)
database.init_database(engine)
db_session_factory = database.get_db_session_factory(engine)
flowlet = FlowManager(db_session_factory)

app = FastAPI(title="Flowlet-API")

# Mount static files using package directory
static_dir = PACKAGE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
else:
    # Fallback for development
    print(f"Warning: Static directory not found at {static_dir}")

# Set up templates using package directory
templates_dir = PACKAGE_DIR / "templates"
if templates_dir.exists():
    templates = Jinja2Templates(directory=str(templates_dir))
else:
    # Fallback for development
    print(f"Warning: Templates directory not found at {templates_dir}")
    templates = Jinja2Templates(directory="./templates")
client = TestClient(app)


def get_db_session():
    return db_session_factory()

DBDependency = Annotated[Session, Depends(get_db_session)]


def get_repository(db: DBDependency):
    return FlowsRepository(flowlet, db)

FlowsRepositoryDependency = Annotated[FlowsRepository, Depends(get_repository)]

# -----------------------------------------------------------------------------
# endregion

# region Models
# -----------------------------------------------------------------------------

def humanize_timedelta(td: timedelta) -> str:
    """Convert a timedelta into a friendly 'x m ago' string."""
    seconds = int(td.total_seconds())
    if seconds < 60:
        return f"{seconds}s ago"
    elif seconds < 3600:
        return f"{seconds // 60}m ago"
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m ago"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h ago"


HumanDuration = Annotated[timedelta, AfterValidator(humanize_timedelta)]

class FlowListItem(BaseModel):
    name: str
    last_status: str
    finished_ago: HumanDuration
    duration: HumanDuration
    doc: str | None = None


class StartFlowRequest(BaseModel):
    # arguments are simply JSON serializable and passed as positional/keyword args.
    # For simplicity we accept an object of kwargs only.
    kwargs: Dict[str, Any] | None = []


class FlowRunInfo(BaseModel):
    flow_id: uuid.UUID
    flow_name: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    error: str | None = None


class TaskRunInfo(BaseModel):
    task_id: uuid.UUID
    task_name: str
    flow_id: uuid.UUID
    flow_name: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    result: str | None = None
    error: str | None = None

# -----------------------------------------------------------------------------
# endregion

# region API routes
# -----------------------------------------------------------------------------

@app.get("/flows")
def list_flows(repository: FlowsRepositoryDependency) -> List[FlowListItem]:
    return repository.read_flow_many()


@app.post("/flows/{flow_name}/run")
def run_flow(
    flow_name: str,
    payload: StartFlowRequest,
    repository: FlowsRepositoryDependency,
) -> FlowRunInfo | None:
    if flow_name not in flowlet:
        raise HTTPException(status_code=404, detail="Flow not found")
    fn = flowlet[flow_name]
    # For simplicity, accept only kwargs
    kwargs = payload.kwargs or {}
    # TODO: schedule background task, get immediate status
    # TODO: add azure job to execution and PubSub sockets
    run_id = fn(**kwargs)

    try:
        fr = repository.read_flow_run(run_id=run_id)
    except Exception:
        fr = None
    finally:
        repository.db.close()
    return fr


@app.get("/runs")
def list_runs(
    repository: FlowsRepositoryDependency,
    offset: int = 0,
    limit: int = 50,
) -> List[FlowRunInfo]:
    try:
        rows = repository.read_flow_run_many(offset=offset, limit=limit)
    except Exception:
        rows = []
    finally:
        repository.db.close()
    return rows
    

@app.get("/runs/{run_id}")
def get_run(
    run_id: uuid.UUID,
    repository: FlowsRepositoryDependency,
) -> FlowRunInfo:
    try:
        run = repository.read_flow_run(run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return run
    finally:
        repository.db.close()


@app.get("/runs/{run_id}/tasks")
def get_run_tasks(
    run_id: uuid.UUID,
    repository: FlowsRepositoryDependency,
) -> List[TaskRunInfo]:
    try:
        return repository.read_run_tasks(run_id=run_id)
    finally:
        repository.db.close()

# -----------------------------------------------------------------------------
# endregion

# region UI routes
# -----------------------------------------------------------------------------

@app.get("/ui")
def ui_home(request: Request):
    flows = client.get("/flows").json()
    return templates.TemplateResponse("flows.html", {"request": request, "flows": flows})


@app.get("/ui/flows/{flow_name}/runs")
def ui_flow_runs(request: Request, flow_name: str):
    runs = client.get("/flows/{flow_name}/runs").json()
    return templates.TemplateResponse(
        "runs.html", {"request": request, "flow_name": flow_name, "runs": runs}
    )


@app.get("/ui/runs/{run_id}/tasks")
def ui_run_tasks(request: Request, run_id: str):
    tasks = client.get("/runs/{run_id}/tasks").json()
    return templates.TemplateResponse(
        "tasks.html", {"request": request, "run_id": run_id, "tasks": tasks}
    )

# -----------------------------------------------------------------------------
# endregion

# ---------------------------
# If run directly
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)