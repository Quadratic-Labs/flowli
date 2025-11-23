from typing import Mapping, TypedDict, Unpack

from fastapi import APIRouter
import sqlalchemy.orm

from .config import FlowletConfig
from .controllers import FlowController
from .database import DatabaseSettings
from .register import FlowRegister
from .repositories.query import FlowQueryRepository
from .repositories.tracker import FlowTracker


class FlowletDependencies(TypedDict):
    configs: FlowletConfig
    db_session_factory: sqlalchemy.orm.Session
    tracker: FlowTracker
    register: FlowRegister
    query_repository: FlowQueryRepository
    controller: FlowController


class Flowlet:
    def __init__(self, **deps: Unpack[FlowletDependencies]):
        self.configs = deps["configs"]
        self.db_session_factory = deps["db_session_factory"]
        self.tracker = deps["tracker"]
        self.register = deps["register"]
        self.query_repository = deps["query_repository"]
        self.controller = deps["controller"]

    def get_router(self):
        router = APIRouter()
        router.get("/flows")(self.controller.list_flows)
        router.post("/flows/{flow_name}/execute")(self.controller.run_flow)
        router.get("/flows/{flow_name}/runs")(self.controller.list_flow_runs)
        router.get("/runs")(self.controller.list_runs)
        router.get("/runs/{run_id}")(self.controller.get_run)
        return router

    def init_database(self):
        self.configs.database.init_database()

    def list_flows(self):
        return self.register.list_flows()

    def list_tasks(self):
        return self.register.list_tasks()

    def flow(self, name: str | None=None):
        """
        Decorator to register a function as a flow.
        The decorated function should call task functions (or plain functions).
        """
        return self.register.flow(name=name)

    def task(self, name: str | None=None):
        """
        Decorator to wrap a task function so it logs start/finish/exceptions to DB
        tied to the current flow run (via contextvar).

        Now uses TaskThreadExecutor to separate execution logic from definition.
        """
        return self.register.task(name=name)


def configure(configs: FlowletConfig | Mapping | None=None) -> Flowlet:
    if configs is None:
        configs = FlowletConfig()
    elif isinstance(configs, Mapping):
        configs = FlowletConfig.model_validate(configs)
    deps = {}
    deps["configs"] = configs
    deps["db_session_factory"] = configs.database.db_session_factory

    # Create tracker first (no dependencies on register)
    deps["tracker"] = FlowTracker(**deps)

    # Create register (depends on tracker)
    deps["register"] = FlowRegister(**deps)

    # Create query repository (depends on register)
    deps["query_repository"] = FlowQueryRepository(**deps)

    # Create controller (depends on query_repository)
    deps["controller"] = FlowController(**deps)

    return Flowlet(**deps)