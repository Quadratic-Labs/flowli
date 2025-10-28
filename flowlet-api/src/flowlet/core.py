from typing import Mapping, TypedDict

from fastapi import APIRouter
import sqlalchemy.orm

from .config import FlowletConfig
from .controllers import FlowController
from .database import DatabaseSettings
from .register import FlowRegister
from .repository import FlowRepository

from .controllers import FlowController


class FlowletDependencies(TypedDict):
    configs: FlowletConfig
    db_session_factory: sqlalchemy.orm.Session
    register: FlowRegister
    repository: FlowRepository
    controller: FlowController


class Flowlet:
    def __init__(self, **deps: FlowletDependencies):
        for k, v in deps.items():
            setattr(self, k, v)

    def get_router(self):
        router = APIRouter()
        router.get("/flows")(self.controller.list_flows)
        router.post("/flows/{flow_name}/run")(self.controller.run_flow)
        router.get("/runs")(self.controller.list_runs)
        router.get("/runs/{run_id}")(self.controller.get_run)
        router.get("/runs/{run_id}/tasks")(self.controller.get_run_tasks)
        return router

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


def configure(configs: FlowletConfig | Mapping | None=None) -> FlowletDependencies:
    if configs is None:
        configs = FlowletConfig()
    elif isinstance(configs, Mapping):
        configs = FlowletConfig.model_validate(configs)
    deps = {}
    deps["db_session_factory"] = configs.database.db_session_factory
    deps["register"] = FlowRegister(**deps)
    deps["repository"] = FlowRepository(**deps)
    deps["controller"] = FlowController(**deps)
    return Flowlet(**deps)