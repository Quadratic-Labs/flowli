import logging
from logging import Handler
from typing import Callable, Mapping, TypedDict, Unpack

from fastapi import APIRouter

from .interfaces.context import ContextManagerProtocol
from .interfaces.executor import ExecutorProtocol
from .interfaces.registry import RegistryProtocol
from .interfaces.tracker import TrackerProtocol

from .config import FlowletConfig
from .registry import Registry
from .context import ExecutionContext
from .tracker import Tracker
from .executor import ExecutorInProcess
from .persistence.azure_logging import AzureBlobHandler
from .repositories.query import FlowQueryRepository
from .repositories.tracker import FlowTracker
from .controllers import FlowController


class FlowletDependencies(TypedDict):
    """Type definition for Flowlet dependency injection container.

    Attributes:
        configs: Flowlet configuration settings.
        registry: Flows and tasks registry.
        context_manager: Execution runs' context manager.
        tracker: Execution tracker for logging lifecycle events.
        executor: Flow execution orchestrator.
        log_handlers: Flow logs exporters.
        run_log_handlers: Flow run logs exporters.
        query_repository: Repository for querying flow execution history.
        controller: FastAPI controller for flow endpoints.
    """
    configs: FlowletConfig
    registry: RegistryProtocol
    context_manager: ContextManagerProtocol
    tracker: TrackerProtocol
    executor: ExecutorProtocol
    log_handlers: list[Handler]
    run_log_handlers: list[Handler]
    query: FlowQueryRepository
    controller: FlowController


class Flowlet:
    """Main Flowlet framework class for workflow orchestration.

    Provides decorators for defining flows and tasks, and integrates with FastAPI
    for REST API endpoints to execute and monitor workflows.

    Attributes:
        configs: Flowlet configuration settings.
        registry: Flow and task registration manager.
        tracker: Execution tracker for recording runs.
        executor: Execution logic manager.
        log_handlers: Exporters for flowlet logs.
        run_log_handlers: Exporters for flowlet run summaries.
        query: Data access interface for execution history.
        controller: FastAPI endpoint controller.

    Example:
        >>> from flowlet import configure
        >>> flowlet = configure({"database": {"url": "postgresql://localhost/mydb"}})
        >>>
        >>> @flowlet.task()
        >>> def fetch_data():
        ...     return {"data": [1, 2, 3]}
        >>>
        >>> @flowlet.flow()
        >>> def my_workflow():
        ...     data = fetch_data()
        ...     return data
    """
    def __init__(self, **deps: Unpack[FlowletDependencies]):
        """Initialize Flowlet with dependency injection.

        Args:
            **deps: Unpack of FlowletDependencies containing all required components.
        """
        self.configs = deps["configs"]
        self.registry = deps["registry"]
        self.context_manager = deps["context_manager"]
        self.tracker = deps["tracker"]
        self.executor = deps["executor"]
        self.log_handlers = deps["log_handlers"]
        self.run_log_handlers = deps["run_log_handlers"]
        self.query = deps["query"]
        self.controller = deps["controller"]

        for hdl in self.log_handlers:
            self.tracker.logger.addHandler(hdl)
        for hdl in self.run_log_handlers:
            self.tracker.run_logger.addHandler(hdl)

    def get_router(self):
        """Create a FastAPI router with all Flowlet endpoints.

        Returns:
            APIRouter: Configured router with flow execution and query endpoints.
        """
        router = APIRouter()
        router.get("/flows")(self.controller.list_flows)
        router.post("/flows/{flow_name}/execute")(self.controller.run_flow)
        router.get("/flows/{flow_name}/runs")(self.controller.list_flow_runs)
        router.get("/runs")(self.controller.list_runs)
        router.get("/runs/{run_id}")(self.controller.get_run)
        return router

    def list_flows(self):
        """List all registered flow names.

        Returns:
            list[str]: Names of registered flows.
        """
        return self.registry.list_flows()

    def list_tasks(self):
        """List all registered task names.

        Returns:
            list[str]: Names of registered tasks.
        """
        return self.registry.list_tasks()

    def flow(self, name: str | None=None, executor: ExecutorProtocol | None = None):
        """Decorator to register a function as a flow.

        Flows are the top-level orchestration units that coordinate task execution.
        When a flow executes, its execution is tracked in the database.

        Args:
            name: Optional custom name for the flow. Defaults to function name.

        Returns:
            Callable: Decorator function.

        Example:
            >>> @flowlet.flow()
            >>> def my_workflow():
            ...     result = my_task()
            ...     return result
        """
        def _decorator(fn: Callable) -> Callable:
            flow_name = name
            exe = executor or self.executor
            wrapped = self.registry.register_flow(exe, fn, flow_name)
            return wrapped
        return _decorator

    def task(self, name: str | None=None, executor: ExecutorProtocol | None = None):
        """Decorator to register a function as a task.

        Tasks are individual units of work within a flow. Task execution is tracked
        and linked to the parent flow run via context variables.

        Args:
            name: Optional custom name for the task. Defaults to function name.

        Returns:
            Callable: Decorator function.

        Example:
            >>> @flowlet.task()
            >>> def my_task():
            ...     return {"status": "completed"}
        """
        def _decorator(fn: Callable) -> Callable:
            exe = executor or self.executor
            wrapped = self.registry.register_task(exe, fn, name)
            return wrapped
        return _decorator

    def get_logger(self):
        return self.tracker.logger


def configure(configs: FlowletConfig | Mapping | None=None) -> Flowlet:
    """Configure and initialize a Flowlet instance with dependency injection.

    Creates and wires together all framework components including database sessions,
    execution tracker, flow register, query repository, and API controller.

    Args:
        configs: Configuration as FlowletConfig instance, dict, or None for defaults.
            If dict, will be validated against FlowletConfig schema.

    Returns:
        Flowlet: Fully configured Flowlet instance ready for use.

    Example:
        >>> # Use defaults
        >>> flowlet = configure()
        >>>
        >>> # Use dict configuration
        >>> flowlet = configure({"database": {"url": "postgresql://localhost/mydb"}})
        >>>
        >>> # Use FlowletConfig instance
        >>> from flowlet import FlowletConfig
        >>> config = FlowletConfig(database={"url": "sqlite:///flows.db"})
        >>> flowlet = configure(config)
    """
    if configs is None:
        configs = FlowletConfig()
    elif isinstance(configs, Mapping):
        configs = FlowletConfig.model_validate(configs)
    deps = {}
    deps["configs"] = configs
    deps["registry"] = Registry(**deps)
    deps["context_manager"] = ExecutionContext(**deps)
    deps["tracker"] = Tracker.setup(**deps)
    deps["executor"] = ExecutorInProcess(**deps)
    fmt = logging.Formatter("%(message)s")
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    deps["log_handlers"] = [sh]
    deps["run_log_handlers"] = [sh]
    deps["query"] = FlowQueryRepository(**deps)
    deps["controller"] = FlowController(**deps)

    return Flowlet(**deps)