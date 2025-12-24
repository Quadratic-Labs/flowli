from typing import Mapping, TypedDict, Unpack

from fastapi import APIRouter
import sqlalchemy.orm

from .config import FlowletConfig
from .controllers import FlowController
from .database import DatabaseSettings
from .execution_observer import RelationalDBObserver
from .register import FlowRegister
from .repositories.query import FlowQueryRepository
from .repositories.tracker import FlowTracker


class FlowletDependencies(TypedDict):
    """Type definition for Flowlet dependency injection container.

    Attributes:
        configs: Flowlet configuration settings.
        db_session_factory: SQLAlchemy session factory for database connections.
        tracker: Flow execution tracker for recording run lifecycle events.
        observer: Execution observer for logging and tracking lifecycle events.
        register: Flow and task registration manager.
        query_repository: Repository for querying flow execution history.
        controller: FastAPI controller for flow endpoints.
    """
    configs: FlowletConfig
    db_session_factory: sqlalchemy.orm.Session
    tracker: FlowTracker
    observer: RelationalDBObserver
    register: FlowRegister
    query_repository: FlowQueryRepository
    controller: FlowController


class Flowlet:
    """Main Flowlet framework class for workflow orchestration.

    Provides decorators for defining flows and tasks, and integrates with FastAPI
    for REST API endpoints to execute and monitor workflows.

    Attributes:
        configs: Flowlet configuration settings.
        db_session_factory: SQLAlchemy session factory.
        tracker: Execution tracker for recording runs.
        observer: Execution observer for logging/tracking lifecycle events.
        register: Flow and task registration manager.
        query_repository: Query interface for execution history.
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
        self.db_session_factory = deps["db_session_factory"]
        self.tracker = deps["tracker"]
        self.observer = deps["observer"]
        self.register = deps["register"]
        self.query_repository = deps["query_repository"]
        self.controller = deps["controller"]

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

    def init_database(self):
        """Initialize database tables for flow run tracking."""
        self.configs.database.init_database()

    def list_flows(self):
        """List all registered flow names.

        Returns:
            list[str]: Names of registered flows.
        """
        return self.register.list_flows()

    def list_tasks(self):
        """List all registered task names.

        Returns:
            list[str]: Names of registered tasks.
        """
        return self.register.list_tasks()

    def flow(self, name: str | None=None):
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
        return self.register.flow(name=name)

    def task(self, name: str | None=None):
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
        return self.register.task(name=name)


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
    deps["db_session_factory"] = configs.database.db_session_factory

    # Create tracker first (no dependencies on register)
    deps["tracker"] = FlowTracker(**deps)

    # Create execution observer (depends on tracker and db_session_factory)
    # Note: log_manager can be set later via initialize_logging()
    deps["observer"] = RelationalDBObserver(
        tracker=deps["tracker"],
        db_session_factory=deps["db_session_factory"]
    )

    # Create register (depends on tracker and observer)
    deps["register"] = FlowRegister(**deps)

    # Create query repository (depends on register)
    deps["query_repository"] = FlowQueryRepository(**deps)

    # Create controller (depends on query_repository)
    deps["controller"] = FlowController(**deps)

    return Flowlet(**deps)