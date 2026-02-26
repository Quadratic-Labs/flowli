# TODO: review this in light of refactor
import functools
import json
import logging
from logging import Handler
from typing import Callable, Mapping, TypedDict, Unpack

from fastapi import APIRouter

from .config import FlowletConfig
from .context import ContextManager
from .logging import FlowletLogger
from .registry import Registry
from .types import SpanType


class FlowletDependencies(TypedDict):
    """
    Type definition for Flowlet dependency injection container.

    Attributes:
        configs: Flowlet configuration settings.
        registry: Flows and tasks registry.
        context_manager: Execution runs' context manager.
        logger: FlowLogger.
        log_handlers: Flow logs exporters.
        run_log_handlers: Flow run logs exporters.
        querier: Repository for querying flow execution history.
        queue: Optional job queue for asynchronous execution.
        controller: FastAPI controller for flow endpoints.
    """
    configs: FlowletConfig
    registry: Registry
    context_manager: ContextManager
    logger: FlowletLogger
    querier: RunQueryProtocol
    queue: JobQueueProtocol | None
    controller: FlowController


class Flowlet:
    """
    Main Flowlet framework class for workflow orchestration.

    Provides decorators for defining flows and tasks, and integrates with FastAPI
    for REST API endpoints to execute and monitor workflows.

    Attributes:
        configs: Flowlet configuration settings.
        registry: Flow and task registration manager.
        tracker: Execution tracker for recording runs.
        executor: Execution logic manager.
        log_handlers: Exporters for flowlet logs.
        run_log_handlers: Exporters for flowlet run summaries.
        querier: Data access interface for execution history.
        queue: Optional job queue for asynchronous execution.
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
        """
        Initialize Flowlet with dependency injection.

        Args:
            **deps: Unpack of FlowletDependencies containing all required components.
        """
        self.configs = deps["configs"]
        self.registry = deps["registry"]
        self.context_manager = deps["context_manager"]
        self.logger = deps["logger"]
        self.querier = deps["querier"]
        self.queue = deps["queue"]
        self.controller = deps["controller"]

        for hdl in self.log_handlers:
            self.tracker.logger.addHandler(hdl)
        for hdl in self.run_log_handlers:
            self.tracker.run_logger.addHandler(hdl)

    @classmethod
    def configure(cls, configs: FlowletConfig | Mapping | None=None) -> Flowlet:
        """
        Configure and initialize a Flowlet instance with dependency injection.

        Creates and wires together all framework components including database sessions,
        execution tracker, flow register, query repository, and API controller.

        Args:
            configs: Configuration as FlowletConfig instance, dict, or None for defaults.
                If dict, will be validated against FlowletConfig schema.

        Returns:
            Flowlet: Fully configured Flowlet instance ready for use.

        Example:
            >>> # Use defaults (no storage exporters)
            >>> flowlet = configure()
            >>>
            >>> # Single filesystem storage exporter
            >>> flowlet = configure({
            ...     "storage": [{"type": "filesystem", "base_path": "./storage"}]
            ... })
            >>>
            >>> # Multiple exporters - Azure Blob + SQLite
            >>> from flowlet import FlowletConfig
            >>> config = FlowletConfig(storage=[
            ...     {
            ...         "type": "azure_blob",
            ...         "connection_string": "...",
            ...         "container_name": "logs"
            ...     },
            ...     {
            ...         "type": "sqlite",
            ...         "database_path": "./flowlet.db"
            ...     }
            ... ])
            >>> flowlet = configure(config)
            >>>
            >>> # All three storage backends
            >>> flowlet = configure({
            ...     "storage": [
            ...         {"type": "filesystem", "base_path": "./storage"},
            ...         {"type": "azure_blob", "connection_string": "...", "container_name": "logs"},
            ...         {"type": "sqlite", "database_path": "./flowlet.db"}
            ...     ]
            ... })
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

        # Setup queue if configured
        queue: JobQueueProtocol | None = None
        if configs.queue is not None:
            from .queue.config import AzureQueueStorageConfig, InMemoryQueueConfig
            from .queue.azure import AzureQueueStorage
            from .queue.memory import InMemoryQueue

            if isinstance(configs.queue, AzureQueueStorageConfig):
                queue = AzureQueueStorage.setup(configs.queue)
            elif isinstance(configs.queue, InMemoryQueueConfig):
                queue = InMemoryQueue.setup(configs.queue)

        deps["queue"] = queue

        # Setup formatters
        json_formatter = JSONSpanFormatter()
        text_formatter = logging.Formatter("%(message)s")

        # Initialize log handlers (always include stream handler)
        log_handlers: list[Handler] = []
        run_log_handlers: list[Handler] = []

        # Add default stream handlers
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(json_formatter)
        log_handlers.append(sh)

        rh = logging.StreamHandler()
        rh.setLevel(logging.INFO)
        rh.setFormatter(text_formatter)
        run_log_handlers.append(rh)

        # Add storage-specific handlers for each configured storage backend
        for storage_config in configs.storage:
            if isinstance(storage_config, FilesystemStorageConfig):
                # Filesystem storage handlers
                logs_path = storage_config.base_path / "logs"
                runs_path = storage_config.base_path / "runs"

                fs_log_handler = FilesystemHandler(
                    path=logs_path,
                    level=logging.INFO,
                    router=lambda r: f"{getattr(getattr(r, '_span'), 'run_id')}.jsonl",
                )
                fs_log_handler.setFormatter(json_formatter)
                log_handlers.append(fs_log_handler)

                fs_run_handler = FilesystemHandler(
                    path=runs_path,
                    level=logging.INFO,
                    router=lambda r: f"{json.loads(r.message).get('span_name', 'undefined')}.jsonl",
                )
                fs_run_handler.setFormatter(text_formatter)
                run_log_handlers.append(fs_run_handler)
                deps["querier"] = FileQuery(base_path=storage_config.base_path, **deps)

            elif isinstance(storage_config, AzureBlobStorageConfig):
                from .storage.azure.path import AzureBlobPath

                # Azure Blob storage handlers
                # For logs, we'll use run_id in the blob name (handled by handler)
                # For now, use a timestamp-based approach or organize by date
                base_path = storage_config.base_path.rstrip('/')
                logs_prefix = f"{base_path}/logs" if base_path else "logs"
                runs_prefix = f"{base_path}/runs" if base_path else "runs"

                # Note: AzureBlobHandler needs to be enhanced to support run_id-based blob names
                # For now, we'll create a handler that appends to a single log file
                # In production, you may want to implement a custom handler that creates
                # separate blobs per run_id
                azure_path = AzureBlobPath(
                    connection_string=storage_config.connection_string,
                    container_name=storage_config.container_name,
                )
                azure_log_handler = FilesystemHandler(
                    path = azure_path / logs_prefix,
                    level = logging.INFO,
                    chunk_size = 4*1024,  # 4 KiB
                )
                azure_log_handler.setFormatter(json_formatter)
                log_handlers.append(azure_log_handler)

                azure_run_handler = FilesystemHandler(
                    path = azure_path / runs_prefix,
                    level = logging.INFO,
                    chunk_size=4*1024,  # 4 KiB
                )
                azure_run_handler.setFormatter(text_formatter)
                run_log_handlers.append(azure_run_handler)
                deps["querier"] = FileQuery(base_path=azure_path, **deps)

            elif isinstance(storage_config, SQLiteStorageConfig):
                # SQLite storage handlers
                sqlite_log_handler = SQLiteHandler(
                    database_path=storage_config.database_path,
                    table_name=storage_config.logs_table_name
                )
                sqlite_log_handler.setLevel(logging.INFO)
                sqlite_log_handler.setFormatter(json_formatter)
                log_handlers.append(sqlite_log_handler)

                sqlite_run_handler = SQLiteRunHandler(
                    database_path=storage_config.database_path,
                    table_name=storage_config.runs_table_name
                )
                sqlite_run_handler.setLevel(logging.INFO)
                sqlite_run_handler.setFormatter(text_formatter)
                run_log_handlers.append(sqlite_run_handler)

                deps["querier"] = None

        deps["log_handlers"] = log_handlers
        deps["run_log_handlers"] = run_log_handlers
        deps["controller"] = FlowController(**deps)
        return cls(**deps)

    def get_router(self):
        """
        Create a FastAPI router with all Flowlet endpoints.

        Returns:
            APIRouter: Configured router with flow execution and query endpoints.
        """
        router = APIRouter()

        # Execution endpoints
        router.post(
            "/execute/{flow_name}",
            summary="Execute a registered flow",
            description="""
            Execute a flow with validated arguments.

            To see the exact parameter schema for a specific flow:
            GET /flows/{flow_name}/schema

            The request body should contain a 'kwargs' object with parameters
            matching the flow's type signature.
            """,
            responses={
                200: {"description": "Flow executed successfully"},
                404: {"description": "Flow not found"},
                422: {"description": "Invalid flow arguments - see error details"}
            },
            tags=["Execution"]
        )(self.controller.run_flow)

        # Async execution endpoint (if queue is configured)
        if self.controller.queue is not None:
            from .controllers import FlowSubmissionResponse

            router.post(
                "/submit/{flow_name}",
                summary="Submit a flow for asynchronous execution",
                description="""
                Submit a flow to the job queue for asynchronous execution.
                Returns immediately with a job_id and run_id for tracking.

                To check execution status, use the query endpoints:
                POST /runs/query with the returned run_id

                To see the exact parameter schema for a specific flow:
                GET /flows/{flow_name}/schema

                The request body should contain a 'kwargs' object with parameters
                matching the flow's type signature.
                """,
                response_model=FlowSubmissionResponse,
                responses={
                    200: {"description": "Flow submitted successfully"},
                    404: {"description": "Flow not found"},
                    422: {"description": "Invalid flow arguments - see error details"},
                    503: {"description": "Queue not configured"}
                },
                tags=["Execution"]
            )(self.controller.submit_flow)

        # Query endpoints
        router.post(
            "/runs/query",
            tags=["Query"]
        )(self.controller.query_runs)

        router.post(
            "/logs/query",
            tags=["Query"]
        )(self.controller.query_logs)

        # Schema introspection endpoints
        router.get(
            "/flows",
            summary="List all registered flows with their schemas",
            description="Returns metadata for all flows including parameter information",
            response_model=list[dict],
            tags=["Introspection"]
        )(self.controller.list_flows_with_schemas)

        router.get(
            "/flows/{flow_name}/schema",
            summary="Get parameter schema for a specific flow",
            description="Returns detailed schema including JSON Schema format for client generation",
            response_model=dict,
            responses={404: {"description": "Flow not found"}},
            tags=["Introspection"]
        )(self.controller.get_flow_schema)

        return router

    def list_flows(self):
        """
        List all registered flow names.

        Returns:
            list[str]: Names of registered flows.
        """
        return self.registry.list_flows()

    def list_tasks(self):
        """
        List all registered task names.

        Returns:
            list[str]: Names of registered tasks.
        """
        return self.registry.list_tasks()

    def flow(self, name: str | None=None):
        """
        Decorator to register a function as a flow.

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
            flow_name = name or fn.__name__
            wfn = self._orchestrated(fn, flow_name, "flow")
            self.registry.register_flow(wfn, flow_name)
            return wfn
        return _decorator

    def task(self, name: str | None=None):
        """
        Decorator to register a function as a task.

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
            flow_name = name or fn.__name__
            wfn = self._orchestrated(fn, flow_name, "task")
            self.registry.register_flow(wfn, flow_name)
            return wfn
        return _decorator

    def get_logger(self):
        return self.logger

    def _orchestrated(self, fn: Callable, name: str, typ: SpanType) -> Callable:
        # Convert a usual function into a function with run logging.
        # Should also handle orchestration via the queue.
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = None
            with self.context_manager.begin_span(name, typ):
                root = self.context_manager.get_root_span()
                run_id = root.run_id
                self.logger.info(f"Run-{run_id}: running...")
                try:
                    result = fn(*args, **kwargs)
                except Exception as err:
                    self.logger.exception(f"Run-{run_id} Error: {err}")
                    if not self.context_manager.is_root():
                        raise err
                else:
                    self.logger.success(f"Run-{run_id}: completed successfully.")
            return result
        return wrapper
