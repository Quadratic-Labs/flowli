"""Flowlet framework entry point.

Provides the Flowlet class with flow/task registration decorators and the
configure() classmethod that wires the full dependency graph from a
FlowletConfig.
"""
from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable, TypedDict

from .config import FlowletConfig
from .context import ContextManager
from .registry import Registry

if TYPE_CHECKING:
    from fastapi import APIRouter
    from .api.controller import FlowController
    from .api.query import RunQuery
    from .api.repository import SnapshotRepository
    from .queue import JobQueueProtocol
    from .repository.log import LogRepository


# region @app
# ---
# role: orchestration
# intent: framework façade — expose flow/task decorators and the configure() factory
# description: >
#   Flowlet is the thin user-facing façade.  It owns the Registry and the
#   FlowController and exposes:
#     - flow() / task() — registration decorators delegating to Registry.
#     - configure()     — classmethod building the full dependency graph
#                         from a FlowletConfig (or dict / None for defaults).
#     - get_router()    — returns the FastAPI APIRouter for all Flowlet endpoints.
#   configure() accumulates a deps dict sequentially and passes it to each
#   component constructor.  Every constructor accepts **_ to absorb unknowns
#   so configure() does not need to cherry-pick arguments.
#   FlowletDeps documents the keys and their construction order.
# rules:
#   - MUST NOT implement execution or query logic; delegate to Registry / FlowController.
#   - configure() MUST set up context logging before constructing repositories.
#   - configure() MUST guard all storage-dependent components behind configs.storage.
# dependencies:
#   - registry.registry
#   - controller
#   - config
#   - logger
# aliases:
#   - flowlet-app
#   - configure
# triggers:
#   - how to set up flowlet
#   - configure flowlet
#   - register a flow
# ---


class FlowletDeps(TypedDict, total=False):
    """Ordered dependency map built by Flowlet.configure().

    Keys are added sequentially; each component is constructed by passing the
    full dict via ``**deps`` and relying on ``**_`` in the constructor to
    absorb unused keys.  The order below reflects the required construction
    sequence.

    Attributes:
        configs: Validated application configuration.
        registry: Flow and task registry (constructed first; no deps).
        context_manager: Span context holder (stateless singleton pattern).
        log_repo: Log-file reader; present only when storage is configured.
        snapshot_repo: SQLite snapshot repository; present only when storage
            is configured.
        querier: Read-side query object; None when no storage is configured.
        queue: Async job queue; None when not configured.
        controller: FastAPI controller wiring all endpoint handlers.
    """
    configs: FlowletConfig
    registry: Registry
    context_manager: ContextManager
    log_repo: LogRepository
    snapshot_repo: SnapshotRepository
    querier: RunQuery | None
    queue: JobQueueProtocol | None
    controller: FlowController
    router: APIRouter


class Flowlet:
    """Façade for the Flowlet workflow orchestration framework.

    Provides flow/task registration decorators and delegates all HTTP and
    query operations to the injected FlowController.

    Attributes:
        registry: Flow and task registry.
        controller: FastAPI controller owning all HTTP endpoints.

    Example:
        >>> from flowlet import Flowlet
        >>> flowlet = Flowlet.configure()
        >>>
        >>> @flowlet.flow()
        ... def my_workflow(x: int) -> int:
        ...     return my_task(x)
        >>>
        >>> @flowlet.task()
        ... def my_task(x: int) -> int:
        ...     return x * 2
        >>>
        >>> app.include_router(flowlet.get_router(), prefix="/flowlet")
    """

    def __init__(self, **deps):
        """Initialise Flowlet from the accumulated dependency map.

        Args:
            **deps: Full FlowletDeps dict produced by configure().
        """
        self.registry: Registry = deps["registry"]  # type: ignore[assignment]
        self.controller: FlowController = deps["controller"]  # type: ignore[assignment]
        self.router: APIRouter = deps["router"]  # type: ignore[assignment]

    @classmethod
    def configure(cls, configs: FlowletConfig | Mapping | None = None) -> "Flowlet":
        """Wire the full dependency graph and return a ready Flowlet instance.

        Builds a ``deps`` dict sequentially, passing it to each component
        constructor so that every component self-selects its own dependencies
        via keyword arguments.  See :class:`FlowletDeps` for the full key list
        and construction order.

        Args:
            configs: Configuration as a FlowletConfig instance, a plain dict,
                or None to use environment variables / defaults.

        Returns:
            Fully configured Flowlet instance.

        Example:
            >>> # No storage — execution only
            >>> flowlet = Flowlet.configure()
            >>>
            >>> # Filesystem storage
            >>> flowlet = Flowlet.configure(
            ...     {"storage": {"type": "filesystem", "base_path": "./data"}}
            ... )
            >>>
            >>> # Typed config
            >>> flowlet = Flowlet.configure(FlowletConfig(storage=...))
        """
        if configs is None:
            configs = FlowletConfig()
        elif isinstance(configs, Mapping):
            configs = FlowletConfig.model_validate(configs)

        deps: dict = {}
        deps["configs"] = configs
        deps["registry"] = Registry(**deps)
        deps["context_manager"] = ContextManager(**deps)

        # Context logging must be configured before any repository is built so
        # that every log record emitted through 'flowlet.log' carries span context.
        from .logger import configure_context_logging
        configure_context_logging(deps["context_manager"])

        deps["querier"] = None
        if configs.storage is not None:
            from .logger import configure_run_file_logging
            from .repository.log import LogRepository
            from .api.repository import SnapshotRepository
            from .api.query import RunQuery

            storage_path = configs.storage_path
            assert storage_path is not None  # guaranteed: configs.storage is not None
            configure_run_file_logging(storage_path)

            # LogRepository and SnapshotRepository are added to deps so that
            # RunQuery(**deps) can pick them up via its own named parameters.
            deps["log_repo"] = LogRepository(base_path=storage_path)
            deps["snapshot_repo"] = SnapshotRepository.from_configs(configs=configs)
            deps["querier"] = RunQuery(**deps)

        deps["queue"] = None
        if configs.queue is not None:
            from .queue.config import AzureQueueStorageConfig, InMemoryQueueConfig
            from .queue.azure import AzureQueueStorage
            from .queue.memory import InMemoryQueue

            if isinstance(configs.queue, AzureQueueStorageConfig):
                deps["queue"] = AzureQueueStorage.setup(configs.queue)
            elif isinstance(configs.queue, InMemoryQueueConfig):
                deps["queue"] = InMemoryQueue.setup(configs.queue)

        from .api.controller import FlowController
        deps["controller"] = FlowController(**deps)

        from .api.router import build_router
        deps["router"] = build_router(**deps)

        return cls(**deps)

    def list_flows(self) -> list[str]:
        """Return the names of all registered flows.

        Returns:
            List of flow names.
        """
        return self.registry.list_flows()

    def list_tasks(self) -> list[str]:
        """Return the names of all registered tasks.

        Returns:
            List of task names.
        """
        return self.registry.list_tasks()

    def flow(self, name: str | None = None) -> Callable:
        """Decorator that registers a function as a flow.

        The decorated function is wrapped with instrumentation so every call
        is recorded as a structured root span in the active storage backend.

        Args:
            name: Override name for the flow; defaults to the function name.

        Returns:
            Decorator callable.

        Example:
            >>> @flowlet.flow()
            ... def my_workflow(x: int) -> int:
            ...     return my_task(x)
        """
        def _decorator(fn: Callable) -> Callable:
            return self.registry.register_flow(fn, name or fn.__name__)
        return _decorator

    def task(self, name: str | None = None) -> Callable:
        """Decorator that registers a function as a task.

        The decorated function is wrapped with instrumentation so every call
        is linked to the enclosing flow's run span.

        Args:
            name: Override name for the task; defaults to the function name.

        Returns:
            Decorator callable.

        Example:
            >>> @flowlet.task()
            ... def my_task(x: int) -> int:
            ...     return x * 2
        """
        def _decorator(fn: Callable) -> Callable:
            return self.registry.register_task(fn, name or fn.__name__)
        return _decorator

# ---
# endregion
