"""Flowlet framework entry point.

Provides the Flowlet class with flow/task registration decorators and the
configure() classmethod that wires the full dependency graph from a
FlowletConfig.
"""
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable, TypedDict

logger = logging.getLogger(__name__)

from .config import FlowletConfig
from .registry import Registry

if TYPE_CHECKING:
    from fastapi import APIRouter
    from .api.cache import CacheRepository
    from .api.controller import FlowController
    from .api.query import RunQuery
    from .events import RunEventLog
    from .history import RunHistory
    from .queue import JobQueueProtocol
    from .repository.dispatch import DispatchKeyRepository
    from .repository.log import LogRepository
    from .repository.state import StateRepository


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
        log_repo: Span-file reader; present only when storage is configured.
        cache_repo: Pull-refreshed SQLite run cache; present only when
            storage is configured.
        querier: Read-side query object; None when no storage is configured.
        dispatch_repo: Dispatch-key → run_id mapping enabling idempotent
            submissions; present only when storage is configured.
        events: Run lifecycle event-log writer; present only when storage
            is configured.
        history: Run-history event-log writer; None unless configs.history
            is enabled with storage configured.
        queue: Async job queue; None when not configured.
        controller: FastAPI controller wiring all endpoint handlers.
    """
    configs: FlowletConfig
    registry: Registry
    log_repo: LogRepository
    state_repo: StateRepository
    cache_repo: CacheRepository
    querier: RunQuery | None
    dispatch_repo: "DispatchKeyRepository | None"
    events: "RunEventLog | None"
    history: "RunHistory | None"
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
        self.queue = deps.get("queue")
        self.state_repo = deps.get("state_repo")
        self.history = deps.get("history")
        self.dispatch_repo = deps.get("dispatch_repo")
        self.events = deps.get("events")

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

        logger.info("flowlet_configure_start")

        deps: dict = {}
        deps["configs"] = configs
        deps["registry"] = Registry(**deps)

        # Route 'flowlet.log' records into span events so user logging inside
        # flows lands in the run record.
        from .tracing import configure_run_logging, configure_tracing
        configure_run_logging()

        deps["querier"] = None
        deps["dispatch_repo"] = None
        deps["events"] = None
        if configs.storage is not None:
            from .repository.log import LogRepository
            from .api.cache import CacheRepository
            from .api.query import RunQuery

            store = configs.store
            assert store is not None  # guaranteed: configs.storage is not None
            configure_tracing(store)
            logger.info(
                "flowlet_storage_configured",
                extra={"store": type(store).__name__},
            )

            # LogRepository and CacheRepository are added to deps so that
            # RunQuery(**deps) can pick them up via its own named parameters.
            from .events import RunEventLog
            from .repository.dispatch import DispatchKeyRepository
            from .repository.state import StateRepository
            deps["log_repo"] = LogRepository(store=store)
            deps["state_repo"] = StateRepository(store=store)
            deps["dispatch_repo"] = DispatchKeyRepository(store=store)
            deps["events"] = RunEventLog(store=store)
            deps["cache_repo"] = CacheRepository.from_deps(**deps)
            deps["querier"] = RunQuery(**deps)

        deps["history"] = None
        if configs.history and configs.store is not None:
            from .history import RunHistory

            deps["history"] = RunHistory(store=configs.store)
            logger.info("flowlet_history_configured")

        deps["queue"] = None
        if configs.queue is not None:
            from .queue.config import AzureQueueStorageConfig, InMemoryQueueConfig
            from .queue.azure import AzureQueueStorage
            from .queue.memory import InMemoryQueue

            if isinstance(configs.queue, AzureQueueStorageConfig):
                deps["queue"] = AzureQueueStorage.setup(configs.queue)
            elif isinstance(configs.queue, InMemoryQueueConfig):
                deps["queue"] = InMemoryQueue.setup(configs.queue)
            if deps["queue"] is not None:
                logger.info(
                    "flowlet_queue_configured",
                    extra={"queue_type": type(deps["queue"]).__name__},
                )

        from .api.controller import FlowController
        deps["controller"] = FlowController(
            registry=deps["registry"],
            querier=deps["querier"],
            queue=deps["queue"],
            state_repo=deps.get("state_repo"),
            dispatch_repo=deps.get("dispatch_repo"),
            events=deps.get("events"),
        )

        from .api.router import build_router
        deps["router"] = build_router(**deps)

        logger.info("flowlet_configured")
        return cls(**deps)

    def start(self) -> None:
        """No-op retained for API compatibility.

        The pull-based read side has no background services to start: the
        run cache refreshes lazily inside query calls.
        """
        logger.info("flowlet_started")

    def stop(self) -> None:
        """No-op retained for API compatibility."""
        logger.info("flowlet_stopped")

    @property
    def lifespan(self):
        """FastAPI lifespan context manager.

        Nothing needs starting or stopping — the API is fully stateless
        (scale-to-zero safe) — but the hook is kept so applications can pass
        ``FastAPI(lifespan=flowlet.lifespan)`` uniformly.
        """
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _lifespan(app):
            self.start()
            try:
                yield
            finally:
                self.stop()

        return _lifespan

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

    def flow(
        self,
        name: str | None = None,
        *,
        timeout: int | None = None,
        max_retries: int = 3,
    ) -> Callable:
        """Decorator that registers a function as a flow.

        The decorated function is wrapped with instrumentation so every call
        is recorded as a structured root span in the active storage backend.

        Args:
            name: Override name for the flow; defaults to the function name.
            timeout: Lease duration in seconds per execution attempt.  A
                worker claiming this flow sets ``deadline_at = now + timeout``;
                a run past its deadline is reclaimable by the sweeper or
                another worker.  None uses the worker default.
            max_retries: Maximum number of execution attempts.

        Returns:
            Decorator callable.

        Example:
            >>> @flowlet.flow(timeout=600, max_retries=5)
            ... def my_workflow(x: int) -> int:
            ...     return my_task(x)
        """
        def _decorator(fn: Callable) -> Callable:
            return self.registry.register_flow(
                fn, name or fn.__name__, timeout=timeout, max_retries=max_retries
            )
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
