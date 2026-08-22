"""Flowlet framework entry point.

Provides the Flowlet class with flow/task registration decorators and the
configure() classmethod that wires the full dependency graph from a
FlowletConfig.
"""
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

logger = logging.getLogger(__name__)

from .config import FlowletConfig

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
    from .repository.resources import ResourceLeaseRepository
    from .repository.signals import SignalRepository
    from .repository.state import StateRepository
    from .repository.timers import TimerRepository


# region @app
# ---
# role: orchestration
# intent: framework façade — expose flow/task decorators and the configure() factory
# description: >
#   Flowlet is the kernel façade: it wires the account repositories, the
#   FlowController (account surface), and the kernel router from a
#   FlowletConfig.  It owns no registry and no authoring surface — those
#   are layer-2 concerns (taskflow's Taskflow facade wraps this one and
#   injects validate_kwargs / gate_policy plus its own routes).
#     - configure()  — classmethod building the dependency graph.
#     - get_router() — the account-surface APIRouter.
#   configure() accumulates a deps dict sequentially and passes it to each
#   component constructor.  Every constructor accepts **_ to absorb unknowns
#   so configure() does not need to cherry-pick arguments.
#   FlowletDeps documents the keys and their construction order.
# rules:
#   - MUST NOT implement execution or query logic; delegate to FlowController.
#   - configure() MUST set up context logging before constructing repositories.
#   - configure() MUST guard all storage-dependent components behind configs.storage.
# dependencies:
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
        log_repo: Span-file reader; present only when storage is configured.
        cache_repo: Pull-refreshed SQLite run cache; present only when
            storage is configured.
        querier: Read-side query object; None when no storage is configured.
        dispatch_repo: Dispatch-key → run_id mapping enabling idempotent
            submissions; present only when storage is configured.
        events: Run lifecycle event-log writer; present only when storage
            is configured.
        history: Run-history event-log writer and durable-projection reader
            (merged into RunQuery.list_recent_states); None unless
            configs.history is enabled with storage configured.
        queue: Async job queue; None when not configured.
        controller: FastAPI controller wiring all endpoint handlers.
    """
    configs: FlowletConfig
    log_repo: LogRepository
    state_repo: StateRepository
    signals: "SignalRepository | None"
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
        controller: FastAPI controller owning the account-surface endpoints.

    Example:
        >>> from flowlet import Flowlet
        >>> kernel = Flowlet.configure(
        ...     {"storage": {"type": "filesystem", "path": "./data"}}
        ... )
        >>> app.include_router(kernel.get_router(), prefix="/flowlet")

    Layer-2 controllers (taskflow's Prefect-like authoring, CodeFlow's agent
    orchestration) wrap this facade: they pass ``prepare_submission`` /
    ``gate_policy`` via ``extra_deps`` and mount their own routes beside the
    account surface.
    """

    # Declared attribute types: deps.get() would otherwise infer Unknown,
    # poisoning every downstream consumer (taskflow, codeflow).
    controller: "FlowController"
    router: "APIRouter"
    queue: "JobQueueProtocol | None"
    state_repo: "StateRepository | None"
    signals: "SignalRepository | None"
    timers: "TimerRepository | None"
    resources: "ResourceLeaseRepository | None"
    history: "RunHistory | None"
    dispatch_repo: "DispatchKeyRepository | None"
    events: "RunEventLog | None"

    def __init__(self, **deps):
        """Initialise Flowlet from the accumulated dependency map.

        Args:
            **deps: Full FlowletDeps dict produced by configure().
        """
        self.controller = deps["controller"]
        self.router = deps["router"]
        self.queue = deps.get("queue")
        self.state_repo = deps.get("state_repo")
        self.signals = deps.get("signals")
        self.timers = deps.get("timers")
        self.resources = deps.get("resources")
        self.history = deps.get("history")
        self.dispatch_repo = deps.get("dispatch_repo")
        self.events = deps.get("events")

    @classmethod
    def configure(
        cls,
        configs: FlowletConfig | Mapping | None = None,
        **extra_deps,
    ) -> "Flowlet":
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
            ...     {"storage": {"type": "filesystem", "path": "./data"}}
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
        deps.update(extra_deps)

        # Route 'flowlet.log' records into span events so user logging inside
        # flows lands in the run record.
        from .tracing import configure_run_logging, configure_tracing
        configure_run_logging()

        deps["querier"] = None
        deps["dispatch_repo"] = None
        deps["signals"] = None
        deps["timers"] = None
        deps["resources"] = None
        deps["events"] = None

        # Built before the storage block below so RunQuery(**deps) can pick
        # it up via its own named parameter.
        deps["history"] = None
        if configs.history and configs.store is not None:
            from .history import RunHistory

            deps["history"] = RunHistory(
                store=configs.store, db_path=configs.history_db_path
            )
            logger.info("flowlet_history_configured")

        if configs.storage is not None:
            from .api.cache import CacheRepository
            from .api.query import RunQuery
            from .repository.log import LogRepository

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
            from .repository.signals import SignalRepository
            from .repository.state import StateRepository
            deps["log_repo"] = LogRepository(store=store)
            from .repository.resources import ResourceLeaseRepository
            from .repository.timers import TimerRepository
            deps["state_repo"] = StateRepository(store=store)
            deps["signals"] = SignalRepository(store=store)
            deps["timers"] = TimerRepository(store=store)
            deps["resources"] = ResourceLeaseRepository(store=store)
            deps["dispatch_repo"] = DispatchKeyRepository(store=store)
            deps["events"] = RunEventLog(store=store)
            deps["cache_repo"] = CacheRepository.from_deps(**deps)
            deps["querier"] = RunQuery(**deps)

        deps["queue"] = None
        if configs.queue is not None:
            from .queue.azure import AzureQueueStorage
            from .queue.config import AzureQueueStorageConfig, InMemoryQueueConfig
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
            querier=deps["querier"],
            queue=deps["queue"],
            state_repo=deps.get("state_repo"),
            signals=deps.get("signals"),
            dispatch_repo=deps.get("dispatch_repo"),
            events=deps.get("events"),
            prepare_submission=deps.get("prepare_submission"),
            gate_policy=deps.get("gate_policy"),
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

# ---
# endregion
