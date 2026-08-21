"""Taskflow — the Prefect-like flow-run controller (layer 2).

Taskflow wraps the Flowlet kernel with the authoring experience: ``@flow``/
``@task`` decorators, schema-validated submission with per-flow defaults,
synchronous execution, and the in-process worker that runs registered
callables through the kernel's executor seam.  The kernel stays ignorant of
all of it — taskflow injects two hooks (``prepare_submission``,
``gate_policy``) and mounts its authoring routes beside the account
surface.
"""
import logging
from collections.abc import Callable, Mapping

from flowlet.app import Flowlet
from flowlet.config import FlowletConfig

from .registry import Registry

logger = logging.getLogger(__name__)


class Taskflow:
    """Facade for Prefect-like task orchestration over the account kernel.

    Attributes:
        kernel: The wrapped Flowlet kernel facade (repositories, account
            surface, router).
        registry: Flow and task registry (decorated callables + options).

    Example:
        >>> from taskflow import Taskflow
        >>> tf = Taskflow.configure(
        ...     {"storage": {"type": "filesystem", "path": "./data"},
        ...      "queue": {"type": "memory"}}
        ... )
        >>>
        >>> @tf.flow(timeout=600, max_retries=5)
        ... def my_workflow(x: int) -> int:
        ...     return my_task(x)
        >>>
        >>> @tf.task()
        ... def my_task(x: int) -> int:
        ...     return x * 2
        >>>
        >>> app.include_router(tf.get_router(), prefix="/flowlet")
    """

    def __init__(self, kernel: Flowlet, registry: Registry):
        self.kernel = kernel
        self.registry = registry
        self._router = None

    @classmethod
    def configure(cls, configs: FlowletConfig | Mapping | None = None) -> "Taskflow":
        """Wire the kernel with taskflow's hooks and return the facade.

        The registry-backed hooks: submissions are schema-validated and
        filled with the flow's declared timeout/budget; adjudication
        eligibility comes from the flow's gate policy.
        """
        registry = Registry()

        def prepare_submission(flow_name, payload):
            from fastapi import HTTPException

            if flow_name not in registry.list_flows():
                raise HTTPException(status_code=404, detail="Flow not found")
            schema = registry.get_flow_schema(flow_name)
            if schema is not None:
                from pydantic import ValidationError

                try:
                    validated = schema.pydantic_model(**(payload.kwargs or {}))
                    payload = payload.model_copy(
                        update={"kwargs": validated.model_dump()}
                    )
                except ValidationError as e:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "message": "Invalid flow arguments",
                            "flow": flow_name,
                            "errors": e.errors(),
                        },
                    )
            options = registry.get_flow_options(flow_name)
            return payload.model_copy(
                update={
                    "timeout_seconds": payload.timeout_seconds
                    or options.timeout_seconds,
                    "max_retries": payload.max_retries or options.max_retries,
                }
            )

        def gate_policy(flow_name):
            return registry.get_flow_options(flow_name).gate

        kernel = Flowlet.configure(
            configs,
            prepare_submission=prepare_submission,
            gate_policy=gate_policy,
        )
        return cls(kernel, registry)

    # -- authoring -----------------------------------------------------------

    def flow(
        self,
        name: str | None = None,
        *,
        timeout: int | None = None,
        max_retries: int = 3,
        gated: bool = False,
        gate: Callable | None = None,
    ) -> Callable:
        """Decorator registering a function as a flow.

        Args:
            name: Override name; defaults to the function name.
            timeout: Lease duration in seconds per execution attempt.
            max_retries: Attempt budget.
            gated: Suspend as awaiting_adjudication when an attempt returns,
                instead of the auto-verdict.
            gate: Adjudication eligibility policy ``(actor, record) → bool``.
        """
        def _decorator(fn: Callable) -> Callable:
            return self.registry.register_flow(
                fn, name or fn.__name__, timeout=timeout,
                max_retries=max_retries, gated=gated, gate=gate,
            )
        return _decorator

    def task(self, name: str | None = None) -> Callable:
        """Decorator registering a function as a task (span-instrumented)."""
        def _decorator(fn: Callable) -> Callable:
            return self.registry.register_task(fn, name or fn.__name__)
        return _decorator

    def adjudication_for(self, flow_name: str) -> str:
        """The adjudication policy stamped on this flow's new obligations."""
        return "gated" if self.registry.get_flow_options(flow_name).gated else "auto"

    # -- surfaces --------------------------------------------------------------

    def get_router(self):
        """The combined router: authoring surface + the kernel's account surface."""
        from .api import build_taskflow_router

        router = build_taskflow_router(self)
        router.include_router(self.kernel.router)
        return router

    @property
    def router(self):
        """The combined router, built once."""
        if self._router is None:
            self._router = self.get_router()
        return self._router

    def start(self) -> None:
        """Kernel lifecycle passthrough."""
        self.kernel.start()

    def stop(self) -> None:
        """Kernel lifecycle passthrough."""
        self.kernel.stop()

    # kernel passthroughs the worker/sweeper CLI and tests rely on
    @property
    def queue(self):
        return self.kernel.queue

    @property
    def state_repo(self):
        return self.kernel.state_repo

    @property
    def signals(self):
        return self.kernel.signals

    @property
    def timers(self):
        return self.kernel.timers

    @property
    def events(self):
        return self.kernel.events

    @property
    def history(self):
        return self.kernel.history
