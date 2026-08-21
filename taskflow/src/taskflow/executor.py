"""RegistryExecutor — the in-process implementation of the kernel's seam.

The kernel's :class:`flowlet.worker.Executor` protocol is the boundary
between claim machinery and work.  This implementation resolves the
obligation's flow to a registered Python callable and invokes it under the
run's tracing root, flushing spans before the kernel writes the terminal
account state.  It is taskflow's half of the same role CodeFlow's harness
plays for detached agents.
"""
import logging

from flowlet import tracing
from flowlet.models import Obligation

from .registry import Registry

logger = logging.getLogger(__name__)


class RegistryExecutor:
    """Executes obligations by invoking their registered flow callables.

    Attributes:
        registry: The flow registry (decorated callables + options).
    """

    def __init__(self, registry: Registry):
        self.registry = registry

    def execute(self, obligation: Obligation, attempt: int) -> None:
        """Invoke the registered callable under the attempt's trace root.

        Exceptions propagate to the kernel, which accounts them
        (RunCancelled → interrupted, anything else → raised); spans flush
        in ``finally`` so the run record is complete before the terminal
        account write.
        """
        fn = self.registry.get_flow(obligation.flow_name)
        try:
            with tracing.run_root(
                obligation.id, obligation.flow_name, attempt=attempt
            ):
                fn(**obligation.kwargs)
        finally:
            tracing.force_flush()
