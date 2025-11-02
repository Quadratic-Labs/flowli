from typing import Callable, Protocol


class FlowRegisterProtocol(Protocol):
    """Protocol defining the interface for flow registration.

    This protocol breaks the circular dependency between FlowRepository
    and FlowRegister by defining only the interface that FlowRepository
    needs, without importing FlowRegister directly.
    """
    flows: dict[str, Callable]
    tasks: dict[str, Callable]

    def list_flows(self) -> list[str]:
        """Return list of registered flow names."""
        ...

    def list_tasks(self) -> list[str]:
        """Return list of registered task names."""
        ...
