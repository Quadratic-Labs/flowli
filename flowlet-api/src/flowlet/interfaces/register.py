from typing import Callable, Protocol


class FlowRegisterProtocol(Protocol):
    """
    Protocol defining the interface for flow registration.
    
    Attributes:
        flows: flows' register, mapping name to the flow's callable.
        tasks: tasks' register, mapping name to the task's callable.
    """
    flows: dict[str, Callable]
    tasks: dict[str, Callable]

    def list_flows(self) -> list[str]:
        """Return list of registered flow names."""
        ...

    def list_tasks(self) -> list[str]:
        """Return list of registered task names."""
        ...
