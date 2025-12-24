"""Protocol definition for flow registration interface.

This module defines the structural typing protocol for flow and task registration,
enabling type checking without concrete dependencies.
"""
from typing import Callable, Protocol


class FlowRegisterProtocol(Protocol):
    """Protocol for flow and task registration.

    Defines the interface for registering and accessing flows and tasks.
    Implementations should maintain registries of decorated functions
    and provide methods to list registered names.

    Attributes:
        flows: Registry mapping flow names to decorated callable functions.
        tasks: Registry mapping task names to decorated callable functions.

    Example:
        >>> register: FlowRegisterProtocol = FlowRegister(tracker=tracker)
        >>> flow_names = register.list_flows()
        >>> task_names = register.list_tasks()
    """
    flows: dict[str, Callable]
    tasks: dict[str, Callable]

    def list_flows(self) -> list[str]:
        """Return list of all registered flow names.

        Returns:
            list[str]: Names of registered flows.
        """
        ...

    def list_tasks(self) -> list[str]:
        """Return list of all registered task names.

        Returns:
            list[str]: Names of registered tasks.
        """
        ...

    def list_flows_and_tasks(self) -> list[str]:
        """Return list of all registered task names.

        Returns:
            list[str]: Names of registered tasks and flows.
        """
        return self.list_flows() + self.list_tasks()
