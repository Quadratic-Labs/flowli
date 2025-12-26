"""Flow and task registration system.

This module provides the FlowRegister class which manages registration of flows
and tasks, wrapping them with execution tracking context managers.
"""
from typing import Callable, Dict

from attrs import define

from .interfaces.executor import ExecutorProtocol
from .interfaces.registry import RegistryProtocol


@define
class Registry(RegistryProtocol):
    """Registry for flows and tasks with automatic execution tracking.

    Maintains registries of flows and tasks, providing decorators that wrap
    functions with execution context managers for database tracking.

    Attributes:
        tracker: Flow execution tracker for recording runs.
        db_session_factory: SQLAlchemy session factory.
        flows: Registry mapping flow names to decorated functions.
        tasks: Registry mapping task names to decorated functions.
    """
    flows: dict[str, Callable]
    tasks: dict[str, Callable]

    def __init__(self, **_):
        """Initialize the flow register.

        Args:
            tracker: Flow tracker for recording execution.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.flows: Dict[str, Callable] = {}
        self.tasks: Dict[str, Callable] = {}

    def list_flows(self):
        """List all registered flow names.

        Returns:
            list[str]: Names of registered flows.
        """
        return list(self.flows.keys())

    def list_tasks(self):
        """List all registered task names.

        Returns:
            list[str]: Names of registered tasks.
        """
        return list(self.tasks.keys())

    def get_flow(self, name: str) -> Callable:
        return self.flows[name]

    def get_task(self, name: str) -> Callable:
        return self.tasks[name]

    def register_flow(
        self, 
        executor: ExecutorProtocol, 
        fn: Callable, 
        name: str | None = None,
    ) -> Callable:
        """Registers a flow"""
        flow_name = name or fn.__name__
        if flow_name in self.list_flows():
            raise ValueError(f"Flow {flow_name!r} already registered")
        wrapper = executor.executable(fn, flow_name, "flow")
        wrapper.__flow_name__ = flow_name  # type: ignore
        wrapper.__flow_type__ = "flow"  # type: ignore
        self.flows[flow_name] = wrapper
        return wrapper

    def register_task(
        self, 
        executor: ExecutorProtocol, 
        fn: Callable, 
        name: str | None = None,
    ) -> Callable:
        """Registers a task"""
        task_name = name or fn.__name__
        if task_name in self.list_tasks():
            raise ValueError(f"Flow {task_name!r} already registered")
        wrapper = executor.executable(fn, task_name, "task")
        wrapper.__flow_name__ = task_name  # type: ignore
        wrapper.__flow_type__ = "task"  # type: ignore
        self.tasks[task_name] = wrapper
        return wrapper