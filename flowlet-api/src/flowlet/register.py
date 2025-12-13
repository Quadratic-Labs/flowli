"""Flow and task registration system.

This module provides the FlowRegister class which manages registration of flows
and tasks, wrapping them with execution tracking context managers.
"""
import functools
from typing import Callable, Dict, TYPE_CHECKING

from .context import FlowContext, TaskContext
from .interfaces.repository.protocols import FlowTrackerProtocol


class FlowRegister:
    """Registry for flows and tasks with automatic execution tracking.

    Maintains registries of flows and tasks, providing decorators that wrap
    functions with execution context managers for database tracking.

    Attributes:
        tracker: Flow execution tracker for recording runs.
        db_session_factory: SQLAlchemy session factory.
        flows: Registry mapping flow names to decorated functions.
        tasks: Registry mapping task names to decorated functions.
    """
    def __init__(self, *, tracker: FlowTrackerProtocol, **_):
        """Initialize the flow register.

        Args:
            tracker: Flow tracker for recording execution.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.tracker = tracker
        self.db_session_factory = tracker.db_session_factory
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

    def flow(self, name: str | None=None):
        """Decorator to register a function as a flow.

        Wraps the function with FlowContext to track execution in the database.
        The flow name is stored on the wrapper function as __flow_name__.

        Args:
            name: Optional custom name for the flow. Defaults to function name.

        Returns:
            Callable: Decorator function.

        Raises:
            ValueError: If a flow with the same name is already registered.

        Example:
            >>> register = FlowRegister(tracker=tracker)
            >>> @register.flow()
            >>> def my_workflow():
            ...     return "done"
        """
        def _decorator(fn: Callable):
            flow_name = name or fn.__name__
            if flow_name in self.list_flows():
                raise ValueError(f"Flow {flow_name!r} already registered")
            context = FlowContext(flow_name, tracker=self.tracker)

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                with context:
                    result = fn(*args, **kwargs)
                return result

            wrapper.__flow_name__ = flow_name  # type: ignore
            self.flows[flow_name] = wrapper
            return wrapper

        return _decorator

    def task(self, name: str | None=None):
        """Decorator to register a function as a task.

        Wraps the function with TaskContext to track execution in the database.
        Tasks must be called within a flow context. The task name is stored
        on the wrapper function as __task_name__.

        Args:
            name: Optional custom name for the task. Defaults to function name.

        Returns:
            Callable: Decorator function.

        Raises:
            ValueError: If a task with the same name is already registered.

        Example:
            >>> register = FlowRegister(tracker=tracker)
            >>> @register.task()
            >>> def process_data():
            ...     return {"processed": True}
        """
        def _decorator(fn: Callable):
            task_name = name or fn.__name__
            if task_name in self.list_tasks():
                raise ValueError(f"Task {task_name!r} already registered")
            context = TaskContext(task_name, tracker=self.tracker)

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                with context:
                    return fn(*args, **kwargs)

            wrapper.__task_name__ = task_name  # type: ignore
            self.tasks[task_name] = wrapper
            return wrapper

        return _decorator