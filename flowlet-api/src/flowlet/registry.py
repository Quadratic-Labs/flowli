"""Flow and task registration system.

This module provides the FlowRegister class which manages registration of flows
and tasks, wrapping them with execution tracking context managers.
"""
import logging
from typing import Callable, Dict

from attrs import define

from .interfaces.executor import ExecutorProtocol
from .interfaces.registry import RegistryProtocol
from .schema_generator import FlowSchema, extract_flow_schema
from .types import SpanType

logger = logging.getLogger(__name__)


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
        flow_schemas: Registry mapping flow names to their type schemas.
        task_schemas: Registry mapping task names to their type schemas.
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
        self.flow_schemas: Dict[str, FlowSchema] = {}
        self.task_schemas: Dict[str, FlowSchema] = {}

    def __contains__(self, name: str) -> bool:
        return name in self.flows.keys()

    def has_flow(self, name: str) -> bool:
        return name in self.flows.keys()

    def has_task(self, name: str) -> bool:
        return name in self.tasks.keys()

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

    def get_flow_schema(self, name: str) -> FlowSchema | None:
        """Get the schema for a registered flow.

        Args:
            name: Name of the flow.

        Returns:
            FlowSchema object if available, None otherwise.
        """
        return self.flow_schemas.get(name)

    def get_task_schema(self, name: str) -> FlowSchema | None:
        """Get the schema for a registered task.

        Args:
            name: Name of the task.

        Returns:
            FlowSchema object if available, None otherwise.
        """
        return self.task_schemas.get(name)

    def register_flow(
        self,
        executor: ExecutorProtocol,
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Registers a flow and extracts its schema from type hints.

        Args:
            executor: Executor to wrap the function with.
            fn: The flow function to register.
            name: Optional custom name for the flow (defaults to function name).

        Returns:
            The wrapped flow function.

        Raises:
            ValueError: If a flow with the same name is already registered.
        """
        flow_name = name or fn.__name__
        if self.has_flow(flow_name):
            raise ValueError(f"Flow {flow_name!r} already registered")

        # Generate schema from original function (before wrapping)
        try:
            schema = extract_flow_schema(fn, flow_name)
            if schema is not None:
                self.flow_schemas[flow_name] = schema
                logger.debug(f"Generated schema for flow {flow_name!r}")
            else:
                logger.warning(
                    f"Could not generate schema for flow {flow_name!r}. "
                    "Add type hints to enable schema validation."
                )
        except Exception as e:
            # Graceful degradation: log warning, continue without schema
            logger.warning(
                f"Failed to generate schema for flow {flow_name!r}: {e}",
                exc_info=True
            )
            schema = None

        # Wrap function with executor
        wrapper = executor.executable(fn, flow_name, SpanType.flow)
        setattr(wrapper, "__flow_name__", flow_name)
        setattr(wrapper, "__flow_type__", SpanType.flow)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.flows[flow_name] = wrapper
        return wrapper

    def register_task(
        self,
        executor: ExecutorProtocol,
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Registers a task and extracts its schema from type hints.

        Args:
            executor: Executor to wrap the function with.
            fn: The task function to register.
            name: Optional custom name for the task (defaults to function name).

        Returns:
            The wrapped task function.

        Raises:
            ValueError: If a task with the same name is already registered.
        """
        task_name = name or fn.__name__
        if self.has_task(task_name):
            raise ValueError(f"Task {task_name!r} already registered")

        # Generate schema from original function (before wrapping)
        try:
            schema = extract_flow_schema(fn, task_name)
            if schema is not None:
                self.task_schemas[task_name] = schema
                logger.debug(f"Generated schema for task {task_name!r}")
        except Exception as e:
            logger.warning(
                f"Failed to generate schema for task {task_name!r}: {e}",
                exc_info=True
            )
            schema = None

        # Wrap function with executor
        wrapper = executor.executable(fn, task_name, SpanType.task)
        setattr(wrapper, "__flow_name__", task_name)
        setattr(wrapper, "__flow_type__", SpanType.task)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.tasks[task_name] = wrapper
        return wrapper