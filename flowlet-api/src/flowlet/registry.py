"""
Flow and task registration system.

This module provides the FlowRegister class which manages registration of flows
and tasks, wrapping them with execution tracking context managers.

It also implements schema generation logic from Python type hints for flow
arguments' validation and introspection.
"""
from inspect import Parameter, signature
import logging
from typing import Any, Callable, get_type_hints

from attrs import define
from pydantic import BaseModel, Field, create_model

from .instrumentation import instrument
from .models import SpanType

logger = logging.getLogger(__name__)


# region @registry.parameters
# ---
# role: core
# intent: schema and validation for flows' parameters
# description: >
#   Flows can be invoked through RPC with parameters defined via pydantic
#   models, explicitely defined or extracted from their signature.
# rules:
# dependencies:
# aliases:
# triggers:
# ---

@define(slots=True, kw_only=True)
class FlowParameterSchema:
    """Metadata about a single flow parameter."""
    name: str
    type_annotation: type
    default: Any
    required: bool
    description: str | None = None


@define(slots=True, kw_only=True)
class FlowSchema:
    """Complete schema for a flow including parameters and metadata."""

    flow_name: str
    parameters: list[FlowParameterSchema]
    pydantic_model: type[BaseModel]
    docstring: str | None = None
    return_type: type | None = None


def python_type_to_pydantic_field(
    param_name: str, param_type: Any, default: Any
) -> tuple[Any, Any]:
    """
    Convert Python type hint to Pydantic field configuration.

    Args:
        param_name: Name of the parameter
        param_type: Python type annotation (can be type, Any, or other typing constructs)
        default: Default value (Parameter.empty if required)

    Returns:
        Tuple of (field_type, field_info) for Pydantic create_model()
    """
    # Use the type annotation as-is (Pydantic handles Any and other typing constructs)
    field_type = param_type

    # Determine if field has a default value
    if default == Parameter.empty:
        # Required field - use ... as Pydantic's "required" marker
        field_info = Field(description=f"Parameter: {param_name}")
        return (field_type, field_info)
    else:
        # Optional field with default value
        return (field_type, default)


def extract_flow_schema(fn: Callable, flow_name: str) -> FlowSchema | None:
    """
    Extract schema from a flow function using type hints.

    Args:
        fn: The flow function to inspect
        flow_name: Name of the flow

    Returns:
        FlowSchema object or None if schema extraction fails

    Raises:
        Various exceptions if type hints are invalid or cannot be processed
    """
    try:
        # Get function signature
        sig = signature(fn)

        # Get type hints (this resolves string annotations and forward refs)
        try:
            type_hints = get_type_hints(fn)
        except Exception as e:
            logger.warning(
                f"Failed to get type hints for {flow_name}: {e}. "
                "Falling back to annotations."
            )
            # Fallback to __annotations__ if get_type_hints fails
            type_hints = getattr(fn, "__annotations__", {})

        # Extract parameter metadata
        parameters: list[FlowParameterSchema] = []
        pydantic_fields: dict[str, tuple[Any, Any]] = {}

        for param_name, param in sig.parameters.items():
            # Skip *args and **kwargs
            if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
                logger.debug(f"Skipping {param.kind} parameter: {param_name}")
                continue

            # Get type annotation (default to Any if not provided)
            type_annotation = type_hints.get(param_name, Any)

            # Determine if required
            required = param.default == Parameter.empty

            # Create parameter schema
            param_schema = FlowParameterSchema(
                name=param_name,
                type_annotation=type_annotation,
                default=param.default,
                required=required,
                description=None,  # Could extract from docstring in future
            )
            parameters.append(param_schema)

            # Build Pydantic field
            field_type, field_info = python_type_to_pydantic_field(
                param_name, type_annotation, param.default
            )
            pydantic_fields[param_name] = (field_type, field_info)

        # If there are no parameters, create a simple model
        if not pydantic_fields:
            pydantic_fields = {}

        # Create dynamic Pydantic model
        # The __base__ parameter ensures proper Pydantic inheritance
        pydantic_model = create_model(
            f"{flow_name}Arguments", **pydantic_fields  # type: ignore
        )

        # Get return type hint if available
        return_type = type_hints.get("return", None)

        # Extract docstring
        docstring = fn.__doc__

        return FlowSchema(
            flow_name=flow_name,
            parameters=parameters,
            pydantic_model=pydantic_model,
            docstring=docstring,
            return_type=return_type,
        )

    except Exception as e:
        logger.error(f"Failed to extract schema for {flow_name}: {e}", exc_info=True)
        return None

# ---
# endregion


# region @registry.registry
# ---
# role: core
# intent: registry for flows and tasks
# description: >
#   Global registry for flows and tasks, which are instrumented functions with
#   unique names. Instrumented means that they include proper run logging.
#   Registered flows can also be invoked through RPC, so the registry also
#   keeps flows' arguments schemas.
# rules:
# dependencies:
# aliases:
# triggers:
# ---

@define
class Registry:
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
    flow_schemas: dict[str, FlowSchema]

    def __init__(self, **_):
        """Initialize the flow register.

        Args:
            tracker: Flow tracker for recording execution.
            **_: Additional unused dependencies (for flexible dependency injection).
        """
        self.flows: dict[str, Callable] = {}
        self.tasks: dict[str, Callable] = {}
        self.flow_schemas: dict[str, FlowSchema] = {}
        self.task_schemas: dict[str, FlowSchema] = {}

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
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Registers a flow and extracts its schema from type hints.

        Args:
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

        wrapper = instrument(fn, flow_name, SpanType.flow)
        setattr(wrapper, "__flow_name__", flow_name)
        setattr(wrapper, "__flow_type__", SpanType.flow)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.flows[flow_name] = wrapper
        return wrapper

    def register_task(
        self,
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Registers a task and extracts its schema from type hints.

        Args:
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

        wrapper = instrument(fn, task_name, SpanType.task)
        setattr(wrapper, "__flow_name__", task_name)
        setattr(wrapper, "__flow_type__", SpanType.task)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.tasks[task_name] = wrapper
        return wrapper

# ---
# endregion
