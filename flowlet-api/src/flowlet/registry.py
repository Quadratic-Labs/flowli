"""
Flow and task registration system.

This module provides the :class:`Registry` class which manages registration of
flows and tasks, wrapping them with execution instrumentation.

It also implements schema generation logic from Python type hints for flow
argument validation and introspection.
"""
from inspect import Parameter, signature
import logging
from typing import Any, Callable, get_type_hints

from attrs import define
from pydantic import BaseModel, Field, create_model

from .instrumentation import instrument
from .models import RunType

logger = logging.getLogger(__name__)


# region @registry.parameters
# ---
# role: core
# intent: extract and validate flow/task parameter schemas from type hints
# description: >
#   Flows can be invoked through RPC with parameters validated via dynamically
#   generated Pydantic models. Schemas are derived from Python type annotations,
#   either explicitly declared or inferred from the function signature.
# rules:
#   - MUST extract type hints before the function is wrapped by instrumentation
#   - SHOULD fall back to __annotations__ when get_type_hints() raises
#   - MUST skip *args and **kwargs parameters
# dependencies:
# aliases:
#   - schema
#   - parameters
#   - flow-schema
# triggers:
#   - flow parameters
#   - schema validation
#   - pydantic model from signature
#   - extract flow schema
# ---

@define(slots=True, kw_only=True)
class FlowParameterSchema:
    """Metadata about a single flow parameter.

    Attributes:
        name (str): Parameter name as it appears in the function signature.
        type_annotation (type): Resolved Python type annotation; Any when
            no annotation is present.
        default (Any): Default value, or inspect.Parameter.empty if required.
        required (bool): True when the parameter has no default value.
        description (str | None): Optional human-readable description,
            reserved for future docstring extraction.
    """
    name: str
    type_annotation: type
    default: Any
    required: bool
    description: str | None = None


@define(slots=True, kw_only=True)
class FlowSchema:
    """Complete schema for a flow, including all parameters and metadata.

    Attributes:
        flow_name (str): Registered name of the flow.
        parameters (list[FlowParameterSchema]): Ordered list of parameter
            descriptors extracted from the function signature.
        pydantic_model (type[BaseModel]): Dynamically generated Pydantic model
            used for argument validation and serialisation on RPC invocation.
        docstring (str | None): Raw docstring of the flow function, if any.
        return_type (type | None): Resolved return type annotation, or None
            if absent.
    """

    flow_name: str
    parameters: list[FlowParameterSchema]
    pydantic_model: type[BaseModel]
    docstring: str | None = None
    return_type: type | None = None


def python_type_to_pydantic_field(
    param_name: str, param_type: Any, default: Any
) -> tuple[Any, Any]:
    """Convert a Python type hint to a Pydantic field configuration.

    Args:
        param_name (str): Name of the parameter, used in the field description.
        param_type (Any): Python type annotation; may be a concrete type, Any,
            or any typing construct accepted by Pydantic.
        default (Any): Default value, or Parameter.empty if the field is
            required.

    Returns:
        tuple[Any, Any]: A (field_type, field_info) pair suitable for passing
            to pydantic.create_model().
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
    """Extract a FlowSchema from a callable using its type hints.

    Args:
        fn (Callable): The flow or task function to inspect.
        flow_name (str): Name used to label the schema and the generated
            Pydantic model.

    Returns:
        FlowSchema | None: The extracted schema, or None if extraction fails.
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
# intent: store and expose instrumented flows and tasks by name
# description: >
#   Global registry for flows and tasks. Each registered callable is wrapped
#   by the instrumentation layer so every call emits structured run telemetry.
#   Registered flows can also be invoked through RPC, so the registry keeps
#   their argument schemas for validation.
# rules:
#   - MUST extract schema from the original function before wrapping it
#   - MUST NOT allow duplicate flow or task names
#   - SHOULD log a warning and continue when schema extraction fails
# dependencies:
#   - registry.parameters
#   - instrumentation
#   - models
# aliases:
#   - flow-registry
#   - register
# triggers:
#   - register a flow
#   - register a task
#   - how to add a flow
#   - list registered flows
# ---

@define
class Registry:
    """Registry for flows and tasks with automatic execution instrumentation.

    Stores instrumented callables indexed by name and provides schema lookup
    for RPC invocation. Each callable registered through this class is wrapped
    by instrument() so that every call records a structured run entry.

    Attributes:
        flows (dict[str, Callable]): Mapping of flow name to instrumented
            callable.
        tasks (dict[str, Callable]): Mapping of task name to instrumented
            callable.
        flow_schemas (dict[str, FlowSchema]): Mapping of flow name to its
            extracted FlowSchema.
        task_schemas (dict[str, FlowSchema]): Mapping of task name to its
            extracted FlowSchema.
    """
    flows: dict[str, Callable]
    tasks: dict[str, Callable]
    flow_schemas: dict[str, FlowSchema]
    task_schemas: dict[str, FlowSchema]

    def __init__(self, **_):
        """Initialize the registry.

        Args:
            **_: Unused keyword arguments accepted for flexible dependency
                injection compatibility.
        """
        self.flows: dict[str, Callable] = {}
        self.tasks: dict[str, Callable] = {}
        self.flow_schemas: dict[str, FlowSchema] = {}
        self.task_schemas: dict[str, FlowSchema] = {}

    def __contains__(self, name: str) -> bool:
        """Return True if name is a registered flow."""
        return name in self.flows.keys()

    def has_flow(self, name: str) -> bool:
        """Return True if a flow with the given name is registered."""
        return name in self.flows.keys()

    def has_task(self, name: str) -> bool:
        """Return True if a task with the given name is registered."""
        return name in self.tasks.keys()

    def list_flows(self):
        """Return the names of all registered flows.

        Returns:
            list[str]: Names of registered flows.
        """
        return list(self.flows.keys())

    def list_tasks(self):
        """Return the names of all registered tasks.

        Returns:
            list[str]: Names of registered tasks.
        """
        return list(self.tasks.keys())

    def get_flow(self, name: str) -> Callable:
        """Return the instrumented callable for the named flow.

        Args:
            name (str): Registered flow name.

        Returns:
            Callable: The wrapped callable.

        Raises:
            KeyError: If no flow with name is registered.
        """
        return self.flows[name]

    def get_task(self, name: str) -> Callable:
        """Return the instrumented callable for the named task.

        Args:
            name (str): Registered task name.

        Returns:
            Callable: The wrapped callable.

        Raises:
            KeyError: If no task with name is registered.
        """
        return self.tasks[name]

    def get_flow_schema(self, name: str) -> FlowSchema | None:
        """Return the schema for a registered flow.

        Args:
            name (str): Registered flow name.

        Returns:
            FlowSchema | None: The schema if available, None otherwise.
        """
        return self.flow_schemas.get(name)

    def get_task_schema(self, name: str) -> FlowSchema | None:
        """Return the schema for a registered task.

        Args:
            name (str): Registered task name.

        Returns:
            FlowSchema | None: The schema if available, None otherwise.
        """
        return self.task_schemas.get(name)

    def register_flow(
        self,
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Register a flow and extract its schema from type hints.

        Args:
            fn (Callable): The flow function to register.
            name (str | None): Custom name for the flow; defaults to
                fn.__name__.

        Returns:
            Callable: The instrumented flow callable.

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

        wrapper = instrument(fn, flow_name, RunType.flow)
        setattr(wrapper, "__flow_name__", flow_name)
        setattr(wrapper, "__flow_type__", RunType.flow)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.flows[flow_name] = wrapper
        return wrapper

    def register_task(
        self,
        fn: Callable,
        name: str | None = None,
    ) -> Callable:
        """Register a task and extract its schema from type hints.

        Args:
            fn (Callable): The task function to register.
            name (str | None): Custom name for the task; defaults to
                fn.__name__.

        Returns:
            Callable: The instrumented task callable.

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

        wrapper = instrument(fn, task_name, RunType.task)
        setattr(wrapper, "__flow_name__", task_name)
        setattr(wrapper, "__flow_type__", RunType.task)
        if schema is not None:
            setattr(wrapper, "__flow_schema__", schema)
        self.tasks[task_name] = wrapper
        return wrapper

# ---
# endregion
