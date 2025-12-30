"""Schema generation from Python type hints for flow validation and introspection."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from inspect import Parameter, signature
from typing import Any, Callable, get_type_hints

from pydantic import BaseModel, Field, create_model

logger = logging.getLogger(__name__)


@dataclass
class FlowParameterSchema:
    """Metadata about a single flow parameter."""

    name: str
    type_annotation: type
    default: Any
    required: bool
    description: str | None = None


@dataclass
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
