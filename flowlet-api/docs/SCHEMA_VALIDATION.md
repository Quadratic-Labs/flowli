# Schema Validation and Introspection

This document describes the schema validation and introspection features added to Flowlet.

## Overview

Flowlet now automatically generates schemas from your flow function type hints, providing:

- **Runtime validation**: Arguments are validated against type hints before execution
- **Auto-generated OpenAPI documentation**: Better API docs in Swagger UI
- **Schema introspection endpoints**: Programmatic access to flow parameter schemas
- **Type coercion**: Automatic conversion of compatible types (e.g., `"123"` → `123`)
- **Backwards compatibility**: Untyped flows continue to work without validation

## Quick Start

### Adding Type Hints to Flows

Simply add Python type hints to your flow parameters:

```python
from flowlet import configure

flowlet = configure()

@flowlet.flow()
def process_data(user_id: int, name: str, active: bool = True) -> dict:
    """Process user data."""
    return {"user_id": user_id, "name": name, "active": active}
```

That's it! Flowlet will automatically:
1. Extract the type schema during flow registration
2. Validate arguments when executing via API
3. Provide schema introspection endpoints

### Making API Requests

**Valid request:**
```bash
curl -X POST http://localhost:8000/execute/process_data \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"user_id": 123, "name": "Alice"}}'
```

**Invalid request (wrong type):**
```bash
curl -X POST http://localhost:8000/execute/process_data \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"user_id": "not_a_number", "name": "Alice"}}'
```

Response (HTTP 422):
```json
{
  "message": "Invalid flow arguments",
  "flow": "process_data",
  "errors": [
    {
      "loc": ["user_id"],
      "msg": "value is not a valid integer",
      "type": "type_error.integer"
    }
  ]
}
```

## Schema Introspection Endpoints

### List All Flows with Schemas

**Endpoint:** `GET /flows`

Returns metadata for all registered flows including parameter information.

**Example Response:**
```json
[
  {
    "name": "process_data",
    "docstring": "Process user data.",
    "has_schema": true,
    "parameters": [
      {"name": "user_id", "type": "int", "required": true},
      {"name": "name", "type": "str", "required": true},
      {"name": "active", "type": "bool", "required": false}
    ]
  },
  {
    "name": "legacy_flow",
    "docstring": "Old flow without type hints.",
    "has_schema": false
  }
]
```

### Get Schema for a Specific Flow

**Endpoint:** `GET /flows/{flow_name}/schema`

Returns detailed schema for a specific flow, including JSON Schema format.

**Example Response:**
```json
{
  "flow_name": "process_data",
  "docstring": "Process user data.",
  "has_schema": true,
  "parameters": [
    {
      "name": "user_id",
      "type": "int",
      "required": true,
      "default": null,
      "description": null
    },
    {
      "name": "name",
      "type": "str",
      "required": true,
      "default": null,
      "description": null
    },
    {
      "name": "active",
      "type": "bool",
      "required": false,
      "default": true,
      "description": null
    }
  ],
  "json_schema": {
    "type": "object",
    "properties": {
      "user_id": {"type": "integer"},
      "name": {"type": "string"},
      "active": {"type": "boolean", "default": true}
    },
    "required": ["user_id", "name"]
  }
}
```

## Supported Type Hints

### Basic Types

All Python built-in types are supported:

```python
@flowlet.flow()
def example_flow(
    count: int,
    name: str,
    active: bool,
    ratio: float
) -> dict:
    ...
```

### Optional Parameters

Use default values to make parameters optional:

```python
@flowlet.flow()
def example_flow(
    required_param: str,
    optional_param: int = 42
) -> dict:
    ...
```

### Complex Types

Use Python's typing module for complex types:

```python
from typing import List, Dict, Optional

@flowlet.flow()
def example_flow(
    ids: List[int],
    metadata: Dict[str, str],
    config: Optional[str] = None
) -> dict:
    ...
```

### Pydantic Models

For complex nested objects, use Pydantic models:

```python
from pydantic import BaseModel

class UserConfig(BaseModel):
    username: str
    age: int
    email: str

@flowlet.flow()
def process_user(config: UserConfig) -> dict:
    return {
        "user": config.username,
        "age": config.age
    }
```

**API Request:**
```bash
curl -X POST http://localhost:8000/execute/process_user \
  -H "Content-Type: application/json" \
  -d '{
    "kwargs": {
      "config": {
        "username": "alice",
        "age": 30,
        "email": "alice@example.com"
      }
    }
  }'
```

## Type Coercion

Pydantic automatically coerces compatible types:

```python
@flowlet.flow()
def math_flow(x: int, y: float) -> dict:
    return {"sum": x + y}
```

**Request with string values:**
```json
{"kwargs": {"x": "123", "y": "45.6"}}
```

**Automatically coerced to:**
```json
{"kwargs": {"x": 123, "y": 45.6}}
```

## Backwards Compatibility

Flows without type hints continue to work without validation:

```python
@flowlet.flow()
def legacy_flow(data, options):
    """Old flow without type hints - still works!"""
    return {"data": data}
```

This flow:
- ✓ Still executes normally
- ✓ Accepts any arguments via API
- ✓ Shows `has_schema: false` in introspection endpoints
- ✓ No validation errors

## Error Handling

### Validation Errors

When validation fails, you'll receive HTTP 422 with detailed error information:

```json
{
  "message": "Invalid flow arguments",
  "flow": "process_data",
  "errors": [
    {
      "loc": ["user_id"],
      "msg": "field required",
      "type": "value_error.missing"
    },
    {
      "loc": ["active"],
      "msg": "value is not a valid boolean",
      "type": "type_error.bool"
    }
  ]
}
```

Each error includes:
- `loc`: Field location (path in nested objects)
- `msg`: Human-readable error message
- `type`: Error type for programmatic handling

### Flow Not Found

HTTP 404 if the flow doesn't exist:

```json
{
  "detail": "Flow not found"
}
```

## Best Practices

### 1. Always Add Type Hints

Add type hints to all new flows for better API documentation and validation:

```python
# Good ✓
@flowlet.flow()
def good_flow(x: int, y: str) -> dict:
    return {"x": x, "y": y}

# Avoid ✗
@flowlet.flow()
def bad_flow(x, y):
    return {"x": x, "y": y}
```

### 2. Add Docstrings

Docstrings appear in the schema introspection responses:

```python
@flowlet.flow()
def process_order(order_id: int, priority: str = "normal") -> dict:
    """
    Process a customer order.

    Args:
        order_id: Unique order identifier
        priority: Order priority level (normal, high, urgent)
    """
    ...
```

### 3. Use Descriptive Parameter Names

Clear parameter names improve API usability:

```python
# Good ✓
@flowlet.flow()
def send_email(recipient_email: str, subject: str, body: str):
    ...

# Avoid ✗
@flowlet.flow()
def send_email(e: str, s: str, b: str):
    ...
```

### 4. Provide Sensible Defaults

Use defaults for optional parameters:

```python
@flowlet.flow()
def fetch_data(
    limit: int = 100,
    offset: int = 0,
    include_metadata: bool = True
):
    ...
```

### 5. Use Pydantic Models for Complex Objects

Instead of deeply nested dicts, use Pydantic models:

```python
# Good ✓
class FilterConfig(BaseModel):
    min_value: float
    max_value: float
    include_nulls: bool = False

@flowlet.flow()
def filter_data(filters: FilterConfig):
    ...

# Avoid ✗
@flowlet.flow()
def filter_data(filters: dict):  # What fields does this dict have?
    ...
```

## OpenAPI/Swagger UI

The enhanced API automatically appears in Swagger UI at `/docs`:

- Execute endpoint shows validation information
- Schema endpoints are grouped under "Introspection" tag
- Query endpoints are grouped under "Query" tag

## Client Code Generation

Use the schema introspection endpoints to generate type-safe clients:

### TypeScript Example

```typescript
// Fetch schema
const schema = await fetch('/flows/process_data/schema').then(r => r.json());

// Generate TypeScript interface
interface ProcessDataArgs {
  user_id: number;
  name: string;
  active?: boolean;
}

// Type-safe client function
async function executeProcessData(args: ProcessDataArgs): Promise<void> {
  await fetch('/execute/process_data', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({kwargs: args})
  });
}

// Usage with autocomplete and type checking
await executeProcessData({
  user_id: 123,
  name: "Alice"
  // TypeScript will error if you pass wrong types!
});
```

### Python Example

```python
import requests
from pydantic import BaseModel

# Fetch schema and create Pydantic model
schema = requests.get('http://localhost:8000/flows/process_data/schema').json()

# Use the returned json_schema to create a client model
class ProcessDataArgs(BaseModel):
    user_id: int
    name: str
    active: bool = True

# Type-safe client function
def execute_process_data(args: ProcessDataArgs):
    response = requests.post(
        'http://localhost:8000/execute/process_data',
        json={'kwargs': args.model_dump()}
    )
    response.raise_for_status()

# Usage with validation
args = ProcessDataArgs(user_id=123, name="Alice")
execute_process_data(args)
```

## Limitations

### Current Limitations

1. **No `*args` or `**kwargs`**: Flows with variadic arguments are not supported for schema generation
2. **Forward references**: Complex forward type references may not work in all cases
3. **Custom validators**: Only type-based validation is supported (no custom validation logic)

### Workarounds

For flows that need dynamic arguments, you can skip type hints:

```python
@flowlet.flow()
def dynamic_flow(**kwargs):
    """Flow that accepts arbitrary arguments."""
    # This will work but won't have schema validation
    return kwargs
```

## Migration Guide

### Adding Schemas to Existing Flows

1. **Add type hints gradually**: Start with your most-used flows
2. **Test validation**: Verify existing API calls still work
3. **Update clients**: Update frontend/client code to use the new schemas
4. **Document parameters**: Add docstrings explaining parameters

### Example Migration

**Before:**
```python
@flowlet.flow()
def process_order(order_id, priority="normal"):
    ...
```

**After:**
```python
@flowlet.flow()
def process_order(
    order_id: int,
    priority: str = "normal"
) -> dict:
    """
    Process a customer order.

    Args:
        order_id: Unique order identifier
        priority: Priority level (normal, high, urgent)

    Returns:
        Order processing result
    """
    ...
```

## Future Enhancements

Potential future features:

- Custom validators beyond type checking
- Schema versioning
- Auto-generated client libraries (TypeScript, Python, etc.)
- Dynamic OpenAPI examples per flow
- Parameter descriptions from docstrings
