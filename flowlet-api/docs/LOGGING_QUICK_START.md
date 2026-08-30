# Flowlet Logging - Quick Start

## 1. Initialize (Once at Application Startup)

```python
from flowlet import Flowlet
from flowlet.logging_manager import initialize_logging

flowlet = Flowlet()

# Setup structured logging
initialize_logging(
    logs_dir="./storage/logs",
    runs_dir="./storage/runs"
)
```

## 2. Use Loggers in Your Code

```python
import logging

# Create application logger
app_logger = logging.getLogger("myapp.component")

@flowlet.task()
def my_task():
    # Logs automatically include obligation_id, span_id, etc.
    app_logger.info("processing started")

    # Your code here
    result = do_work()

    app_logger.info("processing completed")
    return result
```

## 3. Execute and Emit Summary

```python
from flowlet.logging_manager import get_logging_manager
from flowlet.context import ExecutionContext

@flowlet.flow()
def my_flow():
    return my_task()

# Run the flow
result = my_flow()

# Emit run summary
all_runs = ExecutionContext.get_all_runs()
if all_runs:
    obligation_id = str(all_runs[0].obligation_id)
    get_logging_manager().emit_run_summary(obligation_id)
```

## What You Get

### 1. Full Logs (logs/{obligation_id}.jsonl)
- All logs (Flowlet + your application)
- JSON format, one per line
- Automatic context injection

```jsonl
{"logger":"flowlet","obligation_id":"019b...","span_id":"019b...","status":"starting",...}
{"logger":"myapp.component","obligation_id":"019b...","span_id":"019b...","message":"processing started",...}
```

### 2. Run Summary (runs/{obligation_id}.json)
- Only Flowlet logs (filtered)
- Hierarchical structure
- Timing information

```json
{
  "obligation_id": "019b...",
  "span": {
    "span_id": "019b...",
    "span_type": "flow",
    "status": "success",
    "duration_ms": 1234,
    "children": [...]
  }
}
```

## Automatic Context Injection

Every log automatically includes:

| Field | Description | Example |
|-------|-------------|---------|
| `obligation_id` | UUID7 for this execution | `"019b49b8..."` |
| `span_id` | Same as obligation_id | `"019b49b8..."` |
| `parent_span_id` | Parent flow/task ID | `"019b49a1..."` |
| `span_type` | Type of execution | `"flow"` or `"task"` |
| `name` | Flow/task name | `"my_task"` |
| `flow_name` | Root flow name | `"my_flow"` |
| `status` | Execution status | `"starting"`, `"success"`, `"failed"` |
| `ts` | ISO timestamp | `"2025-12-23T06:41:55.123Z"` |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Your Code          Flowlet Framework                │
│  logger.info()      logger.info()                    │
└───────────┬──────────────────┬──────────────────────┘
            │                  │
            v                  v
    ┌───────────────────────────────────┐
    │  ContextInjectingFilter            │
    │  (adds obligation_id, span_id, etc.)     │
    └───────────────┬───────────────────┘
                    │
        ┌───────────┼───────────┐
        v           v           v
   ┌────────┐  ┌────────┐  ┌──────────────┐
   │ File   │  │Console │  │FlowletLog    │
   │Handler │  │Handler │  │Buffer        │
   └────┬───┘  └────────┘  └──────┬───────┘
        │                          │
        v                          v
 logs/{obligation_id}              runs/{obligation_id}.json
 .jsonl                     (summary)
```

## See Also

- Full documentation: [LOGGING.md](./LOGGING.md)
- Example code: [/examples/logging_example.py](/examples/logging_example.py)
