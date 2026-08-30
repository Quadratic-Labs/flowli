# Flowlet Structured Logging

Flowlet provides a comprehensive structured logging system with automatic context injection, JSON formatting, and separation of framework logs from application logs.

## Overview

The logging system provides:

1. **JSON-formatted logs** - All logs are structured as JSON with consistent schema
2. **Automatic context injection** - Run metadata (obligation_id, span_id, parent_span_id, etc.) is automatically added to all logs
3. **Dual-handler architecture** - Separate handling for full logs and Flowlet-only logs
4. **UUID7 identifiers** - All run IDs use UUID7 for datetime range queries and id-based queries
5. **Run summaries** - Compact hierarchical summaries generated from execution logs

## Architecture

### Log Output Structure

```
storage/
├── logs/           # Full append-only JSONL logs
│   └── {obligation_id}.jsonl
└── obligations/           # Compacted obligation summaries
    └── {obligation_id}.json
```

### Log Flow

```
User Code               Flowlet Framework
   │                         │
   ├─> logger.info()         ├─> logger.info()
   │                         │
   └──────────┬──────────────┘
              ↓
    ContextInjectingFilter
    (adds obligation_id, span_id, etc.)
              │
              ├──────────────────────┬──────────────────────┐
              ↓                      ↓                      ↓
       JSONFormatter          JSONFormatter          JSONFormatter
              ↓                      ↓                      ↓
      File Handler          Console Handler      FlowletLogBuffer
   (logs/{obligation_id}.jsonl)                         (Flowlet logs only)
                                                           ↓
                                                  Compact & Emit
                                                           ↓
                                                  obligations/{obligation_id}.json
```

### Components

#### 1. ContextInjectingFilter

Automatically injects execution context into all log records:

- `obligation_id` - UUID7 identifier for this execution
- `span_id` - Same as obligation_id (used for distributed tracing compatibility)
- `parent_span_id` - UUID7 of parent flow/task
- `span_type` - Either "flow" or "task"
- `name` - Name of the flow/task
- `flow_name` - Name of the root flow
- `status` - Current execution status ("starting", "running", "success", "failed")

#### 2. JSONFormatter

Formats log records as JSON with the following schema:

```json
{
  "logger": "flowlet",
  "obligation_id": "019b49b8-f27e-73a5-97f4-ef37fd763918",
  "span_id": "019b49f1-69c5-75f4-8c44-7d41c2a7179d",
  "parent_span_id": null,
  "span_type": "flow",
  "name": "hello-world",
  "flow": "data_pipeline.hello-world",
  "status": "starting",
  "ts": "2025-12-23T06:41:55.123Z",
  "message": "starting execution of hello-world flow",
  "level": "INFO",
  "extra": {}
}
```

#### 3. FlowletLogBuffer

A custom logging handler that:
- Filters logs to only capture those from the `flowlet` logger
- Buffers logs in memory per obligation_id
- Provides logs for summary generation
- Can be cleared after summary emission

#### 4. JSONLFileHandler

A file handler that:
- Writes logs in JSONL format (one JSON object per line)
- Appends to run-specific log files
- Creates files as `logs/{obligation_id}.jsonl`

## Usage

### Basic Setup

```python
from flowlet import Flowlet
from flowlet.logging_manager import initialize_logging

# Initialize Flowlet
flowlet = Flowlet()

# Setup structured logging
initialize_logging(
    logs_dir="./storage/logs",
    runs_dir="./storage/obligations",
    enable_file_logging=True
)
```

### Using Loggers in Your Code

```python
import logging

# Create a logger for your application
app_logger = logging.getLogger("myapp.database")

@flowlet.task()
def load_data():
    # This log will automatically include:
    # - obligation_id, span_id, parent_span_id
    # - span_type="task", name="load_data"
    # - Timestamp, level, etc.
    app_logger.info("connecting to database")

    # Your code here
    data = fetch_from_db()

    app_logger.info("successfully loaded data")
    return data
```

### Emitting Run Summaries

```python
from flowlet.logging_manager import get_logging_manager
from flowlet.context import ExecutionContext

@flowlet.flow()
def my_flow():
    # Flow logic here
    result = load_data()
    return result

# Execute flow
result = my_flow()

# Get the root obligation from execution stack
all_runs = ExecutionContext.get_all_runs()
if all_runs:
    root_run = all_runs[0]
    obligation_id = str(root_run.obligation_id)

    # Emit summary
    manager = get_logging_manager()
    summary = manager.emit_run_summary(obligation_id)
```

### Advanced: Per-Run Log Files

For more control over when run log files are created and closed:

```python
from flowlet.logging_manager import get_logging_manager

log_manager = get_logging_manager()

# Start logging for an obligation
obligation_id = "019b49b8-f27e-73a5-97f4-ef37fd763918"
log_manager.start_run_logging(obligation_id)

# Execute your flow/task
# All logs during execution will go to logs/{obligation_id}.jsonl

# Stop logging for this obligation
log_manager.stop_run_logging(obligation_id)
```

## Log Formats

### Full Logs (JSONL)

Located in `logs/{obligation_id}.jsonl`:

```jsonl
{"logger":"flowlet","obligation_id":"019b49b8-f27e-73a5-97f4-ef37fd763918","span_id":"019b49f1-69c5-75f4-8c44-7d41c2a7179d","parent_span_id":null,"span_type":"flow","name":"hello-world","flow":"data_pipeline.hello-world","status":"starting","ts":"2025-12-23T06:41:55.123Z","message":"starting execution of hello-world flow","level":"INFO","extra":{}}
{"logger":"flowlet","obligation_id":"019b49b8-f27e-73a5-97f4-ef37fd763918","span_id":"019b49f2-4e13-7c7b-8c13-2cf9356ecb4d","parent_span_id":"019b49f1-69c5-75f4-8c44-7d41c2a7179d","span_type":"task","name":"load_users","flow":"data_pipeline.load_users","status":"starting","ts":"2025-12-23T06:42:55.123Z","message":"loading users from db","level":"INFO","extra":{}}
{"logger":"example.db","obligation_id":"019b49b8-f27e-73a5-97f4-ef37fd763918","span_id":"019b49f2-4e13-7c7b-8c13-2cf9356ecb4d","parent_span_id":"019b49f1-69c5-75f4-8c44-7d41c2a7179d","span_type":"task","name":"load_users","flow":"data_pipeline.load_users","status":"running","ts":"2025-12-23T06:42:58.123Z","message":"connection to db established","level":"INFO","extra":{}}
```

**Key characteristics:**
- One JSON object per line (JSONL format)
- Includes **all** logs (Flowlet + user application logs)
- Append-only (new logs added to end of file)
- Can be streamed and processed line-by-line

### Run Summaries (JSON)

Located in `obligations/{obligation_id}.json`:

```json
{
    "obligation_id": "019b49b8-f27e-73a5-97f4-ef37fd763918",
    "span": {
        "span_id": "019b49f1-69c5-75f4-8c44-7d41c2a7179d",
        "span_type": "flow",
        "name": "hello-world",
        "flow": "data_pipeline.hello-world",
        "status": "success",
        "start_ts": "2025-12-23T06:41:55.123Z",
        "end_ts": "2025-12-23T06:43:56.955Z",
        "duration_ms": 121832,
        "retry": 0,
        "children": [
            {
                "span_id": "019b49f2-4e13-7c7b-8c13-2cf9356ecb4d",
                "span_type": "task",
                "name": "load_users",
                "flow": "data_pipeline.load_users",
                "status": "success",
                "start_ts": "2025-12-23T06:42:55.123Z",
                "end_ts": "2025-12-23T06:43:00.123Z",
                "duration_ms": 5000,
                "retry": 0,
                "children": []
            }
        ]
    }
}
```

**Key characteristics:**
- Pretty-printed JSON
- Only **Flowlet** logs (user logs filtered out)
- Hierarchical span structure (parent-child relationships)
- Timing information (start, end, duration)
- Status tracking (starting → success/failed)

## UUID7 Benefits

All `obligation_id` and `span_id` values use UUID7, which provides:

1. **Time-ordered** - Can be sorted chronologically
2. **Datetime range queries** - First part encodes timestamp
3. **Unique** - Globally unique across distributed systems
4. **Compatible** - Standard UUID format, works with existing tools

Example queries:

```python
# Find all obligations from the last hour
import uuid_utils as uuid
from datetime import datetime, timedelta

one_hour_ago = datetime.utcnow() - timedelta(hours=1)
min_uuid = uuid.uuid7(one_hour_ago)

# All UUIDs >= min_uuid are from the last hour
recent_runs = [r for r in obligations if r.obligation_id >= min_uuid]
```

## Integration Points

### Custom Filters

Add custom filters to inject additional context:

```python
import logging

class CustomFilter(logging.Filter):
    def filter(self, record):
        record.environment = "production"
        record.version = "1.0.0"
        return True

# Add to logger
logger = logging.getLogger('flowlet')
logger.addFilter(CustomFilter())
```

### Custom Handlers

Add handlers for different outputs:

```python
import logging
from flowlet.logging import JSONFormatter, ContextInjectingFilter

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(JSONFormatter())
console_handler.addFilter(ContextInjectingFilter())

# Add to logger
logger = logging.getLogger('flowlet')
logger.addHandler(console_handler)
```

### Log Aggregation

The JSONL format works well with log aggregation tools:

- **Elasticsearch**: Direct ingestion of JSONL
- **Loki**: Parse JSON and extract labels
- **CloudWatch**: Use JSON processor
- **Datadog**: JSON log parsing

## Best Practices

### 1. Use Named Loggers

Create specific loggers for different components:

```python
db_logger = logging.getLogger("myapp.database")
api_logger = logging.getLogger("myapp.api")
cache_logger = logging.getLogger("myapp.cache")
```

This helps filter and analyze logs by component.

### 2. Log at Appropriate Levels

- `DEBUG` - Detailed information for debugging
- `INFO` - General informational messages
- `WARNING` - Warning messages (non-critical)
- `ERROR` - Error messages
- `CRITICAL` - Critical failures

### 3. Include Structured Extra Data

Use the `extra` parameter for additional context:

```python
logger.info(
    "user login successful",
    extra={
        "user_id": user.id,
        "ip_address": request.ip,
        "user_agent": request.user_agent
    }
)
```

### 4. Clean Up Buffers

After emitting summaries, clear buffers to free memory:

```python
manager = get_logging_manager()
summary = manager.emit_run_summary(obligation_id, clear_buffer=True)
```

### 5. Handle Long-Running Flows

For long-running flows, consider:
- Rotating log files
- Periodically flushing buffers
- Streaming summaries to external storage

## Troubleshooting

### Logs Missing Context

**Problem**: Logs don't have `obligation_id`, `span_id`, etc.

**Solution**: Ensure you're logging from within an execution context:

```python
@flowlet.flow()
def my_flow():
    # Logs here will have context
    logger.info("this has context")

# Logs outside flow don't have context
logger.info("this doesn't have context")
```

### Duplicate Logs

**Problem**: Logs appear multiple times.

**Solution**: Check logger propagation settings:

```python
logger = logging.getLogger('myapp')
logger.propagate = False  # Don't propagate to parent
```

### Missing Summaries

**Problem**: No summary files generated.

**Solution**: Ensure you're calling `emit_run_summary()` after execution:

```python
# After flow completes
manager = get_logging_manager()
manager.emit_run_summary(obligation_id)
```

### Large Log Files

**Problem**: Log files growing too large.

**Solution**: Implement log rotation:

```python
from logging.handlers import RotatingFileHandler

handler = RotatingFileHandler(
    'app.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
```

## API Reference

### LoggingManager

Main class for managing logging configuration.

#### Methods

- `setup()` - Initialize logging system
- `start_run_logging(obligation_id)` - Start file logging for an obligation
- `stop_run_logging(obligation_id)` - Stop file logging for an obligation
- `emit_run_summary(obligation_id, clear_buffer=True)` - Generate and write run summary
- `get_run_logs(obligation_id)` - Get buffered logs without emitting summary

### Functions

- `initialize_logging(logs_dir, runs_dir, enable_file_logging)` - Initialize global manager
- `get_logging_manager()` - Get global manager instance
- `emit_current_run_summary()` - Emit summary for current run

### Logging Components

- `ContextInjectingFilter` - Filter that adds execution context to logs
- `JSONFormatter` - Formatter for JSON output
- `FlowletLogBuffer` - Handler that buffers Flowlet logs
- `JSONLFileHandler` - File handler for JSONL output
- `compact_logs_to_summary(logs, obligation_id)` - Compact logs to summary structure
- `write_run_summary(summary, obligation_id, runs_dir)` - Write summary to file

## Examples

See `/examples/logging_example.py` for a complete working example.
