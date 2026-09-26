# API reference

This page is generated from the docstrings of the code by `sphinx.ext.autodoc`.
The [specifications](https://github.com/Quadratic-Labs/flowli/tree/main/specs) give the rules behind each signature.

## flowli.runtime

The runtime: what you call, and what runs your workflows.

### Engine

The operator API. One engine holds the ports, the site and the registry.

```{eval-rst}
.. autoclass:: flowli.runtime.Engine
.. autoclass:: flowli.runtime.EngineConfig
.. autoclass:: flowli.runtime.engine.StartResult
.. autoexception:: flowli.runtime.UnknownExecution
```

### Context

The API for the author of a workflow. Every method creates one frame.

```{eval-rst}
.. autoclass:: flowli.runtime.Context
.. autoclass:: flowli.runtime.Wait
.. autoclass:: flowli.runtime.Returned
.. autoclass:: flowli.runtime.Raised
.. autoclass:: flowli.runtime.Suspended
```

### Registry

```{eval-rst}
.. autoclass:: flowli.runtime.Registry
.. autoclass:: flowli.runtime.WorkflowRef
```

### Worker, sweeper and retention

```{eval-rst}
.. autoclass:: flowli.runtime.Worker
.. autoclass:: flowli.runtime.Sweeper
.. autoclass:: flowli.runtime.SweepReport
.. autoclass:: flowli.runtime.Retention
.. autoclass:: flowli.runtime.RetentionReport
.. autoclass:: flowli.runtime.ControlSource
.. autoclass:: flowli.runtime.ControlView
.. autoclass:: flowli.runtime.KnownExecution
```

### Consumer

The loop of a consumer of delegate tasks, minus the work.

```{eval-rst}
.. autoclass:: flowli.runtime.Consumer
.. autoclass:: flowli.runtime.ConsumerConfig
.. autoclass:: flowli.runtime.ConsumerReport
.. autoclass:: flowli.runtime.Handler
.. autoclass:: flowli.runtime.Held
.. autoexception:: flowli.runtime.Refused
```

## flowli.patterns

Helpers written against the `Context` API only. The engine does not know them.

```{eval-rst}
.. autofunction:: flowli.patterns.delegate
.. autofunction:: flowli.patterns.review
.. autoclass:: flowli.patterns.Decision
.. autoclass:: flowli.patterns.Reviews
.. autofunction:: flowli.patterns.saga
.. autofunction:: flowli.patterns.fan_out
.. autofunction:: flowli.patterns.on_tick
```

## flowli.domain

The objects, the errors and the ports. This layer has no I/O.

### Execution and frames

```{eval-rst}
.. autoclass:: flowli.domain.Execution
.. autoclass:: flowli.domain.ExecutionStatus
.. autoclass:: flowli.domain.FrameRef
.. autoclass:: flowli.domain.Frame
.. autoclass:: flowli.domain.FrameKind
.. autoclass:: flowli.domain.RetryPolicy
.. autoclass:: flowli.domain.Attempt
.. autoclass:: flowli.domain.Completed
.. autoclass:: flowli.domain.Failed
```

### The journal

```{eval-rst}
.. autoclass:: flowli.domain.Entry
.. autoclass:: flowli.domain.EntryType
.. autoclass:: flowli.domain.MemoTable
.. autoclass:: flowli.domain.Sequenced
.. autoclass:: flowli.domain.Condition
```

### Tasks, messages and timers

```{eval-rst}
.. autoclass:: flowli.domain.Task
.. autoclass:: flowli.domain.TaskKind
.. autoclass:: flowli.domain.DelegateTask
.. autoclass:: flowli.domain.Message
.. autoclass:: flowli.domain.Timer
.. autofunction:: flowli.domain.execution_channel
```

### Provenance

```{eval-rst}
.. autoclass:: flowli.domain.Actor
.. autoclass:: flowli.domain.Site
.. autoclass:: flowli.domain.Code
.. autoclass:: flowli.domain.Provenance
```

### Errors

```{eval-rst}
.. automodule:: flowli.domain.errors
```

### Ports

Each port is a `Protocol`. An adapter implements all of them.

```{eval-rst}
.. automodule:: flowli.domain.ports
```

### Names and ids

```{eval-rst}
.. automodule:: flowli.domain.names
```

## flowli.adapters

### CairnDB

```{eval-rst}
.. autoclass:: flowli.adapters.cairndb.CairnBackend
.. autoclass:: flowli.adapters.cairndb_projection.WorkflowProjection
```

### In memory

For the tests. No bucket, no disk.

```{eval-rst}
.. autoclass:: flowli.adapters.memory.MemoryBackend
.. autoclass:: flowli.adapters.memory.ManualClock
```

## flowli.api

The HTTP service over one engine. Install the `api` extra.

```{eval-rst}
.. autofunction:: flowli.api.create_app
.. autoclass:: flowli.api.ApiConfig
.. autoclass:: flowli.api.Catalog
.. autoclass:: flowli.api.Principal
.. autoclass:: flowli.api.StaticAuthenticator
.. autoclass:: flowli.api.OIDCAuthenticator
.. autoclass:: flowli.api.OIDCConfig
.. autoclass:: flowli.api.CachingAuthenticator
```

## flowli.log

```{eval-rst}
.. automodule:: flowli.log
```

## flowli.evidence

```{eval-rst}
.. automodule:: flowli.evidence
```

## flowli.codec

```{eval-rst}
.. automodule:: flowli.codec
```
