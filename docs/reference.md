# API reference

This page is generated from the docstrings of the code by `sphinx.ext.autodoc`.
The [specifications](specs/00-overview.md) give the rules behind each signature.

## flowlet.runtime

The runtime: what you call, and what runs your workflows.

### Engine

The operator API. One engine holds the ports, the site and the registry.

```{eval-rst}
.. autoclass:: flowlet.runtime.Engine
.. autoclass:: flowlet.runtime.EngineConfig
.. autoclass:: flowlet.runtime.engine.StartResult
.. autoexception:: flowlet.runtime.UnknownExecution
```

### Context

The API for the author of a workflow. Every method creates one frame.

```{eval-rst}
.. autoclass:: flowlet.runtime.Context
.. autoclass:: flowlet.runtime.Wait
.. autoclass:: flowlet.runtime.Returned
.. autoclass:: flowlet.runtime.Raised
.. autoclass:: flowlet.runtime.Suspended
```

### Registry

```{eval-rst}
.. autoclass:: flowlet.runtime.Registry
.. autoclass:: flowlet.runtime.WorkflowRef
```

### Worker, sweeper and retention

```{eval-rst}
.. autoclass:: flowlet.runtime.Worker
.. autoclass:: flowlet.runtime.Sweeper
.. autoclass:: flowlet.runtime.SweepReport
.. autoclass:: flowlet.runtime.Retention
.. autoclass:: flowlet.runtime.RetentionReport
.. autoclass:: flowlet.runtime.ControlSource
.. autoclass:: flowlet.runtime.ControlView
.. autoclass:: flowlet.runtime.KnownExecution
```

### Consumer

The loop of a consumer of delegate tasks, minus the work.

```{eval-rst}
.. autoclass:: flowlet.runtime.Consumer
.. autoclass:: flowlet.runtime.ConsumerConfig
.. autoclass:: flowlet.runtime.ConsumerReport
.. autoclass:: flowlet.runtime.Handler
.. autoclass:: flowlet.runtime.Held
.. autoexception:: flowlet.runtime.Refused
```

## flowlet.patterns

Helpers written against the `Context` API only. The engine does not know them.

```{eval-rst}
.. autofunction:: flowlet.patterns.delegate
.. autofunction:: flowlet.patterns.review
.. autoclass:: flowlet.patterns.Decision
.. autoclass:: flowlet.patterns.Reviews
.. autofunction:: flowlet.patterns.saga
.. autofunction:: flowlet.patterns.fan_out
.. autofunction:: flowlet.patterns.on_tick
```

## flowlet.domain

The objects, the errors and the ports. This layer has no I/O.

### Execution and frames

```{eval-rst}
.. autoclass:: flowlet.domain.Execution
.. autoclass:: flowlet.domain.ExecutionStatus
.. autoclass:: flowlet.domain.FrameRef
.. autoclass:: flowlet.domain.Frame
.. autoclass:: flowlet.domain.FrameKind
.. autoclass:: flowlet.domain.RetryPolicy
.. autoclass:: flowlet.domain.Attempt
.. autoclass:: flowlet.domain.Completed
.. autoclass:: flowlet.domain.Failed
```

### The journal

```{eval-rst}
.. autoclass:: flowlet.domain.Entry
.. autoclass:: flowlet.domain.EntryType
.. autoclass:: flowlet.domain.MemoTable
.. autoclass:: flowlet.domain.Sequenced
.. autoclass:: flowlet.domain.Condition
```

### Tasks, messages and timers

```{eval-rst}
.. autoclass:: flowlet.domain.Task
.. autoclass:: flowlet.domain.TaskKind
.. autoclass:: flowlet.domain.DelegateTask
.. autoclass:: flowlet.domain.Message
.. autoclass:: flowlet.domain.Timer
.. autofunction:: flowlet.domain.execution_channel
```

### Provenance

```{eval-rst}
.. autoclass:: flowlet.domain.Actor
.. autoclass:: flowlet.domain.Site
.. autoclass:: flowlet.domain.Code
.. autoclass:: flowlet.domain.Provenance
```

### Errors

```{eval-rst}
.. automodule:: flowlet.domain.errors
```

### Ports

Each port is a `Protocol`. An adapter implements all of them.

```{eval-rst}
.. automodule:: flowlet.domain.ports
```

### Names and ids

```{eval-rst}
.. automodule:: flowlet.domain.names
```

## flowlet.adapters

### CairnDB

```{eval-rst}
.. autoclass:: flowlet.adapters.cairndb.CairnBackend
.. autoclass:: flowlet.adapters.cairndb_projection.WorkflowProjection
```

### In memory

For the tests. No bucket, no disk.

```{eval-rst}
.. autoclass:: flowlet.adapters.memory.MemoryBackend
.. autoclass:: flowlet.adapters.memory.ManualClock
```

## flowlet.api

The HTTP service over one engine. Install the `api` extra.

```{eval-rst}
.. autofunction:: flowlet.api.create_app
.. autoclass:: flowlet.api.ApiConfig
.. autoclass:: flowlet.api.Catalog
.. autoclass:: flowlet.api.Principal
.. autoclass:: flowlet.api.StaticAuthenticator
.. autoclass:: flowlet.api.OIDCAuthenticator
.. autoclass:: flowlet.api.OIDCConfig
.. autoclass:: flowlet.api.CachingAuthenticator
```

## flowlet.log

```{eval-rst}
.. automodule:: flowlet.log
```

## flowlet.evidence

```{eval-rst}
.. automodule:: flowlet.evidence
```

## flowlet.codec

```{eval-rst}
.. automodule:: flowlet.codec
```
