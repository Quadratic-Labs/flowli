# Configuration

Every setting of Flowli lives in one of six places: the storage, the engine,
the jobs, the HTTP service, the consumer, and the logs. This page lists all of
them with their defaults.

## The storage

Flowli stores everything in one bucket, through CairnDB. You name that bucket
in one of three ways.

::::{tab-set}
:::{tab-item} Environment variables

```bash
export CAIRNDB_STORAGE_TYPE=s3
export CAIRNDB_S3_BUCKET=my-bucket
export CAIRNDB_S3_REGION=eu-west-3
```

The CLI reads them when you give no `--storage-path`.
:::

:::{tab-item} A CLI option

```bash
flowli worker --app myapp.flows:registry --storage-path ./bucket
```

`--storage-path` selects the filesystem backend. It also reads the
variable `CAIRNDB_STORAGE_PATH`.
:::

:::{tab-item} A dictionary, in Python

```python
backend = CairnBackend.configure({"storage": {"type": "s3", "bucket": "my-bucket"}})
```

Use this when your application builds the engine itself.
:::

::::

| Variable | Content |
|---|---|
| `CAIRNDB_STORAGE_TYPE` | `filesystem`, `s3`, `azure` or `gcs`. Default `filesystem`. |
| `CAIRNDB_STORAGE_PATH` | the root path of the filesystem backend |
| `CAIRNDB_STORAGE_PREFIX` | the key prefix inside a bucket or a container |
| `CAIRNDB_S3_BUCKET` | the bucket, for `s3` and for `gcs` |
| `CAIRNDB_S3_REGION` | the AWS region |
| `CAIRNDB_S3_ENDPOINT_URL` | a custom S3 endpoint, for MinIO or another service |
| `CAIRNDB_AZURE_CONTAINER` | the Azure container |
| `CAIRNDB_AZURE_CONNECTION_STRING` | the Azure connection string |
| `CAIRNDB_AZURE_ACCOUNT_URL` | the Azure account URL |
| `CAIRNDB_GCS_PROJECT` | the GCP project id |
| `CAIRNDB_GCS_CREDENTIALS_PATH` | the GCP key file |

:::{admonition} One bucket is one installation
:class: warning

Every process of one installation reads and writes the same bucket. Two
installations that share a bucket share their executions, their queues and
their control log. There is no tenant field.
:::

## The engine

{py:obj}`EngineConfig <flowli.runtime.EngineConfig>` holds the timings of the
runtime.

| Setting | Default | Content |
|---|---|---|
| `exec_ttl` | `120.0` | the TTL of an execution lease, in seconds |
| `task_ttl` | `60.0` | the TTL of a task lease, which is the visibility timeout |
| `poll_interval` | `1.0` | the idle sleep of a worker between two polls |
| `default_queue` | `"default"` | the queue of a start with no queue |
| `code_ref` | `None` | the git sha or the image digest, for the provenance |
| `nack_delay` | `5.0` | the delay when a worker cannot handle a task |

```python
engine = Engine(
    backend.ports,
    Site.local("w-1"),
    registry=registry,
    config=EngineConfig(exec_ttl=300.0, code_ref="git:abc123"),
)
```

A worker renews each lease every `ttl / 3` seconds. Raise `exec_ttl` when a
step runs for longer than two minutes without a suspension. Raise `task_ttl`
with it, because the task lease is the visibility timeout of the same work.

:::{admonition} Set `code_ref` in a deployment
:class: tip

It names the code that acted. A journal that is read one year later tells
you which commit ran each frame. The CLI reads `FLOWLI_CODE_REF`.
:::

## The jobs

All three jobs take `--app`, the storage options, and the logging options.

### `flowli worker`

```bash
flowli worker --app myapp.flows:registry --queue default --queue finance
```

| Option | Default | Content |
|---|---|---|
| `--app`, `-a` | — | `module:attr` naming a `Registry`, an `Engine`, or a callable that returns one |
| `--queue`, `-q` | the default queue | a queue to poll. Repeat the option for more. The order is the priority. |
| `--worker-id` | `{host}-{pid}` | the id in the provenance of every write |
| `--exec-ttl` | `120.0` | the TTL of an execution lease |
| `--task-ttl` | `60.0` | the TTL of a task lease |
| `--poll-interval` | `1.0` | the idle sleep between two polls |
| `--once` | off | run one pass, then exit |
| `--storage-path` | — | the filesystem bucket |
| `--code-ref` | — | the git sha or the image digest |

### `flowli sweeper`

```bash
flowli sweeper --app myapp.flows:registry --interval 60
```

| Option | Default | Content |
|---|---|---|
| `--interval` | `60.0` | the seconds between two sweeps |
| `--repair-window` | `60.0` | the seconds before the sweeper repairs an entry |
| `--projection` | — | the path of a SQLite projection to read the statuses from |
| `--once` | off | run one pass, then exit |

Without `--projection` the sweeper folds the control log in memory. A fresh
process then reads the log from the start. The projection makes that cheap.

### `flowli retention`

```bash
flowli retention --app myapp.flows:registry --delay-days 30 --once
```

| Option | Default | Content |
|---|---|---|
| `--delay-days` | `30.0` | archive the executions that finished before this |
| `--interval` | `3600.0` | the seconds between two runs |
| `--projection` | — | the path of a SQLite projection |
| `--once` | off | run one pass, then exit |

### The operator commands

```bash
flowli status  EID --journal
flowli signal  EID payments '{"amount": 100}' --by bank@example.com
flowli cancel  EID --by ops@example.com
flowli migrate EID 2 --by ops@example.com
```

`--by` names the actor that the provenance records: an email, or `kind:id` with
the kind `human`, `system`, `schedule` or `worker`. It defaults to the local
user, or to `FLOWLI_BY`. A command exits with the code 1 on an unknown
execution, or on a refused action such as a cancel of a finished execution.

### The environment variables of the CLI

| Variable | Replaces |
|---|---|
| `FLOWLI_APP` | `--app` |
| `FLOWLI_WORKER_ID` | `--worker-id` |
| `FLOWLI_CODE_REF` | `--code-ref` |
| `FLOWLI_BY` | `--by` |
| `FLOWLI_LOG_LEVEL` | `--log-level` |
| `FLOWLI_LOG_FORMAT` | `--log-format` |
| `CAIRNDB_STORAGE_PATH` | `--storage-path` |

## The projection

The projection is a local, read-only SQLite view of the control log. It answers
the questions of an operator without a read of any journal.

```python
projection = backend.projection(db_path="./wf_view.v1.sqlite", poll_interval=5.0)
```

| Setting | Default | Content |
|---|---|---|
| `db_path` | in memory | the SQLite file of this process |
| `version` | `"1"` | the schema version, part of the file name |
| `poll_interval` | `5.0` | the seconds between two refreshes |

:::{admonition} One projection file per process
:class: warning

Two processes each build their own file. Do not put one file on a shared
disk for two processes.
:::

## The HTTP service

{py:obj}`ApiConfig <flowli.api.ApiConfig>` holds the settings of the service.

| Setting | Default | Content |
|---|---|---|
| `queues` | `("default",)` | the queues that the service shows and relays |
| `default_queue` | `"default"` | the queue of a start with no queue |
| `wait_ms` | `2000` | the limit of a read-your-writes wait |
| `default_limit` | `50` | the page size of a list |
| `max_limit` | `500` | the largest page size a client can ask for |
| `refresh_after_write` | `True` | catch the projection up after each write |
| `task_ttl` | `300.0` | the visibility timeout that the worker plane asks for |
| `max_task_ttl` | `3600.0` | the largest TTL a consumer can ask for |
| `nack_delay` | `5.0` | the delay when the relay returns a task that is not a delegate |
| `max_wait_seconds` | `20.0` | the limit of a long poll on a dequeue |
| `poll_interval` | `0.5` | the poll period inside a long poll |
| `title` | `"flowli"` | the title of the OpenAPI document |

```python
app = create_app(
    engine,
    projection=projection,
    authenticator=authenticator,
    config=ApiConfig(queues=("default", "finance", "agents")),
)
```

### The identity provider

The service accepts a bearer token in the `Authorization` header. It checks the
signature against the JWKS of the issuer, the issuer, the audience and the
expiry.

```python
from flowli.api import CachingAuthenticator, OIDCAuthenticator, OIDCConfig

authenticator = CachingAuthenticator(OIDCAuthenticator(OIDCConfig(
    issuer="https://id.example.com/",
    audience="flowli",
    jwks_url="https://id.example.com/.well-known/jwks.json",
    roles={
        "flowli-operators": ["executions:read", "executions:start",
                              "executions:cancel", "reviews:decide:finance"],
        "flowli-viewers": ["workflows:read", "executions:read"],
        "flowli-agents": ["tasks:consume:agents"],
    },
)))
```

| Setting | Default | Content |
|---|---|---|
| `issuer`, `audience`, `jwks_url` | — | the identity provider |
| `actor_claim` | `"email"` | the claim that names a person. Use `"sub"` when the provider re-assigns addresses. |
| `groups_claim` | `"groups"` | the claim that holds the groups |
| `scope_claim` | `"scope"` | the claim that holds the scopes |
| `client_claim` | `"client_id"` | the claim that names a machine |
| `roles` | `{}` | the map from a group to a list of capabilities |
| `machine_actor_kind` | `"worker"` | the actor kind of a machine token |
| `leeway` | `30.0` | the accepted clock skew, in seconds |

:::{admonition} One deployment uses one claim
:class: danger

A journal must not hold two forms of the same identity. Choose `email` or
`sub` before the first execution, and keep it.
:::

### The capabilities

| Capability | Permits |
|---|---|
| `workflows:read` | the catalog |
| `executions:read` | the executions, the journals, the frames, the announcements |
| `executions:start` | a start |
| `executions:signal` | a signal |
| `executions:cancel` | a cancel |
| `executions:migrate` | a migrate. Give it to operators only. |
| `reviews:read` | the review list |
| `reviews:decide:{queue}` | a decision on one review queue |
| `queues:read` | the queue depth and the pending tasks |
| `tasks:consume:{queue}` | the worker plane on one queue |
| `evidence:read` | the attempt logs and the attachments |

`StaticAuthenticator` takes a fixed map from a token to a `Principal`. Use it in
a test and in a local run. An unknown token is refused.

## The consumer

{py:obj}`ConsumerConfig <flowli.runtime.ConsumerConfig>` holds the settings of a
consumer of delegate tasks.

| Setting | Default | Content |
|---|---|---|
| `queues` | `("agents",)` | the queues to poll |
| `holder` | `"consumer"` | the stable id of this consumer |
| `ttl` | `300.0` | the TTL of a task lease |
| `poll_interval` | `1.0` | the idle sleep between two polls |
| `grace` | `120.0` | the budget an interrupt gives the work to stop, in seconds |
| `nack_delay` | 30 seconds | the delay when the consumer returns a task it refuses |
| `conflict_delay` | 10 seconds | the delay when the consumer cannot acquire its substrate |

:::{admonition} The holder must survive a restart
:class: danger

A restarted consumer attaches to the tasks it still holds with that string.
A random holder loses every attempt that was in flight. Configure it, and
keep it.
:::

## The logs

Every component emits structured events through structlog, the library that
CairnDB uses, so one configuration produces one stream.

```python
from flowli.log import configure_logging

configure_logging("INFO", "json")      # or "console"
```

All three CLIs take `--log-level` and `--log-format console|json`. The logs go
to stderr, so the report of a job on stdout stays pipeable on its own.

| Level | What you see |
|---|---|
| `DEBUG` | every frame event, and the events of CairnDB |
| `INFO` | the lifecycle of each execution, each message, each lease |
| `WARNING` | a lost lease, a refused task, a repair |

The events are named with snake_case nouns: `execution_started`,
`execution_suspended`, `frame_failed`, `execution_recovered`,
`execution_archived`. While a worker holds a task, `worker_id`, `task_id`,
`eid` and `epoch` are bound to every event it emits. A live frame adds `fid`
and `attempt`.

### The evidence

The same stream feeds the evidence of an attempt. The settings are the
constants of `flowli.evidence`:

| Setting | Default | Content |
|---|---|---|
| `DEFAULT_LIMIT` | 1 MiB | the size limit of the log of one attempt |
| `DEFAULT_FLUSH_INTERVAL` | `10.0` | the seconds between two flushes |
| `TAIL_LINES` | `64` | the lines that survive past the limit |

Evidence follows the level of the process. At `INFO` a step that logs nothing
leaves no object at all, so evidence costs nothing until somebody logs. Use
`NullEvidence` in place of the adapter to store none of it.

## A retry policy

{py:obj}`RetryPolicy <flowli.domain.RetryPolicy>` is a setting of one frame, not of
the installation.

```python
RetryPolicy(max_attempts=5, backoff=timedelta(seconds=2),
            backoff_factor=2.0, max_backoff=timedelta(minutes=5))
```

| Field | Default | Content |
|---|---|---|
| `max_attempts` | `1` | `1` means no retry |
| `backoff` | 0 seconds | the delay before the second attempt |
| `backoff_factor` | `1.0` | multiplies the delay at each attempt |
| `max_backoff` | `None` | the ceiling of the delay |

The delay of the attempt `n + 1` is
`min(backoff * backoff_factor ** (n - 1), max_backoff)`. A delay of zero starts
the next attempt at once. A delay above zero suspends the frame on a timer, so
the sweeper must run.
