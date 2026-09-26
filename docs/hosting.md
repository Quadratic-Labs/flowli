# Hosting

Flowli has no server of its own. To host it, you run processes and you give
them one bucket. This page says which processes, with which rights, and what to
watch.

## The smallest installation that works

| Process | How many | Why |
|---|---|---|
| **worker** | 1 or more | nothing runs without it |
| **sweeper** | 1 | a timer, a timeout and a retry delay all need it |

Everything else is optional. Add the retention job when the bucket grows. Add
the HTTP service when people need access. Add a consumer when a workflow
delegates work outside the engine.

:::{admonition} An installation without a sweeper looks healthy and is not
:class: danger

Nothing in a bucket fires at a time by itself. Without a sweeper, a
`ctx.sleep` never ends, a receive timeout never expires, a delayed retry
never starts, and a dead worker is never recovered.
:::

## The bucket

One installation uses one bucket, or one prefix inside one bucket. Every
process reads and writes the same keys.

```bash
export CAIRNDB_STORAGE_TYPE=s3
export CAIRNDB_S3_BUCKET=flowli-prod
export CAIRNDB_STORAGE_PREFIX=eu/          # optional
```

Each process needs the rights to read, to write, to list and to delete under
the prefix `wf/` and the prefix `logs/wf`. A conditional write is how the
engine claims, so the object store must honour put-if-absent and CAS. The
filesystem, S3, Azure Blob Storage and GCS backends all do.

| Backend | Use it for |
|---|---|
| `filesystem` | one host, a test, a demonstration |
| `s3` | production, and any S3-compatible service through `CAIRNDB_S3_ENDPOINT_URL` |
| `azure` | production on Azure |
| `gcs` | production on GCP |

:::{admonition} Do not point two installations at one prefix
:class: warning

They share the control log, the queues and the dispatch keys. One bucket is
one tenant. Separate installations with separate buckets or prefixes.
:::

## The workers

```bash
flowli worker --app myapp.flows:registry --queue default --queue finance
```

A worker polls its queues in order, so the first queue has the priority. Give
a slow workload its own queue and its own workers, so one long step does not
delay the rest.

:::{code-block} ini
:caption: /etc/systemd/system/flowli-worker@.service

[Unit]
Description=flowli worker %i
After=network-online.target

[Service]
Environment=CAIRNDB_STORAGE_TYPE=s3
Environment=CAIRNDB_S3_BUCKET=flowli-prod
Environment=FLOWLI_APP=myapp.flows:registry
Environment=FLOWLI_CODE_REF=git:0f3c1ab
Environment=FLOWLI_LOG_FORMAT=json
ExecStart=/opt/myapp/.venv/bin/flowli worker --queue default --worker-id %H-%i
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=180

[Install]
WantedBy=multi-user.target
:::

Rules for a worker:

- It stops cleanly on `SIGINT` and on `SIGTERM`. Give it a stop timeout above
  the duration of your longest step.
- A worker that is killed loses nothing. Its lease expires, the sweeper
  enqueues a resume, and another worker replays the execution.
- Scale the workers with the number of tasks, not with the number of live
  executions. A suspended execution costs no worker.

## The sweeper

```bash
flowli sweeper --app myapp.flows:registry --interval 60 --projection /var/lib/flowli/wf_view.sqlite
```

The sweeper has no lease of its own, and each of its actions is idempotent, so
two sweepers do no harm. One is enough.

`--projection` makes it read the statuses from a local SQLite view instead of a
fold of the control log in memory. Use it when the control log is large: a
fresh process then catches up from the file, not from the first entry.

As a cron job:

```text
* * * * * /opt/myapp/.venv/bin/flowli sweeper --once >> /var/log/flowli-sweeper.log 2>&1
```

`--once` prints one line of counters, which suits a cron job that mails its
output:

```text
timers_fired=1 recovered=0 restarted=0 repaired=0 waits_cleared=0
```

## The retention job

```text
17 3 * * * /opt/myapp/.venv/bin/flowli retention --delay-days 30 --once
```

The job folds each execution that finished before the delay into one archive
object, then deletes the journal, the scoped channels, the timers, the wait
markers, the evidence, the lease and the record.

:::{admonition} The archive does not hold the evidence
:class: warning

Retention deletes the attempt logs and the attachments of an execution. An
installation that must keep a transcript copies it out of the bucket before
the delay expires.
:::

After the fold, `flowli status` still answers and `engine.journal(eid)` still
returns the entries. They come from the archive.

## The HTTP service and the interface

The service is an ASGI application. You build it in a module of your own:

:::{code-block} python
:caption: myapp/service.py

import os

from cairndb import CairnDB
from cairndb.storage.config import StorageConfig

from flowli.adapters.cairndb import CairnBackend
from flowli.api import ApiConfig, CachingAuthenticator, OIDCAuthenticator, OIDCConfig, create_app
from flowli.domain import Site
from flowli.runtime import Engine

from myapp.flows import registry

backend = CairnBackend(CairnDB(StorageConfig.from_env().create_storage()))
engine = Engine(backend.ports, Site.local(os.environ.get("INSTANCE", "api-1")), registry=registry)
projection = backend.projection(db_path="/var/lib/flowli/api.sqlite", poll_interval=5.0)

authenticator = CachingAuthenticator(OIDCAuthenticator(OIDCConfig(
    issuer=os.environ["OIDC_ISSUER"],
    audience="flowli",
    jwks_url=os.environ["OIDC_JWKS_URL"],
    roles={
        "flowli-operators": ["workflows:read", "executions:read", "executions:start",
                              "executions:signal", "executions:cancel", "queues:read",
                              "reviews:read", "reviews:decide:finance", "evidence:read"],
        "flowli-viewers": ["workflows:read", "executions:read", "reviews:read"],
        "flowli-agents": ["tasks:consume:agents"],
    },
)))

app = create_app(
    engine,
    projection=projection,
    authenticator=authenticator,
    config=ApiConfig(queues=("default", "finance", "agents")),
)
:::

```bash
uvicorn myapp.service:app --host 0.0.0.0 --port 8000 --workers 1
```

Rules for the service:

- **It runs no worker.** A worker is a different process.
- **One process owns one projection file.** Run one uvicorn worker per
  container, and give each container its own file on a local disk. Two
  processes must not share one file.
- Two or more instances can run at the same time behind a load balancer. They
  share the bucket and not the projection.
- The service shows the catalog of the code that answers. Deploy one version of
  the code at a time, or two instances show two catalogs.
- The actor of every write comes from the access token. The service refuses an
  actor in a body.

The interface is a static build over that service:

```bash
cd web && npm install && npm run build      # dist/
```

Serve `dist/` from any static host, and point it at the service. The interface
polls with `If-None-Match`, so a view that does not change costs one request
and no work.

### Health

`GET /health` needs no token and shows no execution data:

```json
{"status": "ok", "projection_seq": 812, "checked_at": "2026-09-21T07:14:23.524853Z"}
```

It refreshes the projection, so it also proves that the bucket answers. It
returns 503 with the status `degraded` when the refresh raises. Use it as the
readiness probe.

## The consumers

A `DELEGATE` task waits for a process that you run.

```bash
flowli-runner --app myapp.flows:registry --queue agents \
    --repo /srv/repo --runner-id runner-a --command 'claude -p {intent}'
```

| Rule | Why |
|---|---|
| The `--runner-id` is stable and configured. | A restart attaches to the tasks it still holds with that string. |
| A consumer and a worker are two processes. | A consumer never runs workflow code. |
| A consumer that reaches the bucket uses the ports. | The HTTP worker plane is one more hop and one more trust boundary. |
| A consumer asks for a long TTL and renews. | An agent session runs for minutes or hours. |

A consumer that cannot reach the bucket uses the worker plane of the service
with the capability `tasks:consume:{queue}`. The service mints an unguessable
holder string at the dequeue; every later call of that task presents it.

## Changing the code

The version of a workflow is part of its identity. A live execution keeps the
version it started with.

1. Register the new code under a new version. Two versions of one name can live
   at the same time.
2. Deploy the workers with both versions. The old executions finish on the old
   code.
3. Retire the old version when no execution uses it.

A change under a live execution raises `NondeterminismError` at the replay. The
execution suspends and waits for an operator:

```bash
flowli migrate EID 4 --by ops@example.com     # replay with the version 4
flowli cancel  EID --by ops@example.com       # or give up on it
```

Old memos stay valid when their frame ids and their argument digests match.

## Sizing and limits

| Limit | Value | What to do |
|---|---|---|
| queue throughput | a few dequeues per second per queue | split the load over more queues |
| the dequeue cost | one list plus one lease acquisition on the object store | keep the poll interval above a second when the queue is idle |
| the execution lease | 120 seconds by default | raise `--exec-ttl` when one step runs for longer |
| the journal of one execution | grows with the number of frames | anything endless is a process, not a workflow |
| one bucket | one tenant | separate the installations |
| the log of one attempt | 1 MiB | the buffer keeps the head and the tail, and counts what it dropped |

There are no server-sent events and no WebSocket in this version. The interface
polls.

## What to watch

The logs are the first source. Set `--log-format json` and ship stderr.

| Event | Meaning |
|---|---|
| `execution_recovered` | a worker died and the sweeper enqueued a resume |
| `execution_failed` | a workflow raised, and no attempt is left |
| `lease_lost` | a worker was fenced. It stopped. This is normal after a pause. |
| `worker_pass_done` | one pass of a `--once` worker |
| `execution_archived` | the retention job folded an execution |

Watch four numbers:

1. the queue depth, from `GET /queues` or `Queue.depth`,
2. the count of executions by status, from `GET /stats`,
3. `projection_seq` from `GET /health`, which must increase,
4. the counters of the sweeper, from its `--once` output.

## Backup and recovery

The bucket is the whole state. There is nothing else to back up: no database,
no broker, no local state that matters.

- A projection file is derived. Delete it, and the process builds it again.
- A worker holds nothing between two tasks. Replace a host at any time.
- The journal of a live execution is the authority. The control log and the
  projection are derived from it, and the sweeper repairs the control log.
- Snapshots and the garbage collection of the control log are jobs of CairnDB.

## A checklist before production

- [ ] One bucket or prefix per installation.
- [ ] A sweeper runs, and you see its counters.
- [ ] `--exec-ttl` is above the duration of the longest step.
- [ ] `FLOWLI_CODE_REF` names the commit in each deployment.
- [ ] The logs are JSON and they are shipped.
- [ ] The retention job runs, and its delay matches your audit rules.
- [ ] Each service instance has its own projection file on a local disk.
- [ ] `executions:migrate` is given to operators only.
- [ ] Each workflow has a version, and a plan to retire the old one.
- [ ] Each step is idempotent for its external effect.
