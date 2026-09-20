# 09 — HTTP API

*Status: draft, 2026-09-09. Writing style: ASD-STE100 Strict.*

## 1. Purpose

The engine is a library. This document specifies the service that puts the
engine on a network: `flowlet-api`. Two groups of clients use it.

- **Operators and the web application.** They read executions, start them,
  send signals, cancel, migrate, and decide reviews. This is the *control
  plane*.
- **Executors that run outside the engine process.** They take `DELEGATE`
  tasks and answer them. This is the *worker plane*.

This document specifies the HTTP contract only. It does not specify the
user interface, the deployment topology, or the identity provider.

## 2. Principles

1. **The service holds no state of its own.** Every read comes from the
   projection or from a port. Every write goes through the engine. The
   bucket stays the only shared component.
2. **Reads use the projection. Writes use the engine.** A read of a journal
   is the one exception. It uses the `Journal` port.
3. **The actor comes from the access token.** The service never reads an
   actor from a request body or from a query parameter.
4. **The control plane is neutral.** A workflow of steps and a workflow that
   delegates to an agent use the same endpoints. There is no endpoint for a
   paradigm.
5. **The worker plane is a relay.** It puts the `Queue` and `Channel` ports
   on HTTP for a consumer that cannot reach the bucket. A consumer that can
   reach the bucket must use the ports. The relay is one more hop and one
   more trust boundary.
6. **Policy lives here.** The engine records who acted. It does not check
   what that actor is permitted to do. Section 6 specifies those checks.

## 3. Composition

```python
app = create_app(
    engine,                            # runtime.Engine
    projection=projection,             # adapters.cairndb_projection, required
    authenticator=authenticator,       # section 5
    catalog=None,                      # built from engine.registry when absent
    config=ApiConfig(...),             # the table below
)
```

The app owns the refresh loop of the projection: it catches up once at
startup and polls while it runs.

| Setting | Content |
|---|---|
| `--app module:attr` | the `Registry` or the `Engine`, as for the CLI |
| `CAIRNDB_*` | the storage of the bucket, as for the CLI |
| `projection_path` | the SQLite file of this process |
| `projection_interval` | the refresh period in seconds. Default 5 |
| `issuer`, `audience`, `jwks_url` | the identity provider, section 5 |
| `roles` | the map from group to capability, section 6 |
| `queues` | the queue names that the service shows and relays |

Rules:

- One process has one projection file (`08-projection.md`, section 6). The
  service starts the refresh loop at startup. It stops the loop at shutdown.
- The service does not run a worker. A worker is a different process.
- Two instances of the service can run at the same time. They share the
  bucket. They do not share the projection file.

## 4. Common rules

### 4.1 Data

- The media type is `application/json`. Evidence is the one exception. See
  section 10.
- A timestamp in a body is the ISO form of `Timestamp` (`01-domain-model.md`).
- An `eid` in a path is the canonical 36-character UUID.
- A `fid` never appears as a path segment. It holds `/`, and a
  percent-encoded slash does not survive routing, so it is a query
  parameter (section 10).

### 4.2 Errors

An error body is `application/problem+json` (RFC 9457). It holds `type`,
`title`, `status`, `detail` and `code`. `code` is the name of the domain
error.

| Condition | Status | `code` |
|---|---|---|
| no token, or an invalid token | 401 | `unauthenticated` |
| a valid token without the capability | 403 | `forbidden` |
| unknown execution, review, or workflow | 404 | `unknown_execution`, … |
| a body that fails the argument schema | 422 | `invalid_arguments` |
| an invalid queue name, channel name or id | 400 | `invalid_name` |
| migrate or cancel on a terminal execution | 409 | `terminal_execution` |
| a task that another holder owns | 409 | `not_holder` |
| a lease that moved under the caller | 409 | `lease_lost` |
| the projection is more than `max_lag` behind | 503 | `projection_stale` |

Rule: an error body never holds a token, a JWKS value, or a bucket key.

### 4.3 Caching

Every `GET` returns an `ETag`. A client sends `If-None-Match`. The service
returns 304 when the value did not change. The web application depends on
this. A 304 lets it keep the same object, so the interface does not redraw.

| Resource | ETag source |
|---|---|
| an execution, a list of executions, reviews, announcements | the last applied sequence of the projection, and the query |
| a journal, a frame tree | `Journal.tail(eid)` |
| a catalog entry | the digest of the schema |
| a queue | the depth, which changes at each enqueue and each ack |

### 4.4 Read-your-writes

The projection is eventually consistent. A write returns the header
`X-Control-Seq`. A client puts that value in the query parameter `min_seq`
of its next read. The service then calls `projection.wait_for(seq)` with a
limit of `wait_ms` (default 2000).

The engine's write methods return the result of the write, not the sequence
of the announcement they made. So the service catches the projection up
after a write (`refresh_after_write`, on by default) and reports the
sequence it reached. A client that sends that value back therefore waits
for nothing. A deployment that wants a faster write turns the setting off
and accepts a read that lags.

- The read returns the fresh data when the projection catches up.
- The read returns the current data with the header `X-Projection-Stale:
  true` when the limit expires. The status stays 200.

### 4.5 Lists

A list endpoint takes `limit` (default 50, maximum 500) and `cursor`. A
cursor is opaque. A response holds `{"items": [...], "next_cursor": null}`.

### 4.6 Repeated requests

- `POST /executions` is idempotent when the body holds `dispatch_key`. The
  engine claims that key. The response says `deduplicated`.
- `POST /reviews/{rid}/decide` is idempotent. The engine claims the
  decision of `rid`. A second call returns the first decision.
- `POST /executions/{eid}/cancel` is idempotent.
- `POST /executions/{eid}/signal` and `/deliver` are **not** idempotent.
  Each call sends one message. A repeated call sends a second message. A
  receive frame consumes the first one only, so a repeat is usually
  harmless. It stays visible in the channel log.

## 5. Authentication

### 5.1 Tokens

The service accepts a bearer token in the `Authorization` header. The token
is an OIDC access token in JWT form. The service checks the signature
against the JWKS of the issuer, the issuer, the audience and the expiry. It
caches the JWKS and refreshes it when a key id is unknown.

The service does not start an OIDC flow of its own. The web application uses
the authorization-code flow with PKCE and holds no client secret. A machine
client uses the client-credentials flow.

### 5.2 The actor

| Token | Claim used | `Actor` |
|---|---|---|
| a person | `email`, with `email_verified` true | `Actor.human(email)` |
| an agent runner, or a CI job | `client_id` | `Actor.worker(client_id)` |
| another service | `client_id` | `Actor.system(name)` |

Rules:

- `01-domain-model.md`, section 1 makes the id of a human actor the email
  address. That keeps a journal legible years later. An identity provider
  that re-assigns an email address breaks that. A deployment of that kind
  sets `actor_claim = "sub"` in the configuration and shows names in the
  interface only. One deployment uses one claim. A journal must not hold
  both forms.
- A machine token that acts for a person sends the header
  `X-On-Behalf-Of`. The service puts that value in `Actor.on_behalf_of`. It
  accepts the header only when the token holds the scope
  `act:on_behalf_of`.
- **The service ignores an actor in a request body.** Flowlet v1 read the
  actor of a review from the body, so a decision was self-asserted. This
  service refuses that field with 400.
- The CLI keeps its `--by` option. The CLI runs in a trusted shell. The
  HTTP service never honours `by`.

### 5.3 The site

The service builds one `Site` at startup. `worker_id` is the instance name
of the service. `epoch` stays `None`, because the service holds no
execution lease.

## 6. Authorization

The engine records an actor and checks nothing. Authorization is not a
domain concept, so it lives here. Each endpoint in sections 7 to 10 names
one capability. The service maps a group of the
identity provider to a set of capabilities.

| Capability | Permits |
|---|---|
| `workflows:read` | the catalog |
| `executions:read` | executions, journals, frame trees, announcements |
| `executions:start` | `POST /executions` |
| `executions:signal` | `POST /executions/{eid}/signal` |
| `executions:cancel` | `POST /executions/{eid}/cancel` |
| `executions:migrate` | `POST /executions/{eid}/migrate` |
| `reviews:read` | the review list |
| `reviews:decide:{queue}` | a decision on one review queue |
| `queues:read` | queue depth and pending tasks |
| `tasks:consume:{queue}` | the worker plane on one queue |
| `evidence:read` | attempt logs and attachments |

Rules:

- `reviews:decide` is queue-scoped. The check happens here. The engine
  writes the decision of whoever calls it.
- `executions:migrate` replaces the code of a live execution. Give it to
  operators only.
- A caller with a valid token and no capability gets 403.
- A capability is not a filter. A caller with `executions:read` reads every
  execution. Per-execution access control is not in this version.

## 7. Catalog

The catalog replaces the authoring layer of Flowlet v1 (`taskflow`). It
holds no state. It reads the `Registry` of this process.

| Method and path | Capability | Source |
|---|---|---|
| `GET /workflows` | `workflows:read` | `Registry`, one item per `(name, version)` |
| `GET /workflows/{name}/versions/{version}` | `workflows:read` | the item, with its argument schema |

An item holds `name`, `version`, `summary` (the first line of the
docstring), `queue` and `has_schema`. The detail adds `description` and
`schema`.

Rules:

- `schema` is JSON Schema 2020-12. The service derives it from the
  signature of the workflow function and its type hints. It skips the first
  parameter, which is the `Context`.
- The service builds every schema at startup. A parameter with a type that
  the codec cannot render makes `has_schema` false. The workflow still
  starts. Its arguments are not checked.
- `POST /executions` validates `args` against the schema. It answers 422
  with the errors of the validator.
- The catalog shows the code of this process. Two instances with different
  code show different catalogs. Put one version of the code in one
  deployment.

## 8. Control plane

| Method and path | Capability | Engine or projection call |
|---|---|---|
| `POST /executions` | `executions:start` | `engine.start` |
| `GET /executions` | `executions:read` | `projection.executions` |
| `GET /executions/{eid}` | `executions:read` | `projection.execution`, then `engine.status` |
| `GET /executions/{eid}/journal` | `executions:read` | `engine.journal` |
| `GET /executions/{eid}/frames` | `executions:read` | a fold of the journal |
| `GET /executions/{eid}/children` | `executions:read` | `projection.children` |
| `GET /executions/{eid}/archive` | `executions:read` | `engine.archive` |
| `POST /executions/{eid}/signal` | `executions:signal` | `engine.signal` |
| `POST /executions/{eid}/cancel` | `executions:cancel` | `engine.cancel` |
| `POST /executions/{eid}/migrate` | `executions:migrate` | `engine.migrate` |
| `GET /reviews` | `reviews:read` | `projection.pending_reviews` |
| `GET /reviews/{rid}` | `reviews:read` | `projection.review` |
| `POST /reviews/{rid}/decide` | `reviews:decide:{queue}` | `engine.reviews.decide` |
| `GET /announcements` | `executions:read` | `projection.announcements` |
| `GET /queues` | `queues:read` | `Queue.depth` for each queue |
| `GET /queues/{queue}/tasks` | `queues:read` | `Queue.pending` |
| `GET /stats` | `executions:read` | `projection.counts_by_status` |
| `GET /health` | none | see section 8.6 |

### 8.1 Start

```http
POST /executions
{"workflow": "invoice_approval", "version": "3",
 "args": {"invoice_id": "INV-7"}, "queue": "default", "dispatch_key": null}
```

The response is 201 with `{"eid": ..., "deduplicated": false}` and the
header `X-Control-Seq`. `deduplicated` is true when the dispatch key had a
winner already. The status of the response stays 201, because the client
holds a valid `eid` either way.

### 8.2 Read an execution

The service reads the projection row. The row is absent in the short window
between `engine.start` and the next refresh. The service then calls
`engine.status(eid)`, which folds the control log. A `GET` with `min_seq`
(section 4.4) makes that window shorter.

The body holds the fields of the projection row (`08-projection.md`,
section 3): status, workflow, version, queue, parent, times, epoch, worker,
`suspended_on`, result or error.

### 8.3 Journal and frames

`GET /executions/{eid}/journal?after=0` returns the entries in order. One
entry holds `seq`, `type`, `fid`, `payload` and `provenance`. The endpoint
reads the archive when the live journal is gone (`engine.journal` does
this).

`GET /executions/{eid}/frames` returns the frame tree. The service folds the
journal with `MemoTable` (`02-journal.md`). One node holds:

```json
{"fid": "root/fetch#0", "kind": "step", "name": "fetch",
 "status": "completed", "attempts": 2,
 "started_at": "...", "ended_at": "...",
 "value": {...}, "error": null, "suspended_on": null,
 "evidence": true, "children": []}
```

Rules:

- The tree comes from the frame ids. A frame id is a path
  (`01-domain-model.md`, section 3.1), so the parent of a node is a prefix
  of it. The service does not reconstruct a tree from spans.
- `attempts` is the count of attempts of that frame. `evidence` says
  whether an attempt log exists. The interface reads the log only when a
  user asks for it (section 10).
- This endpoint feeds the flamegraph of the web application.

### 8.4 Signal, cancel, migrate

```http
POST /executions/{eid}/signal
{"channel": "payments", "payload": {"amount": 100}, "correlation": null}
```

The service scopes the channel to the execution, as `engine.signal` does.
`POST /executions/{eid}/deliver` is in the worker plane (section 9),
because it names a full channel.

`POST /executions/{eid}/migrate` takes `{"version": "4"}`. It answers 409
for a terminal execution.

### 8.5 Reviews

```http
POST /reviews/{rid}/decide
{"verdict": "approve", "data": null}
```

Rules:

- The service reads `eid` and `queue` from the projection row of `rid`. It
  never reads them from the body. This stops a caller from answering a
  review of another queue.
- The check of `reviews:decide:{queue}` uses the queue of that row.
- The actor is the token holder (section 5.2).
- `engine.reviews.decide` is idempotent. A second call returns the first
  decision with status 200.

### 8.6 Health

`GET /health` returns `{"status": "ok", "projection_seq": 812,
"checked_at": "..."}`. It refreshes the projection, so it also proves that
the bucket answers. The status is `degraded` with a 503 when the refresh
raises. The endpoint needs no token. It shows no execution data.

There is no lag in seconds. A control log with no recent entry is not a
projection that is behind, and nothing in the log says when the projection
read it. `projection_seq` and the time of the check are what the service
knows.

## 9. Worker plane

A consumer of a `DELEGATE` task uses this plane when it cannot reach the
bucket. An agent runner that reaches the bucket uses the `Queue` and
`Channel` ports directly.

| Method and path | Capability |
|---|---|
| `POST /queues/{queue}/dequeue` | `tasks:consume:{queue}` |
| `POST /queues/{queue}/tasks/{task_id}/renew` | `tasks:consume:{queue}` |
| `POST /queues/{queue}/tasks/{task_id}/state` | `tasks:consume:{queue}` |
| `POST /queues/{queue}/tasks/{task_id}/ack` | `tasks:consume:{queue}` |
| `POST /queues/{queue}/tasks/{task_id}/nack` | `tasks:consume:{queue}` |
| `POST /queues/{queue}/tasks/{task_id}/cancel` | `tasks:consume:{queue}` |
| `POST /executions/{eid}/deliver` | `tasks:consume:{queue}` |
| `PUT /evidence/{eid}/attempts/{attempt}/{name}` | `tasks:consume:{queue}` |

### 9.1 Dequeue

```http
POST /queues/agents/dequeue
{"ttl_seconds": 300, "wait_seconds": 20}
```

The response is 200 with the task and the holder, or 204 when the queue has
no visible task.

```json
{"task": {"task_id": "delegate:...", "kind": "delegate",
          "target": {"eid": "...", "fid": "root/agent#0"},
          "reply_channel": "...", "payload": {...}},
 "holder": "http:runner-a:0199...", "epoch": 7,
 "deadline_at": "2026-09-09T10:05:00.000000Z"}
```

Rules:

- The service mints the holder as
  `http:{client_id}:{token}`, with `token` from `secrets.token_urlsafe(16)`. The consumer sends that
  holder in every later call for this task. The string must be
  unguessable, because `Queue.attach` accepts whoever presents it. A UUIDv7
  is not acceptable here: its high bits are a timestamp. The capability
  `tasks:consume:{queue}` is the first gate and the holder is the second.
- `ttl_seconds` is the visibility timeout. An agent session is long, so a
  consumer asks for a long TTL and renews.
- `wait_seconds` is a long poll, at most 20. The service polls the queue
  while it waits. The queue gives a few dequeues per second per queue
  (`03-ports.md`, section 7). Set the number of consumers accordingly.
- The service returns a `DELEGATE` task only. A `START`, `RESUME` or
  `RUN_STEP` task belongs to a worker. The service nacks such a task back
  onto its queue and answers 409, so a misconfigured consumer delays the
  engine's own work but never takes it.

### 9.2 Renew, ack and nack

Each call takes `{"holder": "..."}`. `nack` also takes `delay_seconds`.

`renew` answers `{"epoch": 7, "deadline_at": "...", "state": {...}}`. The
state is how a consumer sees `cancel_requested`
(`10-agent-runner.md`, section 5), so a relayed consumer must read the body
of every renew, not only its status.

`state` merges a JSON object into the lease state with `update_state`. A
consumer of a long task writes the address of its work there, so a restart
or a steal can find it (`10-agent-runner.md`, section 4.2). The engine
never writes that field.

`cancel` asks the holder of a task to stop (`10-agent-runner.md`, section
5). It writes `cancel_requested` into the lease state and fences nobody, so
the holder keeps the task and reads the request on its next renew.

The service re-attaches to the lease with `Queue.attach` (`03-ports.md`,
section 7), which is `db.attach_lease` in CairnDB. Because the lease is a
document in the bucket, any instance of the service can serve a renew of a
task that another instance dequeued.
The call answers 409 with `not_holder` when the holder does not match, and
409 with `lease_lost` when the epoch moved.

### 9.3 Deliver

```http
POST /executions/{eid}/deliver
{"queue": "agents", "task_id": "delegate:...",
 "channel": "<the reply_channel of the task>", "payload": {...},
 "holder": "http:runner-a:V1v..."}
```

The body names the queue and the task, and not only the holder. The service
needs the queue to check `tasks:consume:{queue}`, and the task id to attach
to what the caller claims to hold. A holder string carries neither.

Rule: the service checks that the channel is the reply channel of a task
that this holder holds now. Without that check, one consumer could answer
the frame of another. A caller with `executions:signal` uses
`POST /executions/{eid}/signal` instead, which names a short channel and
scopes it.

The consumer calls `deliver` first and `ack` second. The reverse order can
lose the answer when the consumer stops between the two calls.

### 9.4 Why the relay stays small

Flowlet v1 gave an executor seven endpoints: claim, renew, effects,
messages, recv, outcome and admit. The executor co-authored the account of
its own attempt. In flowlet the queue lease gives the fence and the reply
channel carries the result, so an executor needs three verbs and one
answer. The agent stays a black box.

## 10. Evidence

Evidence is what a step or an agent wrote while it ran. The journal says
what happened. Evidence explains it. Evidence is never authority.

| Method and path | Capability | Content |
|---|---|---|
| `GET /evidence/{eid}` | `evidence:read` | the attempts that hold evidence |
| `GET /evidence/{eid}/attempts/{attempt}?fid=` | `evidence:read` | the items of one attempt |
| `GET /evidence/{eid}/attempts/{attempt}/log?fid=` | `evidence:read` | the attempt log, JSON Lines |
| `GET /evidence/{eid}/attempts/{attempt}/{name}?fid=` | `evidence:read` | one attachment |
| `PUT /evidence/{eid}/attempts/{attempt}/{name}?fid=&queue=` | `tasks:consume:{queue}` | put one attachment |

**The frame id is a query parameter, not a path segment.** A frame id holds
`/`, and a percent-encoded slash does not survive routing. Section 4.1 said
to encode it in the path: that does not work, and this is the correction.
The `PUT` names the queue as well, because a capability is queue-scoped and
nothing else in the request says which queue.

Rules:

- The attempt log is the structlog stream of one attempt. `log.py` binds
  `eid`, `fid` and `attempt` to every event of a worker already, so an
  author writes `log.info("fetched", n=12)` and nothing more.
- The key is the attempt, not the frame. A retry writes a second log. The
  interface shows the attempt that a user selects.
- A replay writes nothing. A frame with a memo does not run.
- An attachment holds what is too large for a payload: an agent
  transcript, a diff, a report. The reply of a `DELEGATE` task carries the
  reference, not the bytes.
- The service returns `text/plain` or the media type of the attachment. It
  streams the object. It can answer with a redirect to a pre-signed URL
  when the storage gives one.
- The `Evidence` port (`03-ports.md`, section 12) specifies the writes, the
  size limit and the deletion by the retention job.

## 11. The web application

The web application polls with `If-None-Match` (section 4.3). Flowlet v1
did the same and had no server-sent events. Suggested periods: 2 seconds
for one open execution, 10 seconds for a list, 30 seconds for the catalog.
A 304 costs one request and no work.

The application needs no endpoint of its own. Its five views map to
sections 7 to 10:

| View | Endpoints |
|---|---|
| dashboard | `/stats`, `/executions?status=`, `/queues` |
| execution list | `/executions` |
| execution detail | `/executions/{eid}`, `/frames`, `/journal`, `/evidence/...` |
| start a workflow | `/workflows`, `POST /executions` |
| review inbox | `/reviews`, `POST /reviews/{rid}/decide` |

## 12. Limits

- No server-sent events and no WebSocket in this version. A later version
  can add a stream over `log.tail` and `objects.wait_for`.
- One bucket is one tenant. There is no tenant field.
- Per-execution access control is not in this version (section 6).
- The relay of section 9 inherits the throughput of the queue.
- The catalog shows the code of the process that answers (section 7).
