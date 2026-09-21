# 03 — Ports and CairnDB mapping

## 1. Principle

The domain defines each primitive as a Python `Protocol`. One adapter module,
`adapters/cairndb.py`, implements all of them with the CairnDB engine API
(`cairndb/docs/ENGINE_API.md`). The domain imports one thing from CairnDB:
the `Timestamp` value type. It never imports storage, logs or coordination.

Each port maps to exactly one CairnDB construct.

| Port | Purpose | CairnDB construct |
|---|---|---|
| `Journal` | trace and memo of one execution | named log |
| `ControlLog` | lifecycle entries of all executions | named log `wf` |
| `Ownership` | one owner per execution | `db.lease` |
| `Dispatch` | idempotent start | `db.claim` |
| `Queue` | tasks | objects plus `db.lease` |
| `Channel` | messages | named log per channel plus wait markers |
| `Timers` | future resumes | objects listed by prefix |
| `ExecutionStore` | the `Execution` record | object, put-if-absent, CAS |
| `Archive` | folded finished executions | object, put-if-absent |
| `Evidence` | attempt logs and attachments | objects listed by prefix |

## 2. Key layout

All keys of the engine start with `wf/`. All logs of the engine start with
`wf`.

| Key or log name | Content | Write mode |
|---|---|---|
| `logs/wf/` | control log | append |
| `logs/wf.exec.{eid}/` | journal | append |
| `logs/wf.ch.{channel}/` | channel messages | append |
| `wf/exec/{eid}/meta` | `Execution` | put-if-absent |
| `wf/exec/{eid}/lease` | ownership lease | CairnDB lease |
| `wf/dispatch/{key}` | `{"eid": ...}` | claim |
| `wf/claims/{key}` | any JSON value | claim |
| `wf/queues/{queue}/t/{not_before}-{task_id}` | `Task` | put-if-absent |
| `wf/queues/{queue}/l/{not_before}-{task_id}` | task lease | CairnDB lease |
| `wf/queues/{queue}/ids/{task_id}` | enqueue marker: the task's `t/` key | put-if-absent |
| `wf/waits/{channel}/{eid}` | `{"fid": ...}` | put, delete |
| `wf/timers/{timer_id}` | `Timer` | put-if-absent, delete |
| `wf/archive/{eid}.msgpack` | folded journal, record and scoped channels | put-if-absent |
| `wf/evidence/{eid}/{frame}/meta` | `{fid, attempt}` | put-if-absent |
| `wf/evidence/{eid}/{frame}/log.{part}` | one flush of the attempt log | put-if-absent |
| `wf/evidence/{eid}/{frame}/a/{name}` | one attachment | put |

Rules:

- `not_before` in a queue key is `Timestamp.to_iso()`: a fixed-width RFC 3339
  UTC instant with microsecond precision. It sorts lexically. A task with no
  delay uses its enqueue time.
- `{eid}` in a key or a log name is the canonical 36-character UUID string.
- The `channel` segment of a log name must match `^[a-z0-9._-]+$`. The
  domain checks this before it calls the adapter.
- The `{frame}` segment of an evidence key is
  `{digest_text(fid)[:16]}.{attempt}`. A frame id holds `/`, `#` and `:`,
  so it is not put in a key. The digest is the same function that
  `FrameRef` uses for its derived channel names (`01-domain-model.md`,
  section 3.2). The map back to the frame id is the `meta` object.

## 3. Journal port

```python
class Journal(Protocol):
    async def append(self, eid: str, entry: Entry) -> int
    async def read(self, eid: str, after: int = 0) -> list[Sequenced[Entry]]
    async def tail(self, eid: str) -> int
    async def delete(self, eid: str) -> None          # retention only
```

Mapping:

- `append` is `db.log(f"wf.exec.{eid}").append(event)`. The returned
  sequence is the CairnDB sequence number.
- `read` is `db.log(...).read(after=after)`.
- `append` is durable when it returns. The domain relies on this for the
  memo rule.
- An `Entry` becomes a CairnDB `Event`: `event_type` is the entry type,
  `timestamp` is the provenance time, `payload` is the payload, and
  `metadata` holds `{"provenance": unstructure(provenance), "fid": fid}`.
- `delete` removes every commit and snapshot of the log. Only the
  retention job calls it, after the archive is written.

## 4. ControlLog port

```python
class ControlLog(Protocol):
    async def announce(self, entry: Entry) -> int
    async def read(self, after: int = 0) -> list[Sequenced[Entry]]
```

Mapping: `db.log("wf")`.

## 5. Ownership port

```python
class Ownership(Protocol):
    async def acquire(self, eid: str, holder: str, ttl: float) -> Lease | None
    async def request_cancel(self, eid: str, by: Provenance) -> None
    async def inspect(self, eid: str) -> LeaseInfo | None
    async def delete(self, eid: str) -> None          # retention only
```

`LeaseInfo` holds `epoch, holder, deadline_at, released, state`. It is
expired when `released` is true or `deadline_at` is not after now.

Mapping:

- `acquire` is `db.lease(f"wf/exec/{eid}/lease", ttl=ttl, holder=holder,
  steal_if_expired=True)`. `None` means another worker holds the lease and
  the lease is not expired.
- `Lease.renew`, `Lease.release` and `Lease.epoch` are the CairnDB methods
  and attribute of the same names.
- `Lease.refresh_state` is `update_state(lambda s: s)`. It absorbs
  cooperative writes without a change.
- `request_cancel` is `db.cooperative_write(key, lambda s: {**(s or {}),
  "cancel_requested": by})`.
- `inspect` is `db.objects.get(key)` and a parse of the lease document. It
  does not change the document.

Rules:

- `ttl` is the execution lease TTL. Default 120 seconds.
- A worker renews the lease at most every `ttl / 3` seconds while it runs a
  live frame.
- `LeaseLost` on any renew means the worker was fenced. See
  `05-protocols.md`, section 8.

## 6. Dispatch port

```python
class Dispatch(Protocol):
    async def claim_start(self, key: str, eid: str) -> tuple[bool, str]
    async def claim(self, key: str, value: Any) -> tuple[bool, Any]
```

Mapping:

- `claim_start` is `db.claim(f"wf/dispatch/{key}", {"eid": eid})`. The
  result is `(result.won, result.value["eid"])`. The loser gets the
  winner's `eid`.
- `claim` is `db.claim(f"wf/claims/{key}", value)`. The result is
  `(result.won, result.value)`. Patterns use it for exactly-one decisions.

## 7. Queue port

```python
@dataclass(frozen=True)
class ClaimedTask:
    task: Task
    key: str
    lease: Lease

@dataclass(frozen=True)
class QueueDepth:
    total: int          # tasks on the queue
    visible: int        # tasks whose not_before is not after now
    claimed: int        # tasks with a lease document

class Queue(Protocol):
    async def enqueue(self, task: Task) -> bool
    async def ensure(self, task: Task) -> bool                       # repair paths
    async def dequeue(self, queue: str, worker: str, ttl: float) -> ClaimedTask | None
    async def take(self, queue: str, task_id: str, worker: str, ttl: float) -> ClaimedTask | None
    async def ack(self, claimed: ClaimedTask) -> None
    async def nack(self, claimed: ClaimedTask, delay: timedelta) -> None
    async def depth(self, queue: str) -> QueueDepth                  # operators
    async def pending(self, queue: str, limit: int = 100) -> list[Task]
    async def peek(self, queue: str, task_id: str) -> Task | None
    async def attach(self, queue: str, task_id: str, holder: str, ttl: float) -> ClaimedTask | None
    async def request_cancel(self, queue: str, task_id: str, by: Provenance) -> None
```

Mapping:

- `enqueue` first writes the enqueue marker `ids/{task_id}` with
  put-if-absent. A `False` result there means this task id is on the queue
  already, so the call is a no-op, not an error. It then writes the task
  under `t/`. The body of the marker is the `t/` key it just chose, which
  makes the marker the rendezvous of everyone who places this task id.
- `ensure` is `enqueue` that repairs. `enqueue` writes the marker before the
  task, so a crash between the two leaves a marker that refuses every later
  `enqueue` of that task id, and the task is lost. `ensure` claims the marker
  put-if-absent; when it loses, it reads the key the winner named and writes
  the task there, if nothing answers to that key already. Both writers
  therefore aim at one key, and put-if-absent on the task settles which of
  them creates it: a repair that runs beside a live `enqueue` cannot queue
  the task twice. `ensure` stays marker-first, so its own interruption is
  repaired by the next call.
- A marker written before markers named their key is empty. `ensure` adopts
  one with a CAS on its etag, so two repairs of one stranded id still agree
  on one key.
- `nack` repoints the marker at the new `t/` key before it deletes the old
  one, so an `ensure` that reads the marker in between always reads the name
  of a key that exists.
- `dequeue`:
  1. `keys = db.objects.list(f"wf/queues/{queue}/t/")`.
  2. Drop keys whose `not_before` segment is after now.
  3. For each remaining key in order, call `db.lease` on the same key with
     `t/` replaced by `l/`, with `ttl=ttl`, `holder=worker` and
     `steal_if_expired=True`.
  4. Return the first success with the task read from the `t/` key. Return
     `None` when no lease succeeds.
- `take` is `dequeue` for one known `task_id`: find its key under the
  prefix, then lease it. `None` when the task is absent, not yet visible,
  or held by someone else.
- `ack` is `lease.release()` then a delete of the task key, the enqueue
  marker and the lease key.
- `nack` re-enqueues the task with `not_before = now + delay` under a new
  key, repoints the marker at it, then acks the old one. The enqueue marker
  stays, because the task id does not change.
- `depth` is one `list` on the `t/` prefix and one `list` on the `l/`
  prefix. It reads no object. The `not_before` segment of a key gives
  `visible`.
- `pending` is `list` on the `t/` prefix plus one `get` for each key, up to
  `limit`.
- `peek` finds the key of one task id under the `t/` prefix and reads it. It
  takes no lease: a lease is for removal, not for a read. The review pattern
  uses it to find a reply channel without holding the task.
- `attach` is `db.attach_lease(lease_key, holder=holder, ttl=ttl)` plus a
  read of the task. It returns `None` when the task is gone, when the
  holder does not match, or when the lease expired.
- `request_cancel` is `db.cooperative_write(lease_key, ...)`.
- The `Lease` a task gives has `update_state(fn)` and `deadline_at`. The
  holder writes its own state with the first and paces its renewals with
  the second.
- `request_cancel` is `db.cooperative_write(lease_key, lambda s: {**(s or
  {}), "cancel_requested": by})`. It fences nobody. The holder reads it on
  its next `renew`. This is `Ownership.request_cancel` one level down.

Rules:

- Only repair paths call `ensure`: the sweeper (`05-protocols.md`, section 9),
  which is the last thing that will put a lost task back. Every other caller
  uses `enqueue`, which reads nothing: `ensure` pays one read of the marker,
  and one more put when it adopts an empty one.
- The task lease TTL is the visibility timeout. Default 60 seconds. A worker
  renews it while it holds the task.
- An expired task lease makes the task visible again. This gives
  at-least-once delivery.
- `list` returns keys in lexical order. Because keys start with
  `not_before`, older tasks come first.
- Throughput is a few dequeues per second per queue, because a dequeue is
  a `list` plus a lease acquisition on blob storage. This is accepted.
- `claimed` counts lease documents. A lease document of a dead holder stays
  until a worker steals it or acks the task. So `claimed` is an upper
  bound of the tasks that run now.
- `attach` exists for the HTTP worker plane (`09-http-api.md`, section 9).
  A lease is a document, so any process can re-attach to it. A worker in
  the engine holds its `ClaimedTask` in memory and does not need `attach`.
- `db.attach_lease` writes nothing and does not bump the epoch. It
  re-enters the ownership period that `dequeue` began. The handle it gives
  and the handle of the process that dequeued converge on each other's
  writes, because ownership identity is `(key, epoch, holder)`.
- The holder string is a credential: whoever knows it can renew, ack and
  nack the task. The HTTP worker plane mints an unguessable one
  (`09-http-api.md`, section 9.1).
- The `state` of a task lease belongs to the holder. The engine writes
  nothing there. A consumer of a long `DELEGATE` task keeps the address of
  its work there, so a restart or a steal can find it
  (`10-agent-runner.md`, sections 4.2 to 4.4).

## 8. Channel port

```python
class Channel(Protocol):
    async def send(self, message: Message) -> int
    async def read(self, channel: str, after: int = 0) -> list[Message]
    async def register_wait(self, channel: str, ref: FrameRef) -> None
    async def clear_wait(self, channel: str, ref: FrameRef) -> None
    async def waiters(self, channel: str) -> list[FrameRef]
    async def all_waits(self) -> list[tuple[str, FrameRef]]
    async def scoped(self, eid: str) -> list[str]     # channels named "{eid}.*"
    async def delete_channel(self, channel: str) -> None   # retention only
```

Mapping:

- `send` is `db.log(f"wf.ch.{channel}").append(event)`. The message `seq`
  is the returned sequence number.
- `read` is `db.log(...).read(after=after)`.
- `register_wait` is `db.objects.put(f"wf/waits/{channel}/{eid}", {"fid":
  fid})`.
- `clear_wait` is `db.objects.delete` of the same key.
- `waiters` is `db.objects.list(f"wf/waits/{channel}/")` plus one `get`
  per key.
- `all_waits` is `db.objects.list("wf/waits/")` plus one `get` per key.
- `scoped` is `db.objects.list(f"logs/wf.ch.{eid}.")`, reduced to log
  names.
- `delete_channel` deletes every commit of the channel log.

Rules:

- A message is durable and ordered when `send` returns.
- One channel has one log. The order of messages on one channel is total.
- A channel with no message and no log does not exist yet. `read` on it
  returns an empty list.

## 9. Timers port

```python
class Timers(Protocol):
    async def schedule(self, timer: Timer) -> None
    async def due(self, now: Timestamp) -> list[Timer]
    async def remove(self, timer: Timer) -> None
    async def remove_for(self, eid: str) -> int      # retention only
```

Mapping:

- `schedule` is `db.objects.put(f"wf/timers/{timer_id}", timer,
  if_absent=True)`.
- `due` is `db.objects.list("wf/timers/")` filtered on
  `timer_id <= now_iso`. The `timer_id` starts with the ISO instant, so the
  filter is a string comparison.
- `remove` is `db.objects.delete`.
- `remove_for` lists `wf/timers/` and deletes every id that contains
  `-{eid}-`.

## 10. ExecutionStore port

```python
class ExecutionStore(Protocol):
    async def create(self, execution: Execution) -> bool
    async def read(self, eid: Eid) -> Execution | None
    async def replace(self, eid: Eid, fn: Callable[[Execution], Execution]) -> Execution | None
    async def delete(self, eid: Eid) -> None          # retention only
```

Ports speak domain objects. The adapter serializes them with
`flowlet.codec`.

Mapping:

- `create` is `db.objects.put(f"wf/exec/{eid}/meta", json(unstructure(execution)),
  if_absent=True)`. It returns `True` when this call created the object.
- `read` is `db.objects.get(f"wf/exec/{eid}/meta")`, then `structure`.
- `replace` is a read, `fn`, then `put(..., if_match=etag)`. On a lost CAS
  it reads again and retries. `fn` must be pure. `None` when the record is
  absent.

Rules:

- The record changes only through `replace`. The `migrate` operation
  (`05-protocols.md`, section 10) is its only caller. It changes `version`.

## 11. Archive port

```python
class Archive(Protocol):
    async def write(self, eid: str, data: dict) -> bool
    async def read(self, eid: str) -> dict | None
```

Mapping: `db.objects.put(f"wf/archive/{eid}.msgpack", msgpack(data),
if_absent=True)` and `db.objects.get`. `data` holds `eid`, `execution`,
`archived_at`, `journal` (a list of `{seq, event}`) and `channels` (a map
from channel name to its messages). See `05-protocols.md`, section 11.

## 12. Evidence port

Evidence is what a step or an agent wrote while it ran. The journal says
what happened. Evidence explains it. The engine never reads evidence, and
no decision depends on it.

```python
@dataclass(frozen=True)
class EvidenceRef:
    eid: Eid
    fid: str
    attempt: int

@dataclass(frozen=True)
class EvidenceItem:
    ref: EvidenceRef
    name: str                     # "log" or the name of an attachment
    media_type: str
    size: int

class Evidence(Protocol):
    async def append_log(self, ref: EvidenceRef, part: bytes) -> None
    async def read_log(self, ref: EvidenceRef) -> bytes
    async def put(self, ref: EvidenceRef, name: str, data: bytes, media_type: str) -> str
    async def get(self, ref: EvidenceRef, name: str) -> bytes | None
    async def list(self, eid: Eid) -> list[EvidenceItem]
    async def delete_for(self, eid: Eid) -> int        # retention only
```

Mapping:

- `append_log` writes `wf/evidence/{eid}/{frame}/log.{part}` with
  put-if-absent. `part` is a counter of the flushes of this attempt, padded
  to six digits so the keys sort as they were written. A lost race moves to
  the next number. The first call also writes the `meta` object.
- `read_log` lists the `log.` keys, reads them in order and joins them. The
  content is JSON Lines.
- `put` writes `wf/evidence/{eid}/{frame}/a/{name}`. It returns the key.
- `list` is one `list` on `wf/evidence/{eid}/` plus a read of each `meta`.
- `delete_for` deletes the prefix `wf/evidence/{eid}/`.

Rules:

- The key of evidence is the attempt, not the frame. A retry writes a
  second log. A replay writes nothing, because a frame with a memo does not
  run.
- The runtime writes the log through a structlog processor. `log.py` binds
  `eid`, `fid` and `attempt` already, so an author writes `log.info(...)`
  and nothing more. The processor is the last one before the renderer, so
  the event it keeps carries the level, the timestamp and the bound context.
- The buffer covers the live run of a frame, and the line that records a
  failure is inside it. A failure is what a reader wants most.
- The processor holds the events of the attempt in memory. It flushes at
  the end of the attempt, at a failure, at a suspension, and every
  `flush_interval` seconds (default 10). One flush is one object. One event
  is never one object.
- **A write of evidence is best effort.** An error is logged and is not
  raised into the frame. A frame must not fail because a log did not write.
- One attempt has a size limit (default 1 MiB). The processor keeps the
  first part and the last part of the log and puts one line between them
  that says how many events it dropped.
- The retention job calls `delete_for` before it deletes the record
  (`05-protocols.md`, section 11). Without that call the objects stay
  forever.
- The processor keeps what the log level lets through, so evidence follows
  the level of the process. At `INFO` a step that logs nothing leaves no
  object at all, and the engine's own frame lines (which are `DEBUG`) do not
  reach the buffer. Evidence therefore costs nothing until somebody logs.
- `NullEvidence` accepts every write and stores nothing. A process that wants
  no evidence uses it in place of the adapter of its backend.

## 13. Consistency summary

| Guarantee | Source |
|---|---|
| A journal entry is durable and ordered when `append` returns. | CairnDB log append |
| The memo of a frame never changes once written. | memo rule plus log immutability |
| One worker owns an execution at a time, within clock skew much smaller than the TTL. | CairnDB lease |
| A fenced worker gets `LeaseLost` on its next renew. | CairnDB lease epoch plus CAS |
| One task is dequeued by one worker at a time. | CairnDB lease on the task key |
| Two starts with one dispatch key produce one execution. | CairnDB claim |
| One channel has a total order of messages. | one log per channel |

Not guaranteed:

- Order between two channels.
- Order between the journal and the control log.
- Exactly-once external effects of a step.
