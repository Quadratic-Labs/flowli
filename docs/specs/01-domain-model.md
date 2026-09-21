# 01 — Domain model

All objects are immutable values. A change of state is a new journal entry,
not a mutation of an object. Types below use Python dataclass syntax.

The dataclasses describe data and check intrinsic invariants only, such as
`attempt >= 1` or `max_attempts >= 1`. They do not coerce their inputs, do
not check storage rules, and do not serialize themselves. Three other places
do those jobs:

- `flowlet.codec` turns objects into JSON-compatible dicts and back
  (`unstructure`, `structure`), with one rule each for `Timestamp`, `UUID`
  and `timedelta`. `structure` is where inbound data is validated.
- Boundaries validate names: the engine and the Context check queue and
  channel names, the CLI parses ids, the adapters check what they store.
- Adapters map objects to their storage form: the CairnDB adapter builds
  `Event` records from `Entry` values, and JSON or msgpack from the rest.

`flowlet.domain` imports neither the codec nor an adapter.

`Timestamp` is CairnDB's value type, imported as `cairndb.Timestamp` by
every layer: a timezone-aware UTC instant that orders, shifts by a
`timedelta`, and round-trips through its `to_iso` and `from_iso` methods in
a fixed-width form (microseconds, `Z` suffix) that sorts lexically as it
sorts in time. flowlet adds no wrapper around it. Every dict form holds
the ISO string. A shared library of such cross-layer value types is planned.

## 1. Provenance

Provenance answers four questions: who, what, where, when. Every journal
entry, every control-log entry, every task and every message carries one
`Provenance` value.

```python
@dataclass(frozen=True)
class Actor:
    kind: Literal["worker", "human", "system", "schedule"]
    id: str                          # worker id, user email, system name, schedule name
    on_behalf_of: str | None = None  # principal that the actor represents

@dataclass(frozen=True)
class Site:
    host: str
    pid: int
    worker_id: str
    instance: str | None = None      # container id or VM id
    region: str | None = None
    epoch: int | None = None         # lease epoch held while acting

@dataclass(frozen=True)
class Code:
    workflow: str
    version: str
    frame_kind: str                  # see FrameKind
    frame_name: str
    code_ref: str | None = None      # git sha or image digest

@dataclass(frozen=True)
class Provenance:
    actor: Actor
    site: Site
    code: Code
    attempt: int
    at: Timestamp
```

Rules:

- A worker builds one `Site` at start. It copies that `Site` with the epoch
  set when it acquires a lease.
- A human actor has `kind="human"` and `id` equal to the user email.
- `on_behalf_of` is set when a system acts for a person. Example: a UI
  service sends a message for a reviewer.

## 2. Execution

```python
class ExecutionStatus(Enum):
    PENDING = "pending"        # created, no worker ran it yet
    RUNNING = "running"        # a worker holds the lease
    SUSPENDED = "suspended"    # no worker holds the lease, a task or timer will resume it
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass(frozen=True)
class Execution:
    eid: Eid                          # uuid.UUID: v7 when started, v5 when derived
    workflow: str
    version: str
    args: Any                         # JSON-compatible
    created_by: Provenance
    parent: FrameRef | None           # set for a child execution
    dispatch_key: str | None          # idempotent start key
    queue: str                        # queue of the START task and of every RESUME task
```

State transitions:

```
PENDING ──start──▶ RUNNING ──suspend──▶ SUSPENDED ──resume──▶ RUNNING
RUNNING ──complete──▶ COMPLETED
RUNNING ──fail──▶ FAILED
RUNNING | SUSPENDED ──cancel──▶ CANCELLED
```

Rules:

- `Eid` is `uuid.UUID`. A started execution gets a UUIDv7, so ids sort by
  creation time and carry their creation instant. A child execution gets a
  UUIDv5 of `"{parent_eid}/{fid}"` under the flowlet namespace, so the id
  is deterministic in the parent frame. See `04-api.md`, section 2.2.
- Storage keys and log names use the canonical 36-character form. It
  matches the CairnDB log-name rule `^[a-z0-9._-]+$`.
- `parse_eid` accepts a `UUID` or its canonical or hex string and raises
  `InvalidName` otherwise. The codec and the CLI call it. Dict forms of
  every object hold the string.
- `COMPLETED`, `FAILED` and `CANCELLED` are terminal. No transition leaves
  them.
- The status of an execution is derived from the control log. The journal
  does not hold a status field.

## 3. Frame

```python
class FrameKind(Enum):
    ROOT = "root"          # the workflow function itself
    STEP = "step"          # a function with effects, run once, memoized
    CHILD = "child"        # a child execution
    RECEIVE = "receive"    # wait for a message on a channel
    SLEEP = "sleep"        # wait until an instant

@dataclass(frozen=True)
class FrameRef:
    eid: Eid
    fid: str

    child_eid: Eid                  # derived: uuid5(namespace, f"{eid}/{fid}")
    child_channel: str              # derived: "{eid}.child.{child_eid}"
    step_channel: str               # derived: "{eid}.step.{digest_text(fid)[:16]}"
    def reply_channel(self, frame: str) -> str   # "{eid}.reply.{digest_text(fid + "/" + frame)[:16]}"

@dataclass(frozen=True)
class Frame:
    ref: FrameRef
    kind: FrameKind
    name: str
    args_digest: str                  # content digest of the arguments, see 02 section 5
    retry: RetryPolicy
```

Everything that follows from the pair `(eid, fid)` is a derived property of
`FrameRef`, never a stored field or a free function: the id of the child
execution this frame starts, and the channels on which a child, a detached
step, or a delegate answers this frame. See section 3.2 for `digest_text`.

### 3.1 Frame id

A frame id is a path. The root frame has `fid = "root"`. A child frame of
frame `P` with name `N` has:

- `fid = P + "/" + N + "#" + n` when the author gives no key. `n` is the
  count of frames with name `N` already created under `P` in this replay.
  The count starts at 0.
- `fid = P + "/" + N + ":" + key` when the author gives `key`.

Rules:

- The engine assigns `n` at the moment the author calls the `Context`
  method. It does not wait for the frame to run. This makes `n` stable under
  `asyncio.gather`.
- `N` and `key` must match `^[A-Za-z0-9_-]+$`.
- A frame id is unique inside one execution. The engine raises
  `DuplicateFrameError` when a key repeats under the same parent.

### 3.2 Argument digest

`args_digest` is `sha256(canonical_json(unstructure(arguments)))`. The codec
renders the arguments into JSON-native data with its one rule set, and the
canonical JSON is sorted, compact, and free of NaN. The domain holds the
digest as a string and never computes it: `flowlet.codec.digest` does.
Names derived from frame ids (`step_channel`, `reply_channel`, `timer_id`)
use `digest_text`, a plain sha256 of a string, which stays in the domain.

### 3.3 Frame status

Frame status is derived from the journal. See `02-journal.md`.

```
RUNNING ──▶ COMPLETED
RUNNING ──▶ FAILED (retry allowed) ──▶ RUNNING
RUNNING ──▶ FAILED (no retry)
RUNNING ──▶ SUSPENDED ──▶ RUNNING          # receive, sleep, child
```

## 4. Attempt and outcome

```python
@dataclass(frozen=True)
class Attempt:
    frame: FrameRef
    number: int                       # 1 for the first attempt
    provenance: Provenance

@dataclass(frozen=True)
class Completed:
    value: Any

@dataclass(frozen=True)
class Failed:
    error_type: str
    message: str
    retryable: bool

Outcome = Completed | Failed
```

Rules:

- The first attempt of a frame has `number = 1`.
- A new attempt gets `number = 1 + count of frame.failed entries for that fid`.
- The memo of a frame is the first `Completed` outcome in the journal for
  that fid. See `02-journal.md`, section 3.

## 5. Retry policy

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: timedelta = timedelta(seconds=0)
    backoff_factor: float = 1.0
    max_backoff: timedelta | None = None
```

Rules:

- `max_attempts = 1` means no retry.
- A `Failed` outcome with `retryable = False` stops the retries.
- A retry with a non-zero delay suspends the frame with a timer. See
  `05-protocols.md`, section 6.

## 6. Task

```python
class TaskKind(Enum):
    START = "start"        # run the root frame of a new execution
    RESUME = "resume"      # run a suspended execution
    RUN_STEP = "run_step"  # run one step frame in another worker pool
    DELEGATE = "delegate"  # consumed outside the engine: humans or an external system

@dataclass(frozen=True)
class Task:
    queue: str
    kind: TaskKind
    target: FrameRef
    reason: str                       # "start", "message:<channel>", "timer", "recovery", "child:<eid>"
    enqueued_by: Provenance
    key: str = ""                     # dedup discriminator inside one kind and target
    not_before: Timestamp | None = None
    payload: Any = None               # RUN_STEP and DELEGATE

    task_id: str                      # derived: "{kind}:{eid}" + (":{key}" if key else "")
```

Rules:

- `task_id` is derived from `kind`, `target.eid` and `key`, so it is never
  stored and cannot disagree with them. `Task.id_for(kind, eid, key)` gives
  the same string without an instance, for lookups. It is unique inside a
  queue. A second enqueue with the same id is a no-op.
- A `START` task has no key: `start:{eid}`. A `RESUME` task's key is its
  cause: the channel name plus message sequence, the timer id,
  `recovery:{epoch}`, `child:{eid}`, `migrate:{version}` or `cancel`. A
  `RUN_STEP` task's key is the frame id. A `DELEGATE` task's key is the
  delegate frame name, or `review:{rid}` for a review.
- A worker never runs a `DELEGATE` task. When a worker dequeues one, it
  returns the task to the queue with a delay.
- `not_before` delays visibility. A worker does not dequeue the task before
  that instant.

## 7. Message

```python
@dataclass(frozen=True)
class Message:
    channel: str
    seq: int                          # position in the channel log
    payload: Any
    sent_by: Provenance
    correlation: str | None = None
```

Rules:

- `channel` must match `^[a-z0-9._-]+$`.
- Channels scoped to an execution have the prefix `{eid}.`. The ones the
  engine derives are properties of `FrameRef` (section 3). Authors name the
  rest through `execution_channel(eid, name)`.
- `seq` is assigned by the channel log. It is dense and total inside one
  channel.

## 8. Timer

```python
@dataclass(frozen=True)
class Timer:
    due_at: Timestamp
    target: FrameRef

    @property
    def timer_id(self) -> str:        # "{due_at_iso}-{eid}-{fid_digest}", derived
```

The id is derived from the two fields, never stored, so a timer cannot
carry an id that disagrees with its instant or its frame. Two timers for one
frame and one instant are one timer. The id starts with the ISO instant, so
ids sort by due time.

## 9. Lease view

The domain sees a lease through this interface. CairnDB implements it.

```python
class Lease(Protocol):
    epoch: int
    state: Any                        # cooperative state, see 05-protocols.md section 8
    deadline_at: Timestamp | None     # when it expires unless it is renewed
    async def renew(self) -> None            # raises LeaseLost
    async def release(self, state: Any) -> None
    async def refresh_state(self) -> Any     # read cooperative writes
    async def update_state(self, fn) -> Any  # write the holder's own state
```
