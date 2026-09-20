# 04 — API

*Status: draft, 2026-09-07. Writing style: ASD-STE100 Strict.*

## 1. Workflow definition

```python
engine = WorkflowEngine(db, site=Site.local(worker_id="w-1"), code_ref="git:abc123")

@engine.workflow("invoice_approval", version="3")
async def invoice_approval(ctx: Context, invoice_id: str) -> str:
    ...
```

Rules:

- The first parameter of a workflow function is a `Context`.
- All other parameters and the return value must be JSON-compatible.
- The engine registers the function under `(name, version)`. Two versions
  of one name can be registered at the same time.
- A workflow function must be deterministic. It must read the clock, random
  numbers and external state through a step only.
- A workflow function must not change its arguments. A replay runs the
  function again with the arguments of the record, so a mutation would make
  the second run see something the first did not. Copy what must change.

## 2. Context API for authors

Every `Context` method below creates one frame. The frame id follows
`01-domain-model.md`, section 3.1.

### 2.1 step

```python
value = await ctx.step(fn, *args, name=None, key=None, retry=RetryPolicy(), **kwargs)
```

- Runs `fn(*args, **kwargs)` once. `fn` can be sync or async.
- `name` defaults to `fn.__name__`.
- Returns the memo when one exists. Runs `fn` otherwise.
- The arguments must be JSON-compatible. The engine computes `args_digest`
  from them.
- On exception, the engine appends `frame.failed`. It applies `retry`. When
  no retry remains, it raises `StepFailed` in the workflow function.
  `StepFailed.failed` holds the error type and message. The original
  exception object exists only on the attempt that raised it.
- A step raises `NonRetryableError`, or a subclass of it, to stop retries at
  once.

### 2.2 child

```python
value = await ctx.child(workflow_fn, *args, name=None, key=None, queue="default", **kwargs)
```

- Starts a new execution of `workflow_fn` with `parent` set to this frame.
- `name` defaults to `workflow_fn.__name__`, and not to the registered
  workflow name: a workflow name may hold `.`, and a frame name may not.
- Suspends this frame on `child:{child_eid}` until the child completes.
- Returns the child's return value. Raises `ChildFailed` when the child
  fails or is cancelled.
- The child `eid` is `FrameRef(parent_eid, fid).child_eid`, a UUIDv5 of the
  parent frame. This makes the child start idempotent without a dispatch
  key. Two fenced parent attempts that start the same child converge on one
  execution.
- The frame has one sub-frame, `{fid}/start#0`. It is a step that creates
  the child execution, with 3 attempts. The child frame itself is a receive
  on the frame's `child_channel` with condition `child:{child_eid}`.
- The child sends `{"status": "completed", "value": ...}` or `{"status":
  "failed" | "cancelled", "error": ...}` on that channel.

### 2.3 send

```python
seq = await ctx.send(channel, payload, correlation=None, name=None, key=None)
```

- A step that sends one message. The memo is the message `seq`.
- A replay does not send the message again.

### 2.4 receive

```python
message = await ctx.receive(channel, timeout=None, scope="execution", name=None, key=None)
```

- `scope="execution"` reads channel `{eid}.{channel}`. `scope="global"`
  reads `channel` as given. `send` takes the same parameter.
- Suspends this frame on `channel:{full_name}` until a message is available.
- Returns the first message on the channel with `seq` greater than the last
  `seq` this execution consumed on that channel.
- Returns `None` when `timeout` expires first.
- The memo is the consumed `seq`, or `None` on timeout. A replay returns the
  same message.

The last consumed `seq` per channel is derived from the memo table: the
maximum `fulfilled[fid]` over `RECEIVE` frames on that channel.

### 2.5 sleep

```python
await ctx.sleep(duration)
await ctx.sleep_until(instant)
```

- Suspends this frame on `timer:{timer_id}`.
- The due instant is computed once, at the first attempt. The memo stores it.
  A replay does not compute it again.

### 2.6 gather

```python
results = await ctx.gather(ctx.step(a), ctx.step(b), ctx.receive("x"))
```

- Runs frames concurrently inside one execution.
- The frame ids are assigned in argument order before any frame runs.
- The execution suspends only when every frame in the gather waits.

### 2.7 announce

```python
await ctx.announce("review.requested", {"rid": rid, "queue": "finance"})
```

- A step that appends `announce.{type}` to the control log. The memo is the
  control-log sequence.
- Projections use these entries. The engine does not interpret them.

### 2.8 enqueue and withdraw

```python
task_id = await ctx.enqueue(queue, payload, task_key=..., kind=TaskKind.DELEGATE,
                            reason="delegate", name="enqueue", key=None)
removed = await ctx.withdraw(queue, task_id, name="withdraw", key=None)
```

- `enqueue` is a step that puts a task on a queue and announces
  `task.enqueued`. The memo is the derived `task_id`. A replay does not
  enqueue again. `task_key` is the task's dedup key (`01-domain-model.md`,
  section 6), so the enqueue is idempotent. `key` is this frame's key.
- `withdraw` is a step that takes the task off the queue when it is still
  there. The memo is `True` when a task was removed.
- These are the queue effects. `send` is the channel effect and `announce`
  is the control-log effect. Patterns build delegate and review on them.

### 2.9 Read-only helpers

```python
ctx.eid: str
ctx.fid: str                       # current frame id
ctx.attempt: int                   # attempt number of the running step, else 1
ctx.now() -> Timestamp             # a step named "now", memoized
ctx.random() -> float              # a step named "random", memoized
ctx.uuid() -> str                  # a step named "uuid", memoized
ctx.cancel_requested() -> bool     # reads the cooperative lease state
```

## 3. WorkflowEngine API for operators

```python
eid = await engine.start(workflow_fn, *args, key=None, queue="default", by=Actor, **kwargs)
```

- Creates the `Execution`. When `key` is given, claims `wf/dispatch/{key}`.
  A loser returns the winner's `eid`.
- `engine.start_result(...)` takes the same arguments and returns
  `StartResult(eid, deduplicated)`. `deduplicated` is true when the dispatch
  key had a winner already. A caller that answers a person needs that fact;
  `start` alone cannot tell a fresh execution from a converged one.
- Writes `wf/exec/{eid}/meta`. Announces `execution.created`.
- Enqueues a `START` task. Returns the `eid`.

```python
seq = await engine.signal(eid, channel, payload, by=Actor, correlation=None)
```

- Sends one message on channel `{eid}.{channel}`.
- Enqueues a `RESUME` task with `task_id = resume:{eid}:{channel}:{seq}`.
- Returns the message `seq`.

```python
seq = await engine.broadcast(channel, payload, by=Actor)
```

- Sends one message on a global channel.
- Enqueues one `RESUME` task per waiter of that channel.

```python
await engine.cancel(eid, by=Actor)
```

- Requests cancel through the lease state. Enqueues a `RESUME` task with
  reason `cancel`.
- A running worker checks `cancel_requested` at each frame boundary. It
  appends `execution.cancelled` and releases the lease.
- A suspended execution is cancelled by the resuming worker before it
  replays.

```python
seq = await engine.deliver(eid, full_channel, payload, by=Actor)
```

- Same as `signal`, for a channel given by its full name. Consumers of a
  delegate task use it to answer on the reply channel.

```python
decision = await engine.reviews.decide(rid, eid=..., queue="finance", verdict="approve",
                                       by=Actor, data=None)
```

- Operator side of the review pattern. See `06-patterns.md`, section 2.

```python
execution = await engine.migrate(eid, version, by=Actor)
```

- Replaces the `version` of a live execution record with CAS. Appends
  `execution.migrated` to the journal and announces it. Enqueues a `RESUME`
  task with reason `migrate:{version}`.
- Raises `ValueError` for a terminal execution. A call with the current
  version is a no-op.
- The next replay runs the new code. Old memos stay valid when their frame
  ids and digests match. See `05-protocols.md`, section 10.

```python
status = await engine.status(eid) -> ExecutionStatus
entries = await engine.journal(eid)          # the live journal, or the archived one
data = await engine.archive(eid)             # the archive object, or None
```

- `status` raises `UnknownExecution` after retention deleted the record.

```python
worker = engine.worker(queues=["default"], ttl=120)
await worker.run_once()          # dequeue one task, run it, return
await worker.run_forever()       # loop until stop
sweeper = engine.sweeper()
await sweeper.run_once()
```

## 4. Errors

| Error | Raised when |
|---|---|
| `NondeterminismError` | replay finds a frame id with a different `args_digest` |
| `StepFailed` | a step has no attempt left. Raised on every replay. |
| `NonRetryableError` | raised by a step to stop retries. Not raised by the engine. |
| `DuplicateFrameError` | a key repeats under one parent |
| `ChildFailed` | a child execution fails or is cancelled |
| `LeaseLost` | the execution lease was stolen, from CairnDB |
| `Cancelled` | raised inside the workflow function at a frame boundary after a cancel request |
| `WorkflowNotRegistered` | a task names a workflow and version that this worker does not know |
