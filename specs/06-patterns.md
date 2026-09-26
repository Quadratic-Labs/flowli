# 06 — Patterns

A pattern is a function written against the `Context` API only. The engine
does not know the pattern. Patterns live in `flowli/patterns/`.

## 1. Delegate

Delegate puts a task on a queue and waits for the answer on a channel. The
consumer of the queue can be a worker pool, a group of humans, or an
external system.

```python
reply = await delegate(ctx, queue, payload, timeout=None, name="delegate", key=None, task_key=None)
```

Procedure:

1. `frame = name`, or `name:key` when `key` is given.
2. `ref = FrameRef(eid, fid)` with `fid` the caller's frame.
   `reply_channel = ref.reply_channel(frame)`.
3. `task_key = frame` unless the caller gives one. The task id is then
   `delegate:{eid}:{task_key}` (`Task.id_for`).
4. `ctx.enqueue(queue, {"target": {eid, fid}, "reply_channel": ...,
   "reply_fid": ..., "payload": payload}, task_key=task_key,
   name="{name}-enqueue", key=key)`.
5. `ctx.receive(reply_channel, scope="global", timeout=timeout,
   name="{name}-receive", key=key)`.
6. On timeout, `ctx.withdraw(queue, task_id, name="{name}-withdraw",
   key=key)`. Return `None`.

Rules:

- Frame names use `-`, not `.`, because a frame name must match
  `^[A-Za-z0-9_-]+$`.
- The consumer reads the task payload as `DelegateTask` and answers with
  `engine.deliver(task.eid, task.reply_channel, result, by=...)`. The `by`
  actor is the who of the answer.
- `reply_fid` is the id of the frame that will wait:
  `{fid}/{name}-receive` plus `:{key}`, or `#0` when there is no key. A
  consumer files its evidence under that frame, so an interface shows it
  where a person looks for it (`10-agent-runner.md`, section 9). It rests on
  the same assumption as the reply channel: one delegate per `(name, key)`
  under one parent frame.
- `DelegateTask` is a domain object, not a pattern object: it is the contract
  between the frame that enqueues and the consumer that answers, so it
  belongs to neither of them.
- A detached step (`05-protocols.md`, section 7) is a delegate whose
  consumer is a worker with `RUN_STEP` support.
- `10-agent-runner.md` specifies the consumer side: the loop, the ownership
  of a long attempt, recovery, and cancel.

## 2. Review

A review is a delegate to a human queue plus announcements for the inbox
projection (`08-projection.md`).

```python
@dataclass(frozen=True)
class Decision:
    verdict: str
    data: Any
    by: Actor
    at: Timestamp

decision = await review(ctx, queue, payload, timeout=None, name="review", key=None)
```

Procedure:

1. `rid = await ctx.uuid()`.
2. `ctx.announce("review.requested", {rid, eid, queue, payload, deadline})`.
   `deadline` is `now + timeout`, or `None`.
3. `delegate(ctx, queue, {rid, payload, deadline}, timeout=timeout,
   name=name, key=key or rid, task_key="review:{rid}")`. Two reviews under
   one frame with one key need two names, as two delegates do.
4. On `None`: `ctx.announce("review.expired", {rid})`. Return `None`.
5. Otherwise the reply payload is a `Decision` dict.
   `ctx.announce("review.decided", {rid, verdict, by})`. Return the
   `Decision`.

The operator side:

```python
inbox = projection.pending_reviews(queue="finance")
decision = await engine.reviews.decide(rid, eid=row.eid, queue=row.queue, verdict="approve",
                                       by=Actor.human("thomas@..."), data=None)
```

`eid` and `queue` come from the inbox row: `review.requested` carries both.

`decide` follows this procedure:

1. Call `Dispatch.claim("reviews/{rid}/decision", decision)`. When the
   claim is lost, the winner's decision replaces the caller's.
2. Call `Queue.peek(queue, Task.id_for(DELEGATE, eid, "review:{rid}"))`.
   Return the decision when the task is gone: it was delivered and acked
   already, or the frame withdrew it on a timeout.
3. Call `engine.deliver(task.eid, task.reply_channel, decision,
   by=decision.by)`.
4. Call `Queue.take` for the same task id, and ack what it gives.
5. Return the decision.

Rules:

- Step 1 gives exactly one decision per review. The workflow does not
  depend on it, because `receive` consumes one message only. Step 1 exists
  so that the second reviewer learns that the first one decided.
- Steps 2 to 4 run even when the claim was lost. A caller that crashed
  between the claim and the delivery is completed by the next caller.
- The delivery reads the task, and does not hold it. A task that another
  consumer holds, or that a worker pushed into the future with a nack,
  would otherwise swallow a decision that is recorded already. A repeat
  sends the same payload again, which the receive frame ignores: it
  consumes the first message only.
- Removing the task is the last step and is best effort. A task that stays
  on the queue shows a decided review to a second human, who then gets the
  first decision back.
- The human is an `Actor` with `kind = "human"`. Provenance of the answer
  message holds the email.
- The review inbox is the `reviews` table of the projection.

## 3. Scheduled start

A schedule starts one execution per tick. The dispatch key gives
exactly-once per tick.

```python
eid = await on_tick(engine, workflow_fn, schedule_name, tick, *args, queue=None, **kwargs)
```

The dispatch key is `{schedule_name}:{to_iso(tick)}`. The actor is
`Actor.schedule(schedule_name)`.

Two schedulers that fire the same tick get the same `eid`.

## 4. Saga with compensation

A saga runs steps in order. On failure it runs compensations in reverse.
Each compensation is a step. A replay does not run a compensation twice.

```python
results = await saga(ctx, [(act, undo), ...], name="saga", undo_retry=RetryPolicy(max_attempts=5))
```

Each `act` runs as step `{name}-act:{i}`. On failure each `undo` of a
completed action runs as step `{name}-undo:{i}` in reverse order, with the
value that `act` returned. Then the error is raised again.

## 5. Fan-out with children

```python
values = await fan_out(ctx, child_fn, items, name=None, queue="default")
```

One child per item, keyed by position. The values come back in item order.

Each child has its own journal and lease. The parent suspends on all child
channels at once. The parent resumes once per child completion, replays, and
suspends again until the last child completes.
