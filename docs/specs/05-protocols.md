# 05 — Protocols

*Status: draft, 2026-09-07. Writing style: ASD-STE100 Strict.*

This document describes the runtime procedures. Each procedure is a numbered
list. A worker follows the steps in order.

## 1. Worker loop

A worker repeats this procedure.

1. Call `Queue.dequeue(queue, worker_id, task_ttl)` for each configured
   queue in order. Stop at the first `ClaimedTask`. When every call returns
   `None`, sleep for the poll interval. Go to step 1.
2. Start a background renew loop that renews the task lease every
   `task_ttl / 3` seconds.
3. Dispatch on `task.kind`:
   - `START` or `RESUME`: run procedure 2.
   - `RUN_STEP`: run procedure 7.
4. When the procedure returns `done`, call `Queue.ack`.
5. When the procedure returns `retry(delay)`, call `Queue.nack(delay)`.
6. When the procedure raises `LeaseLost`, stop the renew loop. Do not ack.
   The task lease expires and another worker takes the task.
7. Go to step 1.

## 2. Run an execution

Input: a `START` or `RESUME` task for execution `eid`.

1. Call `Ownership.acquire(eid, worker_id, exec_ttl)`.
2. If the result is `None`, another worker owns the execution. Return
   `done`. The owner sees new messages at its next receive. Section 3
   covers the boundary case.
3. Set `site = site.with_epoch(lease.epoch)`.
4. Read `wf/exec/{eid}/meta`. Find the workflow function by `(workflow,
   version)`. Raise `WorkflowNotRegistered` when absent.
5. Build the memo table from the journal. See `02-journal.md`, section 4.
6. If `memo.terminal` is set, release the lease. Return `done`.
7. If `lease.state.cancel_requested` is set, append `execution.cancelled`
   to the journal and announce it. Release the lease. Return `done`.
8. If the task kind is `START` and the journal is empty, append
   `execution.started` to the journal. Announce `execution.started`.
   Otherwise append `execution.resumed` with `reason = task.reason`.
   Announce it.
9. Start a background renew loop that renews the execution lease every
   `exec_ttl / 3` seconds. On `LeaseLost`, cancel the coroutine.
10. Run the workflow coroutine with a `Context` in replay mode. Run until
    it returns, raises, or reports that every live frame is suspended.
11. On return with `value`: append `execution.completed` to the journal.
    Announce it. If `meta.parent` is set, run procedure 5. Release the lease
    with `state = {"status": "completed"}`. Return `done`.
12. On an exception that is not `Cancelled`: append `execution.failed`.
    Announce it. If `meta.parent` is set, run procedure 5. Release the lease
    with `state = {"status": "failed"}`. Return `done`.
13. On `Cancelled`: same as step 7.
14. On suspension: run procedure 3. Return `done`.

## 3. Suspend an execution

Input: the set `W` of live frames that wait, each with a condition.

1. For each frame in `W` that has no `frame.suspended` entry yet, append
   `frame.suspended` with its condition.
2. For each channel condition, call `Channel.register_wait(channel, ref)`.
3. For each timer condition, call `Timers.schedule(timer)`.
4. Append `execution.suspended` to the journal with the list of conditions.
   Announce it.
5. Release the execution lease with `state = {"status": "suspended"}`.
6. Check each channel condition again: call `Channel.read(channel,
   after=last_consumed_seq)`. If any call returns a message, enqueue a
   `RESUME` task with `task_id = resume:{eid}:{channel}:{seq}`.
7. Check each child condition again: read the child's status. If it is
   terminal, enqueue a `RESUME` task with `reason = child:{child_eid}`.

Step 6 and step 7 close the race with senders. A sender either sent before
the check, and this worker enqueues the resume, or sent after the release,
and the sender's resume task acquires the lease.

## 4. Deliver a message

`engine.signal(eid, channel, payload, by)` follows this procedure.

1. Build the `Message` with `sent_by = Provenance(actor=by, ...)`.
2. Call `Channel.send(message)`. Get `seq`.
3. Enqueue a `RESUME` task with `task_id = resume:{eid}:{channel}:{seq}`
   and `reason = message:{channel}`.

`engine.broadcast(channel, payload, by)` follows this procedure.

1. Send the message on the global channel. Get `seq`.
2. Call `Channel.waiters(channel)`.
3. For each waiter, enqueue a `RESUME` task with `task_id =
   resume:{eid}:{channel}:{seq}`.

When a resuming worker fulfils a receive frame:

1. Read the channel after the last consumed `seq`. Take the first message.
2. Append `frame.fulfilled` with `message_seq`.
3. Call `Channel.clear_wait(channel, ref)`.
4. Return the message to the workflow function.

## 5. Complete a child

When a child execution reaches a terminal entry and `meta.parent` is set:

1. Send a message on channel `{parent_eid}.child.{child_eid}` with payload
   `{"status": ..., "value": ...}` or `{"status": ..., "error": ...}`.
2. Enqueue a `RESUME` task for the parent with `reason = child:{child_eid}`.

The parent's `child` frame is a receive on that channel. Procedure 4 applies.

## 6. Retry a frame

After `frame.failed` for attempt `n`:

1. If `retryable` is `False`, or `n >= retry.max_attempts`, raise the
   error in the workflow function. Stop.
2. Compute `delay = min(backoff * backoff_factor ** (n - 1), max_backoff)`.
3. If `delay` is zero, start attempt `n + 1` at once. Stop.
4. Otherwise, treat the frame as suspended on a timer with `due_at = now +
   delay`. Store `retry_at` in the `frame.failed` payload. The execution
   suspends when no other frame is runnable.

## 7. Run a detached step

Input: a `RUN_STEP` task with `target = FrameRef(eid, fid)` and `payload
= {fn, args, kwargs}`.

1. Do not acquire the execution lease. The step is independent of the
   owner.
2. Append `frame.started` to the journal of `eid`.
3. Run `fn`.
4. Append `frame.completed` or `frame.failed`.
5. Send a message on channel `{eid}.step.{fid}` with the outcome.
6. Enqueue a `RESUME` task for `eid` with `reason = step:{fid}`.
7. Return `done`.

The owner's frame for a detached step is a `send` of the `RUN_STEP` task
followed by a `receive` on `{eid}.step.{fid}`. See `06-patterns.md`,
section 1.

## 8. Fencing and cooperative cancel

- A worker that gets `LeaseLost` stops all work on the execution at once.
  It does not append any entry after that point. It does not ack the task.
- A journal entry from a fenced worker that landed before the loss is valid.
  Its epoch is in the provenance. See `02-journal.md`, section 3.
- Before it appends an `execution.*` entry, a worker makes one guarded write
  on its lease. A fenced worker gets `LeaseLost` there and appends nothing.
  Frame entries are not guarded: the memo rule makes a duplicate harmless,
  and one guarded write per frame would double the cost of a step.
- `engine.cancel` writes `cancel_requested` into the lease state with
  `cooperative_write`. The owner's next `renew` absorbs it. The `Context`
  checks `cancel_requested` before it starts a live frame. It raises
  `Cancelled` there.
- A `DELEGATE` task of that execution has a consumer outside the engine,
  and that consumer does not watch the execution lease. So a cancel must
  reach the task as well, with `Queue.request_cancel`. The mechanism is the
  same, on the task lease. See `10-agent-runner.md`, section 5.
- **The engine does not do that propagation.** It cannot: the journal holds
  the memo of an enqueue, which is the task id, and not the queue that the
  task went to. Only the control log records the pair, in `task.enqueued`.
  So the service propagates, from the `tasks` table of the projection
  (`08-projection.md`, section 3), which holds exactly that pair. A
  deployment with no service cancels the task with the port.

## 9. Sweeper

The sweeper is a scheduled job. It runs every minute. It has no lease of its
own. Each action is idempotent, so two sweepers can run at the same time.

The sweeper reads the control log to know the status of each execution. It
keeps a cursor while the process lives. A fresh process reads the log from
the start, or from a snapshot when a projection exists.

1. **Timers.** Call `Timers.due(now)`. For each timer, enqueue a `RESUME`
   task with `task_id = resume:{eid}:timer:{timer_id}` and `reason =
   timer`. Then call `Timers.remove(timer)`.
2. **Dead executions.** For each execution with control status `RUNNING`,
   call `Ownership.inspect(eid)`. If the lease is expired, enqueue a
   `RESUME` task with `task_id = resume:{eid}:recovery:{epoch}` and
   `reason = recovery`.
3. **Lost starts.** For each execution with control status `PENDING` whose
   `execution.created` entry is older than the repair window, enqueue its
   `START` task again. The enqueue is a no-op when the task still exists.
4. **Control-log repair.** For each execution with a control status that is
   not terminal, and whose last control entry is older than the repair
   window, read the journal. If the last `execution.*` entry of the journal
   is not the one the control log knows, announce it.
5. **Orphan waits.** For each `(channel, ref)` from `Channel.all_waits()`,
   read the memo table of `ref.eid`. Keep the marker only when the table is
   not terminal and `suspended[ref.fid]` is `channel:{channel}`. Clear the
   marker otherwise.

The repair window is 60 seconds by default. It keeps the sweeper from
racing a live worker between a journal append and its announcement.

## 10. Code change under a live execution

When replay raises `NondeterminismError`:

1. Append `execution.suspended` with `on = ["operator"]`. Announce it.
2. Release the lease with `state = {"status": "suspended", "blocked":
   "nondeterminism"}`.

An operator resolves the block with one of:

- `engine.cancel(eid, by=...)`.
- `engine.migrate(eid, version, by=...)`:
  1. Refuse a terminal execution. Return at once when `version` is the
     current one.
  2. Replace the record with CAS, with the new `version`.
  3. Append `execution.migrated` to the journal. Announce it. The
     provenance names the operator.
  4. Enqueue a `RESUME` task with `task_id = resume:{eid}:migrate:{version}`.

The next worker replays with the code of the new version. Old memos stay
valid when their frame ids and digests match. A frame whose digest changed
still raises `NondeterminismError`.

## 11. Retention

A scheduled job folds finished executions. It reads statuses from a
`ControlSource`, the same interface the sweeper uses, through
`terminal_before(now - delay)`. The default delay is 30 days.

For each execution `eid` in that set:

1. Read the record, the journal, and the names of the channels scoped to
   `eid` (`{eid}.*`). Check whether an archive exists.
2. If nothing is left and an archive exists, announce `execution.archived`
   and stop. If nothing is left and no archive exists, stop.
3. If the journal exists and no archive exists, write the archive with
   put-if-absent. It holds `eid`, the record, `archived_at`, the journal
   entries with their sequences, and the messages of each scoped channel.
4. Delete the journal, the scoped channel logs, the timers of `eid`, the
   wait markers of `eid`, the evidence of `eid`, the lease document, and
   the record.
5. Announce `execution.archived`.

Rules:

- Each step is idempotent. A rerun after a crash finds the archive, finishes
  the deletes, and announces.
- The announcement comes last. It can repeat after a crash between the last
  delete and the announcement. Readers treat a repeat as a no-op.
- A `RESUME` task that reaches a worker after the record is deleted is
  dropped: the worker releases the lease and acks the task.
- A timer that fires after the record is deleted is removed by the sweeper.
- `engine.journal(eid)` returns the archived entries when the live journal
  is gone.
- The archive does not hold evidence (`03-ports.md`, section 12). An
  attempt log or an agent transcript can be large, and the archive is one
  object. Retention deletes the evidence of `eid` with everything else. A
  deployment that must keep transcripts for longer copies them out of the
  bucket before the retention delay expires.
- The control log keeps the lifecycle history. The CairnDB snapshot and GC
  jobs manage the control log itself.
