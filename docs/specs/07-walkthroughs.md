# 07 — Walkthroughs

Each walkthrough lists the journal of one execution. Provenance is shown as
`actor@site/epoch` for brevity. Sequence numbers are the CairnDB sequence
numbers of the journal.

## 1. Invoice approval, no crash

Workflow:

```python
@engine.workflow("invoice_approval", version="3")
async def invoice_approval(ctx, invoice_id):
    invoice = await ctx.step(fetch_invoice, invoice_id)
    score = await ctx.step(score_risk, invoice)
    if score > 0.8:
        decision = await review(ctx, "finance", invoice, timeout=timedelta(days=3))
        if decision is None or decision.verdict != "approve":
            return "rejected"
    await ctx.send(f"invoice:{invoice_id}", {"status": "approved"})
    return "approved"
```

Operator: `eid = await engine.start(invoice_approval, "inv-42", key="invoice:42", by=Actor.human("thomas@..."))`.

Control log receives `execution.created`. Queue `default` receives task
`start:{eid}`. Worker `w-1` dequeues it and acquires the lease with epoch 1.

Journal after the first run, up to suspension:

| seq | type | fid | payload | provenance |
|---|---|---|---|---|
| 1 | `execution.started` | `root` | `args=["inv-42"]` | `w-1@hostA/1` |
| 2 | `frame.started` | `root/fetch_invoice#0` | `attempt=1, digest=d1` | `w-1@hostA/1` |
| 3 | `frame.completed` | `root/fetch_invoice#0` | `value={...}` | `w-1@hostA/1` |
| 4 | `frame.started` | `root/score_risk#0` | `attempt=1, digest=d2` | `w-1@hostA/1` |
| 5 | `frame.completed` | `root/score_risk#0` | `value=0.93` | `w-1@hostA/1` |
| 6 | `frame.started` | `root/uuid#0` | | `w-1@hostA/1` |
| 7 | `frame.completed` | `root/uuid#0` | `value="r7f3"` | `w-1@hostA/1` |
| 8 | `frame.started` | `root/announce#0` | | `w-1@hostA/1` |
| 9 | `frame.completed` | `root/announce#0` | `value=<control seq>` | `w-1@hostA/1` |
| 10 | `frame.started` | `root/review:r7f3.enqueue#0` | | `w-1@hostA/1` |
| 11 | `frame.completed` | `root/review:r7f3.enqueue#0` | `value="task-..."` | `w-1@hostA/1` |
| 12 | `frame.started` | `root/review:r7f3.receive#0` | | `w-1@hostA/1` |
| 13 | `frame.suspended` | `root/review:r7f3.receive#0` | `on="channel:{eid}.reply.x"` | `w-1@hostA/1` |
| 14 | `execution.suspended` | `root` | `on=["channel:{eid}.reply.x"]` | `w-1@hostA/1` |

The worker releases the lease. Queue `finance` holds one task. The inbox
projection shows review `r7f3`.

Two days later, a reviewer decides. `engine.reviews.decide("r7f3",
"approve", by=Actor.human("cfo@..."))` claims the decision, sends the message
on the reply channel with `seq = 1`, and enqueues `resume:{eid}:{channel}:1`.
Worker `w-3` dequeues it and acquires the lease with epoch 2.

The worker builds the memo table. It replays the function. Frames 1 to 11
return their memos without running. The receive frame finds message 1.

| seq | type | fid | payload | provenance |
|---|---|---|---|---|
| 15 | `execution.resumed` | `root` | `epoch=2, reason="message:..."` | `w-3@hostB/2` |
| 16 | `frame.fulfilled` | `root/review:r7f3.receive#0` | `message_seq=1` | `w-3@hostB/2` |
| 17 | `frame.started` | `root/announce#1` | | `w-3@hostB/2` |
| 18 | `frame.completed` | `root/announce#1` | | `w-3@hostB/2` |
| 19 | `frame.started` | `root/send#0` | | `w-3@hostB/2` |
| 20 | `frame.completed` | `root/send#0` | `value=1` | `w-3@hostB/2` |
| 21 | `execution.completed` | `root` | `value="approved"` | `w-3@hostB/2` |

The trace answers the three questions. Who decided: the provenance of
message 1 on the reply channel, `cfo@...`. What ran: `invoice_approval`
version 3, frame by frame. Where: `hostA` under epoch 1, then `hostB` under
epoch 2.

## 2. Crash during a step

Worker `w-1` holds epoch 1. It appends seq 4 (`frame.started` for
`score_risk`). The process dies before seq 5.

1. The lease expires after 120 seconds.
2. The task lease expires after 60 seconds. Worker `w-2` dequeues the same
   `START` task.
3. `w-2` acquires the execution lease with epoch 2. It appends
   `execution.resumed` with `reason = "start"`.
4. The memo table has a memo for `fetch_invoice#0` and none for
   `score_risk#0`. Replay returns the invoice without a fetch. It runs
   `score_risk` as attempt 1 again, because no `frame.failed` entry exists.
5. The journal now has two `frame.started` entries for `score_risk#0`, both
   with `attempt = 1`, and one `frame.completed`. The memo rule takes the
   first `frame.completed`.

If the sweeper ran before the task lease expired, it also enqueued
`resume:{eid}:recovery:1`. Whichever task a worker dequeues first does the
work. The second task finds a terminal or running execution and is acked
without effect.

## 3. Paused worker, then fenced

Worker `w-1` holds epoch 1 and runs `score_risk`. The process pauses for 3
minutes. The lease expires. Worker `w-2` steals it with epoch 2 and runs
`score_risk` too.

Case A: `w-1` resumes first and appends `frame.completed` with
`value = 0.93` under epoch 1. Then `w-2` appends `frame.completed` with
`value = 0.93` under epoch 2. The memo is the epoch 1 entry. On its next
renew, `w-1` gets `LeaseLost` and stops. `w-2` continues with the memo.

Case B: `w-2` appends first. `w-1` appends second, then gets `LeaseLost`.
The memo is the epoch 2 entry. Both entries stay in the journal. Both
epochs are visible in the trace.

In both cases `score_risk` ran twice. Steps must be idempotent for their
external effects.

## 4. Two reviewers decide at once

Two people click approve and reject within the same second.

1. Both UI calls run `decide`. Both call `db.claim("wf/reviews/r7f3/decision", ...)`.
2. One claim wins. The other gets `won = False` and the winner's decision.
   The UI shows the loser that a decision already exists.
3. Only the winner sends the message on the reply channel.

Without the claim, both would send a message. The receive frame would take
the first by channel order. The second message would stay unread. The
outcome is still one decision, but the loser would not know.

## 5. Message arrives during suspension

Worker `w-1` runs `receive("payments")`. The channel is empty.

1. `w-1` appends `frame.suspended` and `execution.suspended`.
2. A bank webhook calls `engine.signal(eid, "payments", ...)`. The message
   gets `seq = 1`. The webhook enqueues `resume:{eid}:payments:1`.
3. Worker `w-2` dequeues that task. It tries to acquire the lease. `w-1`
   still holds it. `w-2` gets `None` and acks the task.
4. `w-1` releases the lease. It reads the channel again. It finds message 1.
   It enqueues `resume:{eid}:payments:1`. The enqueue is a no-op if the key
   still exists, or creates a new task if `w-2` already acked it.
5. A worker dequeues the task, acquires the lease, replays, and fulfils the
   receive frame with message 1.

The message is never lost. The check after the release covers the window
between step 3 and step 4.

## 6. Fan-out with three children

The parent calls `ctx.gather(ctx.child(f, a, key="0"), ctx.child(f, b,
key="1"), ctx.child(f, c, key="2"))`.

1. The parent creates three child executions with derived `eid` values.
   Each child start is idempotent.
2. The parent suspends on three child channels.
3. Child `1` completes first. It sends on `{parent}.child.{child1}` and
   enqueues a resume for the parent.
4. The parent resumes, replays, fulfils child `1`, and suspends on two
   channels.
5. Steps 3 and 4 repeat for the other two children.
6. The gather returns the three values in argument order, not in completion
   order.
