# 10 — Agent runner

## 1. Purpose

A `DELEGATE` task leaves the engine. Someone outside must take it, do the
work, and answer on the reply channel (`06-patterns.md`, section 1). This
document specifies that someone.

Sections 2 to 5 apply to **any** consumer of a `DELEGATE` task: the loop,
the ownership of a long attempt, recovery, and cancel. Sections 6 to 9 apply
to the consumer that runs a **coding agent**, which is the demanding case
and the one that drives the design. A different consumer, such as a group of
humans behind a web form, takes sections 2 to 5 only.

The engine knows nothing of this document. It has no agent concept, no
worktree concept, and no adapter. The runner is a separate process and a
separate package.

## 2. What a consumer is

One consumer holds one task at a time and answers it once.

Two ways to reach the queue:

| Way | When |
|---|---|
| the `Queue` and `Channel` ports, in Python, against the bucket | the runner is yours and can hold bucket credentials |
| the HTTP worker plane (`09-http-api.md`, section 9) | the runner is a third party, or cannot reach the bucket |

The two are the same protocol. The relay adds one hop. Prefer the ports.

Rules:

- A consumer never appends to a journal. It never writes an execution
  record. Its only writes are the reply message, the evidence, and the state
  of its own task lease.
- A consumer answers exactly once for one task. The receive frame consumes
  the first message on the reply channel.
- A consumer is not a worker. It never runs workflow code. Do not put a
  runner and a worker in one process.

## 3. The runner loop

1. **Dequeue.** `dequeue(queue, worker=holder, ttl)` gives a `ClaimedTask`
   and a lease at some epoch. `None` means the queue is empty. Wait, then
   try again.
2. **Refuse what is not yours.** The task kind must be `DELEGATE`. Return
   any other kind with `nack` and a delay.
3. **Acquire the substrate.** Section 7. On a conflict, `nack` with a delay
   and take no further action. The task stays for another runner or another
   pass.
4. **Start the work.** Section 6. Write the handle into the lease state
   **before** the work can produce anything (section 4.2).
5. **Heartbeat.** Every `ttl / 3` seconds: `renew`, then read
   `lease.state`. `renew` raises `LeaseLost` when the lease was stolen. See
   section 4.4. `cancel_requested` in the state starts the ladder of
   section 5.
6. **Finish.** Collect the outcome. Write the evidence (section 9).
7. **Answer, then ack.** `deliver(eid, reply_channel, report)` first, `ack`
   second.
8. **Release the substrate.** Section 7.

Rules:

- Step 7 is ordered. A runner that acks first and stops before it delivers
  loses the answer: the task is gone and the frame waits until its timeout.
  A runner that delivers first and stops before the ack answers twice at
  worst. The second message is ignored, because the receive frame consumed
  the first.
- A runner that gets `LeaseLost` at any point stops at once. It delivers
  nothing and acks nothing. The thief owns the task. See section 4.4.
- A crash between step 1 and step 7 leaves the task on the queue. The lease
  expires and the task becomes visible again. This is at-least-once
  delivery, so the work must tolerate a second run, or the runner must
  recover its own attempt (section 4.3).

## 4. Ownership of a long attempt

An agent session runs for minutes or hours. A `renew` every few seconds for
six hours must survive a restart of the runner, and must not let two agents
work on one task.

### 4.1 The task lease is the ownership

The queue gives the fence already (`03-ports.md`, section 7). The runner
needs no lease of its own, and no record in the account. The lease document
holds `{epoch, holder, deadline_at, state}` and the runner owns `state`.

### 4.2 The lease state is where the handle lives

```json
{"handle": {"kind": "service", "session_id": "…", "url": "…"},
 "started_at": "…", "attempt": 1, "worktree": "…", "scopes": ["src/billing/**"]}
```

Rules:

- The runner writes the handle with `update_state` **before** the agent can
  do anything that outlives the runner. For a hosted session, that is before
  the call that starts it returns work. For a subprocess, that is
  immediately after the fork.
- A start that cannot be made write-ahead needs a two-phase handle: write an
  intent with a deterministic external id, start the session with that id,
  then write the confirmed handle. A restart then finds the session by the
  id, whether or not the start finished.
- `update_state` preserves a cooperative write made from outside, so a
  cancel written between two heartbeats is not lost.

### 4.3 Recovery after a restart of the runner

The holder is a **stable configured id** of the runner, not a value that a
restart changes. On startup:

1. `pending(queue)` lists the tasks on the queue.
2. For each, `attach(queue, task_id, holder=my_id, ttl)`
   (`03-ports.md`, section 7). A success means this task is still mine, at
   the same epoch, with my state intact.
3. Read the handle from the state and `reattach` (section 6). Continue the
   heartbeat.
4. `terminate` the session when the adapter cannot reattach, then let the
   ordinary loop run the attempt again.

`attach` writes nothing and does not bump the epoch, so recovery is not a
new period of ownership. It is the same period, held by the same runner,
after an interruption of the process that held it.

### 4.4 A steal, and the orphan it leaves

A runner that dies and does not come back inside the TTL loses the lease.
Another runner dequeues the task, and the epoch increases. The first
runner's `renew` then raises `LeaseLost`.

The agent session of the first runner can still be alive. The thief inherits
it, because an acquisition preserves the state of the document:

1. The thief reads `handle` from the state it acquired.
2. It calls `reattach`. On success it adopts the session and continues.
3. On failure it calls `terminate` with that handle, discards the substrate
   of the dead attempt (section 7), and starts a new attempt.

Rule: **a thief must always try to terminate an inherited handle it does not
adopt.** Without that step, a hosted session runs and bills forever, with
nobody to read its answer.

### 4.5 The TTL

| Value | Effect |
|---|---|
| too short | a slow restart loses the task, and the agent is duplicated or killed |
| too long | a dead runner holds the task for that long |

Take a TTL of some minutes, not of the length of the session, and renew.
The TTL is the time a restart has to come back. Make it longer than the
start time of the runner, and shorter than the patience of the workflow's
`timeout`.

## 5. Cancel and the ladder

`engine.cancel` reaches an execution through the execution lease
(`05-protocols.md`, section 8). A `DELEGATE` task is not the execution, and
its consumer does not watch the execution. So the cancel must reach the task
as well.

`Queue.request_cancel(queue, task_id, by)` writes `cancel_requested` into
the **task lease** state with a cooperative write. It fences nobody. The
runner reads it on its next heartbeat. This is section 8 of
`05-protocols.md`, one level down.

The ladder, once the runner sees it:

1. Tell the agent to stop and to write its resume artifact. Give it the
   grace budget (default 120 seconds).
2. On expiry, signal the process group, then kill it after 10 seconds.
3. Deliver a report with `outcome: "interrupted"` and the resume artifact,
   or a degraded one that the runner builds from the state of the worktree.
4. Ack, and release the substrate. A killed attempt's worktree is
   quarantined, not deleted (section 7).

Rules:

- The runner answers even when it is cancelled. A cancelled execution
  ignores the answer, and a suspended one gets a real outcome instead of a
  timeout.
- A **resume artifact** is the agent's compaction of its own progress: the
  plan that remains, the working hypothesis, the dead ends, the next
  actions. It is for a *fresh* agent that continues the work later.
- A **handle** (section 4.2) is the address of a *live* session. It is for
  the runner that reattaches to it.
- These two are not the same thing and never substitute for each other. A
  handle without a resume artifact loses the work when the session dies. A
  resume artifact without a handle restarts an agent that is still running.

## 6. The adapter

An adapter is the whole of what differs between agent runtimes.

```python
class AgentAdapter(Protocol):
    def start(self, envelope: Envelope) -> Handle: ...
    def reattach(self, handle: Handle) -> Handle | None: ...
    def poll(self, handle: Handle) -> Outcome | None: ...
    def interrupt(self, handle: Handle, *, grace: float) -> None: ...
    def terminate(self, handle: Handle) -> None: ...
```

`Handle` is JSON-compatible. It goes into the lease state, so it must hold
an address, never an object: a process id and a host, a session id, a URL.

| Family | Example | `start` | `reattach` | Survives a runner restart |
|---|---|---|---|---|
| subprocess | Claude Code, Codex, a shell command | fork a process group | check the pid on this host | yes, on the same host only |
| session | omnigent | ask the pool for a session | ask the pool for the session id | yes |
| service | OpenHands | `POST` a conversation | `GET` the conversation | yes |

Rules:

- `reattach` returns `None` when the session is gone. It never starts a new
  one. A start is always an explicit decision of the runner.
- A subprocess handle holds the host. A runner on another host must not
  believe it can reattach to a pid. It terminates instead, which for a
  foreign host means it marks the attempt lost and quarantines the
  substrate.
- `poll` gives the outcome or `None`. The runner does not block on the
  agent, because it must keep its heartbeat.
- The agent is a black box. It does not talk to the engine, it holds no
  lease, and it writes no journal entry. Flowlet v1 gave the agent's harness
  seven HTTP endpoints, because the executor co-authored the account of its
  attempt. Here the queue lease gives the fence and the reply channel gives
  the result.

## 7. The substrate

One attempt writes in one isolated place.

- **Worktree.** The runner creates `git worktree add wt/{eid}/{key} -b
  agent/{eid}/{key}` from the base ref in the envelope. `{key}` is the task's
  dedup key, which the workflow chose (section 10): a rework is a new key and
  therefore a new branch, and a second dequeue of one task is the same key and
  therefore the same branch. The name is derived, never minted, so a repeat
  converges.
- **The runner creates it, not the workflow.** The runner is the process
  with the repository. A step of the workflow runs in a worker, which may be
  on another machine and usually has no checkout.
- **Write scopes.** The envelope names the path globs the attempt may
  change. The runner acquires one CairnDB lease for each glob, in sorted
  order, before it starts the agent. A conflict releases what it took and
  nacks the task with a delay. Sorted order gives no deadlock. The runner
  holds the scopes for the length of the attempt and renews them with the
  task lease.
- **Effects.** The commits on the branch are the result of the attempt. The
  report carries the SHAs. Nothing merges here: a merge is a later frame of
  the workflow, with one writer.
- **Quarantine.** A killed attempt keeps its worktree as a bundle under
  `quarantine/{eid}/{key}`, then removes the worktree. A person can salvage
  it.

## 8. The two contracts

The delegate payload is the **envelope**. The reply message is the
**report**. Both are data of the workflow, not of the engine.

Envelope (task → agent): the intent, the reference into the specification,
the acceptance criteria, the write scopes, the verification commands, the
scoped knowledge, the base ref, and the agent configuration.

Report (agent → task): the outcome (`completed`, `partial`, `blocked`,
`interrupted`, `failed`), the claims with their evidence references, what
remains, the failures, the summary of the diff, the results of the
verification commands, the proposals, and the cost.

The shapes of Flowlet v1 hold and are reused verbatim
(`CodeFlow/codeflow/docs/specs/functional.md`, sections 9.1 to 9.4).

Rules:

- The runner does not judge the report. It delivers it.
- The **workflow** applies the deterministic gates in a step after the
  reply: every claim has a reference that resolves, the verification covers
  the commands of the envelope, no path outside the write scope changed, and
  a `completed` outcome names every acceptance criterion. A failure of a
  gate is a rework, and no reviewer can overrule it.
- A large report body goes to evidence, and the report carries the
  reference. A message is not a file store.

## 9. Evidence

The transcript of the agent, the diff, and the output of each verification
command are evidence (`03-ports.md`, section 12). The runner writes them
under the frame and the attempt of the delegate, so the interface shows them
under the frame that waited.

Rules:

- The frame is `reply_fid` from the task payload (`06-patterns.md`, section
  1): the frame that waits, not the frame that delegated. The attempt is the
  **epoch of the task lease**, which counts one real run of this task by one
  consumer, so two runs of one task never overwrite each other.
- Evidence is written before the answer. The report references it.
- A write of evidence never fails the attempt.
- Retention deletes evidence with the execution
  (`05-protocols.md`, section 11).

## 10. The workflow side

The runner is one half. This is the other half, which an author writes.

```python
@engine.workflow("codeflow.task", version="1")
async def codeflow_task(ctx: Context, envelope: dict) -> dict:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        key = str(attempt)
        reply = await delegate(ctx, "agents", envelope,
                               timeout=timedelta(hours=6), name="agent", key=key)
        if reply is None:
            return await ctx.step(escalate, envelope, "no answer", name="escalate")

        report = reply.payload
        gates = await ctx.step(run_gates, report, name="gates", key=key)
        if not gates["passed"]:
            envelope = {**envelope, "prior": summarize(report, gates)}
            continue

        decision = await review(ctx, "code-review",
                                {"report": report, "gates": gates},
                                timeout=timedelta(days=2), key=key)
        if decision is not None and decision.verdict == "approve":
            return await ctx.step(merge, report, name="merge")
        envelope = {**envelope, "prior": summarize(report, gates, decision)}

    return await ctx.step(escalate, envelope, "no attempt left", name="escalate")
```

Rules:

- The retry of an attempt is a loop in the workflow, with the attempt number
  as the frame `key`. It is not the retry policy of a step. A retry of a
  step repeats one function. A new attempt of a task is a new agent, a new
  branch, and a new envelope.
- The workflow holds the policy: how many attempts, what the gates are, what
  the routing is, when to escalate. The runner holds none of it.
- The default is a fresh context for each attempt, seeded with the prior
  report, the findings and the diff. A context that already failed tends to
  fail the same way.

## 11. Configuration

| Setting | Content |
|---|---|
| `runner_id` | the stable holder. A restart must reuse it (section 4.3) |
| `queues` | the queues this runner consumes |
| `ttl` | the task lease TTL in seconds. Default 300 |
| `max_parallel` | the tasks this runner holds at one time |
| `repo_path`, `base_ref` | the repository and the integration branch |
| `adapter` | the adapter and its configuration |
| `grace` | the grace budget of an interrupt. Default 120 seconds |
| `evidence_limit` | the size limit of one attempt's evidence |

## 12. Not in this document

- The **planning** of tasks: what a task is, where the intent comes from,
  how a plan becomes tasks. That belongs to a controller above the
  workflow.
- The **routing policy** and the **merge queue** of CodeFlow. Section 10
  shows where they attach. The tables of Flowlet v1 hold
  (`functional.md`, sections 6.3 and 6.4).
- The **board projection** to an issue tracker.
- The reviewer as an agent. A reviewer is a consumer of a review queue and
  takes sections 2 to 5 with a different adapter.
