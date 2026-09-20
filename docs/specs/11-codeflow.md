# 11 — CodeFlow

*Status: draft, 2026-09-11. Writing style: ASD-STE100 Strict.*

## 1. Purpose

CodeFlow is durable orchestration of coding agents. `10-agent-runner.md`
specifies the muscle: how one task becomes an agent session, commits and a
report. This document specifies the decisions around it — where a task comes
from, what happens to an attempt, how work reaches the integration branch, and
where a person decides.

It specifies one application of the engine. The engine has no CodeFlow
concept, and nothing here is a primitive.

## 2. The controller is a workflow

Flowlet v1 built CodeFlow as a reconciler with direct access to the store. It
had to: dependency edges between units of work were deliberately not in the
kernel, so "B runs when A completes" was scheduling semantics that some
outside process had to own.

That reason is gone. **A workflow is a dependency graph.** `ctx.child` says
"this work belongs to that work", `ctx.gather` says "these run together",
`await` says "this one first", and the journal makes all of it durable and
replayable. A reconciler that rebuilds a plan from a board snapshot on every
turn is not needed to express what the plan already is.

| Flowlet v1 | Here |
|---|---|
| board of tasks with states | children of an execution, with their own journals |
| `depends_on` edges, validated acyclic | the shape of the workflow function |
| manager turn on a debounce | a message to the feature workflow |
| `based_on_board_version` optimistic concurrency | not needed: one execution has one writer |
| dispatcher loop | `delegate` to an agent queue |
| gate policy, review eligibility | `review`, plus the capability check of the service |
| control mode at a scope | the state of the parent workflow |
| crash accounting | the engine's recovery (`05-protocols.md`, section 9) |

### 2.1 What stays a process

A workflow must be finite. This engine has no way to continue an execution as
a fresh one, so a journal grows for as long as the execution lives, and an
endless loop is an endless journal.

So: **anything endless is a process, not a workflow.** Three of them, and no
more:

| Process | What it is |
|---|---|
| the agent runner | a `Consumer` of the agent queue (`10-agent-runner.md`) |
| the merge queue | a `Consumer` of the merge queue, one at a time (section 7) |
| the board reconciler | a follower of the control log (section 11) |

Each holds no plan and decides nothing. The decisions are in the workflows.

## 3. The three workflows

### 3.1 Task

One unit of work: one intent, one write scope, one branch. Its shape is
`10-agent-runner.md`, section 10, with the routing of section 4 here:

```python
@engine.workflow("codeflow.task", version="1")
async def task(ctx: Context, spec: dict) -> dict:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        key = str(attempt)
        envelope = await ctx.step(build_envelope, spec, attempt, name="envelope", key=key)
        reply = await delegate(ctx, "agents", envelope, timeout=AGENT_TIMEOUT, key=key)
        if reply is None:
            return await escalate(ctx, spec, "no agent answered")

        report = reply.payload
        gates = await ctx.step(run_gates, report, name="gates", key=key)
        review_result = None
        if gates["passed"]:
            review_result = await review(ctx, "code-review", {"report": report, "gates": gates},
                                         timeout=REVIEW_TIMEOUT, key=key)
        route = decide(gates, review_result, attempt, MAX_ATTEMPTS)

        match route:
            case "approved":
                await delegate(ctx, "merge", merge_request(report),
                               name="merge", key=key)
                return {"outcome": "completed", "attempts": attempt}
            case "escalated":
                return await escalate(ctx, spec, route_reason(gates, review_result))
            case "rework":
                spec = {**spec, "prior": summarize(report, gates, review_result)}
    return await escalate(ctx, spec, "no attempt left")
```

Rules:

- One attempt is one `key`, so its frames, its branch and its evidence are
  separate from the attempt before it.
- **Two delegates under one frame need two names.** A delegate's frames are
  `{name}-enqueue:{key}` and `{name}-receive:{key}`, so the agent and the
  merge would collide on one key. The engine catches it with
  `DuplicateFrameError`, which is a failed execution, not a warning.
- A rework gets a fresh agent, seeded with the prior report, the findings and
  the prior diff. A context that already failed tends to fail the same way.
- The task workflow holds the policy. The runner holds none of it.

### 3.2 Feature

A set of tasks that belong together, with the order between them:

```python
@engine.workflow("codeflow.feature", version="1")
async def feature(ctx: Context, spec: dict) -> dict:
    plan = await ctx.step(validate_plan, spec["plan"], name="plan")
    done: dict[str, Any] = {}
    for stage in plan["stages"]:                 # a stage is what may run together
        results = await ctx.gather(*[
            ctx.child(task, task_spec(plan, tid, done), key=tid) for tid in stage
        ])
        done.update(dict(zip(stage, results, strict=True)))
        if any(r["outcome"] == "escalated" for r in results):
            await review(ctx, "escalation", {"feature": spec["id"], "done": done})
            return {"outcome": "escalated", "tasks": done}
    return {"outcome": "completed", "tasks": done}
```

Rules:

- A stage is the dependency graph, flattened by the planner into what may run
  at the same time. The engine reads no `depends_on` field, because the code
  is the field.
- The `key` of a child is the task id, so the child execution id is derived
  from it (`04-api.md`, section 2.2). A repeat of the plan converges on the
  same children instead of starting a second set.
- A feature is finite: it has the tasks its plan holds, and it ends.

### 3.3 Milestone

The plan, the budget and the control mode. It is the one workflow a person
talks to:

```python
@engine.workflow("codeflow.milestone", version="1")
async def milestone(ctx: Context, spec: dict) -> dict:
    state = await ctx.step(seed, spec, name="seed")
    while not state["complete"]:
        if state["paused"]:
            message = await ctx.receive("control")          # resume, or cancel
            state = await ctx.step(apply_control, state, message.payload, name="control",
                                   key=str(message.seq))
            continue
        ready = next_features(state)
        if not ready:
            message = await ctx.receive("plan", timeout=IDLE)  # a plan delta, or nothing
            if message is None:
                return {"outcome": "stalled", "state": state}
            state = await ctx.step(apply_delta, state, message.payload, name="delta",
                                   key=str(message.seq))
            continue
        results = await ctx.gather(*[
            ctx.child(feature, f, key=f["id"]) for f in ready
        ])
        state = await ctx.step(record, state, results, name="record", key=stage_key(ready))
    return {"outcome": "completed", "state": state}
```

Rules:

- The loop is finite in practice, and bounded by `max_turns` in the
  configuration. A milestone that would run forever is a milestone that is
  not one.
- A plan delta is a **message**, not a turn of a reconciler. Whoever produces
  it — a manager agent, a person, a script — sends it with
  `engine.signal(eid, "plan", delta)`.
- **The plan is local state, never a mutation of the arguments.** A replay
  runs the function again with the arguments of the record, so a workflow
  that changes them changes what its own replay sees. See `04-api.md`,
  section 1.
- The state is the plan, the budget and the mode. It has one writer, so it
  needs no version field and no optimistic concurrency. v1 carried
  `based_on_board_version` because two writers could race; here one execution
  is one writer, always.

## 4. Routing

After an attempt: the deterministic gates first, then the judgment, then a
fixed function. Not a model's output.

| Deterministic gates | Judgment | Route |
|---|---|---|
| fail | any | `rework` |
| pass | `accept` | `approved` |
| pass | `rework`, severity at most major | `rework` |
| pass | `rework`, and no attempt is left | `escalated` |
| pass | `reject` (the spec is wrong, or the task is ill-posed) | `escalated`, with an amendment proposal |
| pass | `accept`, with a blocking finding | `escalated` |

The judgment has three words and no others: `accept`, `rework`, `reject`.
They are the `verdict` of the decision the review pattern delivers
(`06-patterns.md`, section 2), so a reviewer and this table use one
vocabulary. A verdict outside the three is a bug of the reviewer, and it
routes to `escalated`.

Rules:

- **A reviewer cannot overrule a failed gate.** The first row has no
  exception.
- The last row is a contradiction, so it is not resolved: it is escalated,
  and it is a signal about the reviewer.
- `decide` is a pure function of its arguments. It is not a step: it reads
  nothing and writes nothing, so a replay recomputes it.
- The deterministic gates are the ones of `10-agent-runner.md`, section 8:
  every claim has a reference that resolves, the verification covers the
  commands of the envelope, no path outside the write scope changed, and a
  `completed` outcome names every acceptance criterion.

## 5. Planning

A plan delta is an ordered list of operations, exactly as in Flowlet v1:

| Operation | Effect |
|---|---|
| `create_feature`, `create_task` | a new entry in the plan of the milestone |
| `split_task` | one entry becomes several |
| `cancel_task` | `engine.cancel` on that child |
| `reprioritize` | the order of the stages |
| `request_spec_amendment` | section 9 |
| `escalate` | a review on the `escalation` queue |
| `declare_milestone_complete` | `complete` in the state, with its evidence |

Validation, in the step that applies the delta:

- the ids are unique, and the dependencies are acyclic,
- the write scopes of one stage do not overlap,
- the count of open tasks is within `max_open_tasks`,
- an operation that a gate covers goes to a review instead of applying
  (section 8).

Rules:

- A rejected operation comes back with a machine-readable reason, and the next
  delta sees it. Three rejected deltas in a row escalate.
- The delta is applied in one step, so it is memoized: a replay does not apply
  it twice, and the plan cannot drift from the journal.

## 6. Dependencies and scopes

Two different things, and they are often confused.

**Order** is the shape of the feature workflow (section 3.2). The engine never
reads a dependency field.

**Exclusion** is the write scope, and the runner enforces it with leases in
sorted order (`10-agent-runner.md`, section 7). Two tasks of one stage must
not overlap, and the planner checks it; the leases are what makes a mistake
safe rather than corrupting.

A task that changes something shared — a lock file, a migration, generated
code — declares the scope that says so, and serializes against everything
else that touches it.

## 7. The merge queue

One writer to the integration branch, always.

The writer is a `Consumer` of the `merge` queue, with a handler that runs no
agent. One consumer holds one task at a time, so the queue lease is the single
writer: no resident process, and no lock of its own.

Per merge:

1. rebase the attempt's branch on the integration branch,
2. run the full gate suite,
3. merge,
4. run the post-merge gates.

Rules:

- A conflict, or a failed gate, answers the waiting frame with the reason.
  The task goes back to `rework` with that context, and **the queue is not
  blocked by one bad merge**.
- The merge is not a workflow, because it never ends (section 2.1).
- The order is the order of the queue, which is the order of the enqueues.
- A protected branch adds a human gate before step 3 (section 8).

## 8. Human gates and escalation

A gate is `review(ctx, queue, payload, timeout=...)`. That is all it is: a
task on a human queue, and a frame that waits.

| Gate | Queue | On timeout |
|---|---|---|
| spec approval | `spec` | wait, with no timeout |
| plan approval (a gated operation) | `plan` | wait |
| escalation from routing | `escalation` | wait |
| merge to a protected branch | `merge-approval` | wait |
| budget extension | `budget` | deny, and stay paused |

Rules:

- A gate blocks its own frame, and nothing else. Other frames of other
  executions continue because they are other frames: no scope machinery is
  needed to say so.
- `gated_ops` is the main dial between autonomy and oversight. The default:
  `create_feature`, `request_spec_amendment`, `declare_milestone_complete` and
  `cancel_task` are gated; `create_task`, `split_task` and `reprioritize`
  apply within their limits.
- Who may decide is the capability `reviews:decide:{queue}` of the service
  (`09-http-api.md`, section 6). The workflow names the queue; the service
  checks the person.
- The engine may pause a milestone on its own: a budget breach, a rework
  thrash, or a repeated crash sends `pause` on its control channel. **An
  automatic pause is never resumed automatically.**

## 9. Spec amendments and gotchas

An agent that finds the specification wrong must have a way to say so, or it
reinterprets the requirement in silence.

1. The report carries a `spec_amendment` proposal.
2. The task workflow forwards it to its milestone with
   `engine.signal(milestone_eid, "plan", ...)`.
3. The milestone classifies it: editorial, additive, or breaking. The first
   two apply. A breaking one is a gate (section 8).
4. On approval, the milestone computes the impact set — the tasks bound to
   that section of the specification — and cancels the running ones. They
   start again against the new hash, as new children with new keys.
5. A proposal marked `blocking` suspends the task that raised it, rather than
   letting it guess.

**Gotchas** are an append-only, content-addressed registry, each with a scope.
The step that builds an envelope injects the ones whose scope meets the task's
write scope. This is what makes attempt 50 cheaper than attempt 5. Cap the
injection with `max_gotchas_per_envelope`, by specificity and by recency, or
it grows without bound and every envelope carries the whole history.

## 10. Guardrails

Two kinds, and they are not the same thing:

- a **rule** is a deterministic predicate that can refuse,
- a **skill** is knowledge put into an envelope, and it refuses nothing.

| Hook | Where it runs |
|---|---|
| before a plan applies | the step of section 5 |
| before a dispatch | the `envelope` step of section 3.1 |
| **in flight** | the tool proxy of the agent |
| before a review | the `gates` step |
| before and after a merge | the merge consumer of section 7 |

Rule: **the in-flight hook is not this engine's.** A rule that a tool call
must respect is enforced by whatever sits between the agent and its tools. A
prompt that asks an agent to stay in its write scope is a skill, not a
guardrail. What this side gives instead is containment: the worktree, the
scope leases, and a diff that shows every path an attempt touched.

## 11. The board

An issue tracker is a projection of the account and an ingress for people. It
is never the system of record: it has no compare-and-set, no fencing and no
atomic transition, so every invariant would dissolve on it.

- **Outbound.** A reconciler follows the control log (`08-projection.md`) and
  materializes executions as issues. The issue number and the `eid` are pinned
  by a claim, so a repeat creates nothing. Drift always resolves toward the
  account, and the projection is disposable.
- **Inbound.** A webhook becomes a call of the service: a comment that decides
  a review is `POST /reviews/{rid}/decide`, a closed issue is
  `POST /executions/{eid}/cancel`. The service maps the identity of the
  tracker to a principal and checks the capability.
- **Effects.** The commits and the merge are the effects. The account holds
  the SHAs; the pull request is evidence, never authority.

## 12. Failure modes

| Failure | What shows it | What answers it |
|---|---|---|
| rework thrash | attempts rise, diffs churn, gates do not move | `MAX_ATTEMPTS`, then escalation; compare diffs between attempts |
| reviewer capture | every review accepts, findings stay empty | attribute post-merge defects back to reviews; seed a defect now and then |
| scope creep | a diff outside the write scope | a deterministic rework; then narrow the scope |
| spec drift | criteria marked unverifiable | force the amendment path; refuse a `completed` outcome that leaves one |
| planner thrash | many operations, few completions | debounce the deltas; cap the operations per delta |
| zombie agent | the task lease expires | the engine steals the task; the thief adopts or terminates the session (`10-agent-runner.md`, section 4.4) |
| poison task | the same task crashes again and again | pause that task's milestone, and escalate |
| cost runaway | the slope of the budget | a per-scope budget, then an automatic pause |
| scope deadlock | tasks that never start | one sorted order for the leases, and an exclusive scope for shared files |
| interrupt refused | the grace expires often | measure it; give those agents smaller tasks and a shorter checkpoint interval |

## 13. Configuration

| Setting | Content |
|---|---|
| `max_attempts` | attempts per task. Default 3 |
| `max_open_tasks` | tasks a milestone may have open |
| `max_turns` | the bound on the milestone loop |
| `gated_ops` | the operations a person must approve |
| `queues` | the names of the agent, review, merge and escalation queues |
| `integration_branch` | what the merge queue writes to |
| `budget` | tokens, money and wall time, per scope |
| `max_gotchas_per_envelope` | the cap of section 9 |

## 14. Not in this document

- The **runner** and its adapters: `10-agent-runner.md`.
- The **prompts** of the agent and of the reviewer. They are the content of
  an envelope, and they belong to a deployment.
- The **tool proxy** that enforces the in-flight hook (section 10).
- The schema of an **issue tracker**'s API.
