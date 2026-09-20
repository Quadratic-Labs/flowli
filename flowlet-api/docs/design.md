Design
======

This document gives the design of Flowlet at a high level.
Refer to [architecture.md](architecture.md) for component details.

Flowlet is a small orchestration kernel.
The kernel keeps an account of the work: what was promised, what ran, and what was decided.
Execution is not the record.
Execution proposes entries into the record.


The account model
-----------------

The account model has five units:

1. An **obligation** is the unit of intent.
   It records the promise: a flow name, validated arguments, and an attempt budget.
   It also records provenance: what caused it, and against which flow version it was minted.
2. An **attempt** is the unit of execution and cost.
   Each attempt ends with one explicit outcome: `returned`, `raised`, `crashed`, or `interrupted`.
3. A **review** is the unit of judgment.
   A review accepts or rejects the outcome of one attempt.
   A review can carry a reference to its evidence.
4. An **effect** is the unit of value.
   The identity of an effect comes from the obligation, the effect name, and an occurrence key.
   The identity does not come from the attempt.
   A retried attempt therefore converges on the recorded effect and does not fire it again.
5. A **consumption** is the receive checkpoint of the message channel.
   A retried attempt replays the recorded consumptions in order before it consumes new messages.

The **account** of one obligation contains the obligation, its attempts, its effects, and its consumptions.
The account is the payload of the obligation's lease document, at `state/<flow_name>/<obligation_id>.json`.
Sub-obligations get their own account and their own failure domain.
They link to their parent through `parent_id` and `root_id`.

### Who owes what

An obligation names four roles:

- The **content** is what is owed: the flow name and the validated arguments.
  The flow name says which performance discharges the obligation.
- The **obligee** is who is owed.
  A sub-obligation owes its parent, recorded in `parent_id`.
  A root obligation has no recorded obligee.
  `caused_by` records what spawned it, which is provenance and not a claim.
- The **performer** is who owes the doing.
  The obligation does not name one.
  Any worker may claim it, and the account records who ran in `Attempt.executor`.
  Workers are interchangeable, so naming one in advance adds no information
  and blocks a free claim.
- The **reviewer** is who owes the judgment.
  The obligation does name one, in `reviewer_id`, because review carries authority.
  Not every worker may review.

The performer is therefore discovered, and the reviewer is designated.
This is a decision, not a gap.

### Budget

The budget counts consuming attempts only:

- A `raised` outcome consumes the budget.
- A `rejected` review on a `returned` attempt consumes the budget.
- A `crashed` or `interrupted` attempt is free.

Backoff paces every attempt, free or not.
A crash loop thus backs off, but it does not spend the budget.

### Gates

An obligation has two gates.
They are symmetric in structure.
Each is a policy field with the values `auto` and `gated`.
Each holds the obligation in a state that no worker can claim.
Each opens only through a fenced, authorized write.

- The **admission** gate controls entry.
  A gated obligation is born `held`: durable on the books, but not claimable.
  A fenced admission releases it and records who released it, in `admitted_by`.
  This makes a static plan pure kernel data.
- The **review** gate controls exit.
  A gated obligation parks as `awaiting_review` when an attempt ends.
  An authorized review then discharges it, reopens it, or abandons it.
  A rejecting review can extend the budget.
  This is the resume path after exhaustion.

The gates are not symmetric in who they name.
Admission records who released the hold, after the event.
Review names the reviewer in advance, in `reviewer_id`.

Review is itself work.
When a reviewer obligation is minted, the parked account records it in `reviewer_id`.
The account thus answers: "who owes me the review?".


How the user starts work
------------------------

1. The user builds the kernel with `Flowlet.configure(config)`.
2. The user mounts the kernel router in a FastAPI application.
3. A layer-2 surface registers the callables and supplies an executor.
   Taskflow supplies authoring decorators and an in-process executor.
   CodeFlow drives the same role through the HTTP claim surface.
4. The user submits arguments under a flow name, through
   `POST /submit/{flow_name}` or through Python.
   The kernel mints the obligation.


The execution sequence
----------------------

1. The API validates the arguments and records the obligation.
2. A wake-up signal reaches a worker.
   The queue is only a wake-up channel; it holds no retry state.
   A deployment can remove the queue, and the workers then poll the account store.
3. The worker claims the obligation.
   The claim acquires a lease on the account document.
   The claim transition is atomic: it records `crashed` for a dead predecessor, then appends a new attempt.
4. The executor runs the attempt.
   The flow code calls `flowlet.heartbeat()` to renew the lease and to observe the cancel signal.
   The flow code calls `flowlet.effect()` for exactly-once side effects, and `flowlet.recv()` for ordered messages.
   The tracer writes OpenTelemetry spans to the obligation folder `obligations/<flow_name>/<date>/<obligation_id>/`.
5. The worker records the outcome.
   Every write is epoch-fenced.
   A worker that lost its lease gets `LeaseLost` and discards its outcome.
6. A review routes the obligation:
   - `approved` discharges the obligation.
   - `rejected` with budget left reopens it, with backoff and a new wake-up.
   - `rejected` with the budget spent abandons it, or parks it when the exit gate applies.


Failure recovery
----------------

The sweeper runs on a schedule and needs no long-lived process.
Each sweep does three things:

1. It steals expired leases and records the dead attempt as `crashed`.
2. It re-enqueues stuck open obligations whose wake-up message was lost.
3. It archives closed obligations out of the state directory, so the active set stays small.

Ownership moves only through the fenced lease acquisition.
Concurrent sweeps and worker races are therefore safe.


The read surface
----------------

The account is the only authority.
All read paths are projections of it:

- `ObligationSummary` is the flat projection of one account, derived and never stored as authority.
- The API refreshes a local SQLite cache from storage on demand.
  The cache is disposable and has no correctness role.
- The **transitions feed** is an ordered log with a commit-number cursor, served at `GET /transitions`.
  It is a wake-up channel for reconcilers, never authority.
  Consumers confirm each event against the account and keep a fallback poll.
- The optional **history log** records archived obligations for long-horizon queries.
  Its SQLite projection is a disposable cache, fully rebuildable from the log.
