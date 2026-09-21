# 08 — Projection

## 1. Purpose

The projection is a local, read-only SQLite view of the control log. It
answers operator questions without a read of any journal: which executions
run, which wait, which reviews are pending, what a worker did. The sweeper
can read execution statuses from it instead of a fold of the control log in
memory.

The projection uses CairnDB's replay machinery (`docs/ENGINE_API.md`, layer
3). It is derived state. Application code never writes to it.

## 2. Source

The projection reads the control log `wf` only. Frame entries never reach
it. Each control-log entry is applied once, inside one SQLite transaction
per commit. Entry types with no handler are skipped. The projection handles:

- every `execution.*` type, `execution.migrated` and `execution.archived`
  included,
- `task.enqueued`,
- every type with the prefix `announce.`.

## 3. Tables

### executions

One row per execution.

| Column | Source |
|---|---|
| `eid`, `workflow`, `version`, `queue`, `dispatch_key`, `parent_eid`, `parent_fid` | `execution.created` payload |
| `created_at`, `created_by_kind`, `created_by_id` | provenance of `execution.created` |
| `status` | the last `execution.*` entry, see `02-journal.md` section 6 |
| `last_type`, `last_seq`, `updated_at` | the last `execution.*` entry |
| `epoch`, `worker_id`, `host` | provenance site of the last `execution.started` or `execution.resumed` |
| `suspended_on` | `execution.suspended` payload, JSON list. NULL when not suspended |
| `result` | `execution.completed` payload, JSON |
| `error_type`, `error_message` | `execution.failed` payload |
| `version` | replaced by `execution.migrated` |
| `archived_at` | provenance time of `execution.archived` |

Rules:

- A row in a terminal status never changes again. A late lifecycle entry
  for a terminal execution is ignored.
- A lifecycle entry for an unknown `eid` creates the row. This happens when
  the control log was pruned before `execution.created`.

### tasks

One row per `task.enqueued` entry: `seq`, `task_id`, `queue`, `kind`,
`eid`, `fid`, `reason`, `enqueued_at`, `actor_kind`, `actor_id`. The table
is an audit of enqueues. Ack and nack leave no entry.

### announcements

One row per `announce.*` entry: `seq`, `kind` (the type without the
prefix), `eid`, `fid`, `at`, `actor_kind`, `actor_id`, `payload` (JSON).
`eid` comes from the payload when present.

### reviews

Folded from the review pattern (`06-patterns.md`, section 2):

| Entry | Effect |
|---|---|
| `announce.review.requested` | insert `rid, eid, queue, payload, deadline, requested_at`, status `pending` |
| `announce.review.decided` | status `decided`, `verdict`, `decided_by` (from payload `by`), `decided_at` |
| `announce.review.expired` | status `expired`, `decided_at` |

A decision or expiry for a review that is not `pending` is ignored.

## 4. API

```python
projection = backend.projection(db_path="./wf_view.v1.sqlite", poll_interval=5.0)

await projection.refresh()               # catch up now. Returns the last applied seq
await projection.start()                 # poll in the background
await projection.stop()
await projection.wait_for(seq)           # read-your-writes for a ControlLog.announce seq

projection.execution(eid) -> ExecutionRow | None
projection.executions(status=None, workflow=None, parent_eid=None, limit=100,
                      after=None) -> list[ExecutionRow]   # after = (updated_at, eid) keyset
projection.children(eid) -> list[ExecutionRow]
projection.counts_by_status() -> dict[ExecutionStatus, int]
projection.pending_reviews(queue=None) -> list[ReviewRow]
projection.review(rid) -> ReviewRow | None
projection.announcements(kind=None, eid=None, limit=100) -> list[AnnouncementRow]
projection.tasks(eid=None, queue=None, kind=None, limit=100) -> list[TaskRow]
projection.connect() -> sqlite3.Connection    # read-only, row_factory = sqlite3.Row
```

Query methods are synchronous. They open a read-only connection to the
local file, so they never block replay.

## 5. Sweeper source

`Sweeper(engine, source=projection)` makes the sweeper read statuses from
the projection. `projection.snapshot()` refreshes the projection and
returns the non-terminal executions as `KnownExecution` values.
`Retention(engine, source=projection)` uses
`projection.terminal_before(instant)`: the terminal executions with no
`archived_at` whose last entry is older than `instant`. The default source
for both jobs is the in-memory fold `ControlView`.

## 6. Limits

- One projection file per process. Two processes each build their own.
- Snapshots of the projection use CairnDB's snapshot job on the `wf` log.
  This document does not describe them.
- The projection is eventually consistent. `wait_for` gives
  read-your-writes for one sequence.
