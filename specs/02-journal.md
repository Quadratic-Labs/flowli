# 02 — Journal

## 1. Definition

The journal of execution `eid` is the CairnDB named log `wf.exec.{eid}`. It
holds one entry for each state change of each frame. Entries are dense and
totally ordered. The journal is append-only.

Each entry is a CairnDB `Event`:

| `Event` field | Content |
|---|---|
| `event_type` | one of the entry types below |
| `timestamp` | `provenance.at` |
| `payload` | the fields listed for the type |
| `schema_version` | `"1.0.0"` |
| `metadata` | `{"provenance": Provenance, "fid": str}` |

## 2. Entry types

| Type | Payload | Meaning |
|---|---|---|
| `execution.started` | `args` | A worker ran the root frame for the first time. |
| `frame.started` | `kind, name, args_digest, attempt` | An attempt began. |
| `frame.completed` | `attempt, value` | An attempt completed. See section 3. |
| `frame.failed` | `attempt, error_type, message, retryable, retry_at?` | An attempt failed. |
| `frame.suspended` | `attempt, on, deadline?` | The frame waits. `on` names the condition. `deadline` is the ISO instant of a receive timeout or a sleep. |
| `frame.fulfilled` | `attempt, on, message_seq?` | The condition of a suspended frame is met. |
| `execution.suspended` | `on: list[str]` | The worker released the lease. Every runnable frame waits. |
| `execution.resumed` | `epoch, reason` | A worker acquired the lease. |
| `execution.completed` | `value` | The root frame completed. Terminal. |
| `execution.failed` | `error_type, message` | The root frame failed. Terminal. |
| `execution.cancelled` | `by` | An operator cancelled. Terminal. |
| `execution.migrated` | `from_version, version` | An operator pointed the execution at another workflow version. Status unchanged. |

The `on` condition is a string:

| Condition | Format |
|---|---|
| a channel | `channel:{name}` |
| a timer | `timer:{timer_id}` |
| a child | `child:{eid}` |

Rules:

- `fid` is in `metadata` for every entry. For `execution.*` entries,
  `fid = "root"`.
- A `frame.started` entry precedes every `frame.completed`, `frame.failed`
  and `frame.suspended` entry with the same attempt number. A reader does
  not depend on this order. See section 3.

## 3. Memo rule

The memo of frame `fid` is the `value` of the first `frame.completed` entry
in journal order whose `metadata.fid` equals `fid`.

Rules:

- A reader ignores every later `frame.completed` entry for the same `fid`.
- The attempt number of the winning entry is not relevant to the memo.
- The epoch of the winning entry is not relevant to the memo. A fenced
  worker can win. Its effect happened. The engine accepts the value and
  keeps the epoch for audit.
- Because of this rule, two workers can run the same frame at the same time
  without corruption of the execution. Steps must be idempotent for their
  external effects. The engine does not check this.

## 4. Memo table

A memo table is the derived state of one journal. A worker builds it once
when it resumes an execution.

```python
@dataclass
class MemoTable:
    memos: dict[str, Completed]              # fid -> first completed outcome
    failures: dict[str, list[Failed]]        # fid -> failed attempts in order
    suspended: dict[str, str]                # fid -> condition, when not fulfilled
    fulfilled: dict[str, int | None]         # fid -> message_seq of the fulfilling message
    deadlines: dict[str, str]                # fid -> deadline of the wait, when not fulfilled
    retry_at: dict[str, str]                 # fid -> instant of the next attempt, from the last frame.failed
    digests: dict[str, str]                  # fid -> args_digest of the first frame.started
    consumed: dict[str, int]                 # channel -> highest message_seq consumed
    terminal: Completed | Failed | None      # execution outcome
```

Build procedure:

1. Read the journal from sequence 1 to the tail.
2. For each entry, apply one rule:
   - `frame.started`: if `fid` is not in `digests`, store `args_digest`.
   - `frame.completed`: if `fid` is not in `memos`, store the value.
   - `frame.failed`: append to `failures[fid]`. Store `retry_at` in
     `retry_at[fid]`, or remove `fid` from `retry_at` when absent.
   - `frame.suspended`: store `on` in `suspended[fid]`. Store `deadline` in
     `deadlines[fid]` when present.
   - `frame.fulfilled`: remove `fid` from `suspended` and from `deadlines`.
     Store `message_seq` in `fulfilled[fid]`. When `on` is a channel and
     `message_seq` is set, raise `consumed[channel]` to `message_seq`.
   - `execution.completed` or `execution.failed`: set `terminal`.
3. Return the table.

## 5. Replay algorithm

Replay runs the workflow coroutine from the start with a `Context` in replay
mode. The `Context` consults the memo table at each frame.

For a frame with id `fid`, kind `K`, and arguments with digest `D`
(`01-domain-model.md`, section 3.2):

1. If `fid` is in `digests` and `digests[fid] != D`, raise
   `NondeterminismError`. The workflow code changed under a live execution.
2. If `fid` is in `memos`, return `memos[fid].value`. Do not run the frame.
3. If `K` is `RECEIVE`, `SLEEP` or `CHILD` and `fid` is in `fulfilled`,
   read the fulfilling message or child result and return it.
4. Otherwise the frame is live. Leave replay mode for this frame:
   1. Compute `attempt = 1 + len(failures.get(fid, []))`.
   2. If `attempt > retry.max_attempts`, raise `StepFailed` with the last
      error.
   3. If `retry_at[fid]` is set and is in the future, suspend on that timer.
      If it is set and is past, append `frame.fulfilled` for the timer.
   4. If a cancel is requested, raise `Cancelled`.
   5. Append `frame.started`.
   6. Run the frame. See `04-api.md` for each kind.
   7. On success, append `frame.completed`. Store the value in `memos`.
      Return the value.
   8. On error, append `frame.failed`. Apply the retry policy. See
      `05-protocols.md`, section 6.

The engine appends `frame.suspended` at the moment a frame starts to wait,
when the memo table has no `suspended[fid]` equal to the same condition. The
worker appends `execution.suspended` after the run reports suspension.

Rules:

- The engine runs a memoized frame zero times.
- The engine runs the workflow function itself many times. The function
  must be deterministic. It must not read the clock, random numbers, or
  external state outside a step.
- `NondeterminismError` fails the attempt but not the execution. An
  operator must migrate or cancel the execution. See `05-protocols.md`,
  section 10.

## 6. Control log

The control log is the CairnDB named log `wf`. It holds lifecycle entries
for all executions. Its projection is the source for dashboards, queues
views and inboxes.

| Type | Payload |
|---|---|
| `execution.created` | `eid, workflow, version, dispatch_key, parent` |
| `execution.started` | `eid, epoch` |
| `execution.suspended` | `eid, on` |
| `execution.resumed` | `eid, epoch, reason` |
| `execution.completed` | `eid` |
| `execution.failed` | `eid, error_type, message` |
| `execution.cancelled` | `eid` |
| `execution.migrated` | `eid, from_version, version` |
| `execution.archived` | `eid` |
| `task.enqueued` | `task_id, queue, kind, target, reason` |
| `announce.*` | author-defined, see `04-api.md` section 2.7 |

Rules:

- Every `execution.*` entry in the control log has a twin in the journal.
  The journal entry is appended first. If the worker crashes between the two
  appends, the sweeper repairs the control log. See `05-protocols.md`,
  section 9.
- `execution.migrated` and `execution.archived` do not change the status.
  A status reader skips them.
- `execution.archived` is control-log only. The journal no longer exists
  when it is announced.
- Frame entries never go to the control log.
- `announce.*` entries are the only author-defined entries. The prefix is
  fixed.

## 7. Size and retention

- One journal holds the entries of one execution only. Its size is bounded
  by the number of frames.
- After a terminal entry, the retention job folds the journal, the
  execution record and the execution-scoped channels into one archive
  object at `wf/archive/{eid}.msgpack` and deletes the log. See
  `05-protocols.md`, section 11.
