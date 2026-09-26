# flowli-codeflow

The CodeFlow controller. See `flowli/specs/11-codeflow.md`.

**The controller is workflows.** A milestone is an execution, a feature is a
child of it, a task is a child of a feature, routing is a pure function, and a
plan delta is a message. There is no reconciler holding a board, because a
workflow already is the dependency graph.

Three things stay processes, because they never end:

| Process | Where |
|---|---|
| the agent runner | `flowli-runner` |
| the merge queue | `flowli-codeflow merge` |
| the board reconciler | `flowli-codeflow board` |

## Using it

```python
from flowli.runtime import Registry
from flowli_codeflow import Policy, register
from flowli_codeflow.gotchas import Gotcha, Registry as Gotchas

registry, gotchas = Registry(), Gotchas()
gotchas.add(Gotcha(scope=["src/billing/**"], text="the webhook fixture resets per test"))
workflows = register(registry, policy=Policy(max_attempts=3), gotchas=gotchas)
```

Then start a milestone like any execution, and run the processes:

```bash
flowli worker            --app myapp:registry --queue default
flowli-runner            --app myapp:registry --queue agents --repo /srv/repo --runner-id r1
flowli-codeflow merge    --app myapp:registry --repo /srv/repo --gate 'just test'
flowli-codeflow board    --app myapp:registry --projection ./wf_view.sqlite
```

Each takes `--log-level` and `--log-format console|json`, and logs to stderr.
With `--once` they run a single pass and report it on stdout — `merged=1` for
the queue, `reconciled=3` for the board — coloured on a terminal and plain text
when piped, so a cron job's output stays greppable. The root README has the
detail.

## What is where

| File | Content |
|---|---|
| `workflows.py` | the three workflows of spec 11 section 3 |
| `routing.py` | the table of section 4: gates first, and no reviewer overrules one |
| `gates.py` | the deterministic gates on a report |
| `model.py` | the plan: stages from dependencies, scope overlap, deltas |
| `gotchas.py` | the scoped registry injected into an envelope |
| `merge.py` | the merge queue: rebase, gate, merge, gate again |
| `board.py` | the reconciler, and a board in memory to test it against |

## Two rules that bite

**Two delegates under one frame need two names.** A delegate's frames are
`{name}-enqueue:{key}` and `{name}-receive:{key}`, so the agent, the merge and
the escalation would collide on one key. The engine answers with
`DuplicateFrameError`, which fails the execution.

**A step returns JSON.** The plan goes into a step as data and comes back as
data; it is structured again on the other side. A dataclass that survives the
live run is a dict on the replay.
