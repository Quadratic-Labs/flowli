# flowli-runner

Runs coding agents as the consumer of flowli delegate tasks. See
`flowli/specs/10-agent-runner.md`.

The loop is not here. `flowli.runtime.Consumer` owns the dequeue, the
heartbeat, the recovery and the cancel — sections 2 to 5 of the spec, which
hold for any consumer of a delegate task. This package is the work that
consumer drives: adapters, worktrees, write scopes, and the two contracts an
agent task carries.

## Running it

```bash
pip install -e ./runner
flowli-runner \
    --app myapp.flows:registry \
    --queue agents \
    --repo /srv/repo \
    --runner-id runner-a \
    --command 'claude -p {intent}'
```

`run` is not a subcommand: this CLI has one command, so its options sit
directly on `flowli-runner`.

`--once` takes a single task and reports the pass, which suits a cron job:

```
recovered=1 processed=1
  attached delegate:01a093cd-30c1-71d2-9c38-79a845bc2a58:delegate
```

`recovered` counts the tasks this runner reattached to on startup — work it
still held from a previous life — and each is listed beneath. Counters are
coloured on a terminal and plain text when piped; see the root README.

`--runner-id` is the holder of every task this process takes, and **a restart
must reuse it**: a runner that forgets its id cannot attach to what it still
holds, and its work is stolen instead of resumed (section 4.3).

## What one task does

1. **prepare** — take the write-scope leases in sorted order, then carve
   `wt/{eid}/{key}` on the branch `agent/{eid}/{key}`. A scope conflict is a
   refusal, and the task goes back on its queue for another pass.
2. **start** — the adapter starts the agent; the consumer writes the handle it
   returns into the task lease, before anything can outlive this process.
3. **watch** — the consumer renews the lease, which is also how it sees a
   cancel. On one, the runner asks the agent to stop within the grace, then
   stops it.
4. **report** — the commits on the branch, the results of the verification
   commands, and references to the evidence.
5. **release** — give the scopes back, and remove the worktree. An interrupted
   attempt's tree is quarantined as a bundle instead, so a person can salvage
   it.

## The adapter is the only thing that varies

```python
class AgentAdapter(Protocol):
    async def start(self, envelope) -> Handle
    async def reattach(self, handle) -> Handle | None    # never starts one
    async def poll(self, handle) -> dict | None
    async def interrupt(self, handle, *, grace) -> dict
    async def terminate(self, handle) -> None
```

`SubprocessAgent` runs a command in the worktree, in its own process group:
this is Claude Code, Codex, or a session runtime that spawns a harness.
`ServiceAgent` drives a hosted runtime over HTTP, which is the OpenHands
shape — a conversation that outlives this process, which is why a handle is
JSON and `reattach` is a plain read.

A subprocess handle carries its host. A runner on another machine gets `None`
from `reattach` and must not pretend it can stop a pid it cannot reach.
