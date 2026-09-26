# flowli

A workflow engine that runs a workflow as a coroutine execution stack and
journals every frame to CairnDB.

**Documentation: <https://quadratic-labs.github.io/flowli/>** — the quickstart,
the concepts, the architecture, every setting, the hosting guide, and the API
reference from the docstrings. The specifications stay in `specs/`.

```bash
uv sync --extra docs
uv run sphinx-build -b html -W docs docs/_build/html    # or: make -C docs livehtml
```

Layout:

- `src/flowli/domain/` — pure domain objects and ports. No I/O, no CairnDB import.
- `src/flowli/adapters/` — port implementations (CairnDB, in-memory).
- `src/flowli/api/` — the HTTP service (the `api` extra).
- `web/` — the operator interface over that service.
- `runner/` — `flowli-runner`: coding agents as delegate consumers.
- `codeflow/` — `flowli-codeflow`: the controller over the runner.

## Running the jobs

Install with the CLI extra, then point the CLI at your workflow registry:

```bash
pip install "flowli[cli]"
export CAIRNDB_STORAGE_TYPE=s3 CAIRNDB_S3_BUCKET=my-bucket      # or --storage-path ./bucket

flowli worker    --app myapp.flows:registry --queue default --queue finance
flowli sweeper   --app myapp.flows:registry --interval 60
flowli retention --app myapp.flows:registry --delay-days 30 --once
```

`--app module:attr` names a `Registry`, an `Engine`, or a zero-argument callable
returning one. With a `Registry` the CLI builds the CairnDB backend from the
`CAIRNDB_*` variables. `--once` runs one pass and exits, which suits a cron job.
`--projection PATH` makes the sweeper and retention read statuses from the SQLite
projection instead of folding the control log. Every job stops cleanly on
SIGINT or SIGTERM.

Operator commands share the same `--app` and storage options:

```bash
flowli status  EID --journal
flowli signal  EID payments '{"amount": 100}' --by bank@example.com
flowli cancel  EID --by ops@example.com
flowli migrate EID 2 --by ops@example.com
```

`--by` names the actor recorded in provenance: an email, or `kind:id` with kind
`human`, `system`, `schedule` or `worker`. It defaults to the local user, or to
`FLOWLI_BY`. Commands exit with code 1 on an unknown execution or a refused
action, such as cancelling a finished execution.

## What the commands print

`status` answers the two questions an operator arrives with — where is it, and
what happened last:

```
eid        01a093cc-265e-730a-b05c-e7e36d377452
status     completed
workflow   greet v1
queue      default
created    2026-09-12T04:07:05.054046Z by human:thomas@example.com
last       execution.completed at 2026-09-12T04:07:05.128988Z by worker:w-1 on host-3 epoch 1
payload
{
  "value": "hi zoe"
}
```

A suspended execution names what it waits for instead of a payload:

```
status     suspended
last       execution.suspended at 2026-09-12T04:07:05.193632Z by worker:w-1 on host-3 epoch 1
waiting    channel:01a093cc-26cd-7373-8944-38c18b531442.go
```

`--journal` appends the whole log, one event per line:

```
           seq  type                   fid                              at                       actor
       1000000  execution.started      root                             2026-09-12T04:07:05.128988Z worker:w-1
       2000000  frame.started          root/greet#0                     2026-09-12T04:07:05.139587Z worker:w-1
       3000000  frame.completed        root/greet#0                     2026-09-12T04:07:05.139587Z worker:w-1
       4000000  execution.completed    root                             2026-09-12T04:07:05.128988Z worker:w-1
```

Every `--once` job answers with one line of counters, which suits a cron job
that mails its output:

```bash
flowli sweeper   --once     # timers_fired=1 recovered=0 restarted=0 repaired=0 waits_cleared=0
flowli retention --once     # archived=1 cleaned=0, then one line per archived execution
flowli-runner    --once     # recovered=0 processed=1, then one line per reattached task
flowli-codeflow merge --once   # merged=1
flowli-codeflow board --once   # reconciled=3
```

### Colour

On a terminal the output is coloured: the status by what it means (green
completed, red failed, yellow suspended), workflow names in cyan, timestamps
and actor kinds dimmed so the identity stands out, and JSON payloads syntax
highlighted. The theme paints with the terminal's own sixteen colours, so it
suits a light background as well as a dark one.

**Piped output is plain text, byte for byte.** Redirect the output, capture it
in a test, or run it under `cron`, and you get exactly the columns above with
no escape sequences — so `grep`, `awk` and `cut` keep working. Set `NO_COLOR`
to drop the colour on a terminal too.

## The HTTP service and the interface

```bash
pip install "flowli[api]"
uv run python web/dev_server.py        # an engine, a worker and the service
cd web && npm install && npm run dev   # the operator interface
```

`flowli.api.create_app(engine, projection=..., authenticator=...)` is the
service: the catalog and the control plane (`specs/09-http-api.md`,
sections 7 and 8), the worker plane for consumers that cannot reach the bucket
(section 9), and evidence (section 10). It holds no state of its own: reads
come from the projection, writes go through the engine, and the actor of every
write comes from the access token.

`web/` is the interface over it. `web/dev_server.py` runs a demo backend for
it. See `web/README.md`.

## Logging

Every component emits structured events through structlog, the library CairnDB
uses, so one configuration produces one stream:

```python
from flowli.log import configure_logging
configure_logging("INFO", "json")      # or "console"
```

All three CLIs — `flowli`, `flowli-runner` and `flowli-codeflow` — take
`--log-level` and `--log-format console|json`; `flowli` also reads
`FLOWLI_LOG_LEVEL` and `FLOWLI_LOG_FORMAT`. Logs go to stderr, so a job's
report on stdout stays pipeable on its own. Events are named with snake_case
nouns such as `execution_started`, `execution_suspended`, `frame_failed`,
`execution_recovered` and `execution_archived`. While a worker holds a task,
`worker_id`, `task_id`, `eid` and `epoch` are bound to every event it emits, and
live frames add `fid` and `attempt`. Frame events are at DEBUG level.

## License

MIT. See [LICENSE](https://github.com/Quadratic-Labs/flowli/blob/main/LICENSE).
