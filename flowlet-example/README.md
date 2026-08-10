# Flowlet Example

A working demo app for the **Flowlet** v2 architecture: lease-based worker,
OTel-instrumented run traces, filesystem storage, pull-based query API — and
the `flowlet-web` React dashboard on top of it.

There's also a `Taskfile.yml` at the repo root wrapping the commands below
(`task install`, `task api`, `task web`, `task submit FLOW=...`, etc.) if you
have [go-task](https://taskfile.dev) installed (`mise use -g go-task`).

This app is a single Flowlet instance (`flowlet_example.flows:flowlet`)
configured with:

- **storage**: `filesystem`, rooted at `./storage` (relative to wherever the
  process is started — see "Running" below for why that matters)
- **queue**: `memory` (in-process `InMemoryQueue`, dev/demo only — see the
  caveat under "Multi-process topology")

## Project structure

```
flowlet-example/
├── src/flowlet_example/
│   ├── flows.py   # the Flowlet instance + example flows/tasks
│   └── main.py    # FastAPI app: mounts the Flowlet router + an embedded worker thread
├── scripts/worker.py  # standalone worker loop (alternative to the embedded thread)
└── pyproject.toml
```

## 1. Install

From the repo root, into the shared `.venv` (mise-managed Python 3.14):

```bash
cd flowlet
python3.14 -m venv .venv        # skip if .venv already exists
.venv/bin/pip install -e flowlet-api        # needs SSH access to GitHub (private jsonry/chroniql deps)
.venv/bin/pip install -e flowlet-example --no-deps
```

`--no-deps` on the second install avoids re-resolving `flowlet`'s own
dependencies; `flowlet-example` only adds `uvicorn`, already pulled in by
`flowlet-api`.

## 2. Run the quickstart (API + embedded worker, one process)

`main.py`'s lifespan starts a background thread that drains the in-memory
queue, so a single process gives you execution + querying together. Run it
**from inside `flowlet-example/`** so the `./storage` relative path lands at
`flowlet-example/storage/` rather than wherever your shell happens to be:

```bash
cd flowlet-example
../.venv/bin/uvicorn flowlet_example.main:app --reload --port 8001
```

Port **8001** matters if you also want the dashboard (below) — its dev-server
proxy is hardcoded to `http://localhost:8001`.

Open `http://localhost:8001/docs` for interactive Swagger UI, or drive it with curl:

```bash
# List registered flows
curl -s http://localhost:8001/flows | python3 -m json.tool

# Parameter schema for one flow
curl -s http://localhost:8001/flows/hello_world/schema | python3 -m json.tool

# Execute synchronously (blocks until the flow returns).
# Response body is empty (run_flow -> None by design) -- check /runs/query
# or /runs/{run_id} below to see the recorded state/result.
curl -s -X POST http://localhost:8001/execute/hello_world \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"name": "Flowlet"}}'

# Submit asynchronously (returns immediately with a run_id to track)
curl -s -X POST http://localhost:8001/submit/simple_etl \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"source": "api"}}'

# List recent run states
curl -s -X POST http://localhost:8001/runs/query \
  -H "Content-Type: application/json" -d '{"last_n": 10}' | python3 -m json.tool

# Fetch one run (with logs) and its lifecycle event timeline
curl -s http://localhost:8001/runs/<run_id> | python3 -m json.tool
curl -s http://localhost:8001/runs/<run_id>/events | python3 -m json.tool

# Cooperatively cancel a running run
curl -s -X POST http://localhost:8001/runs/<run_id>/cancel
```

Flows worth trying:

| Flow | What it shows |
|---|---|
| `hello_world` | Fast, near-instant — good for `/execute`. |
| `simple_etl`, `data_pipeline` | `fetch_data` sleeps 15s — submit via `/submit` and watch the run sit in `running` on the dashboard/`/runs/query`. |
| `error_handling_demo` | `risky_operation` fails ~50% of the time; `max_retries=2` — watch the run retry then land on `completed` or `failed`. |
| `parallel_tasks` | Several sequential task calls in one flow. |

## 3. Run the dashboard

```bash
cd flowlet-web
npm install   # already done if node_modules/ exists
npm run dev
```

Vite serves on `http://localhost:5173` and proxies `/api/*` to
`http://localhost:8001/*` (see `vite.config.ts`) — so the API from step 2 must
already be running on port 8001, and `main.py` must keep mounting the router
at prefix `""` (it does, by default) so the paths line up.

## Multi-process topology (production-shaped, with a caveat)

The CLI (`flowlet work` / `flowlet sweep` / `flowlet api`) runs each role as
its own OS process against the same configured Flowlet instance:

```bash
.venv/bin/flowlet api   --app flowlet_example.flows:flowlet --port 8001 --prefix ""
.venv/bin/flowlet work  --app flowlet_example.flows:flowlet
.venv/bin/flowlet sweep --app flowlet_example.flows:flowlet
```

(`--prefix ""` is required here to match the dashboard's expectations —
`main.py` already mounts at `""`, but the CLI's `api` command defaults to
`/flowlet`.)

**Caveat:** `queue={"type": "memory"}` is an in-process `InMemoryQueue`. Each
of the commands above re-imports `flowlet_example.flows` and constructs its
*own* queue instance — a job submitted through the `api` process is invisible
to a `work` process started separately. This topology only demonstrates
independent scaling/deployment shape, not actual cross-process job handoff or
sweeper-driven crash recovery. For a real multi-process demo (e.g. to see the
sweeper reclaim a run after `kill -9`-ing a worker), swap `queue` in
`flows.py` for `{"type": "azure_queue", ...}` against an Azurite emulator, or
run everything through the single embedded-worker process from step 2, which
has no such split.

## Tests

```bash
.venv/bin/pytest flowlet-api/tests
```
