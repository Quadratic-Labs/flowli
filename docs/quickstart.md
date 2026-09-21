# Quickstart

This page takes ten minutes. At the end you have a workflow, a bucket on your
disk, a worker that runs the workflow, and a journal that tells you what
happened.

## 1. Install

Flowlet needs Python 3.14 and CairnDB. CairnDB comes from the sibling checkout
`../cairndb`, which `uv` resolves for you.

::::{tab-set}
:::{tab-item} uv

```bash
git clone git@github.com:Quadratic-Labs/flowlet.git
cd flowlet
uv sync --extra cli
```
:::

:::{tab-item} pip

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ../cairndb
pip install -e ".[cli]"
```
:::

::::

The extras choose what you install:

| Extra | Content |
|---|---|
| none | the engine as a library |
| `cli` | the `flowlet` command: worker, sweeper, retention, and the operator commands |
| `api` | the HTTP service, `flowlet.api.create_app` |
| `dev` | the test tools |

## 2. Write a workflow

A workflow is an `async` function. Its first parameter is a
{py:obj}`Context <flowlet.runtime.Context>`. A {py:obj}`Registry <flowlet.runtime.Registry>`
holds the workflows of your application under a name and a version.

:::{code-block} python
:caption: myapp/flows.py

from flowlet.runtime import Context, Registry

registry = Registry()


def make_greeting(name: str) -> str:
    """A step. It runs one time. Its value goes in the journal."""
    return f"hello {name}"


@registry.workflow("greet", version="1")
async def greet(ctx: Context, name: str) -> str:
    """Greet one person."""
    return await ctx.step(make_greeting, name)
:::

Three rules apply to every workflow function:

1. The first parameter is the `Context`. Every other parameter and the return
   value must be JSON-compatible.
2. The function must be deterministic. It reads the clock, random numbers and
   external state through a step only.
3. The function must not change its arguments. A replay runs the function again
   with the arguments of the record.

:::{admonition} What belongs in a step
:class: tip

Put each effect in a step: an HTTP call, a write to a database, an email.
The engine runs a step one time and stores its value in the journal. The
code between the steps runs again at each replay, so keep it pure.
:::

## 3. Start an execution

The CLI runs the jobs. It does not start an execution, because the arguments of
a workflow belong to your code. A short script starts one:

:::{code-block} python
:caption: start.py

import asyncio

from flowlet.adapters.cairndb import CairnBackend
from flowlet.domain import Actor, Site
from flowlet.runtime import Engine

from myapp.flows import greet, registry


async def main() -> None:
    backend = CairnBackend.configure({"storage": {"type": "filesystem", "path": "./bucket"}})
    engine = Engine(backend.ports, Site.local("starter"), registry=registry)
    try:
        eid = await engine.start(greet, "zoe", by=Actor.human("thomas@example.com"))
        print(eid)
    finally:
        await backend.close()


asyncio.run(main())
:::

```bash
python start.py
# 01a0c2d0-9706-742d-b8ec-eda635e1d316
```

The call writes the execution record, announces `execution.created`, and puts a
`START` task on the queue `default`. It returns the execution id, the `eid`.
Nothing runs yet: a worker runs the workflow.

:::{admonition} Who started it
:class: note

`by=` names the actor of the start. The journal keeps that name for as long
as the execution exists. Use {py:obj}`Actor.human <flowlet.domain.Actor>` for a
person, `Actor.system` for a service, `Actor.schedule` for a schedule.
:::

## 4. Run a worker

```bash
flowlet worker --app myapp.flows:registry --storage-path ./bucket --once
```

`--app module:attr` names the `Registry`. The CLI builds the CairnDB backend
and the engine around it. `--once` runs one pass and exits, which suits this
walkthrough and a cron job. Without `--once` the worker polls until you stop it
with `Ctrl-C`.

The worker dequeues the `START` task, acquires the lease of the execution, runs
the function, appends the journal entries, and releases the lease.

## 5. Read what happened

```bash
flowlet status 01a0c2d0-9706-742d-b8ec-eda635e1d316 \
    --app myapp.flows:registry --storage-path ./bucket --journal
```

```text
eid        01a0c2d0-9706-742d-b8ec-eda635e1d316
status     completed
workflow   greet v1
queue      default
created    2026-09-21T07:14:05.190110Z by human:thomas@example.com
last       execution.completed at 2026-09-21T07:14:23.524853Z by worker:host-3 on host-3 epoch 1
payload
{
  "value": "hello zoe"
}

           seq  type                   fid                              at                       actor
       1000000  execution.started      root                             2026-09-21T07:14:23.524853Z worker:host-3
       2000000  frame.started          root/make_greeting#0             2026-09-21T07:14:23.539047Z worker:host-3
       3000000  frame.completed        root/make_greeting#0             2026-09-21T07:14:23.539047Z worker:host-3
       4000000  execution.completed    root                             2026-09-21T07:14:23.524853Z worker:host-3
```

Read the journal from the bottom: the execution completed, and the one step
completed before it. `root/make_greeting#0` is the frame id. It is a path, and
`#0` is the first frame with that name under `root`.

:::{admonition} Colour, and pipes
:class: note

On a terminal the output is coloured. Piped output is plain text, byte for
byte, so `grep`, `awk` and `cut` keep working. `NO_COLOR` drops the colour
on a terminal too.
:::

## 6. Make it wait

An execution that waits is the reason this engine exists. Add a workflow that
waits for a message:

:::{code-block} python
:caption: myapp/flows.py

@registry.workflow("order", version="1")
async def order(ctx: Context, order_id: str) -> str:
    """Wait for the payment of one order."""
    message = await ctx.receive("payments")
    return f"{order_id}:{message.payload['status']}"
:::

Start it, then run one pass of the worker:

```bash
flowlet worker --app myapp.flows:registry --storage-path ./bucket --once
flowlet status $EID --app myapp.flows:registry --storage-path ./bucket
```

```text
status     suspended
last       execution.suspended at 2026-09-21T07:14:40.479657Z by worker:host-3 on host-3 epoch 1
waiting    channel:01a0c2d1-1f0e-766f-9632-fa2be4b1be56.payments
```

The execution holds no worker now. It waits on a channel. Send the message:

```bash
flowlet signal $EID payments '{"status": "paid"}' \
    --app myapp.flows:registry --storage-path ./bucket --by thomas@example.com
```

`signal` sends the message and puts a `RESUME` task on the queue. Run the
worker one more time, then read the journal:

```text
       1000000  execution.started      root                  ... worker:host-3
       2000000  frame.started          root/receive#0        ... worker:host-3
       3000000  frame.suspended        root/receive#0        ... worker:host-3
       4000000  execution.suspended    root                  ... worker:host-3
       5000000  execution.resumed      root                  ... worker:host-9
       6000000  frame.fulfilled        root/receive#0        ... worker:host-9
       7000000  execution.completed    root                  ... worker:host-9
```

A second worker, at a second epoch, continued the same function. The engine ran
the function twice and the receive frame one time.

## 7. Add a person

A {py:obj}`review <flowlet.patterns.review>` puts a task on a queue for people and waits
for the decision:

```python
from datetime import timedelta

from flowlet.patterns import review


@registry.workflow("invoice", version="1")
async def invoice(ctx: Context, invoice_id: str, amount: int) -> str:
    """A small invoice settles on its own. A large one waits for a person."""
    if amount < 500:
        return "auto"
    decision = await review(ctx, "finance", {"invoice_id": invoice_id, "amount": amount},
                            timeout=timedelta(days=3))
    if decision is None:
        return "expired"
    return decision.verdict
```

An operator answers with {py:obj}`engine.reviews.decide <flowlet.patterns.Reviews>`,
or a person answers in the web interface. The review id and the queue come from
the inbox of the projection. The
[HTTP service](hosting.md#the-http-service-and-the-interface) shows that inbox.

## 8. Test it without a bucket

{py:obj}`MemoryBackend <flowlet.adapters.memory.MemoryBackend>` implements every port
in memory. A test needs no bucket and no disk:

:::{code-block} python
:caption: tests/test_greet.py

from flowlet.adapters.memory import MemoryBackend
from flowlet.domain import Actor, Site
from flowlet.runtime import Engine

from myapp.flows import greet, registry


async def test_greet_completes() -> None:
    backend = MemoryBackend()
    engine = Engine(backend.ports, Site.local("test"), registry=registry)

    eid = await engine.start(greet, "zoe", by=Actor.human("test@example.com"))
    await engine.worker().run_once()

    assert (await engine.status(eid)).value == "completed"
    entries = await engine.journal(eid)
    assert entries[-1].item.payload["value"] == "hello zoe"
:::

`MemoryBackend` takes a `ManualClock`. Give the clock to the engine, then move
time with `clock.advance(...)` to test a timeout, a retry delay or a sleep.

## 9. See it in a browser

```bash
uv run --extra api python web/dev_server.py     # the service, a worker and a sweeper
cd web && npm install && npm run dev            # the interface
```

The development server holds four demo workflows, a filesystem bucket under
`web/.local/`, and two tokens: `dev-operator` with every capability, and
`dev-viewer` with the reads. It is a fixture, not a deployment.

## What to read next

| Question | Page |
|---|---|
| What is a frame, a memo, an epoch? | [Concepts](concepts.md) |
| How does the engine store all of this? | [Architecture](architecture.md) |
| Which settings exist? | [Configuration](configurations.md) |
| Which processes must run in production? | [Hosting](hosting.md) |
| What is the signature of `ctx.step`? | [API reference](reference.md) |
