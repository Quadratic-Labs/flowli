# Flowlet

Flowlet is a workflow engine for Python. A workflow is an `async` function. The
engine runs that function as a coroutine execution stack. It appends one journal
entry for each frame of that stack. Because of the journal, an execution is
traceable and it is resumable.

Flowlet has no server of its own. CairnDB gives the storage, the coordination
and the audit log. One bucket holds the journals, the queues, the leases and the
messages. The bucket is the only shared component. You run processes. You do not
run a cluster.

## The idea in one function

```python
@registry.workflow("invoice_approval", version="3")
async def invoice_approval(ctx: Context, invoice_id: str) -> str:
    """Approve one invoice. A large invoice waits for a person."""
    invoice = await ctx.step(fetch_invoice, invoice_id)
    score = await ctx.step(score_risk, invoice)
    if score > 0.8:
        decision = await review(ctx, "finance", invoice, timeout=timedelta(days=3))
        if decision is None or decision.verdict != "approve":
            return "rejected"
    await ctx.send(f"invoice:{invoice_id}", {"status": "approved"})
    return "approved"
```

This function can wait three days for a person. The worker does not wait with
it. The worker writes the journal, releases the execution, and takes other work.
When the decision arrives, a worker reads the journal, runs the function again
from the first line, and returns the memo of each frame that ran already. The
function continues at the line that waits.

## What the engine gives you

| Property | How you get it |
|---|---|
| **Durable execution** | Each frame writes its outcome to the journal before the next frame starts. |
| **Resumption after a crash** | A worker replays the function and reads the memo of each finished frame. |
| **One owner at a time** | A lease gives one worker the ownership of one execution. The epoch fences the others. |
| **Idempotent start** | A dispatch key makes two starts converge on one execution. |
| **A wait that costs nothing** | A suspended execution holds no worker, no thread and no connection. |
| **Humans in the workflow** | A review is a task on a queue and a message on a channel. |
| **Work outside the engine** | A delegate task goes to an agent, to a service, or to a group of people. |
| **A trace you can read** | Each entry names who acted, what code acted, where it ran, and when. |

## Where to start

::::{grid} 1 1 2 3
:gutter: 3

:::{grid-item-card} Quickstart
:link: quickstart
:link-type: doc

Write a workflow, start it, run a worker, and read the journal. Ten minutes.
:::

:::{grid-item-card} Concepts
:link: concepts
:link-type: doc

The words this project uses: execution, frame, memo, channel, lease.
:::

:::{grid-item-card} Architecture
:link: architecture
:link-type: doc

The layers, the ports, the processes, and what each guarantee comes from.
:::

:::{grid-item-card} Configuration
:link: configurations
:link-type: doc

Every setting: the storage, the engine, the jobs, the service, the logs.
:::

:::{grid-item-card} Hosting
:link: hosting
:link-type: doc

The processes to run, the bucket to give them, and the limits to respect.
:::

:::{grid-item-card} API reference
:link: reference
:link-type: doc

The public classes and functions, from the docstrings of the code.
:::

::::

## The parts of the project

| Part | Content |
|---|---|
| `flowlet` | the engine: the domain, the adapters, the runtime, the CLI |
| `flowlet[api]` | the HTTP service over one engine |
| `web/` | the operator interface over that service |
| `flowlet-runner` | coding agents as the consumer of a delegate task |
| `flowlet-codeflow` | the controller over the runner: plan, routing, merge, gates |

## The design principles

1. **One journal per execution.** The journal is the trace and the memo table.
   No other store holds the result of a frame.
2. **First outcome wins.** The first `frame.completed` entry for a frame id is
   the memo of that frame. A duplicate run converges.
3. **Replay from the top.** To resume an execution, a worker runs the workflow
   function again from the start. Each frame with a memo returns the memo.
4. **Deterministic frame ids.** A frame id is a path. It does not depend on the
   clock, on the worker, or on the order of concurrent frames.
5. **Ownership by lease.** One worker owns one execution at a time. The lease
   epoch is a fence token.
6. **Provenance on every write.** Each entry, task and message carries who
   acted, what code acted, and where the code ran.
7. **Humans are actors.** A review is a task plus a message. The engine has no
   review primitive.

:::{admonition} The writing style
:class: note

These documents and the specifications use ASD-STE100 Strict: one word for
one concept, short sentences, and the active voice. The
[glossary](concepts.md#the-words-and-their-meanings) lists the words and the
verbs. The [specifications](specs/00-overview.md) are the authority when a
page here is shorter than the truth.
:::

:::{toctree}
:hidden:
:caption: Guides

quickstart
concepts
architecture
configurations
hosting
:::

:::{toctree}
:hidden:
:caption: Reference

reference
:::

:::{toctree}
:hidden:
:caption: Specifications

specs/00-overview
specs/01-domain-model
specs/02-journal
specs/03-ports
specs/04-api
specs/05-protocols
specs/06-patterns
specs/07-walkthroughs
specs/08-projection
specs/09-http-api
specs/10-agent-runner
specs/11-codeflow
:::
