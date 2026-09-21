# 00 — Overview

## 1. Purpose

Flowlet is a workflow engine. It runs a workflow as a coroutine execution
stack. It writes one journal entry for each frame of that stack to CairnDB.
Because of the journal, an execution is traceable and resumable.

The engine has no server of its own. CairnDB gives the storage, the
coordination and the audit log. The bucket is the only shared component.

## 2. Scope of these specifications

These documents describe the domain layer, and the two surfaces that are
derived from it: the projection and the HTTP service. The domain layer is:

- the core objects and their states,
- the journal and the memo rule,
- the ports that the domain requires, and their CairnDB mapping,
- the API for workflow authors and for operators,
- the runtime protocols (worker, suspension, signals, recovery),
- patterns that the engine does not implement as primitives, such as review.

Two more documents describe the derived surfaces: the SQLite projection
(`08`) and the HTTP service (`09`).

These documents do not describe deployment, the identity provider, or the
user interface.

## 3. Design principles

1. **One journal per execution.** The journal is a CairnDB named log. It is
   both the trace and the memo table. No other store holds frame results.
2. **First outcome wins.** The first `frame.completed` entry for a frame id
   is the memo of that frame. Later entries for the same frame id are
   ignored. Duplicate execution converges.
3. **Replay from the top.** To resume an execution, a worker reads the
   journal and runs the workflow coroutine again from the start. Each frame
   with a memo returns the memo without running.
4. **Deterministic frame ids.** A frame id is a path. The path does not
   depend on wall-clock time, on the worker, or on the order in which
   concurrent frames complete.
5. **Ownership by lease.** Exactly one worker owns an execution at a time.
   The lease epoch is a fence token. Every write made by a worker carries
   that epoch.
6. **Provenance on every write.** Every journal entry, task and message
   carries who acted, what code acted, and where the code ran.
7. **Coarse control log.** A shared log receives lifecycle events only. Hot
   per-frame traffic never touches it.
8. **Humans are actors.** A human review is a task on a queue plus a message
   on a channel. The engine does not have a review primitive.

## 4. Documents

| File | Content |
|---|---|
| `00-overview.md` | this document, principles, glossary |
| `01-domain-model.md` | core objects and their states |
| `02-journal.md` | journal entries, memo rule, replay algorithm |
| `03-ports.md` | required ports and their CairnDB mapping |
| `04-api.md` | `Context` API for authors, `WorkflowEngine` API for operators |
| `05-protocols.md` | worker loop, suspension, signals, children, recovery, retention |
| `06-patterns.md` | delegate, review, scheduled start, built on the API only |
| `07-walkthroughs.md` | worked examples with journal contents |
| `08-projection.md` | SQLite projection of the control log, tables and API |
| `09-http-api.md` | the HTTP service: control plane, worker plane, authentication |
| `10-agent-runner.md` | the consumer of a delegate task, and the coding-agent case |
| `11-codeflow.md` | the controller over the runner: plan, routing, merge, gates |

## 5. Glossary

One word names one concept. These documents use the words below and no
synonyms for them.

| Word | Meaning |
|---|---|
| **workflow** | A named, versioned coroutine function. A definition, not a run. |
| **execution** | One run of a workflow. Identified by an `eid`, a UUID: v7 when started, v5 when derived from a parent frame. |
| **frame** | One node of the execution stack. A step, a child, a receive, a sleep, or the root. |
| **frame id** (`fid`) | The deterministic path of a frame inside its execution. |
| **attempt** | One real run of a frame by one worker. A frame can have many attempts. |
| **outcome** | The result of an attempt: completed with a value, or failed with an error. |
| **memo** | The first completed outcome of a frame in the journal. |
| **journal** | The CairnDB named log of one execution. Holds all frame entries. |
| **control log** | The shared CairnDB named log `wf`. Holds lifecycle entries only. |
| **entry** | One event in the journal or in the control log. |
| **lease** | The CairnDB lease that gives one worker ownership of an execution. |
| **epoch** | The fence token of a lease. Increases at each acquisition. |
| **task** | A unit of work on a queue: start, resume, or run a step. |
| **queue** | A named set of tasks. Workers dequeue from it. |
| **channel** | A named, ordered stream of messages. |
| **message** | One item sent on a channel. |
| **timer** | A future instant at which the sweeper resumes an execution. |
| **worker** | A process that dequeues tasks and runs executions. |
| **sweeper** | A scheduled job that fires timers and recovers dead executions. |
| **actor** | Who acts: a worker, a human, a system, or a schedule. |
| **site** | Where code runs: host, process, instance, region, worker, epoch. |
| **code** | What runs: workflow name, version, frame kind, frame name, code reference. |
| **provenance** | Actor, site, code, attempt number and time, together. |
| **timestamp** | A UTC instant, `cairndb.Timestamp`. The only time type of the domain. |
| **suspended** | A frame waits for a condition. Or, an execution has no owner and waits for a task. |
| **resumed** | A worker acquired the lease of a suspended execution. |
| **announce** | Append an entry to the control log. |
| **claim** | The CairnDB put-if-absent primitive. Exactly one caller wins. |
| **evidence** | What a step or an agent wrote while it ran: an attempt log, an attachment. It explains the journal. It is never authority. |
| **capability** | One permitted operation of the HTTP service. The service maps a group of the identity provider to a set of capabilities. |
| **consumer** | A process outside the engine that takes a delegate task and answers it. |
| **handle** | The address of a live agent session: a process, a session id, a URL. It lives in the task lease state. |
| **resume artifact** | The agent's compaction of its own progress, for a fresh agent that continues the work. Not a handle. |

### Verbs

| Action | Verb | Do not use |
|---|---|---|
| Add an entry to a log | append | write, record, log, emit |
| Store an object | write | save, persist, record |
| Get data | read | fetch, load, retrieve |
| Take a lease | acquire | claim, lock, take |
| Extend a lease | renew | heartbeat, refresh |
| End a lease | release | free, unlock |
| Add a task | enqueue | push, submit, schedule |
| Take a task | dequeue | pull, pop, poll |
| Finish a task | ack | complete, delete |
| Return a task | nack | requeue, retry |
| Put a message on a channel | send | publish, emit, signal |
| Take a message from a channel | receive | consume, read, wait |
| Test a condition | check | verify, validate, confirm |
| Execute a function | run | invoke, execute, call |

### Word to avoid

Do not use **parked**. Use **suspended**.
Do not use **signal** as a noun for a message. Use **message**. The verb
`engine.signal` is the operator action that sends a message and enqueues a
resume task. It is the only use of the word.
