# Migration plan: Flowlet v2 — lease-based control plane, OTel instrumentation, pull-based reads

> **Status (2026-07-11): all five phases implemented.**  141 unit tests pass;
> end-to-end verification includes a SIGKILLed worker recovered by the
> sweeper across process boundaries.  Notable deltas from the plan:
> the sweeper also re-enqueues *stuck pending* runs (lost retry messages),
> archived terminal state lands in the run folder as `state.json` (no
> separate state-archive/ tree), and `run_flow`'s sync path now records
> state like a mini-worker instead of the dead pubsub/snapshot calls.

Goal: a genuinely serverless orchestrator for small/medium loads where every
component scales to zero and the monthly bill is dominated by storage pennies.
The state-file-as-truth + CAS design stays; what changes is everything that
assumed a long-lived process (heartbeats, pubsub pipeline, subscriber threads)
and everything hand-rolled that OTel provides (span context, instrumentation,
log formatting).

## Target architecture

```
submit (API) ──> queue (wake-up signal only) ──> worker job (scale from 0)
                                                    │  claim: CAS state + lease deadline
                                                    │  run:   OTel spans → blob exporter
                                                    │  done:  CAS terminal → run folder, ack
sweeper (cron job, 1–5 min) ──> list state/ ──> re-enqueue expired leases,
                                                fail exhausted, archive closed
API (scale to 0) ──> refresh local SQLite cache from state/ + runs/ on demand
```

- **Control plane**: `state/<flow>/<run_id>.json`, CAS-written, *active runs only*.
  Ownership = lease (`deadline_at`); no heartbeats, no queue-visibility coupling.
- **Durable record**: `runs/<flow>/<yyyy-mm-dd>/<run_id>/` containing
  `spans-<attempt>.jsonl` (OTel spans) and `state.json` (final state, moved
  here on terminal transition). One folder = one run, self-contained.
- **Queue**: pure wake-up. `enqueue / dequeue / ack`. No retry_count, no
  backoff, no renew/release. Message acked immediately after a successful claim.
  Since then made optional per deployment: `queue: {"type": "account"}` replaces
  it with the account-backed job source (submissions recorded directly as
  obligations, workers poll the state directory) — see the deployment guide.
- **Reads**: API rebuilds/refreshes an ephemeral local SQLite from
  `state/` (active) + recent `runs/` partitions (history), TTL a few seconds.
  No pubsub, no subscriber, no snapshot rollout/partition machinery.
- **Observability**: OTel SDK; run_id (uuid7) == trace_id. Mandatory sink is a
  custom `BlobSpanExporter` (product truth); OTLP/App Insights exporters are
  optional add-ons.

## Decisions locked up front

| Decision | Choice | Rationale |
|---|---|---|
| IDs | stdlib `uuid7` everywhere; drop `uuid7_desc` | one convention; uuid7 == 128-bit OTel trace_id; recency via date partitions + explicit `DESC` |
| Span identity | native OTel 64-bit span_id (hex) in records | no impedance with SDK; run_id remains the join key |
| Log layout | `runs/<flow>/<date>/<run_id>/spans-<attempt>.jsonl` | 1 read per run; hierarchy from parent_span_id fields; per-attempt file avoids two-writer appends after takeover |
| Retry authority | `RunState.attempt/max_retries` only | queue knows nothing about retries |
| kwargs persistence | stored in the state file at claim time | sweeper must be able to re-enqueue a crashed run without the original message |
| Lease default | `@flow(timeout=...)`, default 15 min; deadline = now + timeout per attempt | failure detection = deadline + sweep interval |
| Sweeper deploy | Container Apps cron job, same image, `flowlet sweep` | operational homogeneity with workers; ~$0 either way |
| Pubsub | removed from the data path entirely | pull-based reads; WS/live-push can return later as a nicety fed from the cache |
| In-flight visibility | state file only (spans export on completion) | unchanged truth model; crash loses in-flight spans, never status |

## Phases

Each phase merges independently with the full test suite green and a smoke
script exercising the changed path end-to-end.

### Phase 1 — IDs: standard uuid7 (S)

Mechanical, isolated, do first so later phases churn one convention only.

- `models.FlowJob`, `context.RunContext`, `controller.run_flow` → stdlib `uuid7`.
- Revert the desc-ordering sites (all known from the 2026-07-10 fix pass):
  `query.py` orderings back to `.desc()` = newest; rollout/`get_snapshot_period`/
  `ls()` back to natural order; `PeriodUUID.covers()` to normal comparisons;
  `Timestamp.to_uuid()` → ascending encoding.
- Delete `uuid7_desc` and its tests. Note: some of these files are rewritten
  again in Phases 3–4; the standalone phase keeps every commit self-consistent.

### Phase 2 — Control plane: leases + sweeper, queue simplification (L)

The biggest safety payoff; independent of OTel.

- `models.RunState`: drop `heartbeat_at`; add `kwargs` (claim-time copy) and
  keep `deadline_at` as the sole liveness signal. `FlowJob`: drop `retry_count`,
  `visibility_timeout`; add per-flow `timeout` plumbed from the decorator
  (`@flow(timeout=900)`) through registry → job → state.
- `worker.execute_job` v2: dequeue → resolve
  (`new` / `closed` / `busy`=running-and-unexpired / `expired`=running-past-deadline /
  `ready`) → CAS claim with fresh deadline (attempt+1 on ready/expired) →
  **ack immediately** → run → CAS finalize. On failure with retries left:
  CAS to `pending` + self-enqueue a fresh wake-up message. Delete
  `_heartbeat_loop`, `stale_threshold`, renew/release call sites, the
  etag_holder/stop_event/abort_event machinery, and the join-before-write dance.
- `queue/`: protocol shrinks to `enqueue(job, delay=0) / dequeue / ack`.
  Delete nack/backoff/renew/release from both implementations.
- New `sweeper.py` + `flowlet sweep` entrypoint:
  1. list `state/`;
  2. `running` past deadline → attempt < max ? CAS→`pending` + enqueue (kwargs
     from state) : CAS→`failed`;
  3. terminal states older than a grace window → move to the run folder
     (Phase 3 target; until then, a `state-archive/<date>/` holding area).
  Sweeps are idempotent and overlap-safe by construction (CAS transfers
  ownership, duplicate messages die in `closed`/`busy`).
- Tests: rewrite `test_worker_layer.py` for the lease machine; new
  `test_sweeper.py` (expired→requeued, exhausted→failed, closed→archived,
  overlapping sweeps harmless); simplify `test_queue_layer.py`.

### Phase 3 — OTel SDK + run-folder storage (L)

- New `tracing.py`: `TracerProvider` setup + `BlobSpanExporter` (groups spans
  by trace_id, appends JSON lines to
  `runs/<flow>/<date>/<run_id>/spans-<attempt>.jsonl`; flow/attempt carried as
  span attributes). Worker calls `force_flush()` before the terminal CAS write.
- `@flow`/`@task` decorators → `tracer.start_as_current_span`; user logging
  inside flows becomes span events via a small logging-handler shim.
  Delete `context.py` (ContextManager/RunContext), `instrumentation.py`,
  `ContextInjectingFilter`, `JSONSpanFormatter`, and the per-span
  `FilesystemHandler` routing (the LRU handler cache goes with it).
- Terminal transition writes `state.json` into the run folder and deletes from
  `state/` — the state-dir hygiene that keeps sweep cost O(active runs).
- `repository/log.py` v2: read one `spans-*.jsonl` set per run (no recursion,
  no child_span_id convention); `analysis.summarise` v2 builds the tree from
  `parent_span_id`. Optional one-shot converter for existing example data.
- Optional OTLP exporter behind config for users who want App Insights et al.

### Phase 4 — Pull-based read side (M)

- Replace `SnapshotRepository` + rollout strategies + `SnapshotSubscriber` +
  pubsub publishes with a `CacheRepository`: ephemeral local SQLite,
  `refresh(ttl≈3–5 s)` scanning `state/` + recent `runs/` date partitions.
  Optionally persist the cache file to blob as a cold-start accelerator
  (best-effort, zero correctness role).
- `RunQuery` reads through the cache; `controller.run_flow`'s dead
  `snapshot_repo`/`pubsub` branches removed; WS layer either polls the cache
  or is dropped in favour of client-side polling.
- Delete `pubsub/` (memory/zeromq/azure), `api/subscriber.py`.
- `app.configure()` slims down accordingly; API becomes fully stateless
  (scale-to-zero safe: no threads, no engine bound at startup).

### Phase 5 — Packaging, deploy, docs (S/M)

- CLI entrypoints: `flowlet work` (one job, exit code), `flowlet sweep`,
  `flowlet api`.
- Deploy shape: Container Apps — worker job (KEDA queue scaler), sweeper cron
  job (1–5 min), API app (scale to zero). One image.
- Delete remaining dead code; update anchors metadata, `architecture.md`,
  `design.md`, CLAUDE.md notes, and the LOGGING docs (superseded by OTel).
- Final e2e verification: submit → worker completes; submit → kill worker
  mid-run → sweeper recovers → retry completes; exhausted retries → failed;
  dashboard queries correct throughout; API cold-start renders history.

## Risk register

| Risk | Mitigation |
|---|---|
| Crash loses buffered in-flight spans | acceptable: state file records the failure; next attempt gets fresh spans; `force_flush` before terminal CAS bounds loss to crashes only |
| kwargs in state file may contain sensitive values | same exposure as today's queue message; document; storage ACLs are the boundary |
| Failure detection slower than heartbeats (deadline + sweep) | tune per-flow `timeout`; sweep cadence is a config knob costing ~nothing |
| Recorded runs in old layout unreadable by v2 readers | pre-1.0: accept; optional converter script in Phase 3 |
| Global "recent runs" listing scans one date partition per flow | fine at small/medium scale; revisit with a manifest if flow count grows large |
| Phases 1 partially rewritten by 3–4 | accepted cost for keeping every merge self-consistent |

## Order and effort

1 (S) → 2 (L) → 3 (L) → 4 (M) → 5 (S/M). Phases 2 and 3 are independent and
could swap or parallelise; 4 depends on 3's run-folder layout. Suggested
checkpoint after Phase 2: the concurrency surface is already reduced to
CAS + sweeper at that point, worth running under load before touching
observability.
