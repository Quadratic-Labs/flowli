# Deploying Flowlet on Azure

Flowlet's v2 architecture is built for scale-to-zero serverless deployment.
One container image runs three roles on Azure Container Apps:

```
                          ┌────────────────────────────────────────┐
 POST /flows/…/submit ──> │ flowlet-api        (Container App,     │
 GET  /obligations …             │   scale 0→N, stateless — reads storage │
                          │   through a TTL-refreshed local cache) │
                          └────────────┬───────────────────────────┘
                                       │ enqueue FlowJob
                                       ▼
   Azure Queue Storage  ── wake-up ──> flowlet-worker  (Event job, KEDA
        "flowlet-jobs"                   queue scaler, 0→N executions)
                                       │ CAS-claim lease → run → finalize
                                       ▼
   Azure Blob Storage   <── state/  (active obligations, CAS via ETag)
        "flowlet"       <── obligations/<flow>/<date>/<obligation_id>/  (spans + state.json)
                                       ▲
   flowlet-sweeper (Cron job, */5) ────┘ recover expired leases,
                                         re-enqueue, archive closed obligations
```

There is no database, no broker, and no always-on process.  Blob storage is
the source of truth; the queue is a wake-up signal; crash recovery is the
sweeper's cron pass (failure-detection latency = flow lease + sweep interval).
At small/medium load the entire control plane costs cents per month —
storage transactions dominate, compute sits inside the Container Apps and
Functions-equivalent free grants.

## 1. Prerequisites

- `az` CLI ≥ 2.60, logged in, with the `containerapp` extension.
- Docker (or `az acr build` for cloud builds).
- A Flowlet application module (see next section).

```bash
RG=flowlet-rg
LOC=westeurope
STORAGE=flowletstore$RANDOM       # must be globally unique, lowercase
ACR=flowletacr$RANDOM
ENV=flowlet-env
IMAGE=$ACR.azurecr.io/flowlet-app:latest
```

## 2. The application module

All three roles load the same module via `--app`.  It declares flows and
wires the config; secrets come from environment variables.

```python
# myproject/flows.py
from flowlet import Flowlet

flowlet = Flowlet.configure({
    "storage": {"type": "azure_blob", "container_name": "flowlet"},
    "queue":   {"type": "azure_queue", "queue_name": "flowlet-jobs"},
})

@flowlet.flow(timeout=600, max_retries=3)   # 10-minute lease per attempt
def process_data(source: str, batch_size: int = 100) -> None:
    ...
```

Connection strings are **not** put in code — the config classes read them
from the environment:

| Variable | Purpose |
|---|---|
| `FLOWLET_STORAGE_AZURE_BLOB_CONNECTION_STRING` | Blob storage auth |
| `FLOWLET_STORAGE_AZURE_BLOB_CONTAINER_NAME` | Container (default from code) |
| `FLOWLET_QUEUE_AZURE_CONNECTION_STRING` | Queue storage auth |
| `FLOWLET_QUEUE_AZURE_QUEUE_NAME` | Queue name (default `flowlet-jobs`) |

> Note: authentication is connection-string based today.  Managed-identity
> support would require swapping `from_connection_string` for
> `DefaultAzureCredential` in `storage/config.py` and `queue/config.py` —
> a known follow-up, not yet implemented.

### Queue-less mode

The queue is a wake-up signal, never a correctness component — so it is
optional. `queue: {"type": "account"}` drops the Azure Queue entirely:
submissions are recorded directly in the account store (the obligation *is*
the submission) and workers poll the active state directory for claimable
obligations. Requires a storage backend; nothing else changes — worker,
sweeper, and API run as-is.

```python
flowlet = Flowlet.configure({
    "storage": {"type": "azure_blob", "container_name": "flowlet"},
    "queue":   {"type": "account"},
})
```

When to choose it:

- **Take it** for dev, tests, and small deployments: one less resource to
  provision, and cross-process job handoff works over any shared store
  (including the local filesystem).
- **Keep the queue** when you want sub-second dispatch latency, KEDA
  scale-from-zero (the queue scaler in §5 needs a queue to measure — in
  account mode run the worker as a continuously-running app with
  `--min-replicas 1`), or many workers (pollers race on the head obligation;
  the lease CAS arbitrates, but the losers' reads are wasted storage
  transactions).

Dispatch latency becomes the worker's poll interval (`taskflow work
--poll-interval`, default 2 s), each poll lists and reads the active state
documents, and retry backoff is enforced from the account at claim time —
correctness is identical in both modes.

## 3. Container image

One image, three entrypoints (`flowlet work | sweep | api`):

```dockerfile
FROM python:3.14-slim
WORKDIR /app
COPY flowlet-api/ ./flowlet-api/
COPY myproject/ ./myproject/
RUN pip install --no-cache-dir ./flowlet-api
ENTRYPOINT ["flowlet"]
# role is chosen per Container App via args, e.g.:
#   api    --app myproject.flows:flowlet
#   work   --app myproject.flows:flowlet --once
#   sweep  --app myproject.flows:flowlet
```

## 4. Provision the shared resources

```bash
az group create -n $RG -l $LOC

# Storage account: one for both blob (truth) and queue (wake-ups)
az storage account create -n $STORAGE -g $RG -l $LOC \
  --sku Standard_LRS --kind StorageV2
CONN=$(az storage account show-connection-string -n $STORAGE -g $RG -o tsv)
az storage container create -n flowlet --connection-string "$CONN"
az storage queue create -n flowlet-jobs --connection-string "$CONN"

# Registry + image
az acr create -n $ACR -g $RG --sku Basic --admin-enabled true
az acr build -r $ACR -t flowlet-app:latest .

# Container Apps environment (consumption — pay per second, scale to zero)
az containerapp env create -n $ENV -g $RG -l $LOC
```

## 5. Deploy the three roles

Shared environment variables for every app/job:

```bash
FLOWLET_ENV_VARS="FLOWLET_STORAGE_AZURE_BLOB_CONNECTION_STRING=secretref:storage-conn \
FLOWLET_QUEUE_AZURE_CONNECTION_STRING=secretref:storage-conn"
```

### API (scale-to-zero web app)

```bash
az containerapp create -n flowlet-api -g $RG --environment $ENV \
  --image $IMAGE --registry-server $ACR.azurecr.io \
  --args "api" "--app" "myproject.flows:flowlet" \
  --ingress external --target-port 8000 \
  --min-replicas 0 --max-replicas 2 \
  --cpu 0.25 --memory 0.5Gi \
  --secrets "storage-conn=$CONN" \
  --env-vars $FLOWLET_ENV_VARS
```

The API is stateless: cold starts rebuild the obligation cache lazily from storage
on the first query (TTL-refreshed afterwards), so `min-replicas 0` is safe.

### Worker (event-driven job, KEDA queue scaler)

```bash
az containerapp job create -n flowlet-worker -g $RG --environment $ENV \
  --image $IMAGE --registry-server $ACR.azurecr.io \
  --args "work" "--app" "myproject.flows:flowlet" "--once" \
  --trigger-type Event \
  --replica-timeout 1800 --replica-retry-limit 0 \
  --replica-completion-count 1 --parallelism 5 \
  --min-executions 0 --max-executions 10 --polling-interval 30 \
  --cpu 0.5 --memory 1.0Gi \
  --secrets "storage-conn=$CONN" \
  --env-vars $FLOWLET_ENV_VARS \
  --scale-rule-name queue-messages --scale-rule-type azure-queue \
  --scale-rule-metadata "queueName=flowlet-jobs" "queueLength=1" \
                        "accountName=$STORAGE" \
  --scale-rule-auth "connection=storage-conn"
```

Each queue message spawns one execution of `flowlet work --once`, which
processes a single job and exits (exit code 0/1/2 shows up as the job
execution result).  Notes:

- `--replica-timeout` must exceed your longest flow lease (`@flow(timeout=…)`);
  otherwise the platform kills mid-run workers — recoverable (the sweeper
  re-enqueues after the lease expires) but wasteful.
- `--replica-retry-limit 0` is correct: retries belong to the state machine,
  never to the platform.
- Duplicate executions are harmless by design — the worker CAS-claims a
  lease before running and drops messages for busy/closed obligations.

For sustained medium throughput, an alternative is a regular Container App
running the polling loop (`work` without `--once`) scaled 0→N by the same
KEDA rule; jobs give cleaner per-obligation isolation, the loop amortises cold
starts.  Both shapes are supported by the same CLI.

### Sweeper (cron job)

```bash
az containerapp job create -n flowlet-sweeper -g $RG --environment $ENV \
  --image $IMAGE --registry-server $ACR.azurecr.io \
  --args "sweep" "--app" "myproject.flows:flowlet" \
  --trigger-type Schedule --cron-expression "*/5 * * * *" \
  --replica-timeout 300 --replica-retry-limit 1 \
  --cpu 0.25 --memory 0.5Gi \
  --secrets "storage-conn=$CONN" \
  --env-vars $FLOWLET_ENV_VARS
```

Sweeps are idempotent and overlap-safe (ownership transfers only via CAS
writes), so the cadence and retry limit are uncritical.  Every pass prints
`scanned/requeued/failed/archived/errors` counters to the job logs.

## 6. Verify

```bash
API_URL=https://$(az containerapp show -n flowlet-api -g $RG \
  --query properties.configuration.ingress.fqdn -o tsv)/flowlet

# Submit an obligation and watch it complete
curl -X POST $API_URL/flows/process_data/submit \
  -H 'content-type: application/json' \
  -d '{"kwargs": {"source": "smoke-test"}}'
curl "$API_URL/obligations/query" -X POST -H 'content-type: application/json' \
  -d '{"names": ["process_data"], "last_n": 5}'
```

Crash recovery check: kill a worker job execution mid-run from the portal;
after `lease timeout + ≤5 min` the sweeper re-enqueues it and a fresh
execution completes it with `attempt` incremented.

## 7. What it costs (small/medium load)

| Component | Driver | Ballpark |
|---|---|---|
| Blob storage | GB stored + transactions | ~$0.02/GB-month + cents |
| Queue storage | operations | ~$0.0004 per 10k ops |
| Worker job | vCPU/GiB-seconds while running | inside free grant until sustained load |
| Sweeper cron | ~10 s every 5 min | ~$0 (free grant) or <$1/month |
| API | scale-to-zero, per-request seconds | ~$0 idle |
| ACR Basic | flat | ~$5/month (the biggest line item) |

The one cost that grows with *history* rather than load is blob storage of
run folders; a lifecycle-management rule moving `obligations/` blobs to the cool
tier after 30 days keeps that flat too.

## 8. Local development

No Azure needed — same code, filesystem + in-memory backends:

```python
flowlet = Flowlet.configure({
    "storage": {"type": "filesystem", "base_path": "./storage"},
    "queue":   {"type": "memory"},
})
```

Run the roles in three terminals: `flowlet api --app …`,
`flowlet work --app …`, and `flowlet sweep --app …` when needed (with the
in-memory queue, worker and API must share a process for submits to reach
the worker — or submit via `POST /execute` which runs synchronously).
