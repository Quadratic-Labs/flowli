# flowli-web

The operator interface: executions, their frames and journals, the catalog,
the review inbox, the queues. It talks to `flowli.api` and holds no state of
its own.

## Running it

Two processes. The backend first:

```bash
cd flowli
uv run python web/dev_server.py        # http://127.0.0.1:8000
```

`dev_server.py` is a development fixture: a filesystem bucket under
`web/.local/`, four demo workflows, an in-process worker and sweeper, and a
static token map in place of an identity provider. Then the interface:

```bash
cd flowli/web
npm install
npm run dev                            # http://localhost:5173
```

Vite proxies `/api` to the backend (`FLOWLI_API` overrides the target). Sign
in with the token `dev-operator` (every capability) or `dev-viewer` (reads
only) in the bar at the top right.

## What is where

| File | Content |
|---|---|
| `src/lib/api.ts` | the client: DTOs, the bearer token, the ETag cache, `min_seq` |
| `src/lib/format.ts` | durations, statuses and their colours |
| `src/pages/` | dashboard, executions, execution detail, catalog, start, reviews, queues |
| `src/components/FrameFlamegraph.tsx` | the frame tree in time |
| `src/components/EvidencePanel.tsx` | the attempt log and the attachments of one frame |

## Three things worth knowing

**The token is the identity.** No screen asks who you are, and no request
names its own actor: the service derives the actor from the access token, and
a review signed by whoever the client claimed to be would be worth nothing
(spec 09, section 5.2). `TokenBar` is the only place identity enters the app,
so a deployment swaps it for the authorization-code flow and changes nothing
else.

**Polling is cheap, and stops.** Every GET carries an `ETag`; the client sends
`If-None-Match` and, on a 304, hands back the very same object, so React Query
sees an unchanged value and nothing re-renders or redraws. A terminal
execution never changes again, so its views stop polling altogether.

**The flamegraph reads the journal, not a trace.** A frame id is a path, so
the tree is already in the journal and nothing here reconstructs a hierarchy
from spans. A hatched bar is a frame that waits — suspended, not working.
