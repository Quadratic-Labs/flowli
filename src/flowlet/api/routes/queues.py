"""`/queues`, `/stats`, `/health`. See docs/specs/09-http-api.md sections 8 and 8.6."""

from __future__ import annotations

from typing import Annotated

from cairndb import Timestamp
from fastapi import APIRouter, Depends, Request, Response

from ..auth import EXECUTIONS_READ, QUEUES_READ, Principal
from ..deps import Config, Engine, Limit, Projection, require
from ..dto import queue_depth, task_row
from ..reads import etag, json_response

router = APIRouter(tags=["queues"])

QueueReader = Annotated[Principal, Depends(require(QUEUES_READ))]
StatsReader = Annotated[Principal, Depends(require(EXECUTIONS_READ))]


@router.get("/queues")
async def list_queues(
    request: Request, engine: Engine, config: Config, who: QueueReader
) -> Response:
    # Depth is live state, so it is read from the queue. The control log stays
    # coarse: an ack leaves no entry (docs/specs/00-overview.md, principle 7).
    items = [
        queue_depth(queue, await engine.ports.queue.depth(queue)) for queue in config.queues
    ]
    tag = etag("queues", *(f"{i['queue']}:{i['total']}:{i['claimed']}" for i in items))
    return json_response(request, {"items": items}, tag)


@router.get("/queues/{queue}/tasks")
async def list_tasks(
    request: Request, queue: str, engine: Engine, limit: Limit, who: QueueReader
) -> Response:
    tasks = await engine.ports.queue.pending(queue, limit=limit)
    items = [task_row(t) for t in tasks]
    tag = etag("tasks", queue, limit, *(i["task_id"] for i in items))
    return json_response(request, {"items": items, "next_cursor": None}, tag)


@router.get("/stats")
async def stats(request: Request, projection: Projection, who: StatsReader) -> Response:
    counts = {status.value: n for status, n in projection.counts_by_status().items()}
    return json_response(request, {"by_status": counts}, etag("stats", sorted(counts.items())))


@router.get("/health")
async def health(request: Request, projection: Projection) -> Response:
    """Liveness, and how far the projection got. It takes no token and shows
    no execution data."""
    from fastapi.responses import JSONResponse

    body = {"status": "ok", "projection_seq": None, "checked_at": Timestamp.now().to_iso()}
    try:
        body["projection_seq"] = await projection.refresh()
    except Exception as exc:  # the bucket is unreachable, or the file is broken
        body["status"] = "degraded"
        body["detail"] = type(exc).__name__
    return JSONResponse(body, status_code=200 if body["status"] == "ok" else 503)
