"""`/reviews`. See specs/09-http-api.md section 8.5."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse

from ..auth import REVIEWS_DECIDE, REVIEWS_READ, Principal
from ..deps import Config, Engine, Limit, Projection, require
from ..deps import principal as principal_dep
from ..dto import DecideRequest, review_row
from ..problems import Problem
from ..reads import SEQ_HEADER, etag, freshness, json_response

router = APIRouter(tags=["reviews"])

Reader = Annotated[Principal, Depends(require(REVIEWS_READ))]
MinSeq = Annotated[int | None, Query(ge=0)]


@router.get("/reviews")
async def list_reviews(
    request: Request,
    projection: Projection,
    config: Config,
    limit: Limit,
    who: Reader,
    queue: Annotated[str | None, Query()] = None,
    min_seq: MinSeq = None,
) -> Response:
    stale = await freshness(projection, min_seq, config.wait_ms)
    rows = projection.pending_reviews(queue)[:limit]
    items = [review_row(r) for r in rows]
    tag = etag("reviews", queue, limit, len(items), *(i["rid"] for i in items))
    return json_response(request, {"items": items, "next_cursor": None}, tag, stale=stale)


@router.get("/reviews/{rid}")
async def get_review(request: Request, rid: str, projection: Projection, who: Reader) -> Response:
    row = projection.review(rid)
    if row is None:
        raise Problem(404, "unknown_review", "Unknown review", rid)
    body = review_row(row)
    return json_response(request, body, etag("review", rid, row.status, row.decided_at))


@router.post("/reviews/{rid}/decide")
async def decide_review(
    rid: str,
    body: DecideRequest,
    engine: Engine,
    projection: Projection,
    config: Config,
    who: Annotated[Principal, Depends(principal_dep)],
) -> Response:
    row = projection.review(rid)
    if row is None or row.eid is None:
        raise Problem(404, "unknown_review", "Unknown review", rid)
    # The queue and the eid come from the row. A caller that named its own
    # would decide a review of a queue it may not touch.
    who.require(REVIEWS_DECIDE, row.queue)
    decision = await engine.reviews.decide(
        rid, eid=row.eid, queue=row.queue, verdict=body.verdict, by=who.actor, data=body.data
    )
    headers = {}
    if config.refresh_after_write:
        seq = await projection.refresh()
        if seq is not None:
            headers[SEQ_HEADER] = str(seq)
    return JSONResponse(
        {
            "rid": rid,
            "verdict": decision.verdict,
            "decided_by": {"kind": decision.by.kind, "id": decision.by.id},
            "decided_at": decision.at.to_iso(),
        },
        headers=headers,
    )
