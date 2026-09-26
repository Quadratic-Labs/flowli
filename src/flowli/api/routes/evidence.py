"""`/evidence`. See specs/09-http-api.md section 10.

Evidence is what a step or an agent wrote while it ran. The journal says what
happened; evidence explains it. Nothing here is authority, and no decision
depends on it.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse

from flowli.adapters import evidence as keys
from flowli.domain import EvidenceItem, EvidenceRef, parse_eid

from ..auth import EVIDENCE_READ, TASKS_CONSUME, Principal
from ..deps import Config, Engine, require
from ..deps import principal as principal_dep
from ..problems import Problem
from ..reads import etag, json_response

router = APIRouter(tags=["evidence"])

Reader = Annotated[Principal, Depends(require(EVIDENCE_READ))]
MAX_UPLOAD = 32 << 20  # 32 MiB, the limit of one attachment


Fid = Annotated[str, Query(description="the frame id, from the frame tree")]


def _ref(eid: str, fid: str, attempt: int) -> EvidenceRef:
    # A frame id holds `/`, so it is a query parameter and not a path segment:
    # a percent-encoded slash does not survive routing.
    return EvidenceRef(parse_eid(eid), fid, attempt)


def _item(item: EvidenceItem) -> dict[str, Any]:
    return {
        "eid": str(item.ref.eid),
        "fid": item.ref.fid,
        "attempt": item.ref.attempt,
        "name": item.name,
        "media_type": item.media_type,
        "size": item.size,
    }


@router.get("/evidence/{eid}")
async def list_evidence(request: Request, eid: str, engine: Engine, who: Reader) -> Response:
    items = [_item(i) for i in await engine.ports.evidence.list(parse_eid(eid))]
    marks = (f"{i['fid']}#{i['attempt']}:{i['size']}" for i in items)
    tag = etag("evidence", eid, len(items), *marks)
    return json_response(request, {"items": items}, tag)


@router.get("/evidence/{eid}/attempts/{attempt}")
async def list_attempt(
    request: Request, eid: str, attempt: int, fid: Fid, engine: Engine, who: Reader
) -> Response:
    ref = _ref(eid, fid, attempt)
    items = [
        _item(i)
        for i in await engine.ports.evidence.list(ref.eid)
        if i.ref.fid == ref.fid and i.ref.attempt == ref.attempt
    ]
    marks = (f"{i['name']}:{i['size']}" for i in items)
    tag = etag("attempt-evidence", eid, ref.fid, attempt, *marks)
    return json_response(request, {"items": items}, tag)


@router.get("/evidence/{eid}/attempts/{attempt}/{name}")
async def get_evidence(
    eid: str, attempt: int, name: str, fid: Fid, engine: Engine, who: Reader
) -> Response:
    ref = _ref(eid, fid, attempt)
    data = await engine.ports.evidence.get(ref, name)
    if data is None:
        raise Problem(404, "unknown_evidence", "Unknown evidence", f"{name} of {ref.fid}")
    if name == keys.LOG_NAME:
        media = "application/x-ndjson"
    else:
        items = await engine.ports.evidence.list(ref.eid)
        found = next(
            (i for i in items if i.ref.fid == ref.fid and i.ref.attempt == ref.attempt and i.name == name),
            None,
        )
        media = "application/octet-stream" if found is None else found.media_type
    return Response(content=data, media_type=media)


@router.put("/evidence/{eid}/attempts/{attempt}/{name}", status_code=201)
async def put_evidence(
    eid: str,
    attempt: int,
    name: str,
    fid: Fid,
    request: Request,
    engine: Engine,
    config: Config,
    who: Annotated[Principal, Depends(principal_dep)],
    queue: str,
) -> Response:
    """A consumer puts what is too large for a reply payload: a transcript, a
    diff, a report. The reply then carries the reference, not the bytes.

    `queue` names the queue whose `tasks:consume` capability the caller must
    hold, as for every other call of the worker plane.
    """
    who.require(TASKS_CONSUME, queue)
    data = await request.body()
    if len(data) > MAX_UPLOAD:
        raise Problem(413, "too_large", "Attachment too large", f"limit {MAX_UPLOAD} bytes")
    ref = _ref(eid, fid, attempt)
    media = request.headers.get("content-type", "application/octet-stream")
    key = await engine.ports.evidence.put(ref, name, data, media)
    return JSONResponse(
        {"eid": str(ref.eid), "fid": ref.fid, "attempt": attempt, "name": name, "ref": key},
        status_code=201,
    )
