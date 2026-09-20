"""Caching and freshness for reads. See docs/specs/09-http-api.md sections 4.3 and 4.4.

A client polls. An `ETag` plus `If-None-Match` makes an unchanged answer cost
one request and no work, and lets the web application keep the object it
already holds, so nothing redraws.
"""

from __future__ import annotations

import hashlib
import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any

from fastapi import Request, Response
from fastapi.responses import JSONResponse

STALE_HEADER = "X-Projection-Stale"
SEQ_HEADER = "X-Control-Seq"


def etag(*parts: Any) -> str:
    digest = hashlib.sha256(
        "\x1f".join("" if p is None else str(p) for p in parts).encode()
    ).hexdigest()
    return f'"{digest[:32]}"'


def json_response(
    request: Request, payload: Any, tag: str, *, stale: bool = False, status: int = 200
) -> Response:
    """A 304 when the client already holds this value, the body otherwise."""
    headers = {"ETag": tag}
    if stale:
        headers[STALE_HEADER] = "true"
    if request.headers.get("if-none-match") == tag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(payload, status_code=status, headers=headers)


async def freshness(projection: Any, min_seq: int | None, wait_ms: int) -> bool:
    """Wait for the projection to reach `min_seq`. True when it did not.

    A read that answers with stale data still answers: the client asked to
    wait, not to fail. The header says what happened.
    """
    if min_seq is None:
        return False
    caught_up = await projection.wait_for(min_seq, timeout=wait_ms / 1000)
    return not caught_up


def encode_cursor(value: tuple[str, str]) -> str:
    return urlsafe_b64encode(json.dumps(list(value)).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> tuple[str, str] | None:
    if not cursor:
        return None
    padded = cursor + "=" * (-len(cursor) % 4)  # pragma: no mutate
    try:
        first, second = json.loads(urlsafe_b64decode(padded.encode()))
    except Exception:
        from .problems import Problem

        raise Problem(400, "invalid_cursor", "Invalid cursor") from None
    return str(first), str(second)


__all__ = [
    "SEQ_HEADER",
    "STALE_HEADER",
    "decode_cursor",
    "encode_cursor",
    "etag",
    "freshness",
    "json_response",
]
