"""RFC 9457 problem bodies. See specs/09-http-api.md section 4.2.

One error shape for the whole service. `code` is the name of the domain
error, so a client can branch on it without parsing prose.
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from flowli.domain import InvalidName, WorkflowNotRegistered
from flowli.runtime.engine import UnknownExecution

from .auth import Forbidden, Unauthenticated

MEDIA_TYPE = "application/problem+json"


class Problem(Exception):
    """An answer that is not a result."""

    def __init__(
        self, status: int, code: str, title: str, detail: str | None = None, **extra: Any
    ) -> None:
        super().__init__(detail or title)
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.extra = extra

    def response(self) -> JSONResponse:
        body: dict[str, Any] = {
            "type": f"about:blank#{self.code}",
            "title": self.title,
            "status": self.status,
            "code": self.code,
        }
        if self.detail is not None:
            body["detail"] = self.detail
        body.update(self.extra)
        return JSONResponse(body, status_code=self.status, media_type=MEDIA_TYPE)


def unknown_execution(eid: Any) -> Problem:
    return Problem(404, "unknown_execution", "Unknown execution", f"no execution {eid}")


def install(app: Any) -> None:
    """Map every error this service can raise to one problem body."""

    async def _problem(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, Problem)
        return exc.response()

    async def _unauthenticated(_: Request, exc: Exception) -> JSONResponse:
        # The reason a token failed never reaches the client.
        return Problem(401, "unauthenticated", "Unauthenticated").response()

    async def _forbidden(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, Forbidden)
        return Problem(
            403, "forbidden", "Forbidden", f"missing capability {exc.capability}"
        ).response()

    async def _unknown_execution(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, UnknownExecution)
        return unknown_execution(exc.eid).response()

    async def _unknown_workflow(_: Request, exc: Exception) -> JSONResponse:
        return Problem(
            404, "unknown_workflow", "Unknown workflow", str(exc)
        ).response()

    async def _invalid_name(_: Request, exc: Exception) -> JSONResponse:
        return Problem(400, "invalid_name", "Invalid name", str(exc)).response()

    app.add_exception_handler(Problem, _problem)
    app.add_exception_handler(Unauthenticated, _unauthenticated)
    app.add_exception_handler(Forbidden, _forbidden)
    app.add_exception_handler(UnknownExecution, _unknown_execution)
    app.add_exception_handler(WorkflowNotRegistered, _unknown_workflow)
    app.add_exception_handler(InvalidName, _invalid_name)


__all__ = ["MEDIA_TYPE", "Problem", "install", "unknown_execution"]
