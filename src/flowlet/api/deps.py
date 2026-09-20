"""What a route needs: the engine, the projection, the catalog, the caller.

See docs/specs/09-http-api.md sections 3 and 6.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Any

from fastapi import Depends, Query, Request

from .auth import Authenticator, Principal
from .auth import _bearer as bearer
from .catalog import Catalog


@dataclass(frozen=True, slots=True)
class ApiConfig:
    """See docs/specs/09-http-api.md section 3."""

    queues: tuple[str, ...] = ("default",)
    default_queue: str = "default"
    wait_ms: int = 2000  # the limit of a read-your-writes wait
    default_limit: int = 50
    max_limit: int = 500
    refresh_after_write: bool = True
    task_ttl: float = 300.0  # the visibility timeout the relay asks for
    max_task_ttl: float = 3600.0
    nack_delay: float = 5.0  # when the relay returns a task that is not a delegate
    max_wait_seconds: float = 20.0  # the limit of a long poll on dequeue
    poll_interval: float = 0.5
    title: str = "flowlet"


@dataclass
class ApiState:
    engine: Any
    projection: Any
    catalog: Catalog
    authenticator: Authenticator
    config: ApiConfig = field(default_factory=ApiConfig)


def state(request: Request) -> ApiState:
    return request.app.state.flowlet  # type: ignore[no-any-return]


def engine(request: Request) -> Any:
    return state(request).engine


def projection(request: Request) -> Any:
    return state(request).projection


def catalog(request: Request) -> Catalog:
    return state(request).catalog


def config(request: Request) -> ApiConfig:
    return state(request).config


def principal(request: Request) -> Principal:
    """The caller, from the token. Never from the body (section 5.2)."""
    st = state(request)
    token = bearer(request.headers.get("authorization"))  # pragma: no mutate
    on_behalf_of = request.headers.get("x-on-behalf-of")  # pragma: no mutate
    return st.authenticator.authenticate(token, on_behalf_of)


def require(capability: str) -> Callable[..., Principal]:
    """A dependency that refuses a caller without `capability`.

    A queue-scoped capability is checked in the route, where the queue is
    known: for a review the queue comes from the projection row, never from
    the request.
    """

    def dependency(who: Annotated[Principal, Depends(principal)]) -> Principal:
        who.require(capability)
        return who

    return dependency


def limit_param(request: Request, limit: int | None = Query(default=None, ge=1)) -> int:
    cfg = config(request)
    if limit is None:
        return cfg.default_limit
    return min(limit, cfg.max_limit)


Engine = Annotated[Any, Depends(engine)]
Projection = Annotated[Any, Depends(projection)]
CatalogDep = Annotated[Catalog, Depends(catalog)]
Config = Annotated[ApiConfig, Depends(config)]
Limit = Annotated[int, Depends(limit_param)]
MinSeq = Annotated[int | None, Query(alias="min_seq")]
