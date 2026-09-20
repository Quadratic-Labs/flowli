"""`create_app`. See docs/specs/09-http-api.md section 3.

The service holds no state of its own: every read comes from the projection
or from a port, and every write goes through the engine.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from . import problems
from .auth import Authenticator
from .catalog import Catalog
from .deps import ApiConfig, ApiState
from .routes import (
    catalog_router,
    evidence_router,
    executions_router,
    queues_router,
    reviews_router,
    worker_router,
)


def create_app(
    engine: Any,
    *,
    authenticator: Authenticator,
    projection: Any,
    catalog: Catalog | None = None,
    config: ApiConfig | None = None,
) -> FastAPI:
    """Wire one engine, one projection and one catalog into a FastAPI app.

    The app owns the refresh loop of the projection: it catches up once at
    startup and polls while it runs (`08-projection.md`, section 4).
    """
    config = config or ApiConfig()
    catalog = catalog or Catalog(engine.registry, default_queue=config.default_queue)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await projection.refresh()
        await projection.start()
        try:
            yield
        finally:
            await projection.stop()

    app = FastAPI(title=config.title, lifespan=lifespan)
    app.state.flowlet = ApiState(
        engine=engine,
        projection=projection,
        catalog=catalog,
        authenticator=authenticator,
        config=config,
    )
    problems.install(app)
    for router in (
        catalog_router,
        executions_router,
        reviews_router,
        queues_router,
        evidence_router,
        worker_router,
    ):
        app.include_router(router)
    return app
