"""The routers of the service. See specs/09-http-api.md sections 8 and 9."""

from .catalog import router as catalog_router
from .evidence import router as evidence_router
from .executions import router as executions_router
from .queues import router as queues_router
from .reviews import router as reviews_router
from .worker import router as worker_router

__all__ = [
    "catalog_router",
    "evidence_router",
    "executions_router",
    "queues_router",
    "reviews_router",
    "worker_router",
]
