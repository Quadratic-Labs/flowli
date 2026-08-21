"""
The Flowlet Example app.
"""
import logging
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import flows
from flowlet.queue.memory import InMemoryQueue
from flowlet.worker import execute_job
from taskflow import RegistryExecutor

logger = logging.getLogger(__name__)


def _worker_loop(stop_event: threading.Event) -> None:
    """Background worker thread — drains the in-process InMemoryQueue."""
    while not stop_event.is_set():
        try:
            result = execute_job(
                queue=flows.flowlet.queue,
                executor=RegistryExecutor(flows.flowlet.registry),
                state_repo=flows.flowlet.state_repo,
                signals=flows.flowlet.signals,
                worker_id="embedded-worker",
                adjudication_for=flows.flowlet.adjudication_for,
            )
            if result == 2:  # no job available
                time.sleep(1.0)
        except Exception:
            logger.exception("embedded_worker_error")
            time.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = threading.Event()
    worker_thread = None
    if isinstance(flows.flowlet.queue, InMemoryQueue) and flows.flowlet.state_repo is not None:
        worker_thread = threading.Thread(
            target=_worker_loop, args=(stop_event,), daemon=True, name="flowlet-worker"
        )
        worker_thread.start()
        logger.info("embedded_worker_started")
    flows.flowlet.start()
    try:
        yield
    finally:
        stop_event.set()
        if worker_thread is not None:
            worker_thread.join(timeout=5)
        flows.flowlet.stop()


app = FastAPI(
    title="Flowlet Example Application",
    description="Example application demonstrating the Flowlet workflow orchestration framework",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(flows.flowlet.router, prefix="", tags=["Flowlet"])


@app.get("/")
def root():
    """Root endpoint providing information about the application"""
    return {
        "name": "Flowlet Example",
        "version": "0.1.0",
        "description": "Example application demonstrating Flowlet framework",
        "docs": {
            "intent": "visit the API's documentation",
            "route": "/docs",
        },
        "flows": {
            "intent": "list available flows",
            "route": "/flows",
        },
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
