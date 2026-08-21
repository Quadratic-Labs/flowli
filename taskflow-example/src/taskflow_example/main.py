"""
The Taskflow Example app.
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
    """Background worker thread — drains the in-process InMemoryQueue.

    Only started once the lifespan guard has confirmed queue/state_repo/
    signals are configured; the asserts just narrow that for the type
    checker across the thread boundary.
    """
    executor = RegistryExecutor(flows.tf.registry)
    queue, state_repo, signals = flows.tf.queue, flows.tf.state_repo, flows.tf.signals
    assert queue is not None and state_repo is not None and signals is not None
    while not stop_event.is_set():
        try:
            result = execute_job(
                queue,
                executor,
                state_repo,
                signals,
                "embedded-worker",
                events=flows.tf.events,
                adjudication_for=flows.tf.adjudication_for,
            )
            if result == 2:  # no job available
                time.sleep(1.0)
        except Exception:
            logger.exception("embedded_worker_error")
            time.sleep(1.0)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    stop_event = threading.Event()
    worker_thread = None
    if (
        isinstance(flows.tf.queue, InMemoryQueue)
        and flows.tf.state_repo is not None
        and flows.tf.signals is not None
    ):
        worker_thread = threading.Thread(
            target=_worker_loop, args=(stop_event,), daemon=True, name="taskflow-worker"
        )
        worker_thread.start()
        logger.info("embedded_worker_started")
    flows.tf.start()
    try:
        yield
    finally:
        stop_event.set()
        if worker_thread is not None:
            worker_thread.join(timeout=5)
        flows.tf.stop()


app = FastAPI(
    title="Taskflow Example Application",
    description="Example application demonstrating the Taskflow authoring layer over the Flowlet account kernel",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(flows.tf.router, prefix="", tags=["Taskflow"])


@app.get("/")
def root():
    """Root endpoint providing information about the application"""
    return {
        "name": "Taskflow Example",
        "version": "0.1.0",
        "description": "Example application demonstrating the Taskflow framework",
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
