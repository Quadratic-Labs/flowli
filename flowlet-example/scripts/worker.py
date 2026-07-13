#!/usr/bin/env python3
"""Development worker script for local testing.
Usage: python scripts/worker.py
"""
import logging
import time

from flowlet_example.flows import flowlet
from flowlet.worker import execute_job

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_worker(worker_id: str = "dev-worker-1", poll_interval: float = 1.0) -> None:
    """Run a worker loop that continuously polls the queue for jobs."""
    if not flowlet.queue:
        raise RuntimeError("Queue not configured — set queue.type in Flowlet.configure()")

    if not flowlet.state_repo:
        raise RuntimeError("Storage not configured — set storage.type in Flowlet.configure()")

    logger.info("worker_started", extra={"worker_id": worker_id})

    try:
        while True:
            try:
                result = execute_job(
                    queue=flowlet.queue,
                    registry=flowlet.registry,
                    state_repo=flowlet.state_repo,
                    worker_id=worker_id,
                )
                if result == 2:
                    time.sleep(poll_interval)
            except Exception as e:
                logger.error("worker_error: %s", e)
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("worker_stopped", extra={"worker_id": worker_id})


if __name__ == "__main__":
    run_worker()
