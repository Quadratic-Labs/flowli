#!/usr/bin/env python3
"""Development worker script for local testing.
Usage: python scripts/worker.py
"""
import logging
import time

from taskflow import RegistryExecutor
from taskflow_example.flows import tf
from flowlet.worker import execute_job

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_worker(worker_id: str = "dev-worker-1", poll_interval: float = 1.0) -> None:
    """Run a worker loop that continuously polls the queue for jobs."""
    if not tf.queue:
        raise RuntimeError("Queue not configured — set queue.type in Taskflow.configure()")
    if not tf.state_repo or not tf.signals:
        raise RuntimeError("Storage not configured — set storage.type in Taskflow.configure()")

    executor = RegistryExecutor(tf.registry)
    logger.info("worker_started", extra={"worker_id": worker_id})

    try:
        while True:
            try:
                result = execute_job(
                    tf.queue,
                    executor,
                    tf.state_repo,
                    tf.signals,
                    worker_id,
                    events=tf.events,
                    review_policy_for=tf.review_policy_for,
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
