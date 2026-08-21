"""Taskflow CLI — the in-process worker for registered flows."""
import logging
import sys
import time

import click

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("taskflow")


def _load(app_ref: str):
    import importlib

    module_name, _, attr = app_ref.partition(":")
    module = importlib.import_module(module_name)
    tf = getattr(module, attr or "taskflow")

    from taskflow.app import Taskflow

    if not isinstance(tf, Taskflow):
        raise click.ClickException(f"{app_ref} is not a Taskflow instance")
    return tf


@click.group()
def main() -> None:
    """Prefect-like task orchestration over the Flowlet account kernel."""


@main.command()
@click.option("--app", "app_ref", required=True, help="e.g. 'myproject.flows:tf'.")
@click.option("--worker-id", default=None)
@click.option("--once", is_flag=True)
@click.option("--poll-interval", default=2.0, show_default=True)
def work(app_ref: str, worker_id: str | None, once: bool, poll_interval: float):
    """Run the worker: registered callables through the kernel's executor seam."""
    from flowlet.worker import execute_job

    from taskflow.executor import RegistryExecutor

    tf = _load(app_ref)
    if tf.queue is None or tf.state_repo is None or tf.signals is None:
        raise click.ClickException("Worker requires both queue and storage")

    if worker_id is None:
        import os
        import socket

        worker_id = f"{socket.gethostname()}-{os.getpid()}"

    executor = RegistryExecutor(tf.registry)
    logger.info("worker_started", extra={"worker_id": worker_id})
    if once:
        sys.exit(
            execute_job(
                tf.queue, executor, tf.state_repo, tf.signals, worker_id,
                events=tf.events, adjudication_for=tf.adjudication_for,
            )
        )
    try:
        while True:
            rc = execute_job(
                tf.queue, executor, tf.state_repo, tf.signals, worker_id,
                events=tf.events, adjudication_for=tf.adjudication_for,
            )
            if rc == 2:
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("worker_stopped", extra={"worker_id": worker_id})


if __name__ == "__main__":
    main()
