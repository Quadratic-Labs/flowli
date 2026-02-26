"""
Worker for executing a single queued flow job.

Provides a simple execution handler for Azure-triggered job processing.
Azure Queue triggers spawn new executions for each message.
"""
import logging

from .queue import JobQueueProtocol
from .registry import Registry

logger = logging.getLogger(__name__)


# region @worker
# ---
# role: computation
# intent: execute a single flow/task
# description: >
#   A worker communicates with the job queue to acquire job ownership and
#   execute the job, that is the flow on given parameters. It thus handles
#   execution and orchestration with the job queue.
# rules:
#   - MUST acquire job ownership from the queue before execution to ensure
#     flows get executed at most once.
#   - MUST handle failures.
#   - MUST handle timeouts.
# dependencies:
# aliases:
# triggers:
# ---

def execute_job(
    queue: JobQueueProtocol,
    registry: Registry,
    timeout: int | None = None
) -> int:
    """
    Execute a single job from the queue and exit.

    Designed for Azure Queue triggers or similar event-driven execution models.
    Dequeues one message, executes it, and acknowledges or rejects based on outcome.

    Args:
        queue: Job queue to dequeue from.
        registry: Flow registry for retrieving flow functions.
        timeout: Optional visibility timeout override in seconds.

    Returns:
        int: Exit code (0 for success, 1 for failure, 2 for no jobs available).

    Example:
        >>> from flowlet import configure
        >>> flowlet = configure({"queue": {"type": "azure_queue", ...}})
        >>> exit_code = execute_job(flowlet.queue, flowlet.registry)
        >>> sys.exit(exit_code)
    """
    # Dequeue one job
    job = queue.dequeue(timeout=timeout)

    if job is None:
        logger.info("No jobs available in queue")
        return 2  # No jobs

    logger.info(
        f"Processing job {job.job_id} for flow '{job.flow_name}' "
        f"(run_id={job.run_id}, retry={job.retry_count}/{job.max_retries})"
    )

    try:
        # Execute the flow
        fn = registry.get_flow(job.flow_name)
        result = fn(**job.kwargs)
        logger.debug(f"Flow '{job.flow_name}' returned: {result}")

        # Acknowledge success - delete from queue
        queue.ack(job.job_id)
        logger.info(f"Job {job.job_id} completed successfully")
        return 0  # Success

    except Exception as e:
        logger.error(
            f"Job {job.job_id} failed with error: {type(e).__name__}: {e}",
            exc_info=True
        )

        # Reject and requeue with exponential backoff
        queue.nack(job.job_id, requeue=True)

        if job.retry_count + 1 < job.max_retries:
            backoff_seconds = 2 ** job.retry_count
            logger.info(
                f"Job {job.job_id} will retry in {backoff_seconds}s "
                f"(retry={job.retry_count + 1}/{job.max_retries})"
            )
        else:
            logger.warning(
                f"Job {job.job_id} exceeded max retries ({job.max_retries}), "
                "will be discarded"
            )

        return 1  # Failure

# ---
# endregion
