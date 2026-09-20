"""Structured logging for flowlet, on structlog (which CairnDB uses too).

Every runtime component logs events with key-value fields, never formatted strings:

    log.info("execution_completed", eid=eid, epoch=2, worker_id="w-1")

Context variables carry `worker_id`, `task_id`, `eid`, `epoch`, `fid`, `attempt` through
every event emitted while that work is in flight, so a line can be correlated without
repeating the fields at each call site.

Call `configure_logging()` once per process. The CLI does it. A library user who does not
call it gets structlog's defaults, which print to stdout at every level.

The chain holds one processor of its own, `evidence.capture`: it keeps each event as
evidence of the attempt that is running, and does nothing when none is (see
`flowlet.evidence`).
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal

import structlog

LogFormat = Literal["console", "json"]


class _StderrLogger:
    """Writes to whatever sys.stderr is at call time, so redirection and capture work."""

    def msg(self, message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    log = debug = info = warning = error = critical = exception = fatal = msg


def _stderr_factory(*args: Any) -> _StderrLogger:
    return _StderrLogger()


def configure_logging(level: str = "INFO", fmt: LogFormat = "console") -> None:  # pragma: no mutate
    """Configure structlog for this process. Shared with CairnDB's loggers."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    renderer: Any = (
        structlog.processors.JSONRenderer(sort_keys=True)
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    from flowlet.evidence import capture

    # fmt is compared case-insensitively to "iso" by TimeStamper, and utc=True is
    # already its default -- no mutation of this call is observable.
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)  # pragma: no mutate
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Last before the renderer, so the event it keeps carries the level,
            # the timestamp and the bound context.
            capture,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric),
        logger_factory=_stderr_factory,
        cache_logger_on_first_use=False,
    )
    # stdlib loggers (asyncio, aiosqlite, ...) follow the same level
    logging.basicConfig(level=numeric, format="%(levelname)s %(name)s: %(message)s", force=True)


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)  # pragma: no mutate


@contextmanager
def bound(**fields: Any) -> Iterator[None]:
    """Bind fields to every event emitted inside the block, in this task's context."""
    with structlog.contextvars.bound_contextvars(**fields):
        yield


__all__ = ["LogFormat", "bound", "configure_logging", "get_logger"]
