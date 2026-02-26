"""
Function instrumentation for flowlet flows and tasks.

Provides instrument(), which wraps a plain callable with span lifecycle
management and structured logging, turning it into an observable flow or task.
"""
import functools
import logging
from typing import Callable

from .context import ContextManager
from .logger import SUCCESS
from .models import SpanType

logger = logging.getLogger('flowlet.log')


# region @instrumentation
# ---
# role: core
# intent: wrap a plain function into an instrumented flow or task
# description: >
#   An instrumented function is one that opens a new run (span) via a context
#   manager at the beginning of execution, and emits start/success/error logs.
#   It is the single place where context and logging are combined to make a
#   callable observable.
# rules:
#   - MUST be stateless: no class, no mutable state.
#   - MUST reset the context span in a finally block.
#   - MUST NOT know about the registry or the job queue.
# dependencies:
#   - context
#   - logging
# aliases:
#   - executor
# triggers:
#   - how is a function turned into a flow
#   - what wraps a callable for execution
# ---

def instrument(fn: Callable, name: str, span_type: SpanType) -> Callable:
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        spans = ContextManager.append_new_span(name, span_type)
        child_ctx = spans[-1]
        # Emit a child-call reference while still in the parent's context so
        # the record is routed to the parent's log file.  Consumers can follow
        # parent → child by reading child_span_id from a parent's log entry.
        if ContextManager.is_active():
            logger.info(
                f"Calling child {span_type} '{name}'",
                extra={
                    "child_span_id": str(child_ctx.span_id),
                    "child_span_name": child_ctx.span_name,
                    "child_span_type": str(child_ctx.span_type),
                },
            )
        token = ContextManager.runs_stack.set(spans)
        logger.info(f"Starting {span_type} '{name}'")
        try:
            result = fn(*args, **kwargs)
            logger.log(SUCCESS, f"Completed {span_type} '{name}'")
            return result
        except Exception:
            logger.error(f"Failed {span_type} '{name}'", exc_info=True)
            raise
        finally:
            ContextManager.runs_stack.reset(token)
    return wrapper

# ---
# endregion
