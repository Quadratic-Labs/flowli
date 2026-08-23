"""
OpenTelemetry-based instrumentation with a blob/file JSONL span exporter.

Flowlet spans are ordinary OTel spans produced by the OTel SDK; what makes
them Flowlet's is the sink: ``BlobSpanExporter`` writes finished spans as
JSON lines into the run folder

    ``runs/<flow_name>/<yyyy-mm-dd>/<run_id>/spans-<attempt>.jsonl``

which is the product's durable record (the API reads it back — see
``flowlet.repository.log``).  Standard OTLP/vendor exporters can be attached
*in addition* for observability backends; they are never the source of truth.

Identity model:
    - ``run_id`` (uuid7) **is** the OTel trace_id — both are 128 bits.  The
      run folder's date partition is derived from the uuid7 timestamp, so a
      retry executed days later still lands in the same folder.
    - span ids are native OTel 64-bit ids, serialised as 16-char hex.
    - every span carries ``flowlet.flow_name`` and ``flowlet.attempt``
      attributes (stamped from the ambient run context at creation) so the
      exporter can route spans without needing the root span in the same
      batch.

In-flight visibility is NOT provided by spans (they export on completion);
the RunState file is the source of truth for liveness.  Workers call
``force_flush()`` before their terminal CAS write so span loss is bounded
to hard crashes — which the state machine records anyway.
"""
import contextlib
import contextvars
import logging
import threading
import typing
from datetime import UTC, datetime
from uuid import UUID, uuid7

from cairndb.storage.base import BlobStorage
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import Status, StatusCode

from flowlet.models import RunType
from flowlet.storage import append_lines, run_prefix

logger = logging.getLogger(__name__)

FLOW_NAME_KEY = "flowlet.flow_name"
ATTEMPT_KEY = "flowlet.attempt"
SPAN_TYPE_KEY = "flowlet.span_type"


# Trace id to use for the next root span (worker/controller sets the run_id
# here); consumed once by FlowletIdGenerator.
_pending_trace_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "flowlet_pending_trace_id", default=None
)
# (flow_name, attempt) stamped onto every span created in this context.
_run_meta: contextvars.ContextVar[tuple[str, int] | None] = contextvars.ContextVar(
    "flowlet_run_meta", default=None
)

_provider: TracerProvider | None = None
_provider_lock = threading.Lock()


class FlowletIdGenerator(RandomIdGenerator):
    """Id generator whose trace ids are always uuid7 values.

    When a run identity is pending (``run_root`` was entered), the run_id is
    used verbatim; otherwise a fresh uuid7 is generated.  Either way every
    trace_id doubles as a valid run_id with an embedded timestamp, so the
    exporter can always derive the date partition.
    """

    def generate_trace_id(self) -> int:
        pending = _pending_trace_id.get()
        if pending is not None:
            _pending_trace_id.set(None)  # consume — children must not reuse it
            return pending
        return uuid7().int


class BlobSpanExporter(SpanExporter):
    """Span exporter appending JSON lines to per-run objects on the store.

    One code path for every backend: appends go through the storage
    helper's compare-and-swap loop.  Exports are infrequent (batch
    processor) and each run/attempt has a single writer, so contention is
    negligible.

    Attributes:
        store: CairnDB blob store the ``runs/`` tree is written to.
    """

    def __init__(self, store: BlobStorage):
        self.store = store
        self._lock = threading.Lock()

    def export(self, spans: typing.Sequence[ReadableSpan]) -> SpanExportResult:
        """Append finished spans to their run objects, grouped per run/attempt.

        Args:
            spans: Finished spans handed over by the batch processor.

        Returns:
            SUCCESS when every group was written; FAILURE otherwise (the
            batch processor drops the batch — the run's terminal status
            lives in the state object regardless).
        """
        groups: dict[tuple[str, UUID, int], list[str]] = {}
        for span in spans:
            try:
                record = _span_to_record(span)
                key = (record["flow_name"], UUID(record["run_id"]), record["attempt"])
                groups.setdefault(key, []).append(_dumps(record))
            except Exception:
                logger.exception("span_serialization_failed", extra={"span": span.name})

        ok = True
        with self._lock:
            for (flow_name, run_id, attempt), lines in groups.items():
                key = f"{run_prefix(flow_name, run_id)}/spans-{attempt}.jsonl"
                if not append_lines(self.store, key, "\n".join(lines) + "\n"):
                    ok = False
                    logger.error(
                        "span_export_failed",
                        extra={"flow_name": flow_name, "run_id": str(run_id)},
                    )
        return SpanExportResult.SUCCESS if ok else SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Nothing to release — files are opened per batch."""

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Nothing buffered at the exporter level."""
        return True


def configure_tracing(
    store: BlobStorage,
    extra_exporters: typing.Sequence[SpanExporter] = (),
) -> TracerProvider:
    """Create (or replace) the Flowlet tracer provider.

    The provider is module-managed rather than installed as the OTel global,
    so repeated calls (tests, reconfiguration) simply swap it.

    Args:
        store: CairnDB blob store; spans land under ``runs/``.
        extra_exporters: Optional additional sinks (e.g. OTLP) attached
            alongside the mandatory blob exporter.

    Returns:
        The active TracerProvider.
    """
    global _provider
    with _provider_lock:
        if _provider is not None:
            _provider.shutdown()
        provider = TracerProvider(id_generator=FlowletIdGenerator())
        provider.add_span_processor(BatchSpanProcessor(BlobSpanExporter(store)))
        for exporter in extra_exporters:
            provider.add_span_processor(BatchSpanProcessor(exporter))
        _provider = provider
    return provider


def force_flush(timeout_millis: int = 10000) -> None:
    """Flush buffered spans to the exporters; call before terminal CAS writes."""
    provider = _provider
    if provider is not None:
        provider.force_flush(timeout_millis)


def _get_tracer() -> trace.Tracer:
    provider = _provider
    if provider is None:
        return trace.NoOpTracer()
    return provider.get_tracer("flowlet")


@contextlib.contextmanager
def run_root(run_id: UUID, flow_name: str, attempt: int = 1) -> typing.Iterator[None]:
    """Bind a run's identity for the duration of its root execution.

    The next root span created inside (by the instrumented flow function)
    adopts ``run_id`` as its trace_id, and every span in the context carries
    the flow name and attempt as attributes.

    Args:
        run_id: The run's UUID (uuid7) — becomes the trace_id.
        flow_name: Root flow name, used for the storage path.
        attempt: Execution attempt number (1-based).
    """
    id_token = _pending_trace_id.set(int(run_id))
    meta_token = _run_meta.set((flow_name, attempt))
    try:
        yield
    finally:
        _pending_trace_id.reset(id_token)
        _run_meta.reset(meta_token)


def instrument(fn: typing.Callable, name: str, span_type: RunType) -> typing.Callable:
    """Wrap a callable so every invocation records an OTel span.

    Replaces the legacy custom context stack: nesting, ids, and thread/async
    propagation are the OTel SDK's job now.  When no ambient run context
    exists (direct call outside a worker), the wrapper establishes one so
    the span still lands in a valid run folder.

    Args:
        fn: The flow or task function to wrap.
        name: Registered flow/task name (becomes the span name).
        span_type: Whether this is a flow or a task span.

    Returns:
        The wrapped callable.
    """
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        meta = _run_meta.get()
        cm = contextlib.nullcontext()
        if meta is None:
            # Direct invocation (no worker/controller context): this call is
            # its own run — flow_name is this span's name, attempt is 1.
            cm = run_root(uuid7(), name, 1)
            meta = (name, 1)
        flow_name, attempt = meta
        with cm:
            tracer = _get_tracer()
            with tracer.start_as_current_span(
                name,
                attributes={
                    FLOW_NAME_KEY: flow_name,
                    ATTEMPT_KEY: attempt,
                    SPAN_TYPE_KEY: str(span_type),
                },
                record_exception=True,
                set_status_on_exception=True,
            ) as span:
                result = fn(*args, **kwargs)
                span.set_status(Status(StatusCode.OK))
                return result

    return wrapper


class SpanEventHandler(logging.Handler):
    """Logging handler that turns log records into events on the current span.

    Attach to any logger (typically ``flowlet.log``) so user logging inside
    flows is captured in the run record without a separate file pipeline.
    """

    def emit(self, record: logging.LogRecord) -> None:
        span = trace.get_current_span()
        if span is None or not span.is_recording():
            return
        try:
            span.add_event(
                record.getMessage(),
                attributes={
                    "log.level": record.levelname,
                    "log.logger": record.name,
                },
            )
        except Exception:
            self.handleError(record)


def configure_run_logging() -> None:
    """Route the ``flowlet.log`` logger into span events.

    Call once at application startup (idempotent).
    """
    log = logging.getLogger("flowlet.log")
    log.setLevel(logging.INFO)
    if not any(isinstance(h, SpanEventHandler) for h in log.handlers):
        log.addHandler(SpanEventHandler())


def _span_to_record(span: ReadableSpan) -> dict:
    """Convert a finished OTel span to the Flowlet JSONL record schema."""
    ctx = span.get_span_context()
    attributes = dict(span.attributes or {})
    flow_name = attributes.pop(FLOW_NAME_KEY, None) or span.name
    attempt = int(attributes.pop(ATTEMPT_KEY, 1))
    span_type = attributes.pop(SPAN_TYPE_KEY, str(RunType.task))

    if span.status.status_code is StatusCode.ERROR:
        status = "failed"
    else:
        status = "completed"

    return {
        "run_id": str(UUID(int=ctx.trace_id)),
        "span_id": format(ctx.span_id, "016x"),
        "parent_span_id": (
            format(span.parent.span_id, "016x") if span.parent is not None else None
        ),
        "name": span.name,
        "flow_name": flow_name,
        "attempt": attempt,
        "span_type": span_type,
        "status": status,
        "status_message": span.status.description,
        "start_ts": _ns_to_iso(span.start_time),
        "end_ts": _ns_to_iso(span.end_time),
        "events": [
            {
                "ts": _ns_to_iso(event.timestamp),
                "message": event.name,
                "attributes": dict(event.attributes or {}),
            }
            for event in span.events
        ],
        "attributes": attributes,
    }


def _ns_to_iso(ns: int | None) -> str | None:
    if ns is None:
        return None
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).isoformat()


def _dumps(record: dict) -> str:
    import json

    return json.dumps(record, default=str)
