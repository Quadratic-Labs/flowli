from typing import Protocol
from uuid import UUID

from ..logging import FlowletLogBuffer, FlowletLogger
from ..models import RunSummary, SpanLog
from .context import ContextManagerProtocol


class TrackerProtocol(Protocol):
    context_manager: ContextManagerProtocol
    logger: FlowletLogger
    bufferer: FlowletLogBuffer
    run_logger: FlowletLogger

    @classmethod
    def summarise(cls, spans: list[SpanLog]) -> RunSummary:
        """Summaries a run's logs."""
        ...

    def flush_run(self, run_id: UUID, clear_buffer: bool = True) -> RunSummary | None:
        """Summary log from run's logs"""
        ...