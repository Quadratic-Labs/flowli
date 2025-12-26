import logging
from typing import Any, Mapping, Protocol

from ..logging import FlowletLogBuffer, FlowletLogger
from .context import ContextManagerProtocol


class TrackerProtocol(Protocol):
    context_manager: ContextManagerProtocol
    logger: FlowletLogger
    bufferer: FlowletLogBuffer
    run_logger: FlowletLogger

    def flush_run(self, run_id: str, clear_buffer: bool = True) -> Mapping[str, Any] | None:
        """Summary log from run's logs"""
        ...