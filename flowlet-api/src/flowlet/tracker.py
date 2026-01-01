import logging
from typing import cast
from uuid import UUID

import attrs

from .interfaces.context import ContextManagerProtocol
from .interfaces.tracker import TrackerProtocol
from .logging import ContextInjectingFilter, FlowletLogBuffer, FlowletLogger
from .models import RunSummary, SpanLog
from .serdes import to_json
from .types import RunStatus


@attrs.define
class Tracker(TrackerProtocol):
    context_manager: ContextManagerProtocol
    logger: FlowletLogger
    bufferer: FlowletLogBuffer
    run_logger: FlowletLogger

    @classmethod
    def setup(cls, *, context_manager: ContextManagerProtocol, **_) -> Tracker:
        # Run Log handling
        logger = cast(FlowletLogger, logging.getLogger('flowlet.log'))
        logger.setLevel(logging.INFO)
        logger.propagate = True  # Allow propagation to root logger
        context_filter = ContextInjectingFilter(context_manager)
        logger.addFilter(context_filter)
        buffer_handler = FlowletLogBuffer()
        buffer_handler.setLevel(logging.INFO)
        # buffer_handler.setFormatter(json_formatter)
        logger.addHandler(buffer_handler)

        # Run Summaries handling
        run_logger = cast(FlowletLogger, logging.getLogger("flowlet.run"))
        run_logger.setLevel(logging.INFO)
        run_logger.propagate = True  # Allow propagation to root logger

        return cls(
            context_manager=context_manager,
            logger=logger,
            bufferer=buffer_handler,
            run_logger=run_logger,
        )

    @classmethod
    def summarise(cls, spans: list[SpanLog]) -> RunSummary:
        span_logs: dict[UUID, list[SpanLog]] = {}
        for span in spans:
            span_id = span.span_id
            if span_id:
                if span_id not in span_logs:
                    span_logs[span_id] = []
                span_logs[span_id].append(span)

        # Build span objects as RunSummary models
        info: dict[UUID, RunSummary] = {}
        flow_name = None
        for span_id, span_log_list in span_logs.items():
            span_log_list = sorted(span_log_list, key=lambda l: l.ts)
            start_log = span_log_list[0]
            end_log = span_log_list[-1]

            # Capture flow_name from the first span we process (typically root)
            if flow_name is None:
                flow_name = start_log.flow_name

            # Determine status from end_log's level
            status = RunStatus.from_log_level(end_log.level)

            span = RunSummary(
                span_id=span_id,
                span_name=start_log.span_name,
                status=status,
                start_ts=start_log.ts,
                end_ts=end_log.ts,
                children=[]
            )

            info[span_id] = span

        # Build hierarchy by linking parent-child relationships
        root_span = None
        for span_id, span in info.items():
            # Find parent span ID from logs
            parent_span_id = None
            for log in span_logs[span_id]:
                if log.parent_span_id:
                    parent_span_id = log.parent_span_id
                    break

            if parent_span_id and parent_span_id in info:
                info[parent_span_id].children.append(span)
            else:  # This is the root span
                root_span = span

        # Return root span or create an empty placeholder
        if root_span is None:
            raise ValueError("No root span")

        return root_span

    def flush_run(self, run_id: UUID, clear_buffer: bool = True) -> RunSummary | None:
        """
        Generate and write a run summary from buffered logs.

        Takes the buffered Flowlet logs for the specified run, compacts them into
        a hierarchical summary structure, and writes the result to runs/{run_id}.json.

        Args:
            run_id: The run ID to generate summary for
            clear_buffer: Whether to clear the buffer after emitting (default: True)

        Returns:
            The generated RunSummary model

        Raises:
            ValueError: If buffer handler is not initialized
        """
        spans = self.bufferer.get_run_logs(run_id)
        summary = self.summarise(spans)
        if clear_buffer:
            self.bufferer.clear_run_logs(run_id)
        self.run_logger.info(to_json(summary))
        return summary
