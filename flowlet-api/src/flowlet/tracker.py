import json
import logging
from datetime import datetime
from typing import Any, TypedDict, cast
from uuid import UUID

from attrs import define
import isodate

from .interfaces.context import ContextManagerProtocol
from .interfaces.tracker import TrackerProtocol
from .logging import JSONFormatter, ContextInjectingFilter, FlowletLogBuffer
from .types import FlowType, RunStatus


class RunSummary(TypedDict):
    run_id: str
    flow_name: str
    span: SpanSummary


class SpanSummary(TypedDict):
    span_id: str
    span_type: FlowType
    span_name: str
    status: RunStatus
    start_ts: str
    end_ts: str
    duration: str
    children: list[SpanSummary]


@define
class Tracker(TrackerProtocol):
    context_manager: ContextManagerProtocol
    logger: logging.Logger
    bufferer: FlowletLogBuffer
    run_logger: logging.Logger

    @classmethod
    def setup(cls, *, context_manager: ContextManagerProtocol, **_) -> Tracker:
        logger = logging.getLogger('flowlet')
        logger.propagate = True  # Allow propagation to root logger
        context_filter = ContextInjectingFilter(context_manager)
        logger.addFilter(context_filter)

        json_formatter = JSONFormatter()
        buffer_handler = FlowletLogBuffer()
        buffer_handler.setFormatter(json_formatter)
        logger.addHandler(buffer_handler)

        run_logger = logging.getLogger("flowlet-run")
        run_logger.propagate = True  # Allow propagation to root logger

        return cls(
            context_manager=context_manager, 
            logger=logger, 
            bufferer=buffer_handler,
            run_logger=run_logger,
        )

    @classmethod
    def summarise(cls, logs, run_id) -> RunSummary:
        span_logs: dict[str, list[dict[str, Any]]] = {}
        for log in logs:
            span_id = log.get('span_id')
            if span_id:
                if span_id not in span_logs:
                    span_logs[span_id] = []
                span_logs[span_id].append(log)

        # Build span objects
        spans: dict[str, dict[str, Any]] = {}
        flow_name = None
        for span_id, span_log_list in span_logs.items():
            span_log_list = sorted(span_log_list, key=lambda l: datetime.fromisoformat(l["ts"]))
            start_log = span_log_list[0]
            end_log = span_log_list[-1]

            # Capture flow_name from the first span we process (typically root)
            if flow_name is None:
                flow_name = start_log.get('flow', '')

            # Calculate duration if we have both start and end
            duration_str = ""
            if start_log.get('ts') and end_log.get('ts'):
                start_dt = datetime.fromisoformat(start_log['ts'])
                end_dt = datetime.fromisoformat(end_log['ts'])
                duration = end_dt - start_dt
                duration_str = isodate.duration_isoformat(duration)

            span = {
                "span_id": span_id,
                "span_type": start_log.get('span_type'),
                "span_name": start_log.get('name'),
                "status": end_log.get('status', 'running'),
                "start_ts": start_log.get('ts', ''),
                "end_ts": end_log.get('ts', '') if end_log else '',
                "duration": duration_str,
                "children": []
            }

            spans[span_id] = span

        # Build hierarchy by linking parent-child relationships
        root_span = None
        for span_id, span in spans.items():
            # Find parent span ID from logs
            parent_span_id = None
            for log in span_logs[span_id]:
                if log.get('parent_span_id'):
                    parent_span_id = log['parent_span_id']
                    break

            if parent_span_id and parent_span_id in spans:
                spans[parent_span_id]["children"].append(span)
            else:  # This is the root span
                root_span = span

        # Build final summary structure
        summary = {
            "run_id": run_id,
            "flow_name": flow_name or '',
            "span": root_span if root_span else {}
        }
        return cast(RunSummary, summary)

    def flush_run(self, run_id: str, clear_buffer: bool = True) -> RunSummary | None:
        """
        Generate and write a run summary from buffered logs.

        Takes the buffered Flowlet logs for the specified run, compacts them into
        a hierarchical summary structure, and writes the result to runs/{run_id}.json.

        Args:
            run_id: The run ID to generate summary for
            clear_buffer: Whether to clear the buffer after emitting (default: True)

        Returns:
            The generated summary dictionary

        Raises:
            ValueError: If buffer handler is not initialized
        """
        root = self.context_manager.get_root_span()
        if root is None:
            return None
        logs = self.bufferer.get_run_logs(str(root.run_id))
        summary = self.summarise(logs, root.run_id)
        if clear_buffer:
            self.bufferer.clear_run_logs(str(root.run_id))
        self.run_logger.info(json.dumps(summary))
        return summary
