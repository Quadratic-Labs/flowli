"""
Run analysis: derives structured summaries from raw log events.
"""
from uuid import UUID

from .models import RunLog, RunStatus, RunSummary


# region @analysis
# ---
# role: computation
# intent: derive RunSummary from a flat list of RunLogs
# description: >
#   The current run's status can be recovered from its list of RunLog events.
#   In particular, status changes are recorded as any other event type.
# rules:
#   - SHOULD be a pure function.
# dependencies:
#   - models.run
# aliases:
# triggers:
#   - compute the run status for run id <id>.
# ---

def summarise(spans: list[RunLog]) -> RunSummary:
    """
    Create RunSummary
    """
    span_logs: dict[UUID, list[RunLog]] = {}
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

        if flow_name is None:
            flow_name = start_log.flow_name

        status = RunStatus.from_log_level(end_log.level)

        span = RunSummary(
            span_id=span_id,
            span_name=start_log.span_name,
            span_type=start_log.span_type,
            status=status,
            start_ts=start_log.ts,
            end_ts=end_log.ts,
            children=[]
        )

        info[span_id] = span

    # Build hierarchy by linking parent-child relationships
    root_span = None
    for span_id, span in info.items():
        parent_span_id = None
        for log in span_logs[span_id]:
            if log.parent_span_id:
                parent_span_id = log.parent_span_id
                break

        if parent_span_id and parent_span_id in info:
            info[parent_span_id].children.append(span)
        else:
            root_span = span

    if root_span is None:
        raise ValueError("No root span")
    return root_span

# ---
# endregion
