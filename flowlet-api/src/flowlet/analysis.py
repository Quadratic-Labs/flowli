"""
Run analysis: derives structured summaries from recorded spans.
"""
from flowlet.models import RunSummary, SpanRecord

def summarise(spans: list[SpanRecord]) -> RunSummary:
    """Build the run's summary tree from its recorded spans.

    Args:
        spans: All SpanRecords of a run, any order, possibly spanning
            multiple attempts.

    Returns:
        The RunSummary of the latest attempt's root span, with children
        nested recursively.

    Raises:
        ValueError: When *spans* is empty or contains no root span.
    """
    if not spans:
        raise ValueError("No spans to summarise")

    latest_attempt = max(span.attempt for span in spans)
    attempt_spans = [span for span in spans if span.attempt == latest_attempt]

    summaries: dict[str, RunSummary] = {
        span.span_id: RunSummary(
            span_id=span.span_id,
            span_name=span.name,
            span_type=span.span_type,
            status=span.status,
            start_ts=span.start_ts,
            end_ts=span.end_ts,
            children=[],
        )
        for span in attempt_spans
    }

    root: RunSummary | None = None
    for span in attempt_spans:
        parent_id = span.parent_span_id
        if parent_id is not None and parent_id in summaries:
            summaries[parent_id].children.append(summaries[span.span_id])
        else:
            root = summaries[span.span_id]

    if root is None:
        raise ValueError("No root span")
    return root
