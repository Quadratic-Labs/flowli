"""
Factory Boy factories for domain models (attrs-based).

Generates domain model objects for testing without database dependencies.
"""
from datetime import UTC, datetime
from uuid import uuid7

import factory

from flowlet.models import ObligationSummary, ReportedStatus, RunLog, TraceSummary
from flowlet.types import Timestamp


class TimestampFactory(factory.Factory):
    """Factory for creating Timestamp instances."""

    class Meta:
        model = Timestamp

    value = factory.LazyFunction(lambda: datetime.now(UTC))


class RunLogFactory(factory.Factory):
    """Factory for domain RunLog instances."""

    class Meta:
        model = RunLog

    flow_name       = factory.Sequence(lambda n: f"flow_{n}")
    run_id          = factory.LazyFunction(uuid7)
    span_type       = "flow"
    span_name       = factory.SelfAttribute("flow_name")
    span_id         = factory.LazyFunction(uuid7)
    parent_span_id  = None
    ts              = factory.SubFactory(TimestampFactory)
    message         = "log entry"
    level           = "INFO"
    extra           = factory.LazyFunction(dict)


class ObligationSummaryFactory(factory.Factory):
    """Factory for domain ObligationSummary instances."""

    class Meta:
        model = ObligationSummary

    run_id        = factory.LazyFunction(uuid7)
    flow_name     = factory.Sequence(lambda n: f"flow_{n}")
    status        = ReportedStatus.running
    worker_id     = "worker-1"
    started_at    = factory.SubFactory(TimestampFactory)
    ended_at      = None
    attempt       = 1
    max_retries   = 3


class TraceSummaryFactory(factory.Factory):
    """Factory for domain TraceSummary instances."""

    class Meta:
        model = TraceSummary

    span_id    = factory.LazyFunction(uuid7)
    span_name  = factory.Sequence(lambda n: f"flow_{n}")
    span_type  = "flow"
    status     = ReportedStatus.completed
    start_ts   = factory.SubFactory(TimestampFactory)
    end_ts     = factory.SubFactory(TimestampFactory)
    children   = factory.LazyFunction(list)
