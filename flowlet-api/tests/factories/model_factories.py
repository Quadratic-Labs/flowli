"""
Factory Boy factories for domain models (attrs-based).

Generates domain model objects for testing without database dependencies.
"""
from datetime import UTC, datetime

import factory

from flowlet.models import RunLog, RunState, RunStatus, RunSummary, RunType
from uuid import uuid7
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
    span_type       = RunType.flow
    span_name       = factory.SelfAttribute("flow_name")
    span_id         = factory.LazyFunction(uuid7)
    parent_span_id  = None
    ts              = factory.SubFactory(TimestampFactory)
    message         = "log entry"
    level           = "INFO"
    extra           = factory.LazyFunction(dict)


class RunStateFactory(factory.Factory):
    """Factory for domain RunState instances."""

    class Meta:
        model = RunState

    run_id        = factory.LazyFunction(uuid7)
    flow_name     = factory.Sequence(lambda n: f"flow_{n}")
    status        = RunStatus.running
    worker_id     = "worker-1"
    started_at    = factory.SubFactory(TimestampFactory)
    ended_at      = None
    deadline_at   = None
    attempt       = 1
    max_retries   = 3


class RunSummaryFactory(factory.Factory):
    """Factory for domain RunSummary instances."""

    class Meta:
        model = RunSummary

    span_id    = factory.LazyFunction(uuid7)
    span_name  = factory.Sequence(lambda n: f"flow_{n}")
    span_type  = RunType.flow
    status     = RunStatus.completed
    start_ts   = factory.SubFactory(TimestampFactory)
    end_ts     = factory.SubFactory(TimestampFactory)
    children   = factory.LazyFunction(list)
