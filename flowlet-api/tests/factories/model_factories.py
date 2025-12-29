"""
Factory Boy factories for domain models (attrs-based).

These factories generate domain model objects for testing business logic
without database dependencies.
"""
import uuid
from datetime import UTC, datetime

import factory

from flowlet.models import (
    FlowRunSummary,
    FlowSummary,
    RunAttrModel,
    RunLogAttrModel,
    RunModel,
)


class RunAttrModelFactory(factory.Factory):
    """Factory for creating RunAttrModel instances."""

    class Meta:
        model = RunAttrModel

    name = factory.Sequence(lambda n: f"test_run_{n}")
    run_type = "flow"
    run_id = factory.LazyFunction(uuid.uuid4)


class RunLogAttrModelFactory(factory.Factory):
    """Factory for creating RunLogAttrModel instances."""

    class Meta:
        model = RunLogAttrModel

    run_id = factory.LazyFunction(uuid.uuid4)
    status = "running"
    log = ""
    timestamp = factory.LazyFunction(lambda: datetime.now(UTC))
    log_id = factory.LazyFunction(uuid.uuid4)


class FlowSummaryFactory(factory.Factory):
    """Factory for creating FlowSummary instances."""

    class Meta:
        model = FlowSummary

    name = factory.Sequence(lambda n: f"test_flow_{n}")
    status = "success"
    started_at = factory.LazyFunction(lambda: datetime.now(UTC))
    ended_at = factory.LazyFunction(lambda: datetime.now(UTC))
    doc = None


class FlowRunSummaryFactory(factory.Factory):
    """Factory for creating FlowRunSummary instances."""

    class Meta:
        model = FlowRunSummary

    name = factory.Sequence(lambda n: f"test_flow_{n}")
    run_id = factory.LazyFunction(uuid.uuid4)
    status = "success"
    ended_at = factory.LazyFunction(lambda: datetime.now(UTC))


class RunModelFactory(factory.Factory):
    """Factory for creating RunModel instances."""

    class Meta:
        model = RunModel

    run = factory.SubFactory(RunAttrModelFactory)
    logs = factory.List([])
    parent = None
    children = factory.List([])
