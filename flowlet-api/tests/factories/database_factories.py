"""
Factory Boy factories for the snapshot database ORM model.

The Run table is flat (no RunLink); hierarchy is derived from log files.
"""
from datetime import UTC, datetime

import factory

from flowlet.api.database import Run
from flowlet.models import RunStatus
from uuid import uuid7


class RunOrmFactory(factory.Factory):
    """Factory for the flat Run ORM snapshot row.

    Does NOT use SQLAlchemyModelFactory so it can be used without a DB session.
    Use ``session.add(RunOrmFactory())`` explicitly when a live session is needed.
    """

    class Meta:
        model = Run

    run_id        = factory.LazyFunction(uuid7)
    flow_name     = factory.Sequence(lambda n: f"flow_{n}")
    status        = RunStatus.running.value
    worker_id     = "worker-1"
    started_at    = factory.LazyFunction(lambda: datetime.now(UTC))
    ended_at      = None
    attempt       = 1
    max_retries   = 3
