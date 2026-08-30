"""
Factory Boy factories for the snapshot database ORM model.

The ObligationRow table is flat (no RunLink); hierarchy is derived from log files.
"""
from datetime import UTC, datetime
from uuid import uuid7

import factory

from flowlet.api.cache import ObligationRow
from flowlet.models import ReportedStatus


class ObligationRowFactory(factory.Factory):
    """Factory for the flat ObligationRow ORM snapshot row.

    Does NOT use SQLAlchemyModelFactory so it can be used without a DB session.
    Use ``session.add(ObligationRowFactory())`` explicitly when a live session is needed.
    """

    class Meta:
        model = ObligationRow

    run_id        = factory.LazyFunction(uuid7)
    flow_name     = factory.Sequence(lambda n: f"flow_{n}")
    status        = ReportedStatus.running.value
    worker_id     = "worker-1"
    started_at    = factory.LazyFunction(lambda: datetime.now(UTC))
    ended_at      = None
    attempt       = 1
    max_retries   = 3
