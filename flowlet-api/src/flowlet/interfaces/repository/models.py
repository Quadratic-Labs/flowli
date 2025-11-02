from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID, uuid4

from attrs import Factory, define, field


@define(slots=True, kw_only=True)
class FlowSummary:
    """Domain model for a flow runs' summary."""
    name: str
    last_status: str | None = field(default=None)
    finished_ago: timedelta | None = field(default=None)
    duration: timedelta | None = field(default=None)
    doc: str | None = field(default=None)


@define(slots=True, kw_only=True)
class RunAttrModel:
    """Unified model for both flow and task runs."""
    name: str
    run_type: str
    run_id: UUID = Factory(uuid4)


@define(slots=True, kw_only=True)
class RunLogAttrModel:
    """Domain model for a flow run."""
    run_id: UUID
    status: str
    log: str = field(default="")
    timestamp: datetime = Factory(lambda: datetime.now(UTC))
    log_id: UUID = Factory(uuid4)


@define(slots=True, kw_only=True)
class RunModel:
    run: RunAttrModel
    logs: list[RunLogAttrModel] = Factory(list)
    parent: RunAttrModel | None = field(default=None)
