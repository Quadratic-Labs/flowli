from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID, uuid4

from attrs import Factory, define, field


@define(slots=True, kw_only=True)
class FlowSummary:
    """
    A flow's runs' summary. Used to view many flows.

    Attributes:
        name: FLow's name.
        status: Status of the recent flow's run(s).
        started_at: Datetime at which the latest flow's run started.
        ended_at: Datetime at which the latest flow's run ended.
    """
    name: str
    status: str | None = field(default=None)
    started_at: datetime | None = field(default=None)
    ended_at: datetime | None = field(default=None)
    doc: str | None = field(default=None)

    @property
    def duration(self) -> timedelta | None:
        """Duration of the latest flow run."""
        if self.started_at is None or self.ended_at is None:
            return None
        return self.ended_at - self.started_at 


@define(slots=True, kw_only=True)
class FlowRunSummary:
    """
    A flow run's summary. Used to view many runs for a flow.

    Attributes:
        name: flow's name.
        run_id: flow run's id.
        status: flow run's status.
        started_at: datetime at which the run started.
        ended_at: datetime at which the run ended.
    """
    name: str
    run_id: UUID = Factory(uuid4)
    status: str | None = field(default=None)
    ended_at: datetime | None = field(default=None)


@define(slots=True, kw_only=True)
class RunAttrModel:
    """A run's identifiers"""
    name: str
    run_type: str
    run_id: UUID = Factory(uuid4)


@define(slots=True, kw_only=True)
class RunLogAttrModel:
    """A run's log record"""
    run_id: UUID
    status: str
    log: str = field(default="")
    timestamp: datetime = Factory(lambda: datetime.now(UTC))
    log_id: UUID = Factory(uuid4)


@define(slots=True, kw_only=True)
class RunModel:
    """Run's full record."""
    run: RunAttrModel
    logs: list[RunLogAttrModel] = Factory(list)
    parent: RunAttrModel | None = field(default=None)
    children: list[Self]= Factory(list)