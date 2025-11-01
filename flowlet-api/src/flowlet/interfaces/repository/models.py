from datetime import UTC, datetime, timedelta
from typing import Protocol, Self
from uuid import UUID, uuid4

from attrs import Factory, define, field
from sqlalchemy.orm import Session

from ...database import FlowRun as FlowRunORM, TaskRun as TaskRunORM


@define(slots=True)
class FlowSummary:
    """Domain model for a flow runs' summary."""
    name: str
    last_status: str | None = field(default=None)
    finished_ago: timedelta | None = field(default=None)
    duration: timedelta | None = field(default=None)
    doc: str | None = field(default=None)


@define(slots=True)
class FlowRun:
    """Domain model for a flow run."""
    run_id: UUID
    flow_name: str
    status: str
    started_at: datetime
    finished_at: datetime | None = field(default=None)
    error: str | None = field(default=None)

    @classmethod
    def from_orm(cls, orm: FlowRunORM) -> Self:
        """Create domain model from ORM object."""
        return cls(
            run_id=orm.run_id,
            flow_name=orm.flow_name,
            started_at=orm.started_at,
            finished_at=orm.finished_at,
            status=orm.status,
            error=orm.error,
        )


@define(slots=True)
class TaskRun:
    """Domain model for a task run."""
    run_id: UUID
    task_name: str
    flow_run_id: UUID
    flow_name: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    result: str | None
    error: str | None

    @classmethod
    def from_orm(cls, orm: TaskRunORM) -> Self:
        """Create domain model from ORM object."""
        return cls(
            run_id=orm.run_id,
            task_name=orm.task_name,
            flow_run_id=orm.flow_run_id,
            flow_name=orm.flow_name,
            started_at=orm.started_at,
            finished_at=orm.finished_at,
            status=orm.status,
            result=orm.result,
            error=orm.error,
        )


@define
class FlowRunLog:
    run_id: UUID
    flow_name: str
    status: str
    at: datetime = Factory(lambda: datetime.now(UTC))
    log: str = field(default="")
    log_id: UUID = Factory(uuid4)


@define
class TaskRunLog:
    run_id: UUID
    flow_name: str
    task_name: str
    status: str
    at: datetime = Factory(lambda: datetime.now(UTC))
    log: str = field(default="")
    log_id: UUID = Factory(uuid4)
