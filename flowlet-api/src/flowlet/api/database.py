"""
Database models and settings for Flowlet flow run tracking.

This module defines SQLAlchemy ORM models for storing flow and task execution
history. It includes only aggregated summaries of runs for fast retrieval,
detailed logs stored aside.
"""
import functools
from datetime import datetime
import logging
from uuid import UUID, uuid7

from attrs import asdict
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from sqlalchemy import func, select, SmallInteger, create_engine
from sqlalchemy import DateTime, String, Uuid
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..models import SpanType, RunSummary
from ..types import PeriodUUID

logger = logging.getLogger(__name__)


# region @database.settings
# ---
# role: storage
# intent: database configurations
# description:
# rules:
# dependencies:
# aliases:
# triggers:
# ---

class DatabaseSettings(BaseSettings):
    """Database configuration for Flowlet.

    Manages database connection settings and provides SQLAlchemy engine and
    session factory. Can be loaded from environment variables with
    FLOWLET_DATABASE_ prefix or instantiated directly.

    Attributes:
        url: SQLAlchemy database URL (e.g., "postgresql://localhost/mydb").

    Example:
        >>> # From environment variable FLOWLET_DATABASE_URL
        >>> settings = DatabaseSettings()
        >>>
        >>> # Direct instantiation
        >>> settings = DatabaseSettings(url="sqlite:///flows.db")
    """
    model_config = SettingsConfigDict(env_prefix='FLOWLET_DATABASE_')
    url: str

    @functools.cached_property
    def engine(self):
        """SQLAlchemy engine for database connections.

        Returns:
            Engine: Cached SQLAlchemy engine instance.
        """
        return create_engine(self.url)

    @functools.cached_property
    def db_session_factory(self):
        """SQLAlchemy session factory for creating database sessions.

        Returns:
            sessionmaker: Configured session factory with autoflush and autocommit disabled.
        """
        return sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

# ---
# endregion


# region @database.models
# ---
# role: storage
# intent: database model definition
# description:
# rules:
# dependencies:
# aliases:
# triggers:
# ---

class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models.

    Provides utility methods for creating and updating instances from
    Pydantic models or attrs dataclasses.
    """
    @classmethod
    def from_pydantic(cls, data: BaseModel):
        """Create an ORM instance from a Pydantic model.

        Args:
            data: Pydantic model instance to convert.

        Returns:
            ORM model instance.
        """
        return cls(**data.model_dump())

    @classmethod
    def from_attrs(cls, data):
        """Create an ORM instance from an attrs dataclass.

        Args:
            data: Attrs dataclass instance to convert.

        Returns:
            ORM model instance.
        """
        return cls(**asdict(data))

    def update_from_pydantic(self, data):
        """Update this instance from a Pydantic model.

        Args:
            data: Pydantic model with updated values.

        Returns:
            Self for method chaining.

        Raises:
            AttributeError: If data contains attributes not in the model.
        """
        for key, value in data.model_dump(exclude_unset=True).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def update_from_attrs(self, data):
        """Update this instance from an attrs dataclass.

        Args:
            data: Attrs dataclass with updated values.

        Returns:
            Self for method chaining.

        Raises:
            AttributeError: If data contains attributes not in the model.
        """
        for key, value in asdict(data).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def __repr__(self):
        """String representation showing all column values."""
        params = ", ".join(f"{k}={v}" for k, v in self.to_dict().items())
        return f"{self.__class__.__name__}({params})"

    def to_dict(self):
        """Convert instance to dictionary of column values.

        Returns:
            dict: Mapping of column names to values.
        """
        return {str(k): getattr(self, k) for k in self.__table__.columns.keys()}


class Run(Base):
    """
    Database model for a flow or task execution run (current status).

    Tracks individual executions of flows and tasks with unique identifiers.
    Forms a hierarchical structure where task runs are children of flow runs.

    Attributes:
        run_id: Unique identifier for this run.
        run_type: Type of run, either "flow" or "task".
        name: Name of the flow or task being executed.
        logs: Append-only log entries for this run.
        links: Links where this run is the parent.
        parent_links: Links where this run is the child.
        children: Child runs (tasks within a flow).
        parent: Parent run (flow containing this task).
    """
    __tablename__ = "runs"
    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    run_type: Mapped[str] = mapped_column(String, index=True)  # "flow" or "task"
    name: Mapped[str] = mapped_column(String, index=True)
    start_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String)

    # Relationships
    children: Mapped[list["Run"]] = relationship(
        "Run",
        secondary="run_links",
        primaryjoin="Run.run_id == RunLink.parent_run_id",
        secondaryjoin="Run.run_id == RunLink.child_run_id",
        foreign_keys="[RunLink.parent_run_id, RunLink.child_run_id]",
        viewonly=True
    )
    parent: Mapped["Run"] = relationship(
        "Run",
        secondary="run_links",
        primaryjoin="Run.run_id == RunLink.child_run_id",
        secondaryjoin="Run.run_id == RunLink.parent_run_id",
        foreign_keys="[RunLink.parent_run_id, RunLink.child_run_id]",
        viewonly=True
    )


class RunLink(Base):
    """Database model for parent-child relationships between runs.

    Creates hierarchical structure linking task runs to their parent flow runs.
    Enables querying the execution tree and understanding task context.

    Attributes:
        link_id: Unique identifier for this link.
        parent_run_id: Foreign key to the parent run (typically a flow).
        child_run_id: Foreign key to the child run (typically a task).
        parent_run: Relationship to the parent Run.
        child_run: Relationship to the child Run.
    """
    __tablename__ = "run_links"
    link_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    parent_run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)
    child_run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)
    depth: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)

    parent_run: Mapped[Run] = relationship("Run", foreign_keys=[parent_run_id])
    child_run: Mapped[Run] = relationship("Run", foreign_keys=[child_run_id])


# ---
# endregion


# region @database.snapshot
# ---
# role: storage
# intent: pure async functions for building and updating SQLite run-history snapshots
# description: >
#   Source of truth are log traces written to blob or file storage.
#   SQLite snapshots are derived materialised views of those traces for fast
#   querying.
# rules:
#   - MUST be pure functions of their arguments — no storage I/O of any kind.
#   - MUST NOT discover or fetch log files; logs are passed in as arguments.
#   - MUST NOT store state derivable from existing rows (normal form — query, don't duplicate).
# dependencies:
#   - types.time
#   - models.run
#   - database.models
# ---


async def ensure_snapshot_schema(engine: AsyncEngine) -> None:
    """Create all ORM-defined tables in the snapshot SQLite file if not present.

    Uses ``Base.metadata.create_all`` so the schema stays in sync with the
    ORM models automatically — no manual DDL strings needed.

    Args:
        engine: Async SQLAlchemy engine bound to the snapshot SQLite file.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_snapshot_period(session: AsyncSession) -> PeriodUUID:
    """Return the period (min/max run_id) covered by this snapshot.

    Both fields are ``None`` when the snapshot is empty.  Derived on demand
    from MIN/MAX over the indexed primary key — no metadata table needed
    (normal form).

    Args:
        session: Active async SQLAlchemy session for the snapshot DB.
    """
    result = await session.execute(select(func.min(Run.run_id), func.max(Run.run_id)))
    start, end = result.one()
    return PeriodUUID(
        start=UUID(str(start)) if start else None,
        end=UUID(str(end)) if end else None,
    )


async def _upsert_run_tree(
    session: AsyncSession,
    summary: RunSummary,
    parent_id: UUID | None = None,
    depth: int = 0,
) -> None:
    """Recursively upsert a RunSummary tree into runs and run_links.

    Uses SQLite's ``ON CONFLICT DO UPDATE`` so re-processing an existing
    run_id updates the row instead of raising an error.
    """
    if summary.span_type == SpanType.task:
        return
    start_ts = summary.start_ts
    if start_ts is not None:
        start_ts = start_ts.value
    end_ts = summary.end_ts
    if end_ts is not None:
        end_ts = end_ts.value
    await session.execute(
        sqlite_insert(Run).values(
            run_id=summary.span_id,
            run_type=summary.span_type,
            name=summary.span_name,
            start_ts=start_ts,
            end_ts=end_ts,
            status=summary.status.value,
        ).on_conflict_do_update(
            index_elements=[Run.run_id],
            set_=dict(
                name=summary.span_name,
                start_ts=start_ts,
                end_ts=end_ts,
                status=summary.status.value,
            ),
        )
    )
    if parent_id is not None:
        await session.execute(
            sqlite_insert(RunLink).values(
                link_id=uuid7(),
                parent_run_id=parent_id,
                child_run_id=summary.span_id,
                depth=depth,
            ).on_conflict_do_nothing()
        )
    for child in summary.children:
        await _upsert_run_tree(session, child, summary.span_id, depth + 1)


async def snapshot_runs(
    session: AsyncSession,
    summaries: list[RunSummary],
) -> None:
    """
    Pure snapshot function: upsert pre-summarised run trees into the DB.

    Callers are responsible for converting raw logs to RunSummary objects
    (via ``analysis.summarise``) before calling this function, keeping the
    storage layer free of domain logic.

    Args:
        session: Active async SQLAlchemy session for the snapshot DB.
        summaries: Already-summarised run trees to persist.
    """
    if not summaries:
        return

    for summary in summaries:
        try:
            await _upsert_run_tree(session, summary)
        except Exception:
            logger.exception("snapshot_run_failed", extra={"run_id": str(summary.span_id)})

    await session.commit()


async def insert_new_summaries(
    session: AsyncSession,
    summaries: dict[UUID, RunSummary],
) -> int:
    """
    Incremental insert: skip run_ids already present in the snapshot.

    Args:
        session: Active async SQLAlchemy session for the snapshot DB.
        summaries: Candidate runs, keyed by flow run_id (UUIDv7).

    Returns:
        Number of runs newly inserted.
    """
    if not summaries:
        return 0

    result = await session.execute(
        select(Run.run_id).where(Run.run_id.in_(list(summaries.keys())))
    )
    existing = set(result.scalars().all())
    new_summaries = [s for rid, s in summaries.items() if rid not in existing]
    await snapshot_runs(session, new_summaries)
    return len(new_summaries)

# ---
# endregion
