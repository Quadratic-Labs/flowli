"""
Database models and settings for Flowlet flow run tracking.

Flat single-table snapshot of worker-owned RunState objects.  The hierarchy
(RunSummary tree) is computed on-demand from logs by the API — not
materialised in SQLite.
"""
import functools
import logging
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import DateTime, String, Uuid, create_engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from ..models import RunState

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
# intent: database model definition — flat single-table snapshot of RunState
# description: >
#   A single Run table stores one row per run.  No parent-child links are
#   materialised; hierarchy is derived on-demand from log traces.
# rules:
#   - RunLink is removed; hierarchy MUST NOT be stored in SQLite.
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
    """Flat snapshot row for a single flow run.

    One row per run; updated via upsert on every state transition.
    Hierarchy is NOT stored here — query logs instead.

    Attributes:
        run_id: Unique identifier (UUIDv7 — chronologically ordered, the
            largest run_id is the most recent run).
        flow_name: Name of the flow being executed.
        status: Current execution status string.
        worker_id: Identifier of the owning worker.
        started_at: Wall-clock time when the run started.
        ended_at: Wall-clock time when the run finished (NULL if ongoing).
        attempt: Current attempt number (1-based).
        max_retries: Maximum allowed retries.
    """
    __tablename__ = "runs"

    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    flow_name: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[str] = mapped_column(String, index=True)
    worker_id: Mapped[str | None] = mapped_column(String, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt: Mapped[int] = mapped_column(default=1)
    max_retries: Mapped[int] = mapped_column(default=3)

# ---
# endregion


# region @database.snapshot
# ---
# role: storage
# intent: pure async functions for building and updating SQLite run-history snapshots
# description: >
#   Source of truth are state files written to blob or file storage.
#   SQLite snapshots are derived materialised views of those state files for
#   fast querying.
# rules:
#   - MUST be pure functions of their arguments — no storage I/O of any kind.
#   - MUST NOT discover or fetch state files; states are passed in as arguments.
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


async def upsert_run_state(session: AsyncSession, state: RunState) -> None:
    """Upsert a single RunState into the flat runs table.

    Uses SQLite's ``ON CONFLICT DO UPDATE`` so re-processing an existing
    run_id updates the row instead of raising.

    Args:
        session: Active async SQLAlchemy session for the snapshot DB.
        state: RunState to persist.
    """
    started_at = state.started_at.value
    ended_at = state.ended_at.value if state.ended_at is not None else None

    await session.execute(
        sqlite_insert(Run).values(
            run_id=state.run_id,
            flow_name=state.flow_name,
            status=state.status.value,
            worker_id=state.worker_id,
            started_at=started_at,
            ended_at=ended_at,
            attempt=state.attempt,
            max_retries=state.max_retries,
        ).on_conflict_do_update(
            index_elements=[Run.run_id],
            set_=dict(
                flow_name=state.flow_name,
                status=state.status.value,
                worker_id=state.worker_id,
                ended_at=ended_at,
                attempt=state.attempt,
            ),
        )
    )


# ---
# endregion
