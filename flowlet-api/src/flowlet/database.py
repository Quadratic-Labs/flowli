"""Database models and settings for Flowlet flow run tracking.

This module defines SQLAlchemy ORM models for storing flow and task execution
history, including runs, logs, and their hierarchical relationships.
"""
import functools
from datetime import datetime, UTC
from uuid import UUID, uuid4

from attrs import asdict
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy import DateTime, String, Unicode, Uuid
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship


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
    """Database model for a flow or task execution run.

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

    # Relationships
    logs: Mapped[list["RunLog"]] = relationship("RunLog", back_populates="run", foreign_keys="RunLog.run_id")
    links: Mapped[list["RunLink"]] = relationship("RunLink", back_populates="parent_run", foreign_keys="RunLink.parent_run_id")
    parent_links: Mapped[list["RunLink"]] = relationship("RunLink", back_populates="child_run", foreign_keys="RunLink.child_run_id")
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


class RunLog(Base):
    """Database model for append-only execution logs.

    Records timestamped status changes and error messages for flow and task runs.
    Logs are immutable once created, providing an audit trail.

    Attributes:
        log_id: Unique identifier for this log entry.
        run_id: Foreign key to the associated run.
        timestamp: When this log entry was created.
        status: Status of the run at this point (e.g., "running", "success", "failed").
        log: Optional log message or error details.
        run: Relationship to the parent Run.
    """
    __tablename__ = "run_logs"
    log_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String)
    log: Mapped[str] = mapped_column(Unicode)

    run: Mapped[Run] = relationship("Run", back_populates="logs", foreign_keys=[run_id])


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

    parent_run: Mapped[Run] = relationship("Run", back_populates="links", foreign_keys=[parent_run_id])
    child_run: Mapped[Run] = relationship("Run", back_populates="parent_links", foreign_keys=[child_run_id])


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

    def init_database(self):
        """Initialize database by creating all tables if they don't exist.

        Creates tables for Run, RunLog, and RunLink models.
        Safe to call multiple times - uses checkfirst=True.
        """
        for table in Base.metadata.sorted_tables:
            table.create(bind=self.engine, checkfirst=True)
