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
    @classmethod
    def from_pydantic(cls, data: BaseModel):
        return cls(**data.model_dump())

    @classmethod
    def from_attrs(cls, data):
        return cls(**asdict(data))

    def update_from_pydantic(self, data):
        for key, value in data.model_dump(exclude_unset=True).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def update_from_attrs(self, data):
        for key, value in asdict(data).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def __repr__(self):
        params = ", ".join(f"{k}={v}" for k, v in self.to_dict().items())
        return f"{self.__class__.__name__}({params})"

    def to_dict(self):
        return {str(k): getattr(self, k) for k in self.__table__.columns.keys()}


class Run(Base):
    """A flow or task run."""
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
    """Append only logs for a run (flow or task)."""
    __tablename__ = "run_logs"
    log_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String)
    log: Mapped[str] = mapped_column(Unicode)

    run: Mapped[Run] = relationship("Run", back_populates="logs", foreign_keys=[run_id])


class RunLink(Base):
    """Links between runs (e.g., flow-task relationships)."""
    __tablename__ = "run_links"
    link_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    parent_run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)
    child_run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.run_id"), index=True)

    parent_run: Mapped[Run] = relationship("Run", back_populates="links", foreign_keys=[parent_run_id])
    child_run: Mapped[Run] = relationship("Run", back_populates="parent_links", foreign_keys=[child_run_id])


class DatabaseSettings(BaseSettings):
    """
    Database configuration settings.

    Can be loaded from environment variables with FLOWLET_DATABASE_ prefix
    or instantiated directly.
    """
    model_config = SettingsConfigDict(env_prefix='FLOWLET_DATABASE_')
    url: str

    @functools.cached_property
    def engine(self):
        return create_engine(self.url)

    @functools.cached_property
    def db_session_factory(self):
        """
        Create a SQLAlchemy sessionmaker from an engine.
        Returns:
            sessionmaker configured for the engine
        """
        return sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def init_database(self):
        for table in Base.metadata.sorted_tables:
            table.create(bind=self.engine, checkfirst=True)
