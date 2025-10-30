import functools
from datetime import datetime, UTC
from uuid import UUID

from pydantic_settings import BaseSettings, SettingsConfigDict
import sqlalchemy
from sqlalchemy import (
    String,
    DateTime,
    Text,
    ForeignKey,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    @classmethod
    def from_pydantic(cls, data):
        return cls(**data.model_dump())

    def update_from_pydantic(self, data):
        for key, value in data.model_dump(exclude_unset=True).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def __repr__(self):
        params = ", ".join(f"{k}={v}" for k, v in self.to_dict().items())
        return f"{self.__class__.__name__}({params})"

    def to_dict(self):
        return {str(k): getattr(self, k) for k in self.__table__.columns.keys()}


class FlowRun(Base):
    __tablename__ = "flow_runs"
    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    flow_name: Mapped[str] = mapped_column(String, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String, default="running")
    error: Mapped[str] = mapped_column(Text, nullable=True)

    tasks = relationship("TaskRun", back_populates="flow", foreign_keys="TaskRun.flow_run_id")


class TaskRun(Base):
    __tablename__ = "task_runs"
    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    task_name: Mapped[str] = mapped_column(String, index=True)
    flow_run_id: Mapped[UUID] = mapped_column(ForeignKey(FlowRun.run_id), index=True)
    flow_name: Mapped[str] = mapped_column(ForeignKey(FlowRun.flow_name), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String, default="running")  # running, success, failed
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    flow = relationship("FlowRun", back_populates="tasks", foreign_keys="TaskRun.flow_run_id")


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
        return sqlalchemy.create_engine(self.url)

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
