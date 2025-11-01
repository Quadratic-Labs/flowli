import functools
from datetime import datetime, UTC
from uuid import UUID

from attr import asdict
from pydantic_settings import BaseSettings, SettingsConfigDict 
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy import DateTime, String, Unicode, Uuid
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    @classmethod
    def from_pydantic(cls, data: BaseModel):
        return cls(**data.model_dump())

    @classmethod
    def from_attr(cls, data):
        return cls(**asdict(data))

    def update_from_pydantic(self, data):
        for key, value in data.model_dump(exclude_unset=True).items():
            if not hasattr(self, key):
                raise AttributeError(key)
            setattr(self, key, value)
        return self

    def update_from_attr(self, data):
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


class FlowRun(Base):
    """Identifying immutable attributes for a flow run."""
    __tablename__ = "flow_runs"
    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    flow_name: Mapped[str] = mapped_column(String, index=True)

    tasks = relationship("TaskRun", back_populates="flow_run", foreign_keys="TaskRun.flow_run_id")
    logs = relationship("FlowRunLog", back_populates="run")


class FlowRunLog(Base):
    """Append only logs for a flow run."""
    __tablename__ = "flow_run_logs"
    log_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    run_id: Mapped[UUID] = mapped_column(ForeignKey(FlowRun.run_id), index=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=lambda: datetime.now(UTC))
    status: Mapped[str] = mapped_column(String)
    log: Mapped[str] = mapped_column(Unicode)

    run = relationship("FlowRun", back_populates="logs")


class TaskRun(Base):
    """Identifying immutable attributes for a task run."""
    __tablename__ = "task_runs"
    run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    flow_run_id: Mapped[UUID] = mapped_column(ForeignKey(FlowRun.run_id), index=True)
    task_name: Mapped[str] = mapped_column(String, index=True)

    flow_run = relationship("FlowRun", back_populates="tasks", foreign_keys="TaskRun.flow_run_id")
    logs = relationship("TaskRunLog", back_populates="run")


class TaskRunLog(Base):
    """Append only logs for a flow run."""
    __tablename__ = "task_run_logs"
    log_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, index=True)
    run_id: Mapped[UUID] = mapped_column(ForeignKey(TaskRun.run_id), index=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=lambda: datetime.now(UTC))
    status: Mapped[str] = mapped_column(String)
    log: Mapped[str] = mapped_column(Unicode)

    run = relationship("TaskRun", back_populates="logs")


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
