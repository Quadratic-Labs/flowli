"""
Configuration and dependencies
"""
from pydantic import BaseModel, Field

from .database import DatabaseSettings


class FlowletConfig(BaseModel):
    """Configuration for Flowlet framework"""
    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore
        description="Flow runs database settings",
    )

    class Config:
        arbitrary_types_allowed = True