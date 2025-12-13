"""Configuration and dependencies for Flowlet framework.

This module defines the main configuration schema for Flowlet using Pydantic,
allowing configuration via Python objects, dictionaries, or environment variables.
"""
from pydantic import BaseModel, Field

from .database import DatabaseSettings


class FlowletConfig(BaseModel):
    """Configuration schema for Flowlet framework.

    Centralizes all framework settings including database configuration.
    Can be loaded from dictionaries or environment variables.

    Attributes:
        database: Database connection and settings for flow run tracking.

    Example:
        >>> # From dict
        >>> config = FlowletConfig(database={"url": "postgresql://localhost/mydb"})
        >>>
        >>> # From environment variables
        >>> # Set FLOWLET_DATABASE_URL=postgresql://localhost/mydb
        >>> config = FlowletConfig()
    """
    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore
        description="Flow runs database settings",
    )

    class Config:
        """Pydantic model configuration."""
        arbitrary_types_allowed = True