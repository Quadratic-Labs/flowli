from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..storage.types import StoragePath


class SnapshotConfig(BaseSettings):
    """Snapshots configurations.

    Configurations cover storage and rollout strategies.

    Attributes:
        ...
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_SNAPSHOT',
        env_nested_delimiter='_',
        env_nested_max_split=1,
        arbitrary_types_allowed=True
    )

    storage_path: StoragePath = Field(default=Path("."))
    cache_path: Path | None = Field(default=None)
    rollout_runs_min: int | None = Field(default=None)
    rollout_runs_max: int | None = Field(default=None)
    rollout_seconds_min: int | None = Field(default=None)
    rollout_seconds_max: int | None = Field(default=None)

