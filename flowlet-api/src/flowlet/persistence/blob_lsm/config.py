"""Configuration for Blob LSM storage."""

from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class BlobLSMSettings(BaseSettings):
    """Blob LSM storage configuration.

    Settings can be loaded from environment variables with FLOWLET_BLOB_ prefix
    or instantiated directly.

    Example:
        >>> # From environment
        >>> settings = BlobLSMSettings()
        >>>
        >>> # Direct instantiation
        >>> settings = BlobLSMSettings(
        ...     connection_string="DefaultEndpointsProtocol=https;...",
        ...     container_name="flowlet-data"
        ... )
    """

    model_config = SettingsConfigDict(env_prefix='FLOWLET_BLOB_')

    # Azure connection
    connection_string: str
    container_name: str = "flowlet-data"

    # Compaction settings
    l0_compaction_interval_hours: int = 6
    l0_compaction_size_threshold_mb: int = 50
    l1_compaction_interval_days: int = 30
    l1_compaction_size_threshold_mb: int = 500

    # Cache settings
    cache_max_items: int = 1000
    cache_ttl_seconds: int = 300  # 5 minutes

    # Tier retention
    hot_tier_retention_days: int = 7
    warm_tier_retention_days: int = 90

    # Compression
    l1_compression: Literal['snappy', 'gzip', 'zstd'] = 'snappy'
    l2_compression: Literal['zstd'] = 'zstd'
    l2_compression_level: int = 9

    # Query settings
    default_batch_size: int = 1000
    max_results_per_query: int = 10000

    # Write settings
    l0_buffer_size: int = 100  # Buffer this many records before flushing
    l0_flush_interval_seconds: int = 10  # Flush buffer after this many seconds
