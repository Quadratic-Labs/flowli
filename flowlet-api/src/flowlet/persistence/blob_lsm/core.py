"""Core LSM tier manager orchestrating all components."""

from .config import BlobLSMSettings
from .writer import L0Writer
from .reader import MultiTierReader
from .cache import HotCache


class LSMTierManager:
    """Orchestrates all LSM tier components.

    Provides a unified interface for:
    - Writing to L0 (hot tier)
    - Reading from all tiers (L0/L1/L2)
    - Cache management
    - Compaction coordination (future)

    Example:
        >>> settings = BlobLSMSettings(
        ...     connection_string="...",
        ...     container_name="flowlet-data"
        ... )
        >>> async with LSMTierManager(settings) as lsm:
        ...     # Write data
        ...     await lsm.writer.write_run(run)
        ...     await lsm.writer.flush()
        ...
        ...     # Read data
        ...     async for log in lsm.reader.get_run_logs(run_id):
        ...         print(log)
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize LSM tier manager.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings
        self.writer = L0Writer(settings)
        self.reader = MultiTierReader(settings)
        self.cache = self.reader.cache  # Share cache between reader and writer

    async def start(self):
        """Start background tasks (auto-flush, compaction)."""
        await self.writer.start()
        # TODO: Start compaction scheduler

    async def stop(self):
        """Stop background tasks and flush remaining data."""
        await self.writer.stop()
        # TODO: Stop compaction scheduler

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()

    def get_stats(self) -> dict:
        """Get statistics for all components.

        Returns:
            Dictionary with stats for cache, compaction, etc.
        """
        return {
            'cache': self.cache.get_stats(),
            # TODO: Add compaction stats
            # TODO: Add tier sizes
        }
