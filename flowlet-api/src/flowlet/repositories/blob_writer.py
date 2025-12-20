"""Blob-native write repository implementation."""

from uuid import UUID, uuid4
from typing import AsyncIterator

from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel
from flowlet.persistence.blob_lsm import BlobLSMSettings, LSMTierManager


class BlobFlowWriter:
    """Implementation of BlobFlowWriter protocol.

    Provides batch-oriented writes optimized for blob storage.
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize blob flow writer.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings
        self.lsm = LSMTierManager(settings)

    async def start(self):
        """Start background tasks."""
        await self.lsm.start()

    async def stop(self):
        """Stop background tasks and flush."""
        await self.lsm.stop()

    async def write_run_batch(
        self,
        runs: list[RunAttrModel]
    ) -> list[UUID]:
        """Write multiple runs in a single operation.

        Args:
            runs: List of runs to write

        Returns:
            List of run IDs that were written
        """
        await self.lsm.writer.write_runs_batch(runs)
        return [run.run_id for run in runs]

    async def append_logs_stream(
        self,
        run_id: UUID,
        logs: AsyncIterator[RunLogAttrModel]
    ) -> int:
        """Stream logs directly to blob storage.

        Args:
            run_id: Run ID these logs belong to
            logs: Async iterator of log entries

        Returns:
            Number of logs written
        """
        return await self.lsm.writer.append_logs_stream(run_id, logs)

    async def link_runs_batch(
        self,
        links: list[tuple[UUID, UUID]]
    ) -> None:
        """Create parent-child links in batch.

        Args:
            links: List of (parent_id, child_id) tuples
        """
        for parent_id, child_id in links:
            link_id = uuid4()
            await self.lsm.writer.write_link(parent_id, child_id, link_id)

    async def flush(self) -> None:
        """Force flush in-memory buffers to blob storage."""
        await self.lsm.writer.flush()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
