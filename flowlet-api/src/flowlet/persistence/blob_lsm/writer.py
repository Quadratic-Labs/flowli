"""L0 writer with buffering and dual indexing for logs."""

import json
import asyncio
from datetime import datetime, UTC
from uuid import UUID
from typing import AsyncIterator
from collections import defaultdict

from flowlet.persistence.azure_path import AzureBlobPath
from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel
from .config import BlobLSMSettings


class L0Writer:
    """Buffered writer for L0 tier (hot storage).

    Writes data as JSONL files with dual indexing for logs:
    - runs: Single index by date
    - logs: Dual index (by-run and by-time) for different access patterns
    - links: Single index by date

    Features:
    - In-memory buffering to reduce blob operations
    - Automatic flush on buffer size or time interval
    - Dual indexing for optimal drill-down performance
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize L0 writer.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings
        self.blob_root = AzureBlobPath.from_connection_string(
            connection_string=settings.connection_string,
            container=settings.container_name
        )

        # In-memory buffers
        self._run_buffer: list[RunAttrModel] = []
        self._log_buffer: list[RunLogAttrModel] = []
        self._link_buffer: list[tuple[UUID, UUID, UUID]] = []  # (parent, child, link_id)

        # Last flush timestamp
        self._last_flush = datetime.now(UTC)

        # Background flush task
        self._flush_task: asyncio.Task | None = None
        self._running = False

    async def start(self):
        """Start background flush task."""
        self._running = True
        self._flush_task = asyncio.create_task(self._auto_flush_loop())

    async def stop(self):
        """Stop background flush and flush remaining data."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self.flush()

    async def write_run(self, run: RunAttrModel):
        """Write a single run to buffer.

        Args:
            run: Run to write
        """
        self._run_buffer.append(run)

        # Flush if buffer full
        if len(self._run_buffer) >= self.settings.l0_buffer_size:
            await self.flush_runs()

    async def write_runs_batch(self, runs: list[RunAttrModel]):
        """Write multiple runs to buffer.

        Args:
            runs: Runs to write
        """
        self._run_buffer.extend(runs)

        # Flush if buffer full
        if len(self._run_buffer) >= self.settings.l0_buffer_size:
            await self.flush_runs()

    async def write_log(self, log: RunLogAttrModel):
        """Write a single log to buffer.

        Args:
            log: Log to write
        """
        self._log_buffer.append(log)

        # Flush if buffer full
        if len(self._log_buffer) >= self.settings.l0_buffer_size:
            await self.flush_logs()

    async def append_logs_stream(
        self,
        run_id: UUID,
        logs: AsyncIterator[RunLogAttrModel]
    ) -> int:
        """Stream logs directly to blob storage (bypasses buffer).

        Uses Azure AppendBlob for efficient streaming.

        Args:
            run_id: Run ID these logs belong to
            logs: Async iterator of log entries

        Returns:
            Number of logs written
        """
        count = 0
        date_str = datetime.now(UTC).strftime("%Y-%m-%d")

        # Write to by-run index (primary)
        run_log_path = self.blob_root / f"logs/l0/by-run/{date_str}/run-{run_id}/logs.jsonl"

        # Ensure parent directory exists
        run_log_path.parent.mkdir(parents=True, exist_ok=True)

        # Append logs
        with run_log_path.open('a', encoding='utf-8') as f:
            async for log in logs:
                line = json.dumps({
                    'log_id': str(log.log_id),
                    'run_id': str(log.run_id),
                    'timestamp': log.timestamp.isoformat(),
                    'status': log.status,
                    'log': log.log
                })
                f.write(line + '\n')
                count += 1

        return count

    async def write_link(self, parent_id: UUID, child_id: UUID, link_id: UUID):
        """Write a parent-child link to buffer.

        Args:
            parent_id: Parent run ID
            child_id: Child run ID
            link_id: Unique link ID
        """
        self._link_buffer.append((parent_id, child_id, link_id))

        # Flush if buffer full
        if len(self._link_buffer) >= self.settings.l0_buffer_size:
            await self.flush_links()

    async def flush(self):
        """Flush all buffers to blob storage."""
        await asyncio.gather(
            self.flush_runs(),
            self.flush_logs(),
            self.flush_links()
        )

    async def flush_runs(self):
        """Flush run buffer to blob storage."""
        if not self._run_buffer:
            return

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")

        # Path: runs/l0/2025-12-19/runs-20251219-143025-123456.jsonl
        runs_path = self.blob_root / f"runs/l0/{date_str}/runs-{timestamp}.jsonl"
        runs_path.parent.mkdir(parents=True, exist_ok=True)

        # Write JSONL
        lines = []
        for run in self._run_buffer:
            lines.append(json.dumps({
                'run_id': str(run.run_id),
                'run_type': run.run_type,
                'name': run.name
            }))

        runs_path.write_text('\n'.join(lines) + '\n')

        # Clear buffer
        self._run_buffer.clear()
        self._last_flush = datetime.now(UTC)

    async def flush_logs(self):
        """Flush log buffer to blob storage with dual indexing."""
        if not self._log_buffer:
            return

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")

        # Group logs by run_id for by-run index
        logs_by_run: dict[UUID, list[RunLogAttrModel]] = defaultdict(list)
        for log in self._log_buffer:
            logs_by_run[log.run_id].append(log)

        # Write to by-run index (primary)
        for run_id, logs in logs_by_run.items():
            run_log_path = self.blob_root / f"logs/l0/by-run/{date_str}/run-{run_id}/logs.jsonl"
            run_log_path.parent.mkdir(parents=True, exist_ok=True)

            lines = []
            for log in logs:
                lines.append(json.dumps({
                    'log_id': str(log.log_id),
                    'run_id': str(log.run_id),
                    'timestamp': log.timestamp.isoformat(),
                    'status': log.status,
                    'log': log.log
                }))

            # Append to existing file or create new
            if run_log_path.exists():
                existing = run_log_path.read_text()
                run_log_path.write_text(existing + '\n'.join(lines) + '\n')
            else:
                run_log_path.write_text('\n'.join(lines) + '\n')

        # Write to by-time index (secondary) - all logs in time order
        time_log_path = self.blob_root / f"logs/l0/by-time/{date_str}/all-logs-{timestamp}.jsonl"
        time_log_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        for log in sorted(self._log_buffer, key=lambda x: x.timestamp):
            lines.append(json.dumps({
                'log_id': str(log.log_id),
                'run_id': str(log.run_id),
                'timestamp': log.timestamp.isoformat(),
                'status': log.status,
                'log': log.log
            }))

        time_log_path.write_text('\n'.join(lines) + '\n')

        # Clear buffer
        self._log_buffer.clear()
        self._last_flush = datetime.now(UTC)

    async def flush_links(self):
        """Flush link buffer to blob storage."""
        if not self._link_buffer:
            return

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")

        # Path: hierarchy/l0/2025-12-19/links-20251219-143025-123456.jsonl
        links_path = self.blob_root / f"hierarchy/l0/{date_str}/links-{timestamp}.jsonl"
        links_path.parent.mkdir(parents=True, exist_ok=True)

        # Write JSONL
        lines = []
        for parent_id, child_id, link_id in self._link_buffer:
            lines.append(json.dumps({
                'link_id': str(link_id),
                'parent_run_id': str(parent_id),
                'child_run_id': str(child_id)
            }))

        links_path.write_text('\n'.join(lines) + '\n')

        # Clear buffer
        self._link_buffer.clear()
        self._last_flush = datetime.now(UTC)

    async def _auto_flush_loop(self):
        """Background task to auto-flush buffers on interval."""
        while self._running:
            await asyncio.sleep(self.settings.l0_flush_interval_seconds)

            # Check if it's time to flush
            elapsed = (datetime.now(UTC) - self._last_flush).total_seconds()
            if elapsed >= self.settings.l0_flush_interval_seconds:
                await self.flush()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - flush on close."""
        # Run async flush in sync context
        asyncio.run(self.flush())

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
