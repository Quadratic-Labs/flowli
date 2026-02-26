"""
Flowlet client package for querying run history.

Provides high-level API for downloading snapshots, syncing with new logs,
and querying run data from SQLite snapshots.

Example:
    >>> from flowlet.client import FlowletClient, FlowletClientConfig
    >>> from pathlib import Path
    >>>
    >>> config = FlowletClientConfig(
    ...     storage_root=Path("/path/to/storage"),
    ...     cache_dir=Path("~/.flowlet/cache"),
    ...     auto_sync=True
    ... )
    >>>
    >>> async with FlowletClient(config=config) as client:
    ...     runs = client.list_runs(flow_name="my_flow", limit=10)
    ...     for run in runs:
    ...         print(f"{run.name}: {run.status}")
"""

from .client import FlowletClient, FlowletClientConfig
from .downloader import SnapshotDownloader
from .syncer import IncrementalSyncer, BackgroundSyncer

__all__ = [
    "FlowletClient",
    "FlowletClientConfig",
    "SnapshotDownloader",
    "IncrementalSyncer",
    "BackgroundSyncer",
]
