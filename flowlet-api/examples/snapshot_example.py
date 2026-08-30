"""
Example usage of the Flowlet Snapshot System.

This script demonstrates:
1. Server-side: Building snapshots from log files
2. Client-side: Querying run history from snapshots
"""
import asyncio
from pathlib import Path
import sys

# Add src to path for importing
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flowlet.storage.snapshot import (
    LogDiscovery, LogReader, SnapshotBuilder, SnapshotManager
)
from flowlet.storage.manifest import SnapshotConfig
from flowlet.client import FlowletClient, FlowletClientConfig
from flowlet.types import ReportedStatus


async def server_side_example():
    """
    Server-side example: Build snapshots from log files.

    This would typically run as a scheduled task (cron, background worker, etc.)
    """
    print("=" * 60)
    print("SERVER-SIDE: Building Snapshots")
    print("=" * 60)

    # Configure storage (replace with your actual storage path)
    storage_root = Path("./examples/storage")

    # Create storage directories if they don't exist
    (storage_root / "logs").mkdir(parents=True, exist_ok=True)
    (storage_root / "snapshots").mkdir(parents=True, exist_ok=True)
    (storage_root / "metadata").mkdir(parents=True, exist_ok=True)

    # Configure snapshot settings
    config = SnapshotConfig(
        max_hot_runs=10000,          # Rollout after 10k runs
        max_hot_age_days=180,         # Rollout after 6 months
        cold_snapshot_months=3        # 3-month cold snapshot chunks
    )

    # Create components
    log_discovery = LogDiscovery(storage_root=storage_root, log_prefix="logs/")
    log_reader = LogReader(storage_root=storage_root)
    builder = SnapshotBuilder(
        storage_root=storage_root,
        log_discovery=log_discovery,
        log_reader=log_reader,
        config=config,
        snapshot_dir="snapshots/"
    )

    manager = SnapshotManager(
        storage_root=storage_root,
        builder=builder,
        config=config,
        manifest_path="metadata/snapshots-manifest.json"
    )

    # Build/update snapshots
    print("\n1. Updating snapshots...")
    manifest = await manager.update_snapshots()

    # Display results
    print(f"\n2. Snapshot Status:")
    if manifest.hot:
        print(f"   Hot Snapshot:")
        print(f"     - Runs: {manifest.hot.run_count}")
        print(f"     - Size: {manifest.hot.size_bytes:,} bytes")
        if manifest.hot.earliest_run:
            print(f"     - Date Range: {manifest.hot.earliest_run.date()} to {manifest.hot.latest_run.date()}")
        print(f"     - Last Synced: {manifest.hot.last_synced_log}")
    else:
        print("   No hot snapshot yet (no log files found)")

    print(f"\n   Cold Snapshots: {len(manifest.cold)}")
    for i, cold in enumerate(manifest.cold, 1):
        print(f"     {i}. {cold.start_date} to {cold.end_date}")
        print(f"        - Runs: {cold.run_count:,}")
        print(f"        - Size: {cold.size_bytes:,} bytes")

    print("\n✓ Server-side snapshot building complete!")
    return storage_root


async def client_side_example(storage_root: Path):
    """
    Client-side example: Query run history from snapshots.

    This demonstrates how applications/dashboards would query run data.
    """
    print("\n" + "=" * 60)
    print("CLIENT-SIDE: Querying Run History")
    print("=" * 60)

    # Configure client
    config = FlowletClientConfig(
        storage_root=storage_root,
        cache_dir=Path.home() / ".flowlet" / "cache",
        auto_sync=False,  # Disable background sync for this example
        poll_interval_seconds=30.0
    )

    try:
        # Initialize client (downloads snapshot and syncs)
        print("\n1. Initializing client...")
        async with FlowletClient(config=config) as client:
            print("   ✓ Client initialized")

            # Example 1: List recent runs
            print("\n2. Listing recent runs (limit 10)...")
            runs = client.list_runs(limit=10)

            if not runs:
                print("   No runs found in snapshot")
                return

            print(f"   Found {len(runs)} runs:")
            for run in runs:
                duration = (run.end_ts - run.start_ts) if run.end_ts and run.start_ts else None
                duration_str = f"{duration.total_seconds():.1f}s" if duration else "N/A"
                print(f"     - {run.name}: {run.status} ({duration_str})")

            # Example 2: Get specific run with hierarchy
            print("\n3. Getting run hierarchy for first run...")
            first_run = runs[0]
            summary = client.get_run_summary(first_run.obligation_id)

            if summary:
                print(f"   Run: {summary.span_name}")
                print(f"     Status: {summary.status}")
                if summary.duration:
                    print(f"     Duration: {summary.duration.total_seconds():.1f}s")
                print(f"     Children: {len(summary.children)}")

                if summary.children:
                    print("     Child tasks:")
                    for child in summary.children:
                        print(f"       - {child.span_name}: {child.status}")

            # Example 3: Count runs by status
            print("\n4. Counting runs by status...")
            total_runs = client.count_runs()
            completed = client.count_runs(status=ReportedStatus.completed)
            failed = client.count_runs(status=ReportedStatus.failed)

            print(f"   Total: {total_runs}")
            print(f"   Completed: {completed}")
            print(f"   Failed: {failed}")

            # Example 4: Filter by flow name
            if runs:
                flow_name = runs[0].name
                print(f"\n5. Filtering by flow name '{flow_name}'...")
                filtered_runs = client.list_runs(flow_name=flow_name, limit=5)
                print(f"   Found {len(filtered_runs)} runs for flow '{flow_name}'")

            # Example 5: Get run relationships
            if summary and summary.children:
                print("\n6. Exploring run relationships...")
                parent = client.get_run_parent(summary.children[0].span_id)
                if parent:
                    print(f"   Parent of first child: {parent.name}")

                children = client.get_run_children(first_run.obligation_id)
                print(f"   Direct children of root: {len(children)}")

            print("\n✓ Client-side querying complete!")

    except RuntimeError as e:
        print(f"\n✗ Client initialization failed: {e}")
        print("   (This is expected if no snapshots exist yet)")


async def main():
    """Run both server and client examples."""
    print("\n" + "=" * 60)
    print("Flowlet Snapshot System - Example Usage")
    print("=" * 60)

    # Server-side: Build snapshots
    storage_root = await server_side_example()

    # Client-side: Query snapshots
    await client_side_example(storage_root)

    print("\n" + "=" * 60)
    print("Example Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run flows to generate log files in examples/storage/logs/")
    print("  2. Run this script again to build snapshots from the logs")
    print("  3. Query the snapshots using the FlowletClient")
    print()


if __name__ == "__main__":
    asyncio.run(main())
