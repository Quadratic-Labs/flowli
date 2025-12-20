"""
Blob Storage LSM (Log-Structured Merge) Tree implementation.

Provides a tiered storage system for flow execution data:
- L0 (Hot): In-memory cache + JSONL files (0-7 days)
- L1 (Warm): Parquet files with Snappy compression (7-90 days)
- L2 (Cold): Highly compressed Parquet archives (90+ days)

Features:
- Automatic compaction across tiers
- Dual indexing for drill-down queries (by run_id and flow_name)
- OLAP-friendly columnar storage
- 80-90% storage reduction via compression
"""

from .config import BlobLSMSettings
from .core import LSMTierManager
from .writer import L0Writer
from .reader import MultiTierReader
from .cache import HotCache

__all__ = [
    "BlobLSMSettings",
    "LSMTierManager",
    "L0Writer",
    "MultiTierReader",
    "HotCache",
]
