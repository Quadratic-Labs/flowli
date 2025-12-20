"""LRU cache for hot data (recent runs and logs)."""

from datetime import datetime, UTC, timedelta
from uuid import UUID
from collections import OrderedDict

from flowlet.interfaces.repository.models import RunAttrModel, RunLogAttrModel, RunModel
from .config import BlobLSMSettings


class LRUCache[T]:
    """Generic LRU (Least Recently Used) cache with TTL support.

    Features:
    - Automatic eviction of least recently used items
    - Time-to-live (TTL) expiration
    - Thread-safe operations
    """
    def __init__(self, max_items: int, ttl_seconds: int):
        """Initialize LRU cache.

        Args:
            max_items: Maximum number of items to cache
            ttl_seconds: Time-to-live for cached items in seconds
        """
        self.max_items = max_items
        self.ttl = timedelta(seconds=ttl_seconds)
        self._cache: OrderedDict[str, tuple[T, datetime]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> T | None:
        """Get item from cache.

        Args:
            key: Cache key

        Returns:
            Cached item or None if not found/expired
        """
        if key not in self._cache:
            self._misses += 1
            return None

        value, timestamp = self._cache[key]

        # Check TTL
        if datetime.now(UTC) - timestamp > self.ttl:
            del self._cache[key]
            self._misses += 1
            return None

        # Move to end (mark as recently used)
        self._cache.move_to_end(key)
        self._hits += 1
        return value

    def put(self, key: str, value: T):
        """Put item in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        # Update existing item
        if key in self._cache:
            self._cache.move_to_end(key)
            self._cache[key] = (value, datetime.now(UTC))
            return

        # Add new item
        self._cache[key] = (value, datetime.now(UTC))

        # Evict if over capacity
        if len(self._cache) > self.max_items:
            self._cache.popitem(last=False)  # Remove oldest item

    def invalidate(self, key: str):
        """Remove item from cache.

        Args:
            key: Cache key to invalidate
        """
        self._cache.pop(key, None)

    def clear(self):
        """Clear all cached items."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def get_stats(self) -> dict[str, int]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.

        return {
            'size': len(self._cache),
            'max_items': self.max_items,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate_pct': round(hit_rate)
        }


class HotCache:
    """Multi-level cache for frequently accessed data.

    Caches:
    - Recent runs (by run_id)
    - Recent logs (by run_id)
    - Full run trees (with hierarchy)
    """

    def __init__(self, settings: BlobLSMSettings):
        """Initialize hot cache.

        Args:
            settings: Blob LSM configuration
        """
        self.settings = settings

        # Separate caches for different data types
        self.run_cache: LRUCache[RunAttrModel] = LRUCache(
            max_items=settings.cache_max_items,
            ttl_seconds=settings.cache_ttl_seconds
        )

        self.logs_cache: LRUCache[list[RunLogAttrModel]] = LRUCache(
            max_items=settings.cache_max_items // 2,  # Logs can be large
            ttl_seconds=settings.cache_ttl_seconds
        )

        self.run_tree_cache: LRUCache[RunModel] = LRUCache(
            max_items=settings.cache_max_items // 4,  # Trees are largest
            ttl_seconds=settings.cache_ttl_seconds
        )

    def get_run(self, run_id: UUID) -> RunAttrModel | None:
        """Get run from cache.

        Args:
            run_id: Run ID

        Returns:
            Cached run or None
        """
        return self.run_cache.get(str(run_id))

    def put_run(self, run: RunAttrModel):
        """Cache a run.

        Args:
            run: Run to cache
        """
        self.run_cache.put(str(run.run_id), run)

    def get_logs(self, run_id: UUID) -> list[RunLogAttrModel] | None:
        """Get logs for a run from cache.

        Args:
            run_id: Run ID

        Returns:
            Cached logs or None
        """
        return self.logs_cache.get(f"logs:{run_id}")

    def put_logs(self, run_id: UUID, logs: list[RunLogAttrModel]):
        """Cache logs for a run.

        Args:
            run_id: Run ID
            logs: Logs to cache
        """
        self.logs_cache.put(f"logs:{run_id}", logs)

    def get_run_tree(self, run_id: UUID) -> RunModel | None:
        """Get full run tree from cache.

        Args:
            run_id: Run ID

        Returns:
            Cached run tree or None
        """
        return self.run_tree_cache.get(f"tree:{run_id}")

    def put_run_tree(self, run_tree: RunModel):
        """Cache a full run tree.

        Args:
            run_tree: Run tree to cache
        """
        self.run_tree_cache.put(f"tree:{run_tree.run.run_id}", run_tree)

    def invalidate_run(self, run_id: UUID):
        """Invalidate all cache entries for a run.

        Args:
            run_id: Run ID to invalidate
        """
        self.run_cache.invalidate(str(run_id))
        self.logs_cache.invalidate(f"logs:{run_id}")
        self.run_tree_cache.invalidate(f"tree:{run_id}")

    def clear_all(self):
        """Clear all caches."""
        self.run_cache.clear()
        self.logs_cache.clear()
        self.run_tree_cache.clear()

    def get_stats(self) -> dict[str, dict]:
        """Get statistics for all caches.

        Returns:
            Dictionary with stats for each cache
        """
        return {
            'runs': self.run_cache.get_stats(),
            'logs': self.logs_cache.get_stats(),
            'trees': self.run_tree_cache.get_stats()
        }
