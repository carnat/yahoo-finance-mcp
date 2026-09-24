"""Centralized TTL cache and TTL constants.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import datetime
import time


# ---------------------------------------------------------------------------
# Centralized TTL cache
# ---------------------------------------------------------------------------
class ToolCache:
    """TTL cache with a size bound.

    The local server is long-lived, and cache keys include tickers and
    arguments, so the store must not grow without limit. Dict order is
    insertion order: re-setting a key moves it to the back, and on overflow
    expired entries go first, then the oldest.
    """

    def __init__(self, max_entries: int = 2048) -> None:
        self._max_entries = max_entries
        self._store: dict[str, tuple[float, str, float]] = {}  # key -> (stored_at, value, ttl)

    def get(self, key: str) -> tuple[str, bool, str | None] | None:
        """Returns (value, cache_hit, cached_at_iso) or None if miss/expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        stored_at, value, ttl = entry
        age = time.monotonic() - stored_at
        if age >= ttl:
            return None
        cached_at = (
            datetime.datetime.fromtimestamp(time.time() - age, tz=datetime.timezone.utc)
            .isoformat()
        )
        return value, True, cached_at

    def get_value(self, key: str) -> str | None:
        """The cached value, or None if missing or expired."""
        hit = self.get(key)
        return hit[0] if hit else None

    def set(self, key: str, value: str, ttl: float) -> None:
        self._store.pop(key, None)
        self._store[key] = (time.monotonic(), value, ttl)
        if len(self._store) > self._max_entries:
            self._evict()

    def _evict(self) -> None:
        now = time.monotonic()
        for key in [k for k, (stored_at, _, ttl) in self._store.items() if now - stored_at >= ttl]:
            del self._store[key]
        while len(self._store) > self._max_entries:
            del self._store[next(iter(self._store))]

    def is_stale(self, key: str) -> bool:
        """True if age > 2× TTL (stale but still cached)."""
        entry = self._store.get(key)
        if not entry:
            return False
        stored_at, _, ttl = entry
        return (time.monotonic() - stored_at) > 2 * ttl


_tool_cache = ToolCache()

# TTL tiers
TTL_PRICE = 5 * 60          # 5 min
TTL_ANALYST = 15 * 60       # 15 min
TTL_FINANCIALS = 4 * 3600   # 4 hours
TTL_EDGAR = 24 * 3600       # 24 hours
TTL_OPTIONS = 15 * 60       # 15 min
TTL_NEWS = 15 * 60          # 15 min — RSS feed cache

# Backward-compat aliases (old names still work)
_PRICE_TTL = TTL_PRICE
_STMT_TTL = TTL_FINANCIALS

