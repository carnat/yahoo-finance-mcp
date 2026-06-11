"""Centralized TTL cache and TTL constants.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import datetime
import time


# ---------------------------------------------------------------------------
# Centralized TTL cache
# ---------------------------------------------------------------------------
class ToolCache:
    def __init__(self) -> None:
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

    def set(self, key: str, value: str, ttl: float) -> None:
        self._store[key] = (time.monotonic(), value, ttl)

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


def _cache_get(key: str, ttl: float) -> str | None:
    """Legacy cache get — delegates to ToolCache with the given TTL."""
    entry = _tool_cache._store.get(key)
    if entry is None:
        return None
    stored_at, value, stored_ttl = entry
    # honour the caller-supplied TTL (may differ from stored TTL)
    if (time.monotonic() - stored_at) < ttl:
        return value
    return None


def _cache_set(key: str, value: str, ttl: float = TTL_PRICE) -> None:
    """Legacy cache set — delegates to ToolCache."""
    _tool_cache.set(key, value, ttl)
