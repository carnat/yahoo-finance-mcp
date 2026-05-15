interface CacheEntry {
  value: string;
  storedAt: number; // Date.now() ms
  ttl: number;      // ms
}

export class ToolCache {
  private store = new Map<string, CacheEntry>();

  get(key: string): { value: string; cacheHit: true; cachedAt: string } | null {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (Date.now() - entry.storedAt >= entry.ttl) return null;
    return {
      value: entry.value,
      cacheHit: true,
      cachedAt: new Date(entry.storedAt).toISOString(),
    };
  }

  set(key: string, value: string, ttl: number): void {
    this.store.set(key, { value, storedAt: Date.now(), ttl });
  }

  isStale(key: string): boolean {
    const entry = this.store.get(key);
    if (!entry) return false;
    return Date.now() - entry.storedAt > 2 * entry.ttl;
  }
}

export const TTL_PRICE = 5 * 60 * 1000;
export const TTL_ANALYST = 15 * 60 * 1000;
export const TTL_FINANCIALS = 4 * 3600 * 1000;
export const TTL_EDGAR = 24 * 3600 * 1000;
export const TTL_OPTIONS = 15 * 60 * 1000;

export const toolCache = new ToolCache();
