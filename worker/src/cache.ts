/**
 * Process-local TTL cache with a size bound.
 *
 * Worker isolates are long-lived and memory-capped, so module-level caches
 * must expire entries and stop growing. Map iteration order is insertion
 * order, which makes the oldest entry the first key: re-setting a key moves
 * it to the back, and overflow evicts from the front.
 */
export class BoundedTtlCache<V> {
  private store = new Map<string, { value: V; expiresAt: number }>();

  constructor(private readonly maxEntries: number) {}

  get(key: string): V | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (Date.now() >= entry.expiresAt) {
      this.store.delete(key);
      return undefined;
    }
    return entry.value;
  }

  set(key: string, value: V, ttlMs: number): void {
    this.store.delete(key);
    this.store.set(key, { value, expiresAt: Date.now() + ttlMs });
    while (this.store.size > this.maxEntries) {
      const oldest = this.store.keys().next().value;
      if (oldest === undefined) break;
      this.store.delete(oldest);
    }
  }

  delete(key: string): void {
    this.store.delete(key);
  }
}

/**
 * Set a key on a plain Map cache and evict its oldest entries beyond
 * maxEntries. For caches that keep their own storedAt/TTL bookkeeping.
 */
export function setBounded<K, V>(map: Map<K, V>, key: K, value: V, maxEntries: number): void {
  map.delete(key);
  map.set(key, value);
  while (map.size > maxEntries) {
    const oldest = map.keys().next();
    if (oldest.done) break;
    map.delete(oldest.value);
  }
}
