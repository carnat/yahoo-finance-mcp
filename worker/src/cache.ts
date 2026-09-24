/**
 * Process-local TTL cache with a size bound.
 *
 * Worker isolates are long-lived and memory-capped, so module-level caches
 * must expire entries and stop growing. Map iteration order is insertion
 * order, which makes the oldest entry the first key: re-setting a key moves
 * it to the back, and overflow evicts from the front.
 */
export class BoundedTtlCache<V> {
  private store = new Map<string, { value: V; expiresAt: number; weight: number }>();
  private totalWeight = 0;
  private readonly maxEntries: number;
  private readonly maxWeight: number;
  private readonly weigh: (value: V) => number;

  /**
   * @param maxEntries entry limit
   * @param maxWeight optional limit on the summed weight of all entries
   * @param weigh weight of one value (for example a body's length)
   */
  constructor(maxEntries: number, maxWeight = Infinity, weigh: (value: V) => number = () => 0) {
    this.maxEntries = maxEntries;
    this.maxWeight = maxWeight;
    this.weigh = weigh;
  }

  get size(): number {
    return this.store.size;
  }

  get weight(): number {
    return this.totalWeight;
  }

  get(key: string): V | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (Date.now() >= entry.expiresAt) {
      this.delete(key);
      return undefined;
    }
    return entry.value;
  }

  set(key: string, value: V, ttlMs: number): void {
    const weight = this.weigh(value);
    this.delete(key);
    if (weight > this.maxWeight) return;
    this.store.set(key, { value, expiresAt: Date.now() + ttlMs, weight });
    this.totalWeight += weight;
    while (this.store.size > this.maxEntries || this.totalWeight > this.maxWeight) {
      const oldest = this.store.keys().next().value;
      if (oldest === undefined) break;
      this.delete(oldest);
    }
  }

  delete(key: string): void {
    const entry = this.store.get(key);
    if (!entry) return;
    this.store.delete(key);
    this.totalWeight -= entry.weight;
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
