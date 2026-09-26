/**
 * Durable evidence storage behind one interface. The Worker uses the private
 * R2 bucket bound as EVIDENCE_BUCKET; tests inject a memory store. With no
 * store, research still returns its full payload and receipt; only durable
 * retrieval and history are unavailable (storageStatus UNAVAILABLE).
 *
 * Objects are written once: evidence cuts are content-addressed, and a daily
 * consensus observation is kept as first written. No bucket credentials or
 * URLs leave this module; responses carry only object keys.
 */

export interface EvidenceStore {
  kind: string;
  get(key: string): Promise<string | null>;
  exists(key: string): Promise<boolean>;
  put(key: string, body: string, metadata: Record<string, string>): Promise<void>;
  list(prefix: string, limit: number): Promise<string[]>;
}

export type StorageStatus = "STORED" | "ALREADY_STORED" | "UNAVAILABLE" | "FAILED" | "SKIPPED";

let injected: EvidenceStore | null | undefined;
let bucket: R2Bucket | null = null;

/** Called per request from the Worker entry with env.EVIDENCE_BUCKET. */
export function setEvidenceBucket(value: unknown): void {
  bucket = value && typeof value === "object" && typeof (value as R2Bucket).put === "function" ? value as R2Bucket : null;
}

/** Tests only: replace the store (null forces UNAVAILABLE). */
export function setEvidenceStoreForTests(store: EvidenceStore | null | undefined): void {
  injected = store;
}

function r2Store(b: R2Bucket): EvidenceStore {
  return {
    kind: "r2",
    async get(key) {
      const obj = await b.get(key);
      return obj ? await obj.text() : null;
    },
    async exists(key) {
      return (await b.head(key)) !== null;
    },
    async put(key, body, metadata) {
      await b.put(key, body, { httpMetadata: { contentType: "application/json" }, customMetadata: metadata });
    },
    async list(prefix, limit) {
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await b.list({ prefix, cursor, limit: Math.min(1000, limit - keys.length) });
        keys.push(...page.objects.map((o) => o.key));
        cursor = page.truncated ? page.cursor : undefined;
      } while (cursor && keys.length < limit);
      return keys;
    },
  };
}

export function getEvidenceStore(): EvidenceStore | null {
  if (injected !== undefined) return injected;
  return bucket ? r2Store(bucket) : null;
}

export function memoryEvidenceStore(): EvidenceStore & { objects: Map<string, string> } {
  const objects = new Map<string, string>();
  return {
    kind: "memory",
    objects,
    async get(key) {
      return objects.get(key) ?? null;
    },
    async exists(key) {
      return objects.has(key);
    },
    async put(key, body) {
      objects.set(key, body);
    },
    async list(prefix, limit) {
      return [...objects.keys()].filter((k) => k.startsWith(prefix)).sort().slice(0, limit);
    },
  };
}

/** Write once: an existing key is left as it is. Never throws. */
export async function putOnce(key: string, body: string, metadata: Record<string, string>): Promise<{ status: StorageStatus; message?: string }> {
  const store = getEvidenceStore();
  if (!store) return { status: "UNAVAILABLE" };
  try {
    if (await store.exists(key)) return { status: "ALREADY_STORED" };
    await store.put(key, body, metadata);
    return { status: "STORED" };
  } catch (e) {
    return { status: "FAILED", message: e instanceof Error ? e.message : String(e) };
  }
}

export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
