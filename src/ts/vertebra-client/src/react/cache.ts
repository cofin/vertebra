/**
 * Lightweight query cache for React hooks.
 *
 * Map-based, keyed on `method:path:JSON(input)`. Supports staleTime
 * semantics and pattern-based invalidation.
 */

function canonicalize(value: unknown): unknown {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  const obj = value as Record<string, unknown>;
  const sortedKeys = Object.keys(obj).sort();
  const result: Record<string, unknown> = {};
  for (const key of sortedKeys) {
    result[key] = canonicalize(obj[key]);
  }
  return result;
}

export interface CacheEntry {
  data: any;
  timestamp: number;
  promise?: Promise<any> | null;
}

export class QueryCache {
  private entries = new Map<string, CacheEntry>();
  private listeners = new Set<() => void>();
  private _version = 0;

  get version(): number {
    return this._version;
  }

  /** Build a stable cache key from request parameters. */
  static stableKey(method: string, path: string, input: any): string {
    const inputKey = input !== undefined ? JSON.stringify(canonicalize(input)) : "";
    return `${method}:${path}:${inputKey}`;
  }

  /** Get a cache entry if it exists and is not stale. */
  get(key: string, staleTime: number): CacheEntry | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (staleTime > 0 && Date.now() - entry.timestamp >= staleTime) {
      return undefined;
    }
    return entry;
  }

  /** Get or create a pending promise for a cache key. */
  getOrCreate(key: string, staleTime: number, fetcher: () => Promise<any>): Promise<any> {
    const entry = this.entries.get(key);

    // Fresh cache hit — return data immediately
    if (entry && entry.timestamp > 0 && staleTime > 0 && Date.now() - entry.timestamp < staleTime) {
      return Promise.resolve(entry.data);
    }

    // In-flight request — deduplicate
    if (entry?.promise) {
      return entry.promise;
    }

    // Start new fetch
    const promise = fetcher()
      .then((data) => {
        this.entries.set(key, { data, timestamp: Date.now(), promise: undefined });
        this.notify();
        return data;
      })
      .catch((err) => {
        const current = this.entries.get(key);
        if (current?.promise === promise) {
          current.promise = undefined;
          if (current.timestamp === 0) {
            this.entries.delete(key);
          }
        }
        throw err;
      });

    // Store pending promise for dedup
    if (entry) {
      entry.promise = promise;
    } else {
      this.entries.set(key, { data: null, timestamp: 0, promise });
    }

    return promise;
  }

  /** Set a cache entry directly. */
  set(key: string, data: any): void {
    this.entries.set(key, { data, timestamp: Date.now(), promise: undefined });
    this.notify();
  }

  /** Invalidate cache entries. If pattern is provided, only matching keys (prefix). */
  invalidate(pattern?: string): void {
    if (!pattern) {
      this.entries.clear();
    } else {
      for (const key of this.entries.keys()) {
        if (key.startsWith(pattern)) {
          this.entries.delete(key);
        }
      }
    }
    this.notify();
  }

  /** Subscribe to cache changes. Returns unsubscribe function. */
  onChange(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private notify(): void {
    this._version++;
    for (const listener of this.listeners) {
      listener();
    }
  }
}

/** Module-level singleton shared across all hooks. */
export const queryCache = new QueryCache();
