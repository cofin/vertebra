/**
 * Lightweight query cache for React hooks.
 *
 * Map-based, keyed on `method:path:JSON(input)`. Supports staleTime
 * semantics and pattern-based invalidation.
 */

interface CacheEntry {
  data: any;
  timestamp: number;
  promise: Promise<any> | null;
}

export class QueryCache {
  private entries = new Map<string, CacheEntry>();
  private listeners = new Set<() => void>();

  /** Build a stable cache key from request parameters. */
  static stableKey(method: string, path: string, input: any): string {
    const inputKey = input !== undefined ? JSON.stringify(input) : "";
    return `${method}:${path}:${inputKey}`;
  }

  /** Get a cache entry if it exists and is not stale. */
  get(key: string, staleTime: number): CacheEntry | null {
    const entry = this.entries.get(key);
    if (!entry) return null;
    if (staleTime > 0 && Date.now() - entry.timestamp < staleTime) {
      return entry;
    }
    // staleTime=0: return entry for instant Suspense resolve, but caller should refetch
    return entry;
  }

  /** Get or create a pending promise for a cache key. */
  getOrCreate(key: string, staleTime: number, fetcher: () => Promise<any>): Promise<any> {
    const entry = this.entries.get(key);

    // Fresh cache hit — return data immediately
    if (entry && staleTime > 0 && Date.now() - entry.timestamp < staleTime) {
      return Promise.resolve(entry.data);
    }

    // In-flight request — deduplicate
    if (entry?.promise) {
      return entry.promise;
    }

    // Start new fetch
    const promise = fetcher().then((data) => {
      this.entries.set(key, { data, timestamp: Date.now(), promise: null });
      this.notify();
      return data;
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
    this.entries.set(key, { data, timestamp: Date.now(), promise: null });
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
    for (const listener of this.listeners) {
      listener();
    }
  }
}

/** Module-level singleton shared across all hooks. */
export const queryCache = new QueryCache();
