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
  data: unknown;
  timestamp: number;
  promise?: Promise<unknown> | null;
  settledPromise?: Promise<unknown>;
  epoch: number;
}

export class QueryCache {
  private entries = new Map<string, CacheEntry>();
  private epochs = new Map<string, number>();
  private baseEpoch = 0;
  private listeners = new Set<() => void>();
  private _version = 0;

  get version(): number {
    return this._version;
  }

  private getEpoch(key: string): number {
    return this.baseEpoch + (this.epochs.get(key) ?? 0);
  }

  /** Build a stable cache key from request parameters. */
  static stableKey(method: string, path: string, input: unknown): string {
    const inputKey = input !== undefined ? JSON.stringify(canonicalize(input)) : "";
    return `${method}:${path}:${inputKey}`;
  }

  /** Get a cache entry if it exists and is not stale. */
  get(key: string, staleTime: number): CacheEntry | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (entry.epoch !== this.getEpoch(key)) {
      return undefined;
    }
    if (staleTime > 0 && Date.now() - entry.timestamp >= staleTime) {
      return undefined;
    }
    return entry;
  }

  /** Get or create a pending promise for a cache key. */
  getOrCreate<TData = unknown>(key: string, staleTime: number, fetcher: () => Promise<TData>): Promise<TData> {
    const activeEpoch = this.getEpoch(key);
    const entry = this.entries.get(key);

    if (entry && entry.timestamp > 0 && entry.epoch === activeEpoch) {
      if (staleTime === 0 || Date.now() - entry.timestamp < staleTime) {
        return (entry.settledPromise ?? Promise.resolve(entry.data)) as Promise<TData>;
      }
    }

    if (entry?.promise) {
      return entry.promise as Promise<TData>;
    }

    const promise = fetcher()
      .then((data) => {
        const current = this.entries.get(key);
        if (!current || current.promise === promise) {
          this.entries.set(key, {
            data,
            timestamp: Date.now(),
            promise: undefined,
            settledPromise: promise,
            epoch: activeEpoch,
          });
          this.notify();
        }
        return data;
      })
      .catch((err: unknown) => {
        const current = this.entries.get(key);
        if (current?.promise === promise) {
          current.promise = undefined;
          if (current.timestamp === 0) {
            this.entries.delete(key);
          }
        }
        throw err;
      });

    if (entry) {
      entry.promise = promise;
    } else {
      this.entries.set(key, { data: null, timestamp: 0, promise, epoch: activeEpoch });
    }

    return promise;
  }

  /** Set a cache entry directly. */
  set(key: string, data: unknown): void {
    this.entries.set(key, {
      data,
      timestamp: Date.now(),
      promise: undefined,
      epoch: this.getEpoch(key),
    });
    this.notify();
  }

  /** Invalidate cache entries. If pattern is provided, only matching keys (prefix). */
  invalidate(pattern?: string): void {
    if (!pattern) {
      this.entries.clear();
      this.baseEpoch += 1;
    } else {
      const matchingKeys = new Set<string>();
      for (const key of this.entries.keys()) {
        if (key.startsWith(pattern)) {
          matchingKeys.add(key);
        }
      }
      for (const key of this.epochs.keys()) {
        if (key.startsWith(pattern)) {
          matchingKeys.add(key);
        }
      }
      for (const key of matchingKeys) {
        this.entries.delete(key);
        this.epochs.set(key, (this.epochs.get(key) ?? 0) + 1);
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
