/**
 * Session Store following the Google ADK pattern.
 * Provides lifecycle-aware state management with persistence prefixes.
 */
export type StoreListener = (key: string, value: unknown) => void;

class SessionStore {
  private state: Map<string, unknown> = new Map();
  private listeners: Set<StoreListener> = new Set();

  constructor() {
    this.loadPersistence();
  }

  /**
   * Set a value in the store.
   * - user: prefix -> Persists to localStorage
   * - temp: prefix -> Memory only, cleared on reload (default Map behavior)
   */
  set(key: string, value: unknown): void {
    this.state.set(key, value);

    if (key.startsWith("user:")) {
      this.saveToDisk(key, value);
    }

    this.notify(key, value);
  }

  /**
   * Get a value from the store.
   */
  get<T = unknown>(key: string): T | undefined {
    return this.state.get(key) as T | undefined;
  }

  /**
   * Remove a value from the store.
   */
  delete(key: string): void {
    this.state.delete(key);
    if (key.startsWith("user:")) {
      localStorage.removeItem(`vtb:${key}`);
    }
    this.notify(key, undefined);
  }

  /**
   * Subscribe to changes in the store.
   */
  subscribe(callback: StoreListener): () => void {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  private notify(key: string, value: unknown): void {
    for (const listener of this.listeners) {
      listener(key, value);
    }
  }

  private saveToDisk(key: string, value: unknown): void {
    try {
      localStorage.setItem(`vtb:${key}`, JSON.stringify(value));
    } catch (e) {
      console.warn("Failed to persist to localStorage:", e);
    }
  }

  private loadPersistence(): void {
    if (typeof localStorage === "undefined") return;

    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key?.startsWith("vtb:user:")) {
        try {
          const rawValue = localStorage.getItem(key);
          if (rawValue) {
            const cleanKey = key.replace("vtb:", "");
            this.state.set(cleanKey, JSON.parse(rawValue));
          }
        } catch {
          // Ignore malformed JSON
        }
      }
    }
  }
}

// Global singleton instance
export const store = new SessionStore();

export { SessionStore };
