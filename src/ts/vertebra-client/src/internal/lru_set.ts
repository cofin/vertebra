/**
 * Fast O(1) bounded string set using native Set insertion order.
 */

export class BoundedKeySet {
  private readonly set = new Set<string>();
  readonly capacity: number;

  constructor(capacity = 1024) {
    this.capacity = capacity;
  }

  has(key: string): boolean {
    return this.set.has(key);
  }

  add(key: string): boolean {
    if (this.set.has(key)) {
      return false;
    }
    if (this.set.size >= this.capacity) {
      const oldest = this.set.keys().next().value;
      if (oldest !== undefined) {
        this.set.delete(oldest);
      }
    }
    this.set.add(key);
    return true;
  }

  get size(): number {
    return this.set.size;
  }

  clear(): void {
    this.set.clear();
  }
}
