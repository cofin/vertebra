import { BasePoolAdapter } from "./base-pool";
import type { LifecycleHooks, PoolStats } from "./base-pool";

export class SqlPool extends BasePoolAdapter {
  name = "sql";
  private connections = new Map<string, any>();
  private idle = new Set<string>();

  constructor(maxSize: number, hooks: LifecycleHooks = {}) {
    super(maxSize, hooks);
  }

  inUse(): number {
    return this.connections.size - this.idle.size;
  }

  protected idleCount(): number {
    return this.idle.size;
  }

  protected totalCount(): number {
    return this.connections.size;
  }

  async acquire(): Promise<string> {
    // Mock implementation for health check tests
    const token = crypto.randomUUID();
    this.connections.set(token, { query: async () => {} });
    
    // Pre-lease validation
    if (!(await this.validateConnection(token))) {
        this.connections.delete(token);
        this.recordHealthCheck(false, 0); // Validation failed instantly
        this.recordRemoval("health");
        // Retry logic would go here in real impl
        throw new Error("Connection failed validation");
    }

    this.recordAcquire(token);
    this.recordHealthCheck(true, 0);
    
    if (this.hooks.onAcquire) {
      await this.hooks.onAcquire(this.connections.get(token));
    }
    return token;
  }

  async release(token: string): Promise<void> {
    if (this.connections.has(token)) {
        // Post-release validation
        if (!(await this.validateConnection(token))) {
            this.connections.delete(token);
            this.connectionTimestamps.delete(token);
            this.recordHealthCheck(false, 0);
            this.recordRemoval("health");
            return;
        }
        this.recordHealthCheck(true, 0);

        this.idle.add(token);
        this.recordRelease(token);
        
        if (this.hooks.onRelease) {
            await this.hooks.onRelease(this.connections.get(token));
        }
    }
  }

  // ... (execute methods) ...

  protected async validateConnection(token: string): Promise<boolean> {
    // Simple mock validation
    return true; 
  }

  protected async runHealthCheck(): Promise<void> {
    for (const token of this.idle) {
      const start = Date.now();
      try {
        if (!(await this.validateConnection(token))) {
            throw new Error("Validation failed");
        }
        this.recordHealthCheck(true, Date.now() - start);
      } catch (error) {
        this.recordHealthCheck(false, Date.now() - start);
        this.connections.delete(token);
        this.idle.delete(token);
        this.recordRemoval("health");
        
        if (this.hooks.onError) {
            await this.hooks.onError(null, error as Error);
        }
      }
    }
  }

  protected async reapIdleConnections(timeoutMs: number): Promise<void> {
    const now = Date.now();
    for (const token of this.idle) {
      const lastUsed = this.connectionTimestamps.get(token);
      if (lastUsed && now - lastUsed > timeoutMs) {
        this._idleTimeouts++;
        this.connections.delete(token);
        this.idle.delete(token);
        this.connectionTimestamps.delete(token);
        this.recordRemoval("timeout");
        
        if (this.hooks.onTimeout) {
            await this.hooks.onTimeout(this.name);
        }
      }
    }
  }
}

// Mock other pools if necessary for compilation, or leave them as stub
export class RedisPool extends BasePoolAdapter {
    name = "redis";
    inUse(): number { return 0; }
    async acquire(): Promise<string> { return ""; }
    async release(token: string): Promise<void> {}
    async execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>> { return {}; }
    async executeMany(token: string, sql: string, params: unknown[][]): Promise<Record<string, unknown>> { return {}; }
    async command(token: string, command: string, args: unknown[]): Promise<unknown> { return null; }
    async begin(token: string): Promise<void> {}
        async commit(token: string): Promise<void> {}
        async rollback(token: string): Promise<void> {}
        protected async runHealthCheck(): Promise<void> {}
        protected async reapIdleConnections(timeoutMs: number): Promise<void> {}
        protected async validateConnection(token: string): Promise<boolean> { return true; }
    }
    