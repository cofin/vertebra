/**
 * Base class for pool adapters with shared stats tracking.
 */

import type { HealthChecker } from "./health";

export type PoolStats = {
  name: string;
  min_size: number;
  max_size: number;
  total: number;
  idle: number;
  in_use: number;
  waiters: number;
  acquires_total: number;
  releases_total: number;
  errors_total: number;
  health_failures_total: number;
  idle_timeouts_total: number;
};

export interface LifecycleHooks {
  onAcquire?: (connection: any) => Promise<void>;
  onRelease?: (connection: any) => Promise<void>;
  onError?: (connection: any, error: Error) => Promise<void>;
  onTimeout?: (poolName: string) => Promise<void>;
}

export interface PoolAdapter {
  name: string;
  maxSize(): number;
  inUse(): number;
  acquire(): Promise<string>;
  release(token: string): Promise<void>;
  execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>>;
  executeMany(token: string, sql: string, params: unknown[][]): Promise<Record<string, unknown>>;
  command(token: string, command: string, args: unknown[]): Promise<unknown>;
  begin(token: string): Promise<void>;
  commit(token: string): Promise<void>;
  rollback(token: string): Promise<void>;
  stats(): PoolStats;
  startHealthCheckLoop(): void;
  startIdleReaper(): void;
}

export abstract class BasePoolAdapter implements PoolAdapter {
  abstract name: string;

  protected _maxSize: number;
  protected _acquires = 0;
  protected _releases = 0;
  protected _errors = 0;
  protected _healthChecksSuccess = 0;
  protected _healthChecksFailure = 0;
  protected _removalsHealth = 0;
  protected _removalsTimeout = 0;
  protected _removalsError = 0;
  protected _idleTimeouts = 0;
  public hooks: LifecycleHooks;

  protected healthCheckInterval = 30000; // 30s
  protected idleTimeout = 300000; // 5min
  protected healthChecker: HealthChecker | null = null;
  protected connectionTimestamps = new Map<string, number>();

  constructor(maxSize: number, hooks: LifecycleHooks = {}) {
    this._maxSize = maxSize;
    this.hooks = hooks;
  }

  // Hook Registration
  setOnAcquire(fn: (connection: any) => Promise<void>): void {
    this.hooks.onAcquire = fn;
  }

  setOnRelease(fn: (connection: any) => Promise<void>): void {
    this.hooks.onRelease = fn;
  }

  setOnError(fn: (connection: any, error: Error) => Promise<void>): void {
    this.hooks.onError = fn;
  }

  setOnTimeout(fn: (poolName: string) => Promise<void>): void {
    this.hooks.onTimeout = fn;
  }

  // ... (abstract methods) ...

  protected _healthCheckInterval?: Timer;
  protected _idleReaperInterval?: Timer;

  /** Start periodic health checks for idle connections. */
  startHealthCheckLoop(intervalMs = 30000): void {
    if (this._healthCheckInterval) clearInterval(this._healthCheckInterval);

    this._healthCheckInterval = setInterval(async () => {
      await this.runHealthCheck();
    }, intervalMs);
  }

  protected abstract runHealthCheck(): Promise<void>;

  /** Start periodic reaping of idle connections. */
  startIdleReaper(timeoutMs = 300000, checkIntervalMs = 60000): void {
    if (this._idleReaperInterval) clearInterval(this._idleReaperInterval);

    this._idleReaperInterval = setInterval(async () => {
      await this.reapIdleConnections(timeoutMs);
    }, checkIntervalMs);
  }

  protected abstract reapIdleConnections(timeoutMs: number): Promise<void>;

  /** Validate a connection. Return true if healthy. */
  protected abstract validateConnection(token: string): Promise<boolean>;

  protected async checkHealth(): Promise<void> {
    // Deprecated in favor of runHealthCheck
    await this.runHealthCheck();
  }

  protected async reapIdle(token: string): Promise<void> {
    // Deprecated in favor of reapIdleConnections
  }

  protected recordHealthCheck(success: boolean, durationMs: number): void {
    if (success) {
      this._healthChecksSuccess++;
    } else {
      this._healthChecksFailure++;
    }
    // Duration tracking would go to a histogram here if we had one
  }

  protected recordRemoval(reason: "health" | "timeout" | "error"): void {
    switch (reason) {
      case "health":
        this._removalsHealth++;
        break;
      case "timeout":
        this._removalsTimeout++;
        break;
      case "error":
        this._removalsError++;
        break;
    }
  }

  /** Record a successful acquire. Call this after acquiring a connection. */
  protected recordAcquire(token: string): void {
    this._acquires += 1;
    this.connectionTimestamps.set(token, Date.now());
  }

  /** Record a successful release. Call this after releasing a connection. */
  protected recordRelease(token: string): void {
    this._releases += 1;
    this.connectionTimestamps.delete(token);
  }

  /** Update last used timestamp. */
  protected recordUse(token: string): void {
    this.connectionTimestamps.set(token, Date.now());
  }

  /** Record an error. Call this when an operation fails. */
  protected recordError(): void {
    this._errors += 1;
  }

  /** Record a health check failure. */
  protected recordHealthFailure(): void {
    // Deprecated, use recordHealthCheck(false)
    this._healthChecksFailure++;
  }

  /**
   * Get the current total count for stats. Override if different from inUse().
   * Default implementation returns inUse().
   */
  protected totalCount(): number {
    return this.inUse();
  }

  /**
   * Get the current idle count for stats. Override if tracking idle connections.
   * Default implementation returns 0.
   */
  protected idleCount(): number {
    return 0;
  }

  stats(): PoolStats {
    return {
      name: this.name,
      min_size: 0,
      max_size: this._maxSize,
      total: this.totalCount(),
      idle: this.idleCount(),
      in_use: this.inUse(),
      waiters: 0,
      acquires_total: this._acquires,
      releases_total: this._releases,
      errors_total: this._errors,
      health_failures_total: this._healthChecksFailure,
      idle_timeouts_total: this._idleTimeouts,
    };
  }
}
