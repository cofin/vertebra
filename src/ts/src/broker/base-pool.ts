/**
 * Base class for pool adapters with shared stats tracking.
 *
 * All pool implementations (SQL, SQLite, Redis, Storage, External) extend this
 * class to eliminate duplicate counter management and stats reporting code.
 */

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
};

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
}

export abstract class BasePoolAdapter implements PoolAdapter {
  abstract name: string;

  protected _maxSize: number;
  protected _acquires = 0;
  protected _releases = 0;
  protected _errors = 0;

  constructor(maxSize: number) {
    this._maxSize = maxSize;
  }

  maxSize(): number {
    return this._maxSize;
  }

  abstract inUse(): number;
  abstract acquire(): Promise<string>;
  abstract release(token: string): Promise<void>;
  abstract execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>>;
  abstract executeMany(
    token: string,
    sql: string,
    params: unknown[][],
  ): Promise<Record<string, unknown>>;
  abstract command(token: string, command: string, args: unknown[]): Promise<unknown>;
  abstract begin(token: string): Promise<void>;
  abstract commit(token: string): Promise<void>;
  abstract rollback(token: string): Promise<void>;

  /** Record a successful acquire. Call this after acquiring a connection. */
  protected recordAcquire(): void {
    this._acquires += 1;
  }

  /** Record a successful release. Call this after releasing a connection. */
  protected recordRelease(): void {
    this._releases += 1;
  }

  /** Record an error. Call this when an operation fails. */
  protected recordError(): void {
    this._errors += 1;
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
    };
  }
}
