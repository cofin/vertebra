import { type IpcClientConfig, IpcPool, type IpcRequester } from "../ipc";

export interface IpcConnectionPool extends IpcRequester {
  close(): Promise<void>;
  /** Number of worker connections in the pool */
  readonly size: number;
}

export type IpcConnectionPoolOptions = {
  count: number;
  ipc?: IpcClientConfig;
};

export class RoundRobinIpcConnectionPool implements IpcConnectionPool {
  private pools: IpcPool[] = [];
  private index = 0;
  private closed = false;
  private _size: number;

  constructor(options: IpcConnectionPoolOptions) {
    const count = Math.max(1, Math.floor(options.count));
    this._size = count;
    for (let i = 0; i < count; i += 1) {
      this.pools.push(new IpcPool(options.ipc, i));
    }
  }

  get size(): number {
    return this._size;
  }

  async request<T>(payload: unknown): Promise<T> {
    if (this.closed) {
      throw new Error("Worker pool is closed");
    }
    const pool = this.pools[this.index % this.pools.length];
    this.index = (this.index + 1) % this.pools.length;
    return pool.request<T>(payload);
  }

  async *requestStream<T>(payload: unknown): AsyncGenerator<T> {
    if (this.closed) {
      throw new Error("Worker pool is closed");
    }
    const pool = this.pools[this.index % this.pools.length];
    this.index = (this.index + 1) % this.pools.length;
    if (!pool.requestStream) {
      throw new Error("Worker pool does not support streaming IPC");
    }
    for await (const frame of pool.requestStream<T>(payload)) {
      yield frame;
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await Promise.all(this.pools.map((pool) => pool.close()));
  }
}

export class StubIpcConnectionPool implements IpcConnectionPool {
  get size(): number {
    return 0;
  }

  async request<T>(_: unknown): Promise<T> {
    throw new Error("Worker pool not configured");
  }

  async *requestStream<T>(_: unknown): AsyncGenerator<T> {
    throw new Error("Worker pool not configured");
  }

  async close(): Promise<void> {
    return;
  }
}

export function createIpcConnectionPool(options: IpcConnectionPoolOptions): IpcConnectionPool {
  return new RoundRobinIpcConnectionPool(options);
}
