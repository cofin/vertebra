import { IpcPool, type IpcClientConfig, type IpcRequester } from "../ipc";

export interface WorkerPool extends IpcRequester {
  close(): Promise<void>;
}

export type WorkerPoolOptions = {
  count: number;
  ipc?: IpcClientConfig;
};

export class RoundRobinWorkerPool implements WorkerPool {
  private pools: IpcPool[] = [];
  private index = 0;
  private closed = false;

  constructor(options: WorkerPoolOptions) {
    const count = Math.max(1, Math.floor(options.count));
    for (let i = 0; i < count; i += 1) {
      this.pools.push(new IpcPool(options.ipc, i));
    }
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

export class StubWorkerPool implements WorkerPool {
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

export function createWorkerPool(options: WorkerPoolOptions): WorkerPool {
  return new RoundRobinWorkerPool(options);
}
