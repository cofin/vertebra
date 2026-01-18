export interface WorkerPool {
  request(payload: Uint8Array): Promise<Uint8Array>;
  close(): Promise<void>;
}

export class StubWorkerPool implements WorkerPool {
  async request(_: Uint8Array): Promise<Uint8Array> {
    throw new Error("Worker pool not configured");
  }

  async close(): Promise<void> {
    return;
  }
}
