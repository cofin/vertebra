import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { Socket } from "bun";

export type IpcTransport = "tcp" | "uds";

export type IpcClientConfig = {
  transport?: IpcTransport;
  host?: string;
  port?: number;
  path?: string;
  timeoutMs?: number;
  retries?: number;
  retryDelayMs?: number;
  poolSize?: number;
  poolMax?: number;
  acquireTimeoutMs?: number;
};

export type ResolvedIpcConfig = Required<
  Pick<IpcClientConfig, "transport" | "host" | "port" | "timeoutMs" | "retries" | "retryDelayMs" | "poolSize" | "poolMax" | "acquireTimeoutMs">
> &
  Pick<IpcClientConfig, "path">;

export type ResolveIpcOptions = {
  preserveTemplate?: boolean;
};

export interface IpcRequester {
  request<T = unknown>(payload: unknown): Promise<T>;
  requestStream?<T = unknown>(payload: unknown): AsyncIterable<T>;
  close?(): Promise<void>;
}

export class IpcError extends Error {
  override name = "IpcError";
}

export class IpcTimeoutError extends IpcError {
  override name = "IpcTimeoutError";
}

export class IpcPoolExhaustedError extends IpcError {
  override name = "IpcPoolExhaustedError";
}

export class IpcConnectionError extends IpcError {
  override name = "IpcConnectionError";
}

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  timer: number | null;
};

type StreamWaiter = {
  resolve: (value: IteratorResult<unknown>) => void;
  reject: (reason: unknown) => void;
};

class StreamState {
  private queue: unknown[] = [];
  private waiters: StreamWaiter[] = [];
  private done = false;
  private error: unknown = null;

  push(value: unknown): void {
    if (this.done) {
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value, done: false });
      return;
    }
    this.queue.push(value);
  }

  end(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    for (const waiter of this.waiters) {
      waiter.resolve({ value: undefined, done: true });
    }
    this.waiters = [];
  }

  fail(error: unknown): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.error = error;
    for (const waiter of this.waiters) {
      waiter.reject(error);
    }
    this.waiters = [];
  }

  next(): Promise<IteratorResult<unknown>> {
    if (this.error) {
      return Promise.reject(this.error);
    }
    if (this.queue.length > 0) {
      const value = this.queue.shift();
      return Promise.resolve({ value, done: false });
    }
    if (this.done) {
      return Promise.resolve({ value: undefined, done: true });
    }
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }
}

type PoolWaiter = {
  resolve: (connection: IpcConnection) => void;
  reject: (reason: unknown) => void;
  timer: number | null;
};

const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_RETRIES = 1;
const DEFAULT_RETRY_DELAY_MS = 50;
const DEFAULT_POOL_SIZE = 2;
const DEFAULT_POOL_MAX = 10;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}

function parseFrame(buffer: Uint8Array): { frame: Uint8Array | null; rest: Uint8Array } {
  if (buffer.length < 4) {
    return { frame: null, rest: buffer };
  }
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  const size = view.getUint32(0, false);
  if (buffer.length - 4 < size) {
    return { frame: null, rest: buffer };
  }
  const frame = buffer.subarray(4, 4 + size);
  const rest = buffer.subarray(4 + size);
  return { frame, rest };
}

function renderTemplate(value: string, workerId?: number): string {
  const pid = String(process.pid);
  const worker = String(workerId ?? 0);
  return value.replaceAll("{pid}", pid).replaceAll("{worker}", worker);
}

function defaultSocketPath(workerId?: number): string {
  const suffix = workerId === undefined ? "" : `-${workerId}`;
  if (Bun.platform === "win32") {
    return `\\\\.\\pipe\\vertebra-${process.pid}${suffix}`;
  }
  return `/tmp/vertebra-${process.pid}${suffix}.sock`;
}

export function resolveIpcConfig(
  config: IpcClientConfig = {},
  workerId?: number,
  options: ResolveIpcOptions = {},
): ResolvedIpcConfig {
  const transport = config.transport ?? (Bun.platform === "win32" ? "tcp" : "uds");
  const poolSize = config.poolSize ?? DEFAULT_POOL_SIZE;
  const poolMax = config.poolMax ?? Math.max(poolSize, DEFAULT_POOL_MAX);
  const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const retries = config.retries ?? DEFAULT_RETRIES;
  const retryDelayMs = config.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
  const acquireTimeoutMs = config.acquireTimeoutMs ?? timeoutMs;

  let path = config.path;
  if (transport === "uds") {
    if (path) {
      if (!(workerId === undefined && options.preserveTemplate)) {
        path = renderTemplate(path, workerId ?? 0);
      }
    } else {
      path = defaultSocketPath(workerId);
    }
  }

  const basePort = config.port ?? 8008;
  const resolvedPort = transport === "tcp" && workerId !== undefined ? basePort + workerId : basePort;

  return {
    transport,
    host: config.host ?? "127.0.0.1",
    port: resolvedPort,
    path,
    timeoutMs,
    retries,
    retryDelayMs,
    poolSize,
    poolMax,
    acquireTimeoutMs,
  };
}

class IpcConnection {
  private socket: Socket | null = null;
  private buffer = new Uint8Array();
  private pending: PendingRequest | null = null;
  private stream: StreamState | null = null;
  private connecting: Promise<void> | null = null;
  private closed = false;

  constructor(private config: ResolvedIpcConfig) {}

  async request<T>(payload: unknown): Promise<T> {
    await this.ensureConnected();
    if (!this.socket) {
      throw new IpcConnectionError("IPC socket unavailable");
    }
    if (this.pending || this.stream) {
      throw new IpcConnectionError("IPC connection is busy");
    }
    const framed = frameMessage(encodeMessage(payload));
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.failPending(new IpcTimeoutError("IPC request timed out"));
      }, this.config.timeoutMs) as unknown as number;
      this.pending = {
        resolve: (value) => {
          resolve(value as T);
        },
        reject,
        timer,
      };
      try {
        this.socket?.write(framed);
      } catch (error) {
        this.failPending(error);
      }
    });
  }

  async *streamRequest<T>(payload: unknown): AsyncGenerator<T> {
    await this.ensureConnected();
    if (!this.socket) {
      throw new IpcConnectionError("IPC socket unavailable");
    }
    if (this.pending || this.stream) {
      throw new IpcConnectionError("IPC connection is busy");
    }
    const framed = frameMessage(encodeMessage(payload));
    const stream = new StreamState();
    this.stream = stream;
    try {
      this.socket?.write(framed);
    } catch (error) {
      this.failStream(error);
      throw error;
    }
    try {
      while (true) {
        const next = await stream.next();
        if (next.done) {
          return;
        }
        yield next.value as T;
      }
    } finally {
      this.endStream();
    }
  }

  close(): void {
    if (this.socket) {
      this.socket.end();
    }
    this.socket = null;
    this.closed = true;
    this.failPending(new IpcConnectionError("IPC socket closed"));
    this.failStream(new IpcConnectionError("IPC socket closed"));
  }

  isClosed(): boolean {
    return this.closed;
  }

  private async ensureConnected(): Promise<void> {
    if (this.socket && !this.closed) {
      return;
    }
    if (this.connecting) {
      return this.connecting;
    }
    this.connecting = this.open();
    try {
      await this.connecting;
    } finally {
      this.connecting = null;
    }
  }

  private async open(): Promise<void> {
    this.closed = false;
    return new Promise((resolve, reject) => {
      let resolved = false;
      const socketHandlers = {
        open: (socket: Socket) => {
          this.socket = socket;
          this.buffer = new Uint8Array();
          resolved = true;
          resolve();
        },
        data: (_socket: Socket, data: Uint8Array) => {
          this.handleData(data);
        },
        error: (_socket: Socket, error: Error) => {
          if (!resolved) {
            reject(error);
            return;
          }
          this.failPending(new IpcConnectionError(error.message));
          this.failStream(new IpcConnectionError(error.message));
          this.closed = true;
        },
        close: () => {
          this.closed = true;
          this.failPending(new IpcConnectionError("IPC socket closed"));
          this.failStream(new IpcConnectionError("IPC socket closed"));
        },
      };

      const connectOptions: Record<string, unknown> = {
        socket: socketHandlers,
      };

      if (this.config.transport === "uds") {
        if (!this.config.path) {
          reject(new IpcConnectionError("IPC UDS path missing"));
          return;
        }
        connectOptions.unix = this.config.path;
      } else {
        connectOptions.hostname = this.config.host;
        connectOptions.port = this.config.port;
      }

      Bun.connect(connectOptions).catch((error) => {
        if (!resolved) {
          reject(error);
        } else {
          this.failPending(new IpcConnectionError(String(error)));
          this.closed = true;
        }
      });
    });
  }

  private handleData(data: Uint8Array): void {
    this.buffer = concatBuffers(this.buffer, data);
    while (true) {
      const parsed = parseFrame(this.buffer);
      this.buffer = parsed.rest;
      if (!parsed.frame) {
        return;
      }
      const decoded = decodeMessage(parsed.frame);
      if (this.stream) {
        this.stream.push(decoded);
        continue;
      }
      if (!this.pending) {
        continue;
      }
      this.resolvePending(decoded);
      return;
    }
  }

  private resolvePending(value: unknown): void {
    if (!this.pending) {
      return;
    }
    if (this.pending.timer !== null) {
      clearTimeout(this.pending.timer);
    }
    const resolve = this.pending.resolve;
    this.pending = null;
    resolve(value);
  }

  private failPending(reason: unknown): void {
    if (!this.pending) {
      return;
    }
    if (this.pending.timer !== null) {
      clearTimeout(this.pending.timer);
    }
    const reject = this.pending.reject;
    this.pending = null;
    reject(reason);
  }

  private endStream(): void {
    if (!this.stream) {
      return;
    }
    this.stream.end();
    this.stream = null;
  }

  private failStream(reason: unknown): void {
    if (!this.stream) {
      return;
    }
    this.stream.fail(reason);
    this.stream = null;
  }
}

export class IpcClient implements IpcRequester {
  private connection: IpcConnection;
  private config: ResolvedIpcConfig;

  constructor(config: IpcClientConfig, workerId?: number) {
    this.config = resolveIpcConfig({ ...config, poolSize: 1, poolMax: 1 }, workerId);
    this.connection = new IpcConnection(this.config);
  }

  async request<T>(payload: unknown): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.retries; attempt += 1) {
      try {
        return await this.connection.request<T>(payload);
      } catch (error) {
        lastError = error;
        this.connection.close();
        if (attempt < this.config.retries) {
          await sleep(this.config.retryDelayMs);
        }
      }
    }
    throw new IpcError("IPC request failed", { cause: lastError });
  }

  async *requestStream<T>(payload: unknown): AsyncGenerator<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.retries; attempt += 1) {
      try {
        for await (const frame of this.connection.streamRequest<T>(payload)) {
          yield frame;
        }
        return;
      } catch (error) {
        lastError = error;
        this.connection.close();
        if (attempt < this.config.retries) {
          await sleep(this.config.retryDelayMs);
        }
      }
    }
    throw new IpcError("IPC stream failed", { cause: lastError });
  }

  async close(): Promise<void> {
    this.connection.close();
  }
}

export class IpcPool implements IpcRequester {
  private config: ResolvedIpcConfig;
  private connections = new Set<IpcConnection>();
  private idle: IpcConnection[] = [];
  private waiters: PoolWaiter[] = [];
  private total = 0;
  private closed = false;

  constructor(config: IpcClientConfig = {}, workerId?: number) {
    this.config = resolveIpcConfig(config, workerId);
    const warm = Math.min(this.config.poolSize, this.config.poolMax);
    for (let i = 0; i < warm; i += 1) {
      const connection = this.createConnection();
      this.idle.push(connection);
    }
  }

  async request<T>(payload: unknown): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.retries; attempt += 1) {
      const connection = await this.acquire();
      try {
        const response = await connection.request<T>(payload);
        this.release(connection);
        return response;
      } catch (error) {
        lastError = error;
        this.destroy(connection);
        if (attempt < this.config.retries) {
          await sleep(this.config.retryDelayMs);
        }
      }
    }
    throw new IpcError("IPC request failed", { cause: lastError });
  }

  async *requestStream<T>(payload: unknown): AsyncGenerator<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.retries; attempt += 1) {
      const connection = await this.acquire();
      let released = false;
      try {
        for await (const frame of connection.streamRequest<T>(payload)) {
          yield frame;
        }
        this.release(connection);
        released = true;
        return;
      } catch (error) {
        lastError = error;
        this.destroy(connection);
        released = true;
        if (attempt < this.config.retries) {
          await sleep(this.config.retryDelayMs);
        }
      } finally {
        if (!released && !this.closed && !connection.isClosed() && this.connections.has(connection)) {
          this.release(connection);
        }
      }
    }
    throw new IpcError("IPC stream failed", { cause: lastError });
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const waiter of this.waiters) {
      if (waiter.timer !== null) {
        clearTimeout(waiter.timer);
      }
      waiter.reject(new IpcPoolExhaustedError("IPC pool closed"));
    }
    this.waiters = [];
    for (const connection of this.connections) {
      connection.close();
    }
    this.connections.clear();
    this.idle = [];
    this.total = 0;
  }

  private createConnection(): IpcConnection {
    const connection = new IpcConnection(this.config);
    this.connections.add(connection);
    this.total += 1;
    return connection;
  }

  private destroy(connection: IpcConnection): void {
    if (!this.connections.has(connection)) {
      return;
    }
    connection.close();
    this.connections.delete(connection);
    this.total = Math.max(0, this.total - 1);
  }

  private async acquire(): Promise<IpcConnection> {
    if (this.closed) {
      throw new IpcPoolExhaustedError("IPC pool is closed");
    }
    const connection = this.idle.pop();
    if (connection) {
      return connection;
    }
    if (this.total < this.config.poolMax) {
      return this.createConnection();
    }
    return new Promise<IpcConnection>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new IpcPoolExhaustedError("IPC pool exhausted"));
      }, this.config.acquireTimeoutMs) as unknown as number;
      this.waiters.push({ resolve, reject, timer });
    });
  }

  private release(connection: IpcConnection): void {
    if (this.closed || connection.isClosed()) {
      this.destroy(connection);
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      if (waiter.timer !== null) {
        clearTimeout(waiter.timer);
      }
      waiter.resolve(connection);
      return;
    }
    this.idle.push(connection);
  }
}
