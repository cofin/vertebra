import { ShmRing } from "../broker/shm";
import { Flags } from "../protocol/headers";
import { decodeMessage, decodeResponse, encodeMessage, encodeRequest } from "../protocol/serialization";

export type IpcWireProtocol = "msgpack" | "framed";

export type IpcClientConfig = {
  name?: string;
  slotSize?: number;
  slots?: number;
  timeoutMs?: number;
  retries?: number;
  retryDelayMs?: number;
  poolSize?: number;
  poolMax?: number;
  acquireTimeoutMs?: number;
  /** Maximum concurrent in-flight requests per connection (default: 64) */
  maxConcurrentRequests?: number;
  protocol?: IpcWireProtocol;
};

export type ResolvedIpcConfig = Required<
  Pick<
    IpcClientConfig,
    | "name"
    | "slotSize"
    | "slots"
    | "timeoutMs"
    | "retries"
    | "retryDelayMs"
    | "poolSize"
    | "poolMax"
    | "acquireTimeoutMs"
    | "maxConcurrentRequests"
    | "protocol"
  >
>;

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

type PoolWaiter = {
  resolve: (connection: IpcConnection) => void;
  reject: (reason: unknown) => void;
  timer: number | null;
};

const DEFAULT_NAME = "vertebra-asgi";
const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_RETRIES = 1;
const DEFAULT_RETRY_DELAY_MS = 50;
const DEFAULT_POOL_SIZE = 2;
const DEFAULT_POOL_MAX = 10;
const DEFAULT_MAX_CONCURRENT_REQUESTS = 64;
const DEFAULT_PROTOCOL: IpcWireProtocol = "msgpack";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function controlRingName(name: string, workerId: number): string {
  return `/${name}.asgi.${workerId}.control`;
}

function requestRingName(name: string, workerId: number, clientId: string): string {
  return `/${name}.asgi.${workerId}.client.${clientId}.req`;
}

function responseRingName(name: string, workerId: number, clientId: string): string {
  return `/${name}.asgi.${workerId}.client.${clientId}.resp`;
}

function nextId(value?: unknown): string {
  if (typeof value === "string" && value.length > 0) {
    return value;
  }
  return crypto.randomUUID();
}

function isTerminalFrame(frame: unknown): boolean {
  if (!frame || typeof frame !== "object") {
    return false;
  }
  const value = frame as Record<string, unknown>;
  const type = value.type;
  if (type === "http.response.body") {
    return !value.more_body;
  }
  if (type === "http.response.pathsend" || type === "http.response.zerocopysend") {
    return true;
  }
  if (type === "websocket.close" || type === "websocket.disconnect") {
    return true;
  }
  return false;
}

export function resolveIpcConfig(config: IpcClientConfig = {}): ResolvedIpcConfig {
  const name = config.name ?? DEFAULT_NAME;
  const slotSize = config.slotSize ?? 65_536;
  const slots = config.slots ?? 1024;
  const poolSize = config.poolSize ?? DEFAULT_POOL_SIZE;
  const poolMax = config.poolMax ?? Math.max(poolSize, DEFAULT_POOL_MAX);
  const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const retries = config.retries ?? DEFAULT_RETRIES;
  const retryDelayMs = config.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
  const acquireTimeoutMs = config.acquireTimeoutMs ?? timeoutMs;
  const maxConcurrentRequests = config.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT_REQUESTS;
  const protocol = config.protocol ?? DEFAULT_PROTOCOL;

  return {
    name,
    slotSize,
    slots,
    timeoutMs,
    retries,
    retryDelayMs,
    poolSize,
    poolMax,
    acquireTimeoutMs,
    maxConcurrentRequests,
    protocol,
  };
}

class IpcConnection {
  private control: ShmRing | null = null;
  private requestRing: ShmRing | null = null;
  private responseRing: ShmRing | null = null;
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private responseLoopRunning = false;
  private connecting: Promise<void> | null = null;
  private closed = false;
  private seq = 0;
  private correlationSeq = 0n; // Protocol-level correlation ID counter
  private clientId = crypto.randomUUID().replaceAll("-", "");

  constructor(
    private config: ResolvedIpcConfig,
    private workerId: number,
  ) {}

  async request<T>(payload: unknown): Promise<T> {
    await this.ensureConnected();
    if (!this.requestRing || !this.responseRing) {
      throw new IpcConnectionError("IPC ring unavailable");
    }
    if (this.pendingRequests.size >= this.config.maxConcurrentRequests) {
      throw new IpcError("Too many concurrent requests");
    }
    const message = this.ensureId(payload);
    const correlationId = this.nextCorrelationId();
    const requestKey =
      this.config.protocol === "msgpack"
        ? String((message as Record<string, unknown>).id)
        : correlationId.toString();

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.failRequest(requestKey, new IpcTimeoutError("IPC request timed out"));
      }, this.config.timeoutMs) as unknown as number;

      this.pendingRequests.set(requestKey, {
        resolve: (value) => resolve(value as T),
        reject,
        timer,
      });

      try {
        this.sendPayload(message, correlationId);
        this.ensureResponseLoop();
      } catch (error) {
        this.failRequest(requestKey, error);
      }
    });
  }

  async *streamRequest<T>(payload: unknown): AsyncGenerator<T> {
    await this.ensureConnected();
    if (!this.requestRing || !this.responseRing) {
      throw new IpcConnectionError("IPC ring unavailable");
    }
    // Streaming uses payload-level IDs for matching, not correlation IDs
    const requestId = this.extractRequestId(payload);
    const outbound = this.normalizeOutbound(payload, requestId);
    const sendTask = this.sendOutbound(outbound, requestId);

    try {
      while (true) {
        const frame = await this.waitForResponse(requestId);
        yield frame as T;
        if (isTerminalFrame(frame)) {
          return;
        }
      }
    } finally {
      await sendTask;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.failAllPending(new IpcConnectionError("IPC connection closed"));
    if (this.requestRing) {
      this.requestRing.close();
      this.requestRing = null;
    }
    if (this.responseRing) {
      this.responseRing.close();
      this.responseRing = null;
    }
  }

  isClosed(): boolean {
    return this.closed;
  }

  private async ensureConnected(): Promise<void> {
    if (this.requestRing && this.responseRing && !this.closed) {
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
    const controlName = controlRingName(this.config.name, this.workerId);
    const reqName = requestRingName(this.config.name, this.workerId, this.clientId);
    const respName = responseRingName(this.config.name, this.workerId, this.clientId);

    this.requestRing = ShmRing.create(reqName, this.config.slotSize, this.config.slots);
    this.responseRing = ShmRing.create(respName, this.config.slotSize, this.config.slots);

    const control = await this.waitForControl(controlName);
    this.control = control;

    const registerId = this.nextSeq();
    const registerMessage = {
      id: registerId,
      kind: "register_client",
      client_id: this.clientId,
      req_ring: reqName,
      resp_ring: respName,
    };
    const correlationId = this.nextCorrelationId();
    const registerPayload =
      this.config.protocol === "msgpack"
        ? encodeMessage(registerMessage)
        : encodeRequest(registerMessage, correlationId, Flags.NONE);
    this.control.send(registerPayload, 0, this.config.timeoutMs);
    const response = await this.waitForResponse(String(registerId));
    if (!response || !(response as Record<string, unknown>).ok) {
      throw new IpcConnectionError("IPC registration failed");
    }
  }

  private async waitForControl(name: string): Promise<ShmRing> {
    const deadline = Date.now() + this.config.timeoutMs;
    let lastError: unknown = null;
    while (Date.now() < deadline) {
      try {
        return ShmRing.open(name, this.config.slotSize, this.config.slots);
      } catch (error) {
        lastError = error;
        await sleep(this.config.retryDelayMs);
      }
    }
    throw new IpcConnectionError("IPC control ring unavailable", { cause: lastError });
  }

  private ensureResponseLoop(): void {
    if (this.responseLoopRunning) {
      return;
    }
    this.responseLoopRunning = true;
    void this.runResponseLoop();
  }

  private async runResponseLoop(): Promise<void> {
    while (!this.closed && this.pendingRequests.size > 0) {
      if (!this.responseRing) {
        break;
      }
      const chunk = this.responseRing.recv(10);
      if (!chunk) {
        await sleep(1);
        continue;
      }
      try {
        if (this.config.protocol === "msgpack") {
          const payload = decodeMessage<Record<string, unknown>>(chunk.data);
          const key = String(payload?.id ?? "");
          const pending = this.pendingRequests.get(key);
          if (!pending) {
            continue;
          }
          this.pendingRequests.delete(key);
          if (pending.timer !== null) {
            clearTimeout(pending.timer);
          }
          pending.resolve(payload);
        } else {
          const response = decodeResponse<Record<string, unknown>>(chunk.data);
          const key = response.correlationId.toString();
          const pending = this.pendingRequests.get(key);
          if (!pending) {
            // Response for unknown correlation ID - possibly already timed out
            continue;
          }
          this.pendingRequests.delete(key);
          if (pending.timer !== null) {
            clearTimeout(pending.timer);
          }
          if (response.isError) {
            pending.reject(new IpcError("Remote error", { cause: response.payload }));
          } else {
            pending.resolve(response.payload);
          }
        }
      } catch (error) {
        // Decoding error - log but continue processing
        console.error("Failed to decode IPC response:", error);
      }
    }
    this.responseLoopRunning = false;
  }

  private async waitForResponse(expectedId: string): Promise<Record<string, unknown>> {
    // Legacy method for streaming - waits for a specific payload ID
    if (!this.responseRing) {
      throw new IpcConnectionError("IPC response ring unavailable");
    }
    const deadline = Date.now() + this.config.timeoutMs;
    while (Date.now() < deadline) {
      const chunk = this.responseRing.recv(10);
      if (!chunk) {
        await sleep(1);
        continue;
      }
      if (this.config.protocol === "msgpack") {
        const payload = decodeMessage<Record<string, unknown>>(chunk.data);
        const id = payload?.id;
        if (String(id) !== expectedId) {
          continue;
        }
        return payload;
      }
      const response = decodeResponse<Record<string, unknown>>(chunk.data);
      if (response.isError) {
        throw new IpcError("Remote error", { cause: response.payload });
      }
      const id = response.payload?.id;
      if (String(id) !== expectedId) {
        continue;
      }
      return response.payload;
    }
    throw new IpcTimeoutError("IPC response timed out");
  }

  private sendPayload(payload: unknown, correlationId?: bigint): bigint {
    if (!this.requestRing) {
      throw new IpcConnectionError("IPC request ring unavailable");
    }
    const corrId = correlationId ?? this.nextCorrelationId();
    const encoded =
      this.config.protocol === "msgpack"
        ? encodeMessage(payload)
        : encodeRequest(payload, corrId, Flags.NONE);
    this.requestRing.send(encoded, 0, this.config.timeoutMs);
    return corrId;
  }

  private extractRequestId(payload: unknown): string {
    if (payload && typeof payload === "object") {
      const idValue = (payload as Record<string, unknown>).id;
      return nextId(idValue);
    }
    return nextId();
  }

  private normalizeOutbound(payload: unknown, requestId: string): AsyncIterable<unknown> {
    if (payload && typeof payload === "object" && Symbol.asyncIterator in payload) {
      return payload as AsyncIterable<unknown>;
    }
    return (async function* () {
      yield payload;
    })();
  }

  private async sendOutbound(outbound: AsyncIterable<unknown>, requestId: string): Promise<void> {
    for await (const frame of outbound) {
      if (frame && typeof frame === "object") {
        const payload = { ...(frame as Record<string, unknown>), id: requestId };
        this.sendPayload(payload);
      } else {
        this.sendPayload({ id: requestId, payload: frame });
      }
    }
  }

  private ensureId(payload: unknown): Record<string, unknown> {
    if (payload && typeof payload === "object") {
      const value = payload as Record<string, unknown>;
      if (!value.id) {
        return { ...value, id: nextId() };
      }
      return value;
    }
    return { id: nextId(), payload };
  }

  private failRequest(requestKey: string, reason: unknown): void {
    const pending = this.pendingRequests.get(requestKey);
    if (!pending) {
      return;
    }
    this.pendingRequests.delete(requestKey);
    if (pending.timer !== null) {
      clearTimeout(pending.timer);
    }
    pending.reject(reason);
  }

  private failAllPending(reason: unknown): void {
    for (const [requestKey, pending] of this.pendingRequests) {
      if (pending.timer !== null) {
        clearTimeout(pending.timer);
      }
      pending.reject(reason);
    }
    this.pendingRequests.clear();
  }

  private nextSeq(): number {
    this.seq += 1;
    return this.seq;
  }

  private nextCorrelationId(): bigint {
    const id = this.correlationSeq;
    this.correlationSeq += 1n;
    return id;
  }
}

export class IpcClient implements IpcRequester {
  private connection: IpcConnection;
  private config: ResolvedIpcConfig;

  constructor(config: IpcClientConfig = {}, workerId = 0) {
    this.config = resolveIpcConfig({ ...config, poolSize: 1, poolMax: 1 });
    this.connection = new IpcConnection(this.config, workerId);
  }

  async request<T>(payload: unknown): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.retries; attempt += 1) {
      try {
        return await this.connection.request<T>(payload);
      } catch (error) {
        lastError = error;
        await this.connection.close();
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
        await this.connection.close();
        if (attempt < this.config.retries) {
          await sleep(this.config.retryDelayMs);
        }
      }
    }
    throw new IpcError("IPC stream failed", { cause: lastError });
  }

  async close(): Promise<void> {
    await this.connection.close();
  }
}

export class IpcPool implements IpcRequester {
  private config: ResolvedIpcConfig;
  private connections = new Set<IpcConnection>();
  private idle: IpcConnection[] = [];
  private waiters: PoolWaiter[] = [];
  private total = 0;
  private closed = false;

  constructor(
    config: IpcClientConfig = {},
    private workerId = 0,
  ) {
    this.config = resolveIpcConfig(config);
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
        if (
          !released &&
          !this.closed &&
          !connection.isClosed() &&
          this.connections.has(connection)
        ) {
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
      await connection.close();
    }
    this.connections.clear();
    this.idle = [];
    this.total = 0;
  }

  private createConnection(): IpcConnection {
    const connection = new IpcConnection(this.config, this.workerId);
    this.connections.add(connection);
    this.total += 1;
    return connection;
  }

  private destroy(connection: IpcConnection): void {
    if (!this.connections.has(connection)) {
      return;
    }
    void connection.close();
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
