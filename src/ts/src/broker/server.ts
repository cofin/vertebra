import { decode, encode } from "@msgpack/msgpack";
import { log } from "../server/logging";
import { BufferPool, ShmMpmcRing, ShmRing } from "./shm";

// Opcode constants matched with Rust
const OP_HASH_PASSWORD = 8;
const OP_VERIFY_PASSWORD = 9;
const OP_GENERATE_RANDOM = 10;
const OP_CSRF_SIGN = 11;

// Polling strategy constants
const SPIN_ITERATIONS = 100; // Hot spin before backing off
const BACKOFF_MIN_MS = 0; // Start with setImmediate
const BACKOFF_MAX_MS = 1; // Max backoff = 1ms (was the original interval)

export type BrokerConfig = {
  name: string;
  controlName?: string;
  slotSize: number;
  slots: number;
  timeoutMs: number;
  /** Number of buffers to pre-allocate for zero-copy recv */
  bufferPoolSize?: number;
};

type ClientContext = {
  clientId: string;
  reqRing: ShmRing;
  respRing: ShmRing;
  lastActive: number;
};

export class BrokerServer {
  private control: ShmMpmcRing;
  private clients = new Map<string, ClientContext>();
  private stopped = false;
  private pollTimer: Timer | null = null;
  private pollImmediate: boolean = false;
  private backoffMs: number = BACKOFF_MIN_MS;
  private consecutiveEmptyPolls: number = 0;
  private readonly bufferPool: BufferPool;
  /** Inherited notify FD from supervisor (if available) */
  private readonly notifyFd: number | null;
  /** Server start time for uptime calculation */
  private readonly startTime: number = performance.now();

  constructor(private config: BrokerConfig) {
    const controlName = config.controlName ?? `/${config.name}.control`;
    try {
      // Ensure clean state
      ShmMpmcRing.unlink(controlName);
      // Control ring must be MPMC (Multi-Producer Single-Consumer)
      this.control = ShmMpmcRing.create(controlName, config.slotSize, config.slots);
    } catch (err) {
      throw err;
    }

    // Pre-allocate buffer pool for zero-copy operations
    const poolSize = config.bufferPoolSize ?? 64;
    this.bufferPool = new BufferPool(poolSize, config.slotSize);

    // Check for inherited notify FD from supervisor
    const fdEnv = process.env.VERTEBRA_NOTIFY_FD;
    this.notifyFd = fdEnv ? parseInt(fdEnv, 10) : null;
    if (this.notifyFd !== null && !isNaN(this.notifyFd)) {
      log("info", "broker.notify_fd_inherited", { fd: this.notifyFd });
    }
  }

  start(): void {
    if (this.pollTimer || this.pollImmediate) return;
    this.stopped = false;
    this.schedulePoll();
  }

  stop(): void {
    this.stopped = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    this.pollImmediate = false;
    this.control.close();
    for (const client of this.clients.values()) {
      client.reqRing.close();
      client.respRing.close();
    }
    this.clients.clear();
  }

  /**
   * Schedule the next poll iteration with adaptive backoff.
   *
   * When there's work to do, we poll immediately (setImmediate).
   * When idle, we gradually back off to reduce CPU usage.
   * This provides both low latency (when busy) and low CPU (when idle).
   */
  private schedulePoll(): void {
    if (this.stopped) return;

    if (this.backoffMs === 0) {
      // Use setImmediate for minimal latency when active
      this.pollImmediate = true;
      setImmediate(() => {
        this.pollImmediate = false;
        this.pollWithBackoff();
      });
    } else {
      // Use setTimeout for backoff when idle
      this.pollTimer = setTimeout(() => {
        this.pollTimer = null;
        this.pollWithBackoff();
      }, this.backoffMs);
    }
  }

  /**
   * Poll with adaptive backoff strategy.
   *
   * If we processed any messages, reset backoff to 0 (hot path).
   * If we're idle for multiple iterations, gradually increase backoff.
   */
  private pollWithBackoff(): void {
    const processed = this.poll();

    if (processed > 0) {
      // Work was done - reset to hot path
      this.consecutiveEmptyPolls = 0;
      this.backoffMs = BACKOFF_MIN_MS;
    } else {
      // No work - increase backoff gradually
      this.consecutiveEmptyPolls++;
      if (this.consecutiveEmptyPolls > SPIN_ITERATIONS) {
        this.backoffMs = Math.min(this.backoffMs + 0.1, BACKOFF_MAX_MS);
      }
    }

    this.schedulePoll();
  }

  /**
   * Poll all rings for messages.
   *
   * @returns Number of messages processed
   */
  private poll(): number {
    if (this.stopped) return 0;

    let processed = 0;

    // 1. Poll Control Ring (using buffer pool for zero-copy)
    try {
      const msg = this.control.recv(0, this.bufferPool);
      if (msg) {
        processed++;
        this.handleControlMessage(msg.data);
        // Release buffer back to pool after processing
        this.bufferPool.release(msg.data);
      }
    } catch (e) {
      console.error("Broker control poll error", e);
    }

    // 2. Poll Clients (using buffer pool for zero-copy)
    for (const [clientId, client] of this.clients) {
      try {
        const msg = client.reqRing.recv(0, this.bufferPool);
        if (msg) {
          processed++;
          this.handleClientMessage(client, msg);
          // Release buffer back to pool after processing
          this.bufferPool.release(msg.data);
        }
      } catch (e) {
        console.error(`Broker client ${clientId} poll error`, e);
        // If error, maybe remove client?
        this.clients.delete(clientId);
      }
    }

    return processed;
  }

  private handleControlMessage(data: Uint8Array): void {
    try {
      const payload = decode(data) as Record<string, any>;
      if (payload.kind === "register_client") {
        this.registerClient(payload);
      }
    } catch (e) {
      console.error("Invalid control message", e);
    }
  }

  private registerClient(payload: Record<string, any>): void {
    const { client_id, req_ring, resp_ring } = payload;
    if (!client_id || !req_ring || !resp_ring) return;

    try {
      const reqRing = ShmRing.open(req_ring, this.config.slotSize, this.config.slots);
      const respRing = ShmRing.open(resp_ring, this.config.slotSize, this.config.slots);

      const client: ClientContext = {
        clientId: client_id,
        reqRing,
        respRing,
        lastActive: Date.now(),
      };
      this.clients.set(client_id, client);

      // Send Ack
      const response = { ok: true, id: payload.id };
      // We use the control ring to ack?
      // No, AsyncBrokerClient expects response on the client's response ring?
      // AsyncBrokerClient:
      // this.control.send(..., register_client)
      // const response = await this.recvResponse(); // waits on this.response (resp_ring)

      // So we send ACK on the new respRing.
      respRing.send(encode(response), 0, this.config.timeoutMs);

      log("info", "broker.client_registered", { client_id });
    } catch (e) {
      console.error("Failed to register client", e);
    }
  }

  private async handleClientMessage(
    client: ClientContext,
    msg: { data: Uint8Array; flags: number },
  ): Promise<void> {
    client.lastActive = Date.now();
    const { data, flags } = msg;
    let id: string | number | undefined;

    try {
      let result: unknown;
      let error: string | undefined;

      // We need to decode first.
      const decoded = decode(data);

      if (Array.isArray(decoded)) {
        id = decoded[0];
      } else if (typeof decoded === "object" && decoded !== null) {
        id = (decoded as any).id;
      }

      switch (flags) {
        case OP_HASH_PASSWORD: {
          const [_, password] = decoded as [unknown, string];
          result = await Bun.password.hash(password, { algorithm: "bcrypt" });
          break;
        }
        case OP_VERIFY_PASSWORD: {
          const [_, password, hash] = decoded as [unknown, string, string];
          result = await Bun.password.verify(password, hash);
          break;
        }
        case OP_GENERATE_RANDOM: {
          const [_, length] = decoded as [unknown, number];
          const buf = new Uint8Array(length);
          crypto.getRandomValues(buf);
          result = buf;
          break;
        }
        case OP_CSRF_SIGN: {
          const [_, dataStr, secret] = decoded as [unknown, string, string];
          const key = await crypto.subtle.importKey(
            "raw",
            new TextEncoder().encode(secret),
            { name: "HMAC", hash: "SHA-256" },
            false,
            ["sign"],
          );
          const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(dataStr));
          result = Buffer.from(sig).toString("base64");
          break;
        }
        default:
          // Check if it's a stats request (flags=0)
          if (flags === 0) {
            const payload = decoded as Record<string, any>;
            if (payload.kind === "stats") {
              result = { pools: [] };
            } else if (payload.kind === "status") {
              result = this.buildStatusResponse();
            } else {
              error = "Unknown command";
            }
          } else {
            error = `Unknown opcode: ${flags}`;
          }
      }

      // Send response
      const respPayload = error
        ? { id, ok: false, error: { message: error } }
        : { id, ok: true, result };
      client.respRing.send(encode(respPayload), 0, this.config.timeoutMs);
    } catch (e: any) {
      console.error("Handler error", e);
      // Try to send error if we have an ID
      if (id !== undefined) {
        const respPayload = { id, ok: false, error: { message: e.message || String(e) } };
        try {
          client.respRing.send(encode(respPayload), 0, this.config.timeoutMs);
        } catch (sendErr) {
          console.error("Failed to send error response", sendErr);
        }
      }
    }
  }

  /**
   * Build status response for IPC status queries.
   *
   * Returns component health information for CLI banner rendering.
   */
  private buildStatusResponse(): {
    bun: { status: string; version: string; uptime_ms: number };
    broker: { status: string; clients: number; pools: Record<string, number> };
    ipc: { status: string; channels: number };
  } {
    const uptimeMs = Math.round(performance.now() - this.startTime);
    return {
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: uptimeMs,
      },
      broker: {
        status: "ready",
        clients: this.clients.size,
        pools: {},
      },
      ipc: {
        status: "ready",
        channels: this.clients.size,
      },
    };
  }
}
