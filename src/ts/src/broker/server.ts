import { decode, encode } from "@msgpack/msgpack";
import { ShmMpmcRing, ShmRing } from "./shm";
import { log } from "../server/logging";

// Opcode constants matched with Rust
const OP_HASH_PASSWORD = 8;
const OP_VERIFY_PASSWORD = 9;
const OP_GENERATE_RANDOM = 10;
const OP_CSRF_SIGN = 11;

export type BrokerConfig = {
  name: string;
  controlName?: string;
  slotSize: number;
  slots: number;
  timeoutMs: number;
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
  private timer: Timer | null = null;

  constructor(private config: BrokerConfig) {
    const controlName = config.controlName ?? `/${config.name}.control`;
    try {
        // Ensure clean state
        ShmMpmcRing.unlink(controlName);
        // Control ring must be MPMC (Multi-Producer Single-Consumer)
        this.control = ShmMpmcRing.create(controlName, config.slotSize, config.slots);
    } catch (err) {      throw err;
    }
  }

  start(): void {
    if (this.timer) return;

    // Main event loop
    // Since JS is single-threaded, we can use a timer or setImmediate to poll rings.
    // For high performance, we might want a tighter loop or blocking call in a worker?
    // But Bun is single threaded.
    // We'll use a polling interval for now (e.g. 1ms or 0ms/nextTick)

    this.timer = setInterval(() => {
      this.poll();
    }, 1);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.control.close();
    for (const client of this.clients.values()) {
      client.reqRing.close();
      client.respRing.close();
    }
    this.clients.clear();
  }

  private poll(): void {
    if (this.stopped) return;

    // 1. Poll Control Ring
    try {
      const msg = this.control.recv(0); // Non-blocking
      if (msg) {
        this.handleControlMessage(msg.data);
      }
    } catch (e) {
      console.error("Broker control poll error", e);
    }

    // 2. Poll Clients
    for (const [clientId, client] of this.clients) {
      try {
        const msg = client.reqRing.recv(0);
        if (msg) {
          this.handleClientMessage(client, msg);
        }
      } catch (e) {
        console.error(`Broker client ${clientId} poll error`, e);
        // If error, maybe remove client?
        this.clients.delete(clientId);
      }
    }
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

  private async handleClientMessage(client: ClientContext, msg: { data: Uint8Array; flags: number }): Promise<void> {
    client.lastActive = Date.now();
    const { data, flags } = msg;
    let id: string | number | undefined;

    try {
        let result: unknown;
        let error: string | undefined;

        switch (flags) {
        case OP_HASH_PASSWORD: {
          const password = decode(data) as string;
          result = await Bun.password.hash(password);
          break;
        }
        case OP_VERIFY_PASSWORD: {
          const [password, hash] = decode(data) as [string, string];
          result = await Bun.password.verify(password, hash);
          break;
        }
        case OP_GENERATE_RANDOM: {
          const length = decode(data) as number;
          // Bun doesn't have a specific random bytes helper that returns array directly better than WebCrypto?
          // crypto.getRandomValues
          const buf = new Uint8Array(length);
          crypto.getRandomValues(buf);
          result = buf;
          break;
        }
        case OP_CSRF_SIGN: {
          // Mock or real impl?
          // "CSRF & CSP: Implement CSRF token generation/verification ... utilizing Bun's native capabilities"
          // Usually HMAC.
          const [dataStr, secret] = decode(data) as [string, string];
          // Simple HMAC-SHA256
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
          // Normal message or unknown op?
          // If existing AsyncBrokerClient sends payload, it uses flags=0.
          // We should probably just echo or handle "stats" if it's 0.
          if (flags === 0) {
            const payload = decode(data) as Record<string, any>;
            if (payload.kind === "stats") {
              // Return stats
              result = { pools: [] }; // Mock stats for now
            } else {
              error = "Unknown command";
            }
          } else {
            error = `Unknown opcode: ${flags}`;
          }
      }

      const payload = decode(data) as Record<string, any>; // Extract ID if present?
      // Wait, for OP_* we decoded specific types.
      // Does the client wrap it in {id, payload}?
      // AsyncBrokerClient wraps: {id: ..., ...payload}
      // But for Opcode optimized calls, we might send raw args?
      // If we send raw args, we can't extract ID easily unless we enforce a structure.

      // Let's assume standard structure: { id: ..., payload: ... } or just { id: ..., ...args }
      // BUT, decoding twice is wasteful.

      // OPTIMIZATION:
      // The Payload for OP_* should be [id, args...]
      // Or if we stick to `AsyncBrokerClient` pattern, everything is an object with `id`.

      // Let's stick to { id: ..., arg1: ... } pattern for consistency for now,
      // or just accept that `decode` happened in the switch case.

      // Actually, for HashPassword, if I send just "password" string, I lose the ID.
      // RPC needs ID to match response.

      // So the payload MUST be an array or object containing the ID.
      // Let's define the payload as [id, ...args] for Opcodes.

      // Re-implementing switch with [id, ...args]:

        // We need to decode first.
        const decoded = decode(data);
        
        if (Array.isArray(decoded)) {
            id = decoded[0];
        } else if (typeof decoded === 'object' && decoded !== null) {
            id = (decoded as any).id;
        }

        // Re-eval result based on decoded data
      if (!error && result === undefined) {
        switch (flags) {
                          case OP_HASH_PASSWORD: {
                              const [_, password] = decoded as [unknown, string];
                              result = await Bun.password.hash(password, { algorithm: "bcrypt" });
                              break;
                          }          case OP_VERIFY_PASSWORD: {
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
            if (flags === 0) {
              const payload = decoded as Record<string, any>;
              if (payload.kind === "stats") {
                result = { pools: [] };
              } else {
                // Fallback or ignore
              }
            }
        }
      }

      // Send response
      // Format: { id, result, ok: true } or { id, error, ok: false }
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
    }
