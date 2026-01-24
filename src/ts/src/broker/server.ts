import { decode, encode } from "@msgpack/msgpack";
import { log } from "../server/logging";
import { ShmMpmcRing, ShmRing } from "./shm";

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
    } catch (err) {
      throw err;
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
}
