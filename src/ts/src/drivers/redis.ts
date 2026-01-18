import { RedisClient } from "bun";
import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { DriverRequest, DriverResponse } from "../protocol/messages";

export type RedisServerConfig = {
  hostname?: string;
  port?: number;
  url?: string;
  options?: Bun.RedisOptions;
};

type ConnectionState = {
  id: string;
  client: RedisClient;
};

type SocketState = {
  buffer: Uint8Array;
};

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}

function parseFrames(buffer: Uint8Array): { frames: Uint8Array[]; rest: Uint8Array } {
  const frames: Uint8Array[] = [];
  let offset = 0;
  while (buffer.length - offset >= 4) {
    const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
    const size = view.getUint32(0, false);
    if (buffer.length - offset - 4 < size) {
      break;
    }
    const start = offset + 4;
    const end = start + size;
    frames.push(buffer.subarray(start, end));
    offset = end;
  }
  return { frames, rest: buffer.subarray(offset) };
}

function okResponse(request: DriverRequest, result: unknown): DriverResponse {
  return { id: request.id, ok: true, result };
}

function errorResponse(request: DriverRequest, message: string): DriverResponse {
  return { id: request.id, ok: false, error: { code: "redis", message } };
}

export class RedisDriverServer {
  private server: ReturnType<typeof Bun.listen> | null = null;
  private connections = new Map<string, ConnectionState>();

  constructor(private config: RedisServerConfig = {}) {}

  start(): void {
    if (this.server) {
      return;
    }
    const hostname = this.config.hostname ?? "127.0.0.1";
    const port = this.config.port ?? 7004;
    this.server = Bun.listen({
      hostname,
      port,
      socket: {
        open: (socket) => {
          socket.data = { buffer: new Uint8Array() } satisfies SocketState;
        },
        data: (socket, data) => {
          const state = socket.data as SocketState;
          state.buffer = concatBuffers(state.buffer, data);
          const parsed = parseFrames(state.buffer);
          state.buffer = parsed.rest;
          for (const frame of parsed.frames) {
            const request = decodeMessage<DriverRequest>(frame);
            void this.handleRequest(request).then((response) => {
              const payload = encodeMessage(response);
              socket.write(frameMessage(payload));
            });
          }
        },
        close: () => {
          return;
        },
      },
    });
  }

  stop(): void {
    this.server?.stop();
    this.server = null;
    for (const state of this.connections.values()) {
      state.client.close();
    }
    this.connections.clear();
  }

  private async handleRequest(request: DriverRequest): Promise<DriverResponse> {
    try {
      switch (request.method) {
        case "connect":
          return await this.handleConnect(request);
        case "command":
          return await this.handleCommand(request);
        case "close":
          return await this.handleClose(request);
        default:
          return errorResponse(request, `Unknown method: ${request.method}`);
      }
    } catch (error) {
      return errorResponse(request, String(error));
    }
  }

  private async handleConnect(request: DriverRequest): Promise<DriverResponse> {
    const params = (request.params ?? {}) as {
      url?: string;
      options?: Bun.RedisOptions;
    };
    const id = crypto.randomUUID();
    const url = params.url ?? this.config.url ?? "redis://localhost:6379";
    const client = new RedisClient(url, params.options ?? this.config.options);
    await client.connect();
    this.connections.set(id, { id, client });
    return okResponse(request, { connectionId: id });
  }

  private async handleCommand(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as { connectionId: string; command: string; args?: string[] };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return errorResponse(request, "Unknown connection");
    }
    const args = params.args ?? [];
    const result = await connection.client.send(params.command, args);
    return okResponse(request, result);
  }

  private async handleClose(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as { connectionId: string };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return okResponse(request, { closed: false });
    }
    connection.client.close();
    this.connections.delete(params.connectionId);
    return okResponse(request, { closed: true });
  }
}
