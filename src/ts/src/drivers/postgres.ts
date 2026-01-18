import postgres from "postgres";
import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { DriverRequest, DriverResponse } from "../protocol/messages";

export type PostgresServerConfig = {
  hostname?: string;
  port?: number;
};

type ConnectionState = {
  id: string;
  client: ReturnType<typeof postgres>;
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
  return { id: request.id, ok: false, error: { code: "postgres", message } };
}

export class PostgresDriverServer {
  private server: ReturnType<typeof Bun.listen> | null = null;
  private connections = new Map<string, ConnectionState>();

  constructor(private config: PostgresServerConfig = {}) {}

  start(): void {
    if (this.server) {
      return;
    }
    const hostname = this.config.hostname ?? "127.0.0.1";
    const port = this.config.port ?? 7003;
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
      void state.client.end();
    }
    this.connections.clear();
  }

  private async handleRequest(request: DriverRequest): Promise<DriverResponse> {
    try {
      switch (request.method) {
        case "connect":
          return this.handleConnect(request);
        case "execute":
          return await this.handleExecute(request);
        case "close":
          return await this.handleClose(request);
        default:
          return errorResponse(request, `Unknown method: ${request.method}`);
      }
    } catch (error) {
      return errorResponse(request, String(error));
    }
  }

  private handleConnect(request: DriverRequest): DriverResponse {
    const params = (request.params ?? {}) as { connectionString?: string; options?: Record<string, unknown> };
    const id = crypto.randomUUID();
    const client = postgres(params.connectionString ?? "postgres://localhost/postgres", {
      max: 1,
      ...(params.options ?? {}),
    });
    this.connections.set(id, { id, client });
    return okResponse(request, { connectionId: id });
  }

  private async handleExecute(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as {
      connectionId: string;
      sql: string;
      params?: unknown[];
    };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return errorResponse(request, "Unknown connection");
    }
    const rows = await connection.client.unsafe(params.sql, params.params ?? []);
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
    const values = rows.map((row: Record<string, unknown>) => columns.map((col) => row[col]));
    return okResponse(request, { columns, rows: values, rowcount: values.length });
  }

  private async handleClose(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as { connectionId: string };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return okResponse(request, { closed: false });
    }
    await connection.client.end({ timeout: 5 });
    this.connections.delete(params.connectionId);
    return okResponse(request, { closed: true });
  }
}
