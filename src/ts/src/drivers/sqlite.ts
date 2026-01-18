import { Database } from "bun:sqlite";
import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { DriverRequest, DriverResponse } from "../protocol/messages";

export type SqliteServerConfig = {
  hostname?: string;
  port?: number;
};

type ConnectionState = {
  id: string;
  db: Database;
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
  return { id: request.id, ok: false, error: { code: "sqlite", message } };
}

export class SqliteDriverServer {
  private server: ReturnType<typeof Bun.listen> | null = null;
  private connections = new Map<string, ConnectionState>();

  constructor(private config: SqliteServerConfig = {}) {}

  start(): void {
    if (this.server) {
      return;
    }
    const hostname = this.config.hostname ?? "127.0.0.1";
    const port = this.config.port ?? 7002;
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
            const response = this.handleRequest(request);
            const payload = encodeMessage(response);
            socket.write(frameMessage(payload));
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
      state.db.close();
    }
    this.connections.clear();
  }

  private handleRequest(request: DriverRequest): DriverResponse {
    try {
      switch (request.method) {
        case "connect":
          return this.handleConnect(request);
        case "execute":
          return this.handleExecute(request);
        case "close":
          return this.handleClose(request);
        default:
          return errorResponse(request, `Unknown method: ${request.method}`);
      }
    } catch (error) {
      return errorResponse(request, String(error));
    }
  }

  private handleConnect(request: DriverRequest): DriverResponse {
    const params = (request.params ?? {}) as { path?: string };
    const path = params.path ?? ":memory:";
    const id = crypto.randomUUID();
    const db = new Database(path);
    this.connections.set(id, { id, db });
    return okResponse(request, { connectionId: id });
  }

  private handleExecute(request: DriverRequest): DriverResponse {
    const params = request.params as {
      connectionId: string;
      sql: string;
      params?: unknown[];
    };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return errorResponse(request, "Unknown connection");
    }
    const stmt = connection.db.query(params.sql);
    const rows = stmt.all(params.params ?? []) as Record<string, unknown>[];
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
    const values = rows.map((row) => columns.map((col) => row[col]));
    return okResponse(request, { columns, rows: values, rowcount: values.length });
  }

  private handleClose(request: DriverRequest): DriverResponse {
    const params = request.params as { connectionId: string };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return okResponse(request, { closed: false });
    }
    connection.db.close();
    this.connections.delete(params.connectionId);
    return okResponse(request, { closed: true });
  }
}
