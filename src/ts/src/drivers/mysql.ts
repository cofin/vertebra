import { SQL } from "bun";
import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { DriverRequest, DriverResponse } from "../protocol/messages";

export type MySQLServerConfig = {
  hostname?: string;
  port?: number;
  options?: Bun.SQL.Options;
};

type ConnectionState = {
  id: string;
  sql: SQL;
  reserved: Bun.ReservedSQL;
  pooled: boolean;
  poolKey?: string;
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
  return { id: request.id, ok: false, error: { code: "mysql", message } };
}

export class MySQLDriverServer {
  private server: ReturnType<typeof Bun.listen> | null = null;
  private connections = new Map<string, ConnectionState>();
  private pools = new Map<string, { sql: SQL; refs: number }>();

  constructor(private config: MySQLServerConfig = {}) {}

  start(): void {
    if (this.server) {
      return;
    }
    const hostname = this.config.hostname ?? "127.0.0.1";
    const port = this.config.port ?? 7006;
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
      state.reserved.release();
      void state.sql.close({ timeout: 5 });
    }
    this.connections.clear();
    for (const pool of this.pools.values()) {
      void pool.sql.close({ timeout: 5 });
    }
    this.pools.clear();
  }

  private async handleRequest(request: DriverRequest): Promise<DriverResponse> {
    try {
      switch (request.method) {
        case "connect":
          return await this.handleConnect(request);
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

  private async handleConnect(request: DriverRequest): Promise<DriverResponse> {
    const params = (request.params ?? {}) as {
      connectionString?: string;
      options?: Record<string, unknown>;
      pool?: { max?: number };
    };
    const id = crypto.randomUUID();
    const connectionString = params.connectionString ?? "mysql://localhost/mysql";
    const options = params.options ?? {};
    if (params.pool) {
      const key = JSON.stringify({ connectionString, options, pool: params.pool });
      let pool = this.pools.get(key);
      if (!pool) {
        const sql = new SQL({
          ...(options as Bun.SQL.Options),
          url: connectionString,
          adapter: "mysql",
          max: params.pool.max ?? 10,
        });
        await sql.connect();
        pool = { sql, refs: 0 };
        this.pools.set(key, pool);
      }
      pool.refs += 1;
      const reserved = await pool.sql.reserve();
      this.connections.set(id, { id, sql: pool.sql, reserved, pooled: true, poolKey: key });
      return okResponse(request, { connectionId: id });
    }
    const sql = new SQL({
      ...(options as Bun.SQL.Options),
      url: connectionString,
      adapter: "mysql",
      max: 1,
    });
    await sql.connect();
    const reserved = await sql.reserve();
    this.connections.set(id, { id, sql, reserved, pooled: false });
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
    const result = await connection.reserved.unsafe(params.sql, params.params ?? []);
    if (!Array.isArray(result)) {
      return okResponse(request, { columns: [], rows: [], rowcount: 0 });
    }
    const columns = result.length > 0 ? Object.keys(result[0] ?? {}) : [];
    const values = result.map((row: Record<string, unknown>) => columns.map((col) => row[col]));
    return okResponse(request, { columns, rows: values, rowcount: values.length });
  }

  private async handleClose(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as { connectionId: string };
    const connection = this.connections.get(params.connectionId);
    if (!connection) {
      return okResponse(request, { closed: false });
    }
    connection.reserved.release();
    if (connection.pooled && connection.poolKey) {
      const pool = this.pools.get(connection.poolKey);
      if (pool) {
        pool.refs -= 1;
        if (pool.refs <= 0) {
          await pool.sql.close({ timeout: 5 });
          this.pools.delete(connection.poolKey);
        }
      }
    } else {
      await connection.sql.close({ timeout: 5 });
    }
    this.connections.delete(params.connectionId);
    return okResponse(request, { closed: true });
  }
}
