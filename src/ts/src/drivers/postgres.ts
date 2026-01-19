import { SQL } from "bun";

export type PostgresPoolConfig = {
  url?: string;
  max?: number;
  options?: Bun.SQL.Options;
};

export type SqlResult = {
  columns: string[];
  rows: unknown[][];
  rowcount: number;
};

export class PostgresPool {
  private sql: SQL;
  private reserved = new Map<string, Bun.ReservedSQL>();

  constructor(config: PostgresPoolConfig = {}) {
    const url = config.url ?? "postgres://localhost/postgres";
    const max = config.max ?? 10;
    this.sql = new SQL({
      ...(config.options ?? {}),
      url,
      adapter: "postgres",
      max,
    });
  }

  async acquire(): Promise<string> {
    const reserved = await this.sql.reserve();
    const token = crypto.randomUUID();
    this.reserved.set(token, reserved);
    return token;
  }

  async release(token: string): Promise<void> {
    const reserved = this.reserved.get(token);
    if (reserved) {
      reserved.release();
      this.reserved.delete(token);
    }
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<SqlResult> {
    const reserved = this.reserved.get(token);
    if (!reserved) {
      throw new Error("Unknown connection token");
    }
    const result = await reserved.unsafe(sql, params);
    return normalizeSqlResult(result);
  }
}

function normalizeSqlResult(result: unknown): SqlResult {
  if (!Array.isArray(result)) {
    return { columns: [], rows: [], rowcount: 0 };
  }
  const rows = result as Record<string, unknown>[];
  const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
  const values = rows.map((row) => columns.map((col) => row[col]));
  return { columns, rows: values, rowcount: values.length };
}
