import { Database } from "bun:sqlite";

export type SqlitePoolConfig = {
  path?: string;
};

export type SqlResult = {
  columns: string[];
  rows: unknown[][];
  rowcount: number;
};

export class SqlitePool {
  private connections = new Map<string, Database>();
  private path: string;

  constructor(config: SqlitePoolConfig = {}) {
    this.path = config.path ?? ":memory:";
  }

  async acquire(): Promise<string> {
    const token = crypto.randomUUID();
    const db = new Database(this.path);
    this.connections.set(token, db);
    return token;
  }

  async release(token: string): Promise<void> {
    const db = this.connections.get(token);
    if (db) {
      db.close();
      this.connections.delete(token);
    }
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<SqlResult> {
    const db = this.connections.get(token);
    if (!db) {
      throw new Error("Unknown connection token");
    }
    const stmt = db.query(sql);
    const rows = stmt.all(params) as Record<string, unknown>[];
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
    const values = rows.map((row) => columns.map((col) => row[col]));
    return { columns, rows: values, rowcount: values.length };
  }
}
