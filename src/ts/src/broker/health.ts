export interface HealthChecker {
  check(connection: any): Promise<boolean>;
}

export class PostgresHealthChecker implements HealthChecker {
  async check(connection: any): Promise<boolean> {
    try {
      // connection is Bun.ReservedSQL
      await connection.unsafe("SELECT 1");
      return true;
    } catch (error) {
      console.warn("Postgres health check failed:", error);
      return false;
    }
  }
}

export class MySqlHealthChecker implements HealthChecker {
  async check(connection: any): Promise<boolean> {
    try {
      await connection.unsafe("SELECT 1");
      return true;
    } catch (error) {
      console.warn("MySQL health check failed:", error);
      return false;
    }
  }
}

export class RedisHealthChecker implements HealthChecker {
  async check(client: any): Promise<boolean> {
    try {
      const result = await client.send("PING");
      return result === "PONG";
    } catch (error) {
      console.warn("Redis health check failed:", error);
      return false;
    }
  }
}

export class SqliteHealthChecker implements HealthChecker {
  async check(db: any): Promise<boolean> {
    try {
      db.query("SELECT 1").get();
      return true;
    } catch (error) {
      console.warn("SQLite health check failed:", error);
      return false;
    }
  }
}
