import { RedisClient } from "bun";

export type RedisPoolConfig = {
  url?: string;
  options?: Bun.RedisOptions;
};

export class RedisPool {
  private client: RedisClient | null = null;
  private url: string;
  private options?: Bun.RedisOptions;

  constructor(config: RedisPoolConfig = {}) {
    this.url = config.url ?? "redis://localhost:6379";
    this.options = config.options;
  }

  async acquire(): Promise<string> {
    if (!this.client) {
      this.client = new RedisClient(this.url, this.options);
      await this.client.connect();
    }
    return "redis";
  }

  async release(_token: string): Promise<void> {
    return;
  }

  async command(command: string, args: string[]): Promise<unknown> {
    if (!this.client) {
      throw new Error("Redis client not connected");
    }
    return await this.client.send(command, args);
  }
}
