import { Database } from "bun:sqlite";
import { readFileSync } from "node:fs";
import { parse } from "@iarna/toml";
import { decode, encode } from "@msgpack/msgpack";
import { tableFromJSON, tableToIPC } from "apache-arrow";
import { RedisClient, SQL } from "bun";

import {
  type FileSystemParams,
  getS3Client,
  handleGcsDelete,
  handleGcsExists,
  handleGcsList,
  handleGcsRead,
  handleGcsReadFile,
  handleGcsStat,
  handleGcsWrite,
  handleGcsWriteFile,
  handleLocalDelete,
  handleLocalExists,
  handleLocalList,
  handleLocalMkdir,
  handleLocalRead,
  handleLocalReadFile,
  handleLocalRmdir,
  handleLocalStat,
  handleLocalWrite,
  handleLocalWriteFile,
  handleS3Delete,
  handleS3Exists,
  handleS3List,
  handleS3Read,
  handleS3ReadFile,
  handleS3Stat,
  handleS3Write,
  handleS3WriteFile,
  type Backend as StorageBackend,
} from "../drivers/filesystem";
import { BasePoolAdapter, type PoolAdapter, type PoolStats } from "./base-pool";
import { FairScheduler, type SchedulerStats } from "./scheduler";
import { ShmRegion, ShmRing } from "./shm";

type BrokerPoolConfig = {
  name: string;
  driver: "bun-postgres" | "bun-mysql" | "bun-sqlite" | "bun-redis" | string;
  url?: string;
  connection_string?: string;
  path?: string;
  bucket?: string;
  max_connections?: number;
  options?: Record<string, unknown>;
};

type BrokerConfigBroker = {
  name?: string;
  control_name?: string;
  slot_size?: number;
  slots?: number;
  timeout_ms?: number;
  arrow_threshold_bytes?: number;
  arrow_threshold_rows?: number;
  arrow_region_ttl_ms?: number;
  scheduler_quantum?: number;
  scheduler_starvation_ms?: number;
};

type ResolvedBrokerConfig = {
  name: string;
  control_name: string;
  slot_size: number;
  slots: number;
  timeout_ms: number;
  arrow_threshold_bytes: number;
  arrow_threshold_rows: number;
  arrow_region_ttl_ms: number;
  scheduler_quantum: number;
  scheduler_starvation_ms: number;
};

type BrokerConfig = {
  broker?: BrokerConfigBroker;
  pools?: BrokerPoolConfig[];
};

type ClientState = {
  id: string;
  request: ShmRing;
  response: ShmRing;
  weight: number;
};

type BrokerRequest = {
  id: number;
  kind: string;
  pool: string;
  client: ClientState;
  payload: Record<string, any>;
  timeoutMs: number;
  enqueuedAt: number;
};

type PoolState = {
  adapter: PoolAdapter;
  scheduler: FairScheduler<BrokerRequest>;
};

class SqlPool extends BasePoolAdapter {
  name: string;
  private sql: SQL;
  private adapter: "postgres" | "mysql";
  private reserved = new Map<string, Bun.ReservedSQL>();
  private connected = false;

  constructor(
    name: string,
    adapter: "postgres" | "mysql",
    url: string,
    max: number,
    options?: Bun.SQL.Options,
  ) {
    super(max);
    this.name = name;
    this.adapter = adapter;
    this.sql = new SQL({
      ...(options ?? {}),
      url,
      adapter,
      max,
    });
  }

  inUse(): number {
    return this.reserved.size;
  }

  async acquire(): Promise<string> {
    try {
      if (!this.connected) {
        await this.sql.connect();
        this.connected = true;
      }
      const reserved = await this.sql.reserve();
      const token = crypto.randomUUID();
      this.reserved.set(token, reserved);
      this.recordAcquire();
      return token;
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async release(token: string): Promise<void> {
    const reserved = this.reserved.get(token);
    if (reserved) {
      reserved.release();
      this.reserved.delete(token);
      this.recordRelease();
    }
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>> {
    try {
      const reserved = this.reserved.get(token);
      if (!reserved) {
        throw new Error("Unknown connection token");
      }
      const result = await reserved.unsafe(sql, params);
      return normalizeSqlResult(result);
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async executeMany(
    token: string,
    sql: string,
    params: unknown[][],
  ): Promise<Record<string, unknown>> {
    let totalRows = 0;
    let columns: string[] = [];
    for (const paramSet of params) {
      const result = await this.execute(token, sql, paramSet);
      columns = (result.columns as string[]) ?? columns;
      totalRows += (result.rowcount as number) ?? 0;
    }
    return { columns, rows: [], rowcount: totalRows };
  }

  async command(token: string, command: string, args: unknown[]): Promise<unknown> {
    return this.execute(token, command, args);
  }

  async begin(token: string): Promise<void> {
    await this.execute(token, "BEGIN", []);
  }

  async commit(token: string): Promise<void> {
    await this.execute(token, "COMMIT", []);
  }

  async rollback(token: string): Promise<void> {
    await this.execute(token, "ROLLBACK", []);
  }
}

class SqlitePool extends BasePoolAdapter {
  name: string;
  private connections = new Map<string, Database>();

  constructor(
    name: string,
    private path: string,
    maxSize: number,
  ) {
    super(maxSize);
    this.name = name;
  }

  inUse(): number {
    return this.connections.size;
  }

  async acquire(): Promise<string> {
    try {
      const token = crypto.randomUUID();
      const db = new Database(this.path);
      this.connections.set(token, db);
      this.recordAcquire();
      return token;
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async release(token: string): Promise<void> {
    const db = this.connections.get(token);
    if (db) {
      db.close();
      this.connections.delete(token);
      this.recordRelease();
    }
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>> {
    try {
      const db = this.connections.get(token);
      if (!db) {
        throw new Error("Unknown connection token");
      }
      const stmt = db.query(sql);
      const rows = stmt.all(params) as Record<string, unknown>[];
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      const values = rows.map((row) => columns.map((col) => row[col]));
      return { columns, rows: values, rowcount: values.length };
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async executeMany(
    token: string,
    sql: string,
    params: unknown[][],
  ): Promise<Record<string, unknown>> {
    let totalRows = 0;
    for (const paramSet of params) {
      const result = await this.execute(token, sql, paramSet);
      totalRows += (result.rowcount as number) ?? 0;
    }
    return { columns: [], rows: [], rowcount: totalRows };
  }

  async command(token: string, command: string, args: unknown[]): Promise<unknown> {
    return this.execute(token, command, args);
  }

  async begin(token: string): Promise<void> {
    await this.execute(token, "BEGIN", []);
  }

  async commit(token: string): Promise<void> {
    await this.execute(token, "COMMIT", []);
  }

  async rollback(token: string): Promise<void> {
    await this.execute(token, "ROLLBACK", []);
  }
}

class RedisPool extends BasePoolAdapter {
  name: string;
  private client: RedisClient | null = null;
  private token: string | null = null;
  private inUseCount = 0;

  constructor(
    name: string,
    private url: string,
    maxSize: number,
    private options?: Bun.RedisOptions,
  ) {
    super(maxSize);
    this.name = name;
  }

  inUse(): number {
    return this.inUseCount;
  }

  protected totalCount(): number {
    return this.client ? 1 : 0;
  }

  async acquire(): Promise<string> {
    try {
      if (this.inUseCount >= this._maxSize) {
        throw new Error("Redis pool is at capacity");
      }
      if (!this.client) {
        this.client = new RedisClient(this.url, this.options);
        await this.client.connect();
        this.token = crypto.randomUUID();
      }
      this.inUseCount += 1;
      this.recordAcquire();
      return this.token ?? "redis";
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async release(_token: string): Promise<void> {
    this.inUseCount = Math.max(0, this.inUseCount - 1);
    this.recordRelease();
    return;
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>> {
    const result = await this.command(token, sql, params.map(String));
    return { columns: [], rows: [], rowcount: 0, result };
  }

  async executeMany(
    token: string,
    sql: string,
    params: unknown[][],
  ): Promise<Record<string, unknown>> {
    let last: unknown = null;
    for (const paramSet of params) {
      last = await this.command(token, sql, paramSet.map(String));
    }
    return { columns: [], rows: [], rowcount: 0, result: last };
  }

  async command(_token: string, command: string, args: unknown[]): Promise<unknown> {
    try {
      if (!this.client) {
        throw new Error("Redis client not connected");
      }
      return await this.client.send(
        command,
        args.map((value) => String(value)),
      );
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async begin(_token: string): Promise<void> {
    return;
  }

  async commit(_token: string): Promise<void> {
    return;
  }

  async rollback(_token: string): Promise<void> {
    return;
  }
}

class StoragePool extends BasePoolAdapter {
  name: string;
  private backend: StorageBackend;
  private bucket?: string;
  private options?: Record<string, unknown>;
  private tokens = new Set<string>();

  constructor(
    name: string,
    backend: StorageBackend,
    maxSize: number,
    bucket?: string,
    options?: Record<string, unknown>,
  ) {
    super(maxSize);
    this.name = name;
    this.backend = backend;
    this.bucket = bucket;
    this.options = options;
  }

  inUse(): number {
    return this.tokens.size;
  }

  async acquire(): Promise<string> {
    if (this.tokens.size >= this._maxSize) {
      this.recordError();
      throw new Error("Storage pool is at capacity");
    }
    const token = crypto.randomUUID();
    this.tokens.add(token);
    this.recordAcquire();
    return token;
  }

  async release(token: string): Promise<void> {
    if (this.tokens.delete(token)) {
      this.recordRelease();
    }
  }

  async execute(
    _token: string,
    _sql: string,
    _params: unknown[],
  ): Promise<Record<string, unknown>> {
    throw new Error("Storage pools do not support execute()");
  }

  async executeMany(
    _token: string,
    _sql: string,
    _params: unknown[][],
  ): Promise<Record<string, unknown>> {
    throw new Error("Storage pools do not support execute_many()");
  }

  async command(_token: string, command: string, args: unknown[]): Promise<unknown> {
    const params = (args[0] ?? {}) as FileSystemParams;
    const backend = params.backend ?? this.backend;
    if (backend !== this.backend) {
      throw new Error(`Storage backend mismatch: ${backend} != ${this.backend}`);
    }
    const options = { ...(this.options ?? {}), ...(params.options ?? {}) } as Record<
      string,
      unknown
    >;
    const bucket = params.bucket ?? this.bucket ?? "";
    const key = params.key ?? "";
    const path = params.path ?? "";
    const prefix = params.prefix ?? params.key ?? "";
    try {
      switch (command) {
        case "read":
        case "read_range": {
          if (backend === "local") {
            const data = await handleLocalRead(path, params.range);
            return { data };
          }
          if (backend === "s3") {
            const data = await handleS3Read(bucket, key, options, params.range);
            return { data };
          }
          const data = await handleGcsRead(bucket, key, options, params.range);
          return { data };
        }
        case "write": {
          if (!params.data) {
            throw new Error("Missing data for write");
          }
          if (backend === "local") {
            const bytes = await handleLocalWrite(path, params.data);
            return { bytes };
          }
          if (backend === "s3") {
            const bytes = await handleS3Write(
              bucket,
              key,
              params.data,
              options,
              params.contentType,
              params.metadata,
            );
            return { bytes };
          }
          const bytes = await handleGcsWrite(
            bucket,
            key,
            params.data,
            options,
            params.contentType,
            params.metadata,
          );
          return { bytes };
        }
        case "write_file": {
          if (!params.localPath) {
            throw new Error("Missing localPath for write_file");
          }
          if (backend === "local") {
            const bytes = await handleLocalWriteFile(path, params.localPath);
            return { bytes };
          }
          if (backend === "s3") {
            const bytes = await handleS3WriteFile(
              bucket,
              key,
              params.localPath,
              options,
              params.contentType,
              params.metadata,
            );
            return { bytes };
          }
          const bytes = await handleGcsWriteFile(
            bucket,
            key,
            params.localPath,
            options,
            params.contentType,
            params.metadata,
          );
          return { bytes };
        }
        case "list": {
          if (backend === "local") {
            const entries = await handleLocalList(path);
            return { entries };
          }
          if (backend === "s3") {
            const entries = await handleS3List(bucket, prefix, options);
            return { entries };
          }
          const entries = await handleGcsList(bucket, prefix, options);
          return { entries };
        }
        case "stat": {
          if (backend === "local") {
            return await handleLocalStat(path);
          }
          if (backend === "s3") {
            return await handleS3Stat(bucket, key, options);
          }
          return await handleGcsStat(bucket, key, options);
        }
        case "exists": {
          if (backend === "local") {
            const exists = await handleLocalExists(path);
            return { exists };
          }
          if (backend === "s3") {
            const exists = await handleS3Exists(bucket, key, options);
            return { exists };
          }
          const exists = await handleGcsExists(bucket, key, options);
          return { exists };
        }
        case "delete": {
          const recursive = params.recursive ?? false;
          if (backend === "local") {
            await handleLocalDelete(path, recursive);
            return { deleted: true };
          }
          if (backend === "s3") {
            await handleS3Delete(bucket, key, options);
            return { deleted: true };
          }
          await handleGcsDelete(bucket, key, options);
          return { deleted: true };
        }
        case "mkdir": {
          if (backend !== "local") {
            throw new Error("mkdir is only supported for local backend");
          }
          await handleLocalMkdir(path);
          return { created: true };
        }
        case "rmdir": {
          if (backend !== "local") {
            throw new Error("rmdir is only supported for local backend");
          }
          await handleLocalRmdir(path, params.recursive ?? false);
          return { removed: true };
        }
        case "read_file": {
          if (!params.localPath) {
            throw new Error("Missing localPath for read_file");
          }
          if (backend === "local") {
            const bytes = await handleLocalReadFile(path, params.localPath);
            return { bytes };
          }
          if (backend === "s3") {
            const bytes = await handleS3ReadFile(bucket, key, params.localPath, options);
            return { bytes };
          }
          const bytes = await handleGcsReadFile(bucket, key, params.localPath, options);
          return { bytes };
        }
        case "signed_url": {
          if (backend === "local") {
            return { supported: false, url: null };
          }
          if (backend === "s3") {
            const client = getS3Client(options, bucket);
            const presignOptions: Record<string, unknown> = {};
            if (params.expiresIn) {
              presignOptions.expiresIn = params.expiresIn;
            }
            if (params.method) {
              presignOptions.method = params.method;
            }
            if (params.contentType) {
              presignOptions.type = params.contentType;
            }
            if (params.acl) {
              presignOptions.acl = params.acl;
            }
            const url = client.presign(key, presignOptions);
            return { supported: true, url };
          }
          return { supported: false, url: null };
        }
        default:
          throw new Error(`Unknown storage command: ${command}`);
      }
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  async begin(_token: string): Promise<void> {
    return;
  }

  async commit(_token: string): Promise<void> {
    return;
  }

  async rollback(_token: string): Promise<void> {
    return;
  }
}

class ExternalProviderPool extends BasePoolAdapter {
  name: string;
  private request: ShmRing;
  private response: ShmRing;
  private inUseCount = 0;

  constructor(name: string, reqRing: ShmRing, respRing: ShmRing, maxSize: number) {
    super(maxSize);
    this.name = name;
    this.request = reqRing;
    this.response = respRing;
  }

  inUse(): number {
    return this.inUseCount;
  }

  async acquire(): Promise<string> {
    const response = await this.call({ kind: "acquire", pool: this.name });
    this.inUseCount += 1;
    this.recordAcquire();
    return String(response.token);
  }

  async release(token: string): Promise<void> {
    await this.call({ kind: "release", pool: this.name, token });
    this.inUseCount = Math.max(0, this.inUseCount - 1);
    this.recordRelease();
  }

  async execute(token: string, sql: string, params: unknown[]): Promise<Record<string, unknown>> {
    return (await this.call({ kind: "execute", pool: this.name, token, sql, params })) as Record<
      string,
      unknown
    >;
  }

  async executeMany(
    token: string,
    sql: string,
    params: unknown[][],
  ): Promise<Record<string, unknown>> {
    return (await this.call({
      kind: "execute_many",
      pool: this.name,
      token,
      sql,
      params,
    })) as Record<string, unknown>;
  }

  async command(token: string, command: string, args: unknown[]): Promise<unknown> {
    return await this.call({ kind: "command", pool: this.name, token, command, args });
  }

  async releaseArrow(name: string): Promise<void> {
    await this.call({ kind: "arrow_release", name });
  }

  async begin(token: string): Promise<void> {
    await this.call({ kind: "begin", pool: this.name, token });
  }

  async commit(token: string): Promise<void> {
    await this.call({ kind: "commit", pool: this.name, token });
  }

  async rollback(token: string): Promise<void> {
    await this.call({ kind: "rollback", pool: this.name, token });
  }

  private async call(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    try {
      const encoded = encode(payload);
      this.request.send(encoded, 0, 5_000);
      const response = this.response.recv(5_000);
      if (!response) {
        throw new Error("Provider response timed out");
      }
      const decoded = decode(response.data) as Record<string, unknown>;
      if (decoded.ok === false) {
        const error = decoded.error as { message?: string } | undefined;
        throw new Error(error?.message ?? "Provider error");
      }
      return decoded;
    } catch (error) {
      this.recordError();
      throw error;
    }
  }
}

class Broker {
  private config: ResolvedBrokerConfig;
  private control: ShmRing;
  private clients = new Map<string, ClientState>();
  private pools = new Map<string, PoolState>();
  private arrowRegions = new Map<string, { region: ShmRegion; expiresAt: number }>();
  private providerArrows = new Map<string, string>();

  constructor(config: BrokerConfig) {
    const brokerConfig = config.broker ?? {};
    const name = brokerConfig.name ?? "vertebra-broker";
    this.config = {
      name,
      control_name: brokerConfig.control_name ?? `/${name}.control`,
      slot_size: brokerConfig.slot_size ?? 65_536,
      slots: brokerConfig.slots ?? 1024,
      timeout_ms: brokerConfig.timeout_ms ?? 5_000,
      arrow_threshold_bytes: brokerConfig.arrow_threshold_bytes ?? 1_048_576,
      arrow_threshold_rows: brokerConfig.arrow_threshold_rows ?? 1_000,
      arrow_region_ttl_ms: brokerConfig.arrow_region_ttl_ms ?? 30_000,
      scheduler_quantum: brokerConfig.scheduler_quantum ?? 1,
      scheduler_starvation_ms: brokerConfig.scheduler_starvation_ms ?? 2_000,
    };
    this.control = ShmRing.create(
      this.config.control_name,
      this.config.slot_size,
      this.config.slots,
    );
    for (const pool of config.pools ?? []) {
      this.registerPool(pool);
    }
  }

  async run(): Promise<void> {
    while (true) {
      const controlMessage = this.control.recv(10);
      if (controlMessage) {
        await this.handleControl(controlMessage.data);
      }
      for (const client of this.clients.values()) {
        const msg = client.request.recv(0);
        if (msg) {
          await this.handleRequest(client, msg.data);
        }
      }
      await this.drainPools();
      this.cleanupArrowRegions();
      await Bun.sleep(1);
    }
  }

  private registerPool(pool: BrokerPoolConfig): void {
    const name = pool.name;
    if (this.pools.has(name)) {
      return;
    }
    const max = pool.max_connections ?? 10;
    const scheduler = this.createScheduler();
    if (pool.driver === "bun-postgres") {
      const url = pool.connection_string ?? pool.url ?? "postgres://localhost/postgres";
      this.pools.set(name, {
        adapter: new SqlPool(name, "postgres", url, max, pool.options as Bun.SQL.Options),
        scheduler,
      });
      return;
    }
    if (pool.driver === "bun-mysql") {
      const url = pool.connection_string ?? pool.url ?? "mysql://localhost/mysql";
      this.pools.set(name, {
        adapter: new SqlPool(name, "mysql", url, max, pool.options as Bun.SQL.Options),
        scheduler,
      });
      return;
    }
    if (pool.driver === "bun-sqlite") {
      const path = pool.path ?? ":memory:";
      this.pools.set(name, { adapter: new SqlitePool(name, path, max), scheduler });
      return;
    }
    if (pool.driver === "bun-redis") {
      const url = pool.url ?? "redis://localhost:6379";
      this.pools.set(name, {
        adapter: new RedisPool(name, url, max, pool.options as Bun.RedisOptions),
        scheduler,
      });
      return;
    }
    if (pool.driver === "storage-local") {
      this.pools.set(name, {
        adapter: new StoragePool(name, "local", max, pool.bucket, pool.options),
        scheduler,
      });
      return;
    }
    if (pool.driver === "storage-s3") {
      this.pools.set(name, {
        adapter: new StoragePool(name, "s3", max, pool.bucket, pool.options),
        scheduler,
      });
      return;
    }
    if (pool.driver === "storage-gcs") {
      this.pools.set(name, {
        adapter: new StoragePool(name, "gcs", max, pool.bucket, pool.options),
        scheduler,
      });
      return;
    }
  }

  private createScheduler(): FairScheduler<BrokerRequest> {
    return new FairScheduler<BrokerRequest>({
      quantum: this.config.scheduler_quantum,
      starvationMs: this.config.scheduler_starvation_ms,
    });
  }

  private cleanupArrowRegions(now = Date.now()): void {
    for (const [name, entry] of this.arrowRegions.entries()) {
      if (entry.expiresAt <= now) {
        try {
          entry.region.close();
        } catch {
          // ignore cleanup failures
        }
        this.arrowRegions.delete(name);
      }
    }
  }

  private releaseArrowRegion(name: string): boolean {
    const entry = this.arrowRegions.get(name);
    if (!entry) {
      return false;
    }
    try {
      entry.region.close();
    } catch {
      // ignore
    }
    this.arrowRegions.delete(name);
    return true;
  }

  private async drainPools(): Promise<void> {
    const now = Date.now();
    for (const poolState of this.pools.values()) {
      const expired = poolState.scheduler.collectExpired(now);
      for (const entry of expired) {
        this.reply(entry.item.client, entry.item.id, {
          ok: false,
          error: { message: "Acquire request timed out" },
        });
      }
      while (
        poolState.adapter.maxSize() <= 0 ||
        poolState.adapter.inUse() < poolState.adapter.maxSize()
      ) {
        const entry = poolState.scheduler.next(now);
        if (!entry) {
          break;
        }
        const request = entry.item;
        try {
          const token = await poolState.adapter.acquire();
          this.reply(request.client, request.id, { ok: true, token });
        } catch (error) {
          this.reply(request.client, request.id, { ok: false, error: { message: String(error) } });
        }
      }
    }
  }

  private async handleControl(data: Uint8Array): Promise<void> {
    const message = decode(data) as Record<string, any>;
    if (message.kind === "register_client") {
      const clientId = String(message.client_id);
      const reqRing = String(message.req_ring);
      const respRing = String(message.resp_ring);
      const weight = Number(message.weight ?? 1);
      const request = ShmRing.open(reqRing, this.config.slot_size, this.config.slots);
      const response = ShmRing.open(respRing, this.config.slot_size, this.config.slots);
      this.clients.set(clientId, {
        id: clientId,
        request,
        response,
        weight: Number.isFinite(weight) ? weight : 1,
      });
      const payload = encode({ ok: true, id: message.id });
      response.send(payload, 0, this.config.timeout_ms);
      return;
    }
    if (message.kind === "register_provider") {
      return;
    }
  }

  private async handleRequest(client: ClientState, data: Uint8Array): Promise<void> {
    const message = decode(data) as Record<string, any>;
    const kind = String(message.kind || "");
    try {
      if (kind === "stats") {
        const poolStats: PoolStats[] = [];
        const schedulerPools: Array<{ name: string } & SchedulerStats> = [];
        for (const [name, poolState] of this.pools.entries()) {
          const stats = poolState.adapter.stats();
          const schedulerStats = poolState.scheduler.stats();
          stats.waiters = schedulerStats.pending;
          poolStats.push(stats);
          schedulerPools.push({ name, ...schedulerStats });
        }
        this.reply(client, message.id, {
          ok: true,
          stats: { pools: poolStats, scheduler: { pools: schedulerPools } },
        });
        return;
      }
      if (kind === "arrow_release") {
        const name = String(message.name || "");
        let released = name ? this.releaseArrowRegion(name) : false;
        if (!released && name) {
          const poolName = this.providerArrows.get(name);
          if (poolName) {
            const poolState = this.pools.get(poolName);
            if (poolState?.adapter instanceof ExternalProviderPool) {
              await poolState.adapter.releaseArrow(name);
              released = true;
            }
            this.providerArrows.delete(name);
          }
        }
        this.reply(client, message.id, { ok: released });
        return;
      }
      if (kind === "register_provider") {
        const name = String(message.pool || "");
        const reqRing = String(message.req_ring || "");
        const respRing = String(message.resp_ring || "");
        const maxSize = Number(message.max_size ?? 1);
        if (!name || !reqRing || !respRing) {
          this.reply(client, message.id, {
            ok: false,
            error: { message: "Invalid provider registration" },
          });
          return;
        }
        const request = ShmRing.open(reqRing, this.config.slot_size, this.config.slots);
        const response = ShmRing.open(respRing, this.config.slot_size, this.config.slots);
        this.pools.set(name, {
          adapter: new ExternalProviderPool(
            name,
            request,
            response,
            Number.isFinite(maxSize) ? maxSize : 1,
          ),
          scheduler: this.createScheduler(),
        });
        this.reply(client, message.id, { ok: true });
        return;
      }
      const poolName = String(message.pool || "");
      const poolState = this.pools.get(poolName);
      if (!poolState) {
        this.reply(client, message.id, { ok: false, error: { message: "Unknown pool" } });
        return;
      }
      if (kind === "acquire") {
        const timeoutMs = Number(message.timeout_ms ?? this.config.timeout_ms);
        const request: BrokerRequest = {
          id: Number(message.id),
          kind: "acquire",
          pool: poolName,
          client,
          payload: message,
          timeoutMs: Number.isFinite(timeoutMs) ? timeoutMs : this.config.timeout_ms,
          enqueuedAt: Date.now(),
        };
        poolState.scheduler.enqueue(client.id, client.weight, request, request.timeoutMs);
        return;
      }
      if (kind === "release") {
        await poolState.adapter.release(String(message.token));
        this.reply(client, message.id, { ok: true });
        return;
      }
      if (kind === "execute") {
        const result = await poolState.adapter.execute(
          String(message.token),
          String(message.sql),
          message.params ?? [],
        );
        this.reply(client, message.id, this.wrapResult(result, poolName));
        return;
      }
      if (kind === "execute_many") {
        const result = await poolState.adapter.executeMany(
          String(message.token),
          String(message.sql),
          message.params ?? [],
        );
        this.reply(client, message.id, this.wrapResult(result, poolName));
        return;
      }
      if (kind === "command") {
        const result = await poolState.adapter.command(
          String(message.token),
          String(message.command),
          message.args ?? [],
        );
        this.reply(client, message.id, { ok: true, result });
        return;
      }
      if (kind === "begin") {
        await poolState.adapter.begin(String(message.token));
        this.reply(client, message.id, { ok: true });
        return;
      }
      if (kind === "commit") {
        await poolState.adapter.commit(String(message.token));
        this.reply(client, message.id, { ok: true });
        return;
      }
      if (kind === "rollback") {
        await poolState.adapter.rollback(String(message.token));
        this.reply(client, message.id, { ok: true });
        return;
      }
      if (kind === "health") {
        this.reply(client, message.id, { ok: true });
        return;
      }
      this.reply(client, message.id, { ok: false, error: { message: `Unknown kind: ${kind}` } });
    } catch (error) {
      this.reply(client, message.id, { ok: false, error: { message: String(error) } });
    }
  }

  private wrapResult(result: Record<string, unknown>, poolName?: string): Record<string, unknown> {
    if ("arrow" in result) {
      const arrow = result.arrow as { name?: string; size?: number } | undefined;
      const rowcount = (result.rowcount as number) ?? 0;
      if (arrow?.name) {
        if (poolName) {
          this.providerArrows.set(arrow.name, poolName);
        }
        return { ok: true, arrow: { name: arrow.name, size: arrow.size ?? 0 }, rowcount };
      }
    }
    if ("arrow_bytes" in result) {
      const rowcount = (result.rowcount as number) ?? 0;
      return { ok: true, arrow_bytes: result.arrow_bytes, rowcount };
    }
    const rows = (result.rows as unknown[][]) ?? [];
    const columns = (result.columns as string[]) ?? [];
    const rowcount = (result.rowcount as number) ?? rows.length;
    const estimatedBytes = rows.length * columns.length * 8;
    if (
      rows.length >= this.config.arrow_threshold_rows ||
      estimatedBytes >= this.config.arrow_threshold_bytes
    ) {
      const records = rows.map((row) => {
        const obj: Record<string, unknown> = {};
        columns.forEach((col, idx) => {
          obj[col] = row[idx];
        });
        return obj;
      });
      const table = tableFromJSON(records);
      const ipc = tableToIPC(table);
      const name = `/${this.config.name}.arrow.${crypto.randomUUID()}`;
      const region = ShmRegion.create(name, ipc.byteLength);
      region.write(ipc);
      this.arrowRegions.set(name, {
        region,
        expiresAt: Date.now() + this.config.arrow_region_ttl_ms,
      });
      return { ok: true, arrow: { name, size: ipc.byteLength }, rowcount };
    }
    return { ok: true, result: { columns, rows, rowcount } };
  }

  private reply(client: ClientState, id: number, payload: Record<string, unknown>): void {
    const encoded = encode({ id, ...payload });
    client.response.send(encoded, 0, this.config.timeout_ms);
  }
}

function normalizeSqlResult(result: unknown): Record<string, unknown> {
  if (!Array.isArray(result)) {
    return { columns: [], rows: [], rowcount: 0 };
  }
  const rows = result as Record<string, unknown>[];
  const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
  const values = rows.map((row) => columns.map((col) => row[col]));
  return { columns, rows: values, rowcount: values.length };
}

function loadConfig(path?: string): BrokerConfig {
  if (!path) {
    return {};
  }
  const text = readFileSync(path, "utf-8");
  return parse(text) as BrokerConfig;
}

const args = process.argv.slice(2);
const configIndex = args.indexOf("--config");
const configPath = configIndex >= 0 ? args[configIndex + 1] : undefined;
const config = loadConfig(configPath);
const broker = new Broker(config);
await broker.run();
