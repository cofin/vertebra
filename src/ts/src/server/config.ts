import { resolve as resolvePath } from "node:path";
import { pathToFileURL } from "node:url";

import { type IpcClientConfig, resolveIpcConfig } from "../ipc/client";

export type LogLevel = "debug" | "info" | "warn" | "error";
export type WorkerCount = number | "auto";
export type IpcProtocol = "msgpack";

export interface ServerSettings {
  host?: string;
  hostname?: string;
  port?: number;
  reusePort?: boolean;
  staticDir?: string;
  logLevel?: LogLevel;
  workers?: WorkerCount;
  drainTimeoutMs?: number;
}

export interface StaticConfig {
  dir?: string;
  maxAge?: number;
}

export interface WorkerConfig {
  count?: WorkerCount;
}

export interface PythonConfig {
  app?: string;
  workers?: WorkerCount;
  timeout?: number;
  venv?: string;
}

export interface BrokerConfig {
  name?: string;
  controlName?: string;
  slotSize?: number;
  slots?: number;
  timeoutMs?: number;
}

export interface ServerIpcConfig extends IpcClientConfig {
  protocol?: IpcProtocol;
}

export interface ServerConfig {
  server?: ServerSettings;
  static?: StaticConfig;
  ipc?: ServerIpcConfig;
  workers?: WorkerConfig;
  python?: PythonConfig;
  broker?: BrokerConfig;
  hostname?: string;
  port?: number;
  reusePort?: boolean;
  staticDir?: string;
  logLevel?: LogLevel;
}

export interface ResolvedServerSettings {
  hostname: string;
  port: number;
  reusePort: boolean;
  logLevel: LogLevel;
  staticDir?: string;
  drainTimeoutMs: number;
}

export interface ResolvedServerConfig {
  server: ResolvedServerSettings;
  ipc: ServerIpcConfig;
  workers: { count: number };
  static: StaticConfig;
  python: PythonConfig;
  broker: BrokerConfig;
}

export interface ResolveConfigOptions {
  configPath?: string;
  skipFile?: boolean;
}

const DEFAULT_SERVER: ResolvedServerSettings = {
  hostname: "127.0.0.1",
  port: 8000,
  reusePort: false,
  logLevel: "info",
  drainTimeoutMs: 5000,
};

export function defineConfig(config: ServerConfig): ServerConfig {
  return config;
}

type NormalizedConfig = {
  server: ServerSettings;
  static: StaticConfig;
  ipc: ServerIpcConfig;
  workers: WorkerConfig;
  python: PythonConfig;
  broker: BrokerConfig;
};

function readEnvNumber(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function readEnvBoolean(value: string | undefined): boolean | undefined {
  if (!value) {
    return undefined;
  }
  if (value === "1") {
    return true;
  }
  if (value === "0") {
    return false;
  }
  const normalized = value.toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }
  return undefined;
}

function readEnvWorkers(value: string | undefined): WorkerCount | undefined {
  if (!value) {
    return undefined;
  }
  if (value.toLowerCase() === "auto") {
    return "auto";
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function normalizeConfig(config: ServerConfig | undefined): NormalizedConfig {
  return {
    server: {
      host: config?.server?.host ?? config?.server?.hostname ?? config?.hostname,
      hostname: config?.server?.hostname,
      port: config?.server?.port ?? config?.port,
      reusePort: config?.server?.reusePort ?? config?.reusePort,
      staticDir: config?.server?.staticDir ?? config?.staticDir,
      logLevel: config?.server?.logLevel ?? config?.logLevel,
      workers: config?.server?.workers,
      drainTimeoutMs: config?.server?.drainTimeoutMs,
    },
    static: {
      dir: config?.static?.dir ?? config?.staticDir,
      maxAge: config?.static?.maxAge,
    },
    ipc: { ...(config?.ipc ?? {}) },
    workers: {
      count: config?.workers?.count,
    },
    python: {
      ...(config?.python ?? {}),
    },
    broker: {
      ...(config?.broker ?? {}),
    },
  };
}

function mergeNormalized(base: NormalizedConfig, override: NormalizedConfig): NormalizedConfig {
  return {
    server: { ...base.server, ...override.server },
    static: { ...base.static, ...override.static },
    ipc: { ...base.ipc, ...override.ipc },
    workers: { ...base.workers, ...override.workers },
    python: { ...base.python, ...override.python },
    broker: { ...base.broker, ...override.broker },
  };
}

function mergeConfigs(...configs: Array<ServerConfig | undefined | null>): NormalizedConfig {
  const empty: NormalizedConfig = {
    server: {},
    static: {},
    ipc: {},
    workers: {},
    python: {},
    broker: {},
  };
  return configs.reduce(
    (acc, config) => mergeNormalized(acc, normalizeConfig(config ?? undefined)),
    empty,
  );
}

function resolveWorkerCount(config: NormalizedConfig): number {
  const explicit = config.workers.count ?? config.server.workers ?? config.python.workers;
  if (typeof explicit === "number") {
    return Math.max(1, Math.floor(explicit));
  }
  const auto = typeof explicit === "string" && explicit.toLowerCase() === "auto";
  if (auto || explicit === undefined) {
    const available =
      typeof Bun !== "undefined" && typeof Bun.availableParallelism === "number"
        ? Bun.availableParallelism
        : (globalThis.navigator?.hardwareConcurrency ?? 1);
    return Math.max(1, Math.floor(available));
  }
  return 1;
}

function buildResolvedConfig(config: NormalizedConfig): ResolvedServerConfig {
  const workerCount = resolveWorkerCount(config);
  const ipcDefaults = resolveIpcConfig(config.ipc ?? {});
  const ipcProtocol = config.ipc?.protocol ?? "msgpack";
  if (ipcProtocol !== "msgpack") {
    throw new Error(`Unsupported IPC protocol: ${ipcProtocol}`);
  }
  const ipc: ServerIpcConfig = {
    ...ipcDefaults,
    protocol: ipcProtocol,
  };
  return {
    server: {
      hostname: config.server.host ?? config.server.hostname ?? DEFAULT_SERVER.hostname,
      port: config.server.port ?? DEFAULT_SERVER.port,
      reusePort: config.server.reusePort ?? DEFAULT_SERVER.reusePort,
      logLevel: config.server.logLevel ?? DEFAULT_SERVER.logLevel,
      staticDir: config.static.dir ?? config.server.staticDir,
      drainTimeoutMs: config.server.drainTimeoutMs ?? DEFAULT_SERVER.drainTimeoutMs,
    },
    ipc,
    workers: { count: workerCount },
    static: config.static ?? {},
    python: config.python ?? {},
    broker: {
      name: config.broker?.name ?? "vertebra-broker",
      controlName: config.broker?.controlName,
      slotSize: config.broker?.slotSize ?? 65_536,
      slots: config.broker?.slots ?? 1024,
      timeoutMs: config.broker?.timeoutMs ?? 5_000,
    },
  };
}

function loadEnvConfig(): ServerConfig {
  const envHost = process.env.VERTEBRA_HOST;
  const envPort = readEnvNumber(process.env.VERTEBRA_PORT);
  const envStatic = process.env.VERTEBRA_STATIC_DIR;
  const envReuse = readEnvBoolean(process.env.VERTEBRA_REUSE_PORT);
  const envLog = process.env.VERTEBRA_LOG_LEVEL as LogLevel | undefined;
  const envWorkers = readEnvWorkers(process.env.VERTEBRA_WORKERS);
  const envDrainTimeout = readEnvNumber(process.env.VERTEBRA_DRAIN_TIMEOUT_MS);

  const envIpcProtocol = process.env.VERTEBRA_IPC_PROTOCOL as IpcProtocol | undefined;
  const envIpcName = process.env.VERTEBRA_IPC_NAME;
  const envIpcSlotSize = readEnvNumber(process.env.VERTEBRA_IPC_SLOT_SIZE);
  const envIpcSlots = readEnvNumber(process.env.VERTEBRA_IPC_SLOTS);
  const envIpcTimeout = readEnvNumber(process.env.VERTEBRA_IPC_TIMEOUT_MS);
  const envIpcRetries = readEnvNumber(process.env.VERTEBRA_IPC_RETRIES);
  const envIpcRetryDelay = readEnvNumber(process.env.VERTEBRA_IPC_RETRY_DELAY_MS);
  const envIpcPoolSize = readEnvNumber(process.env.VERTEBRA_IPC_POOL_SIZE);
  const envIpcPoolMax = readEnvNumber(process.env.VERTEBRA_IPC_POOL_MAX);
  const envIpcAcquireTimeout = readEnvNumber(process.env.VERTEBRA_IPC_ACQUIRE_TIMEOUT_MS);
  const envPythonApp = process.env.VERTEBRA_PYTHON_APP;
  const envPythonWorkers = readEnvWorkers(process.env.VERTEBRA_PYTHON_WORKERS);
  const envPythonTimeout = readEnvNumber(process.env.VERTEBRA_PYTHON_TIMEOUT);
  const envPythonVenv = process.env.VERTEBRA_PYTHON_VENV;
  const envBrokerName = process.env.VERTEBRA_BROKER_NAME;
  const envBrokerControl = process.env.VERTEBRA_BROKER_CONTROL;
  const envBrokerSlotSize = readEnvNumber(process.env.VERTEBRA_BROKER_SLOT_SIZE);
  const envBrokerSlots = readEnvNumber(process.env.VERTEBRA_BROKER_SLOTS);
  const envBrokerTimeout = readEnvNumber(process.env.VERTEBRA_BROKER_TIMEOUT_MS);

  return {
    server: {
      host: envHost ?? undefined,
      port: envPort ?? undefined,
      reusePort: envReuse ?? undefined,
      logLevel: envLog ?? undefined,
      workers: envWorkers ?? undefined,
      drainTimeoutMs: envDrainTimeout ?? undefined,
    },
    static: {
      dir: envStatic ?? undefined,
    },
    ipc: {
      protocol: envIpcProtocol ?? undefined,
      name: envIpcName ?? undefined,
      slotSize: envIpcSlotSize ?? undefined,
      slots: envIpcSlots ?? undefined,
      timeoutMs: envIpcTimeout ?? undefined,
      retries: envIpcRetries ?? undefined,
      retryDelayMs: envIpcRetryDelay ?? undefined,
      poolSize: envIpcPoolSize ?? undefined,
      poolMax: envIpcPoolMax ?? undefined,
      acquireTimeoutMs: envIpcAcquireTimeout ?? undefined,
    },
    python: {
      app: envPythonApp ?? undefined,
      workers: envPythonWorkers ?? undefined,
      timeout: envPythonTimeout ?? undefined,
      venv: envPythonVenv ?? undefined,
    },
    broker: {
      name: envBrokerName ?? undefined,
      controlName: envBrokerControl ?? undefined,
      slotSize: envBrokerSlotSize ?? undefined,
      slots: envBrokerSlots ?? undefined,
      timeoutMs: envBrokerTimeout ?? undefined,
    },
  };
}

async function loadConfigFile(configPath?: string): Promise<ServerConfig | null> {
  const candidates: string[] = [];
  if (configPath) {
    candidates.push(configPath);
  } else if (process.env.VERTEBRA_CONFIG) {
    candidates.push(process.env.VERTEBRA_CONFIG);
  } else {
    candidates.push("vertebra.config.ts", "vertebra.config.js");
  }

  for (const candidate of candidates) {
    const resolved = resolvePath(process.cwd(), candidate);
    const file = Bun.file(resolved);
    if (await file.exists()) {
      const module = await import(pathToFileURL(resolved).href);
      const config = module.default ?? module.config ?? module.vertebraConfig;
      if (!config || typeof config !== "object") {
        throw new Error(`Invalid vertebra config export in ${resolved}`);
      }
      return config as ServerConfig;
    }
  }

  return null;
}

export function loadConfig(overrides: ServerConfig = {}): ResolvedServerConfig {
  const envConfig = loadEnvConfig();
  const merged = mergeConfigs(envConfig, overrides);
  return buildResolvedConfig(merged);
}

export async function resolveServerConfig(
  overrides: ServerConfig = {},
  options: ResolveConfigOptions = {},
): Promise<ResolvedServerConfig> {
  const envConfig = loadEnvConfig();
  const fileConfig = options.skipFile ? null : await loadConfigFile(options.configPath);
  const merged = mergeConfigs(fileConfig, envConfig, overrides);
  return buildResolvedConfig(merged);
}
