export type LogLevel = "debug" | "info" | "warn" | "error";

export interface ServerConfig {
  hostname?: string;
  port?: number;
  reusePort?: boolean;
  staticDir?: string;
  logLevel?: LogLevel;
}

const DEFAULT_CONFIG: Required<Pick<ServerConfig, "hostname" | "port" | "reusePort" | "logLevel">> = {
  hostname: "127.0.0.1",
  port: 8000,
  reusePort: false,
  logLevel: "info",
};

function readEnvNumber(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function loadConfig(overrides: ServerConfig = {}): ServerConfig {
  const envHost = process.env.VERTEBRA_HOST;
  const envPort = readEnvNumber(process.env.VERTEBRA_PORT);
  const envStatic = process.env.VERTEBRA_STATIC_DIR;
  const envReuse = process.env.VERTEBRA_REUSE_PORT;
  const envLog = process.env.VERTEBRA_LOG_LEVEL as LogLevel | undefined;

  return {
    hostname: overrides.hostname ?? envHost ?? DEFAULT_CONFIG.hostname,
    port: overrides.port ?? envPort ?? DEFAULT_CONFIG.port,
    reusePort:
      overrides.reusePort ??
      (envReuse ? envReuse === "1" || envReuse.toLowerCase() === "true" : DEFAULT_CONFIG.reusePort),
    staticDir: overrides.staticDir ?? envStatic,
    logLevel: overrides.logLevel ?? envLog ?? DEFAULT_CONFIG.logLevel,
  };
}
