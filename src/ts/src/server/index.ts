import { loadConfig, ServerConfig } from "./config";
import { createRouter, RequestHandler } from "./router";
import { StubWorkerPool } from "./worker-pool";
import { AsgiBridge, IpcClientConfig } from "../ipc";

export class BunServer {
  private server: Server | null = null;

  constructor(private handler: RequestHandler, private config: ServerConfig) {}

  start(): this {
    if (this.server) {
      return this;
    }
    const config = loadConfig(this.config);
    this.server = Bun.serve({
      hostname: config.hostname,
      port: config.port,
      reusePort: config.reusePort,
      fetch: this.handler,
    });
    return this;
  }

  stop(closeConnections = true): void {
    if (!this.server) {
      return;
    }
    this.server.stop(closeConnections);
    this.server = null;
  }

  get url(): string {
    if (!this.server) {
      throw new Error("Server is not started");
    }
    return this.server.url.toString();
  }
}

export function createServer(
  config: ServerConfig = {},
  handler: RequestHandler = () => new Response("Not Found", { status: 404 }),
): BunServer {
  return new BunServer(handler, config);
}

export function createRouterServer(config: ServerConfig = {}, handler?: RequestHandler): BunServer {
  const resolved = loadConfig(config);
  const router = createRouter({ staticDir: resolved.staticDir, apiHandler: handler });
  return new BunServer(router, resolved);
}

export function createAsgiServer(
  config: ServerConfig = {},
  ipcConfig: IpcClientConfig = { transport: "tcp", host: "127.0.0.1", port: 8008 },
): BunServer {
  const resolved = loadConfig(config);
  const bridge = new AsgiBridge(ipcConfig);
  const router = createRouter({ staticDir: resolved.staticDir, apiHandler: (request) => bridge.handle(request) });
  return new BunServer(router, resolved);
}

export const defaultWorkerPool = new StubWorkerPool();

export { loadConfig } from "./config";
export { createRouter, type RequestHandler } from "./router";
export { StubWorkerPool } from "./worker-pool";
