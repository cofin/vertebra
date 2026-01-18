import { resolveServerConfig, type ResolvedServerConfig, type ServerConfig } from "./config";
import { createRouter, RequestHandler } from "./router";
import { WorkerManager } from "./worker-manager";
import { createWorkerPool, StubWorkerPool, type WorkerPool } from "./worker-pool";
import { AsgiBridge } from "../ipc";

export class BunServer {
  private server: Server | null = null;

  constructor(
    private handler: RequestHandler,
    private config: ResolvedServerConfig["server"],
    private onStop?: () => Promise<void> | void,
  ) {}

  start(): this {
    if (this.server) {
      return this;
    }
    this.server = Bun.serve({
      hostname: this.config.hostname,
      port: this.config.port,
      reusePort: this.config.reusePort,
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
    if (this.onStop) {
      const result = this.onStop();
      if (result && typeof (result as Promise<void>).catch === "function") {
        (result as Promise<void>).catch((error) => {
          console.error("Server shutdown hook failed", error);
        });
      }
    }
  }

  get url(): string {
    if (!this.server) {
      throw new Error("Server is not started");
    }
    return this.server.url.toString();
  }
}

export async function createServer(
  config: ServerConfig = {},
  handler: RequestHandler = () => new Response("Not Found", { status: 404 }),
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  return new BunServer(handler, resolved.server);
}

export async function createRouterServer(config: ServerConfig = {}, handler?: RequestHandler): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  const router = createRouter({ staticDir: resolved.server.staticDir, apiHandler: handler });
  return new BunServer(router, resolved.server);
}

export async function createAsgiServer(config: ServerConfig = {}, pool?: WorkerPool): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  if (resolved.ipc.protocol === "protobuf") {
    throw new Error("Protobuf IPC protocol is not implemented yet");
  }
  const workerPool = pool ?? createWorkerPool({ count: resolved.workers.count, ipc: resolved.ipc });
  const manager = resolved.python.app
    ? new WorkerManager({ count: resolved.workers.count, ipc: resolved.ipc, python: resolved.python })
    : null;
  manager?.start();
  const bridge = new AsgiBridge({ requester: workerPool });
  const router = createRouter({ staticDir: resolved.server.staticDir, apiHandler: (request) => bridge.handle(request) });
  return new BunServer(router, resolved.server, async () => {
    await Promise.all([workerPool.close(), manager?.stop()]);
  });
}

export const defaultWorkerPool = new StubWorkerPool();

export {
  defineConfig,
  loadConfig,
  resolveServerConfig,
  type IpcProtocol,
  type ResolvedServerConfig,
  type ServerConfig,
  type ServerIpcConfig,
} from "./config";
export { createRouter, type RequestHandler } from "./router";
export { createWorkerPool, RoundRobinWorkerPool, StubWorkerPool, type WorkerPool } from "./worker-pool";
export { WorkerManager } from "./worker-manager";
