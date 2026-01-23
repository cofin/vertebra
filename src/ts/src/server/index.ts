import type { Server, WebSocketHandler } from "bun";
import { AsyncBrokerClient } from "../broker";
import { ShmRouter } from "../broker/shm";
import { AsgiBridge } from "../ipc";
import { collectBrokerMetrics } from "../broker/metrics";
import { collectMcpMetrics } from "../mcp/metrics";
import { collectServerMetrics, metricsHandler, registerCollector, trackConnection, trackRequest } from "./metrics";
import { type ResolvedServerConfig, resolveServerConfig, type ServerConfig } from "./config";
import { log } from "./logging";
import { ProcessManager } from "./process-manager";
import { createRouter, RequestHandler } from "./router";
import {
  createIpcConnectionPool,
  type IpcConnectionPool,
  StubIpcConnectionPool,
} from "./worker-connections";

/**
 * Bun server entrypoint: HTTP server + ASGI bridge + IPC worker coordination.
 *
 * - ProcessManager supervises Python worker processes.
 * - IpcConnectionPool manages shared-memory IPC connections to workers.
 */
export class BunServer {
  private server: Server<unknown> | null = null;
  private shuttingDown = false;
  private shutdownAttached = false;

  constructor(
    private handler: RequestHandler,
    private config: ResolvedServerConfig["server"],
    private websocket?: WebSocketHandler<unknown>,
    private onStop?: () => Promise<void> | void,
  ) {}

  start(): this {
    if (this.server) {
      return this;
    }
    // Build options conditionally to satisfy Bun.serve overload requirements
    const baseOptions = {
      hostname: this.config.hostname,
      port: this.config.port,
      reusePort: this.config.reusePort,
      fetch: (request: Request, server: Server<unknown>) => this.handler(request, server),
    };
    if (this.websocket) {
      this.server = Bun.serve({ ...baseOptions, websocket: this.websocket });
    } else {
      // Cast needed: Bun.serve overloads require websocket/routes but fetch-only is valid
      this.server = Bun.serve(baseOptions as Parameters<typeof Bun.serve>[0]);
    }
    this.attachSignalHandlers();
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

  private attachSignalHandlers(): void {
    if (this.shutdownAttached) {
      return;
    }
    this.shutdownAttached = true;
    const handler = () => {
      void this.gracefulShutdown();
    };
    process.on("SIGTERM", handler);
    process.on("SIGINT", handler);
  }

  private async gracefulShutdown(): Promise<void> {
    if (this.shuttingDown) {
      return;
    }
    this.shuttingDown = true;
    const server = this.server;
    if (!server) {
      return;
    }
    server.stop(false);
    const drainTimeout = this.config.drainTimeoutMs ?? 5000;
    const stopPromise = Promise.resolve(this.onStop?.());
    let completed = false;
    void stopPromise.finally(() => {
      completed = true;
    });
    await Promise.race([stopPromise, sleep(drainTimeout)]);
    if (!completed) {
      server.stop(true);
    }
    this.server = null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function createServer(
  config: ServerConfig = {},
  handler: RequestHandler = () => new Response("Not Found", { status: 404 }),
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  return new BunServer(handler, resolved.server, undefined);
}

export async function createRouterServer(
  config: ServerConfig = {},
  handler?: RequestHandler,
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  const router = createRouter({
    staticDir: resolved.server.staticDir,
    apiHandler: handler,
    assets: resolved.assets,
  });
  return new BunServer(router, resolved.server, undefined);
}

export async function createAsgiServer(
  config: ServerConfig = {},
  pool?: IpcConnectionPool,
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);
  const workerPool =
    pool ?? createIpcConnectionPool({ count: resolved.workers.count, ipc: resolved.ipc });
  const manager = resolved.python.app
    ? new ProcessManager({
        count: resolved.workers.count,
        ipc: resolved.ipc,
        python: resolved.python,
      })
    : null;
  manager?.start();
  const routerInstance = new ShmRouter();
  const bridge = new AsgiBridge({ requester: workerPool, router: routerInstance });
  
  // Register metrics collectors
  registerCollector(collectServerMetrics);
  registerCollector(collectBrokerMetrics);
  registerCollector(collectMcpMetrics);

  const router = createRouter({
    staticDir: resolved.server.staticDir,
    assets: resolved.assets,
    apiHandler: async (request, server) => {
      trackConnection(true); // Increment active connection count (approx)
      const { request: tracedRequest, traceId } = ensureTraceId(request);
      const url = new URL(tracedRequest.url);
      
      try {
        if (url.pathname === "/__metrics__") {
          return await metricsHandler(request);
        }

        const start = performance.now();
        
        if (bridge.upgradeWebSocket(tracedRequest, server)) {
          log("info", "request.upgrade", {
            trace_id: traceId,
            method: tracedRequest.method,
            path: url.pathname,
          });
          return;
        }
        const response = await bridge.handle(tracedRequest);
        
        trackRequest(performance.now() - start);
        
        log("info", "request.complete", {
          trace_id: traceId,
          method: tracedRequest.method,
          path: url.pathname,
          status: response.status,
          duration_ms: Number((performance.now() - start).toFixed(3)),
        });
        return response;
      } catch (error) {
        trackRequest(performance.now() - (performance.now())); // Count as request even if error?
        log("error", "request.error", {
          trace_id: traceId,
          method: tracedRequest.method,
          path: url.pathname,
          error: String(error),
        });
        return new Response("Bad Gateway", { status: 502 });
      } finally {
        trackConnection(false); // Decrement active connection
      }
    },
  });
  const websocket = bridge.createWebSocketHandler();
  return new BunServer(router, resolved.server, websocket, async () => {
    await Promise.all([workerPool.close(), manager?.stop()]);
  });
}

function ensureTraceId(request: Request): { request: Request; traceId: string } {
  const headers = new Headers(request.headers);
  const existing = headers.get("x-request-id") ?? headers.get("traceparent");
  const traceId = existing ?? crypto.randomUUID();
  if (!headers.get("x-request-id")) {
    headers.set("x-request-id", traceId);
  }
  return { request: new Request(request, { headers }), traceId };
}

export const defaultWorkerPool = new StubIpcConnectionPool();

export {
  defineConfig,
  type IpcProtocol,
  loadConfig,
  type ResolvedServerConfig,
  resolveServerConfig,
  type ServerConfig,
  type ServerIpcConfig,
} from "./config";
export { ProcessManager } from "./process-manager";
export { createRouter, type RequestHandler } from "./router";
export {
  createIpcConnectionPool,
  type IpcConnectionPool,
  RoundRobinIpcConnectionPool,
  StubIpcConnectionPool,
} from "./worker-connections";
