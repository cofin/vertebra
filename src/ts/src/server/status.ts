/**
 * Server status endpoint for banner display.
 *
 * This module provides the /internal/status endpoint that returns
 * component health information for Python CLI banner rendering.
 *
 * The health check validates actual IPC connectivity by sending a
 * lightweight ping to workers before reporting "ready" status.
 */

import type { BrokerServer } from "../broker/server";
import type { IpcConnectionPool } from "./worker-connections";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Status level for components.
 */
export type StatusLevel = "ready" | "starting" | "degraded" | "error";

/**
 * Status of the Bun engine component.
 */
export interface BunStatus {
  status: StatusLevel;
  version: string;
  uptime_ms: number;
}

/**
 * Status of the Broker component.
 */
export interface BrokerStatus {
  status: StatusLevel;
  clients: number;
  pools: Record<string, number>;
}

/**
 * Status of the IPC component.
 */
export interface IpcStatus {
  status: StatusLevel;
  channels: number;
  latency_p99_us?: number;
}

/**
 * Complete server status response.
 */
export interface ServerStatus {
  bun: BunStatus;
  broker: BrokerStatus;
  ipc: IpcStatus;
}

// Track server start time for uptime calculation
const serverStartTime = performance.now();

/**
 * Create a status handler for the /internal/status endpoint.
 *
 * The handler validates actual IPC connectivity by sending a lightweight
 * ping to workers. Returns HTTP 200 only when IPC is functional,
 * HTTP 503 otherwise.
 *
 * @param broker - The broker server instance (optional)
 * @param pool - The IPC connection pool (optional)
 * @returns A request handler that returns server status as JSON
 */
export function createStatusHandler(
  broker?: BrokerServer,
  pool?: IpcConnectionPool,
): (req: Request) => Promise<Response> {
  return async () => {
    const uptimeMs = Math.round(performance.now() - serverStartTime);

    // Actually test IPC connectivity
    let ipcStatus: StatusLevel = "starting";
    let ipcChannels = 0;
    let ipcLatencyUs: number | undefined;

    if (pool) {
      try {
        const start = performance.now();
        // Send lightweight ping to any available worker
        const pingResult = await Promise.race([
          pingWorkerViaIpc(pool),
          sleep(2000).then(() => {
            throw new Error("timeout");
          }),
        ]);
        if (pingResult.ok) {
          ipcStatus = "ready";
          ipcChannels = pool.size;
          ipcLatencyUs = Math.round((performance.now() - start) * 1000);
        }
      } catch {
        ipcStatus = "error";
      }
    }

    // Build status response
    const status: ServerStatus = {
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: uptimeMs,
      },
      broker: {
        status: broker ? "ready" : "starting",
        clients: 0, // Would need broker API to get actual count
        pools: {}, // Would need broker API to get pool stats
      },
      ipc: {
        status: ipcStatus,
        channels: ipcChannels,
        latency_p99_us: ipcLatencyUs,
      },
    };

    // Return appropriate HTTP status based on IPC health
    const httpStatus = ipcStatus === "ready" ? 200 : 503;

    return new Response(JSON.stringify(status), {
      status: httpStatus,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
      },
    });
  };
}

/**
 * Ping a worker via IPC to verify connectivity.
 *
 * Sends a minimal ping scope that workers handle without invoking the ASGI app.
 */
async function pingWorkerViaIpc(pool: IpcConnectionPool): Promise<{ ok: boolean }> {
  try {
    // Send a ping request - workers handle this specially
    const response = await pool.request<{ ok?: boolean }>({
      type: "vertebra.ping",
      id: crypto.randomUUID(),
    });
    return { ok: response?.ok !== false };
  } catch {
    return { ok: false };
  }
}

/**
 * Simple status handler that doesn't require external dependencies.
 * Useful for standalone or testing scenarios.
 */
export function createSimpleStatusHandler(): (req: Request) => Promise<Response> {
  return async () => {
    const uptimeMs = Math.round(performance.now() - serverStartTime);

    const status: ServerStatus = {
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: uptimeMs,
      },
      broker: {
        status: "ready",
        clients: 0,
        pools: {},
      },
      ipc: {
        status: "ready",
        channels: 0,
      },
    };

    return new Response(JSON.stringify(status), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
      },
    });
  };
}
