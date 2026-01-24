/**
 * Server status endpoint for banner display.
 *
 * This module provides the /internal/status endpoint that returns
 * component health information for Python CLI banner rendering.
 */

import type { BrokerServer } from "../broker/server";
import type { IpcConnectionPool } from "./worker-connections";

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
        status: pool ? "ready" : "starting",
        channels: pool ? pool.size : 0,
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
