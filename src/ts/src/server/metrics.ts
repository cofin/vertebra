import { Counter, collectDefaultMetrics, Gauge, Histogram, Registry } from "prom-client";

import { AsyncBrokerClient, type BrokerStats } from "../broker";
import {
  type BrokerMetrics,
  createBrokerMetrics,
  updateBrokerMetrics as updateBrokerMetricsShared,
} from "../broker/metrics";
import type { ProcessManager } from "./process-manager";

export type ServerMetrics = BrokerMetrics & {
  requestsTotal: Counter;
  requestDurationSeconds: Histogram;
  activeRequests: Gauge;
  ipcLatencySeconds: Histogram;
  workerActive: Gauge;
  workerRestartsTotal: Counter;
};

export function createMetrics(): ServerMetrics {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry });

  const requestsTotal = new Counter({
    name: "vertebra_http_requests_total",
    help: "Total number of HTTP requests",
    labelNames: ["method", "status"],
    registers: [registry],
  });

  const requestDurationSeconds = new Histogram({
    name: "vertebra_http_request_duration_seconds",
    help: "HTTP request duration in seconds",
    labelNames: ["method", "status"],
    registers: [registry],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  });

  const activeRequests = new Gauge({
    name: "vertebra_http_active_requests",
    help: "Active in-flight HTTP requests",
    registers: [registry],
  });

  const ipcLatencySeconds = new Histogram({
    name: "vertebra_ipc_latency_seconds",
    help: "IPC request latency in seconds",
    labelNames: ["method"],
    registers: [registry],
    buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1],
  });

  const workerActive = new Gauge({
    name: "vertebra_worker_active",
    help: "Active Python workers",
    registers: [registry],
  });

  const workerRestartsTotal = new Counter({
    name: "vertebra_worker_restarts_total",
    help: "Total worker restarts",
    registers: [registry],
  });

  const brokerMetrics = createBrokerMetrics(registry);

  return {
    ...brokerMetrics,
    requestsTotal,
    requestDurationSeconds,
    activeRequests,
    ipcLatencySeconds,
    workerActive,
    workerRestartsTotal,
  };
}

export function trackRequest(metrics: ServerMetrics, method: string): (status: number) => void {
  metrics.activeRequests.inc();
  const end = metrics.requestDurationSeconds.startTimer({ method, status: "pending" });
  return (status: number) => {
    metrics.activeRequests.dec();
    metrics.requestsTotal.inc({ method, status: String(status) });
    end({ status: String(status) });
  };
}

export async function renderMetrics(metrics: ServerMetrics): Promise<string> {
  return metrics.registry.metrics();
}

export function updateWorkerMetrics(
  metrics: ServerMetrics,
  active: number,
  restarts: number,
): void {
  metrics.workerActive.set(active);
  metrics.workerRestartsTotal.reset();
  metrics.workerRestartsTotal.inc(restarts);
}

export function updateBrokerMetrics(metrics: ServerMetrics, stats: BrokerStats): void {
  updateBrokerMetricsShared(metrics, stats);
}

/**
 * Unified metrics registry that combines server metrics with broker and worker stats.
 */
export class MetricsRegistry {
  constructor(
    private metrics: ServerMetrics,
    private brokerClient?: AsyncBrokerClient,
    private workerManager?: ProcessManager | null,
  ) {}

  async render(): Promise<string> {
    await this.refresh();
    return renderMetrics(this.metrics);
  }

  async refresh(): Promise<void> {
    if (this.workerManager) {
      const stats = this.workerManager.getStats();
      updateWorkerMetrics(this.metrics, stats.active, stats.restarts);
    }
    if (this.brokerClient) {
      const stats = await this.safeBrokerStats();
      if (stats) {
        updateBrokerMetricsShared(this.metrics, stats);
      }
    }
  }

  private async safeBrokerStats(): Promise<BrokerStats | null> {
    try {
      return (await this.brokerClient?.stats()) ?? null;
    } catch {
      return null;
    }
  }
}
