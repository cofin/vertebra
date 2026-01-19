import { Counter, collectDefaultMetrics, Gauge, Histogram, Registry } from "prom-client";

import {
  BrokerClient,
  type BrokerStats,
  type PoolStats,
  type SchedulerClientStats,
  type SchedulerPoolStats,
} from "../broker";
import type { WorkerManager } from "./worker-manager";

export type ServerMetrics = {
  registry: Registry;
  requestsTotal: Counter;
  requestDurationSeconds: Histogram;
  activeRequests: Gauge;
  ipcLatencySeconds: Histogram;
  workerActive: Gauge;
  workerRestartsTotal: Counter;
  brokerPoolConnections: Gauge;
  brokerPoolOperations: Gauge;
  brokerPoolErrors: Gauge;
  brokerSchedulerQueueDepth: Gauge;
  brokerSchedulerWorkerRequests: Gauge;
  brokerSchedulerWorkerWaitSeconds: Gauge;
  brokerSchedulerWorkerStarvation: Gauge;
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

  const brokerPoolConnections = new Gauge({
    name: "vertebra_broker_pool_connections",
    help: "Broker pool connection counts",
    labelNames: ["pool", "state"],
    registers: [registry],
  });

  const brokerPoolOperations = new Gauge({
    name: "vertebra_broker_pool_operations_total",
    help: "Broker pool operations",
    labelNames: ["pool", "operation"],
    registers: [registry],
  });

  const brokerPoolErrors = new Gauge({
    name: "vertebra_broker_pool_errors_total",
    help: "Broker pool errors",
    labelNames: ["pool"],
    registers: [registry],
  });

  const brokerSchedulerQueueDepth = new Gauge({
    name: "vertebra_broker_scheduler_queue_depth",
    help: "Pending scheduler queue depth per pool",
    labelNames: ["pool"],
    registers: [registry],
  });

  const brokerSchedulerWorkerRequests = new Gauge({
    name: "vertebra_broker_scheduler_worker_requests_total",
    help: "Total scheduler requests per client",
    labelNames: ["pool", "client"],
    registers: [registry],
  });

  const brokerSchedulerWorkerWaitSeconds = new Gauge({
    name: "vertebra_broker_scheduler_worker_wait_seconds_total",
    help: "Total scheduler wait time per client in seconds",
    labelNames: ["pool", "client"],
    registers: [registry],
  });

  const brokerSchedulerWorkerStarvation = new Gauge({
    name: "vertebra_broker_scheduler_worker_starvation_events_total",
    help: "Scheduler starvation events per client",
    labelNames: ["pool", "client"],
    registers: [registry],
  });

  return {
    registry,
    requestsTotal,
    requestDurationSeconds,
    activeRequests,
    ipcLatencySeconds,
    workerActive,
    workerRestartsTotal,
    brokerPoolConnections,
    brokerPoolOperations,
    brokerPoolErrors,
    brokerSchedulerQueueDepth,
    brokerSchedulerWorkerRequests,
    brokerSchedulerWorkerWaitSeconds,
    brokerSchedulerWorkerStarvation,
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
  metrics.brokerPoolConnections.reset();
  metrics.brokerPoolOperations.reset();
  metrics.brokerPoolErrors.reset();
  metrics.brokerSchedulerQueueDepth.reset();
  metrics.brokerSchedulerWorkerRequests.reset();
  metrics.brokerSchedulerWorkerWaitSeconds.reset();
  metrics.brokerSchedulerWorkerStarvation.reset();
  for (const pool of stats.pools) {
    metrics.brokerPoolConnections.set({ pool: pool.name, state: "total" }, pool.total);
    metrics.brokerPoolConnections.set({ pool: pool.name, state: "idle" }, pool.idle);
    metrics.brokerPoolConnections.set({ pool: pool.name, state: "in_use" }, pool.in_use);
    metrics.brokerPoolConnections.set({ pool: pool.name, state: "waiters" }, pool.waiters);
    metrics.brokerPoolOperations.set(
      { pool: pool.name, operation: "acquire" },
      pool.acquires_total,
    );
    metrics.brokerPoolOperations.set(
      { pool: pool.name, operation: "release" },
      pool.releases_total,
    );
    metrics.brokerPoolErrors.set({ pool: pool.name }, pool.errors_total);
  }
  for (const pool of stats.scheduler?.pools ?? []) {
    metrics.brokerSchedulerQueueDepth.set({ pool: pool.name }, pool.pending);
    for (const client of pool.clients) {
      metrics.brokerSchedulerWorkerRequests.set(
        { pool: pool.name, client: client.id },
        client.requestsTotal,
      );
      metrics.brokerSchedulerWorkerWaitSeconds.set(
        { pool: pool.name, client: client.id },
        client.waitTimeMsTotal / 1000,
      );
      metrics.brokerSchedulerWorkerStarvation.set(
        { pool: pool.name, client: client.id },
        client.starvationEvents,
      );
    }
  }
}

/**
 * Unified metrics registry that combines server metrics with broker and worker stats.
 */
export class MetricsRegistry {
  constructor(
    private metrics: ServerMetrics,
    private brokerClient?: BrokerClient,
    private workerManager?: WorkerManager | null,
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
        updateBrokerMetrics(this.metrics, stats);
      }
    }
  }

  private async safeBrokerStats(): Promise<BrokerStats | null> {
    try {
      return await this.brokerClient?.stats();
    } catch {
      return null;
    }
  }
}
