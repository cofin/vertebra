import { collectDefaultMetrics, Gauge, Registry } from "prom-client";
import type { BrokerStats } from "./client";

export type BrokerMetrics = {
  registry: Registry;
  brokerPoolConnections: Gauge;
  brokerPoolOperations: Gauge;
  brokerPoolErrors: Gauge;
  brokerSchedulerQueueDepth: Gauge;
  brokerSchedulerWorkerRequests: Gauge;
  brokerSchedulerWorkerWaitSeconds: Gauge;
  brokerSchedulerWorkerStarvation: Gauge;
};

export function createBrokerMetrics(registry?: Registry): BrokerMetrics {
  const reg = registry || new Registry();
  if (!registry) {
    collectDefaultMetrics({ register: reg });
  }

  const brokerPoolConnections = new Gauge({
    name: "vertebra_broker_pool_connections",
    help: "Broker pool connection counts",
    labelNames: ["pool", "state"],
    registers: [reg],
  });

  const brokerPoolOperations = new Gauge({
    name: "vertebra_broker_pool_operations_total",
    help: "Broker pool operations",
    labelNames: ["pool", "operation"],
    registers: [reg],
  });

  const brokerPoolErrors = new Gauge({
    name: "vertebra_broker_pool_errors_total",
    help: "Broker pool errors",
    labelNames: ["pool"],
    registers: [reg],
  });

  const brokerSchedulerQueueDepth = new Gauge({
    name: "vertebra_broker_scheduler_queue_depth",
    help: "Pending scheduler queue depth per pool",
    labelNames: ["pool"],
    registers: [reg],
  });

  const brokerSchedulerWorkerRequests = new Gauge({
    name: "vertebra_broker_scheduler_worker_requests_total",
    help: "Total scheduler requests per client",
    labelNames: ["pool", "client"],
    registers: [reg],
  });

  const brokerSchedulerWorkerWaitSeconds = new Gauge({
    name: "vertebra_broker_scheduler_worker_wait_seconds_total",
    help: "Total scheduler wait time per client in seconds",
    labelNames: ["pool", "client"],
    registers: [reg],
  });

  const brokerSchedulerWorkerStarvation = new Gauge({
    name: "vertebra_broker_scheduler_worker_starvation_events_total",
    help: "Scheduler starvation events per client",
    labelNames: ["pool", "client"],
    registers: [reg],
  });

  return {
    registry: reg,
    brokerPoolConnections,
    brokerPoolOperations,
    brokerPoolErrors,
    brokerSchedulerQueueDepth,
    brokerSchedulerWorkerRequests,
    brokerSchedulerWorkerWaitSeconds,
    brokerSchedulerWorkerStarvation,
  };
}

export function updateBrokerMetrics(metrics: BrokerMetrics, stats: BrokerStats): void {
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
