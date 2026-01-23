import type { Metric } from "../server/prometheus";
import type { BasePoolAdapter } from "./base-pool";

const pools = new Set<BasePoolAdapter>();

export function registerPool(pool: BasePoolAdapter) {
  pools.add(pool);
}

export function collectBrokerMetrics(): Metric[] {
  const metrics: Metric[] = [];

  // Pool Connections (Gauge)
  metrics.push({
    name: "pool_connections_total",
    help: "Total connections by pool and state",
    type: "gauge",
    values: [],
  });

  // Acquisitions (Counter)
  metrics.push({
    name: "pool_acquires_total",
    help: "Total successful connection acquisitions",
    type: "counter",
    values: [],
  });

  // Errors (Counter)
  metrics.push({
    name: "pool_errors_total",
    help: "Total pool errors",
    type: "counter",
    values: [],
  });

  // Health Checks (Counter)
  metrics.push({
    name: "pool_health_checks_total",
    help: "Total health check outcomes",
    type: "counter",
    values: [],
  });

  // Idle Timeouts (Counter)
  metrics.push({
    name: "pool_idle_timeouts_total",
    help: "Total idle connections reaped",
    type: "counter",
    values: [],
  });

  for (const pool of pools) {
    const stats = pool.stats();
    const labels = { pool: stats.name };

    // Connections
    metrics[0].values.push(
      { value: stats.idle, labels: { ...labels, state: "idle" } },
      { value: stats.in_use, labels: { ...labels, state: "in_use" } },
    );

    // Acquires
    metrics[1].values.push({ value: stats.acquires_total, labels });

    // Errors
    metrics[2].values.push({ value: stats.errors_total, labels });

    // Health Checks (we tracked failures, success count is implicit/missing in base stats currently)
    // We only exposed health_failures_total in stats()
    metrics[3].values.push({
      value: stats.health_failures_total,
      labels: { ...labels, result: "failure" },
    });

    // Idle Timeouts
    metrics[4].values.push({ value: stats.idle_timeouts_total, labels });
  }

  return metrics;
}
