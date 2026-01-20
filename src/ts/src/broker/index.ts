/**
 * Broker entrypoint: shared-memory pool management and broker client types.
 *
 * Pool adapters here represent broker-owned database/storage pools.
 * Worker IPC connection pools live under `src/ts/src/server/worker-connections.ts`.
 */
export { BasePoolAdapter, type PoolAdapter } from "./base-pool";
export {
  AsyncBrokerClient,
  type BrokerClientConfig,
  type BrokerStats,
  type PoolStats,
  type SchedulerClientStats,
  type SchedulerPoolStats,
} from "./client";
