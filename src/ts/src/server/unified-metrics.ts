import { BrokerClient, type BrokerStats } from "../broker";
import type { WorkerManager } from "./worker-manager";
import { renderMetrics, updateBrokerMetrics, updateWorkerMetrics, type ServerMetrics } from "./metrics";

export class UnifiedMetricsRegistry {
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
