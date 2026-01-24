import { describe, expect, it } from "bun:test";
import { createAsgiServer, defaultWorkerPool } from "../../src/server";
import { registerCollector } from "../../src/server/metrics";
import { registerPool, collectBrokerMetrics } from "../../src/broker/metrics";
import { BasePoolAdapter } from "../../src/broker/base-pool";

class DummyPool extends BasePoolAdapter {
    name = "dummy";
    inUse() { return 1; }
    async acquire() { return "token"; }
    async release() {}
    async execute() { return {}; }
    async executeMany() { return {}; }
    async command() { return null; }
    async begin() {}
    async commit() {}
    async rollback() {}
    protected async runHealthCheck() {}
    protected async reapIdleConnections() {}
    protected async validateConnection() { return true; }
}

describe("Metrics Integration", () => {
  it("should expose unified metrics endpoint", async () => {
    // Manually register a test collector to ensure we have data
    registerCollector(() => [{
      name: "integration_test_metric",
      help: "Test metric",
      type: "counter",
      values: [{ value: 1 }]
    }]);

    // Register a dummy pool to verify broker metrics are collected
    registerPool(new DummyPool(10));

    // Register a dummy MCP metric source if needed, or check if collectMcpMetrics returns empty list gracefully
    // collectMcpMetrics returns 3 metrics even if empty maps (with empty values list), so they might not appear in output if formatPrometheus skips empty values.
    // formatPrometheus: if (metric.values.length === 0) continue;

    // So we need to trigger an MCP tool call or mock trackToolCall to see MCP metrics.
    const { trackToolCall } = await import("../../src/mcp/metrics");
    trackToolCall("dummy_tool", 100, true);

    const server = await createAsgiServer({
      server: { port: 0 },
      workers: { count: 1 },
    }, defaultWorkerPool);

    const { metricsHandler } = await import("../../src/server/metrics");
    const res = await metricsHandler(new Request("http://localhost/__metrics__"));

    expect(res.status).toBe(200);
    const text = await res.text();

    expect(text).toContain("integration_test_metric");
    expect(text).toContain("http_requests_total");
    expect(text).toContain("pool_connections_total");
    expect(text).toContain('pool="dummy"');
    expect(text).toContain("mcp_tool_calls_total");
    expect(text).toContain('tool="dummy_tool"');
  });
});
