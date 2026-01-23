import type { Metric } from "../server/prometheus";

const toolCalls = new Map<string, number>();
const toolErrors = new Map<string, number>();
const toolDuration = new Map<string, number>();

export function trackToolCall(name: string, durationMs: number, success: boolean) {
  toolCalls.set(name, (toolCalls.get(name) || 0) + 1);
  toolDuration.set(name, (toolDuration.get(name) || 0) + durationMs);
  
  if (!success) {
    toolErrors.set(name, (toolErrors.get(name) || 0) + 1);
  }
}

export function collectMcpMetrics(): Metric[] {
  const metrics: Metric[] = [];

  // Tool Calls
  const callMetric: Metric = {
    name: "mcp_tool_calls_total",
    help: "Total MCP tool invocations",
    type: "counter",
    values: []
  };
  
  for (const [name, count] of toolCalls) {
    callMetric.values.push({ value: count, labels: { tool: name } });
  }
  metrics.push(callMetric);

  // Tool Duration
  const durationMetric: Metric = {
    name: "mcp_tool_duration_seconds_sum",
    help: "Total execution time of MCP tools",
    type: "counter",
    values: []
  };

  for (const [name, duration] of toolDuration) {
    durationMetric.values.push({ value: duration / 1000, labels: { tool: name } });
  }
  metrics.push(durationMetric);

  // Tool Errors
  const errorMetric: Metric = {
    name: "mcp_tool_errors_total",
    help: "Total MCP tool execution errors",
    type: "counter",
    values: []
  };

  for (const [name, count] of toolErrors) {
    errorMetric.values.push({ value: count, labels: { tool: name } });
  }
  metrics.push(errorMetric);

  return metrics;
}
