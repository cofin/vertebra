export type MetricType = "counter" | "gauge" | "histogram";

export interface Metric {
  name: string;
  help: string;
  type: MetricType;
  values: {
    value: number;
    labels?: Record<string, string>;
  }[];
}

export function formatPrometheus(metrics: Metric[]): string {
  const lines: string[] = [];

  for (const metric of metrics) {
    if (metric.values.length === 0) continue;

    lines.push(`# HELP ${metric.name} ${metric.help}`);
    lines.push(`# TYPE ${metric.name} ${metric.type}`);

    for (const { value, labels } of metric.values) {
      const labelStr = formatLabels(labels);
      lines.push(`${metric.name}${labelStr} ${value}`);
    }
  }

  return lines.join("\n") + "\n";
}

function formatLabels(labels?: Record<string, string>): string {
  if (!labels || Object.keys(labels).length === 0) {
    return "";
  }
  const parts = Object.entries(labels).map(
    ([k, v]) => `${k}="${escapeLabelValue(v)}"`
  );
  return `{${parts.join(", ")}}`;
}

function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}
