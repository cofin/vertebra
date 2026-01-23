import { formatPrometheus, type Metric } from "./prometheus";

export type MetricsCollector = () => Promise<Metric[]> | Metric[];

const collectors = new Set<MetricsCollector>();
let cachedMetrics: string | null = null;
let lastCollectionTime = 0;
const CACHE_TTL_MS = 5000;

export function registerCollector(collector: MetricsCollector) {
  collectors.add(collector);
}

// Server Metrics State
let requestCount = 0;
let requestDurationSum = 0;
let activeConnections = 0;

export function trackRequest(durationMs: number) {
  requestCount++;
  requestDurationSum += durationMs;
}

export function trackConnection(active: boolean) {
  if (active) activeConnections++;
  else activeConnections--;
}

export function collectServerMetrics(): Metric[] {
  return [
    {
      name: "http_requests_total",
      help: "Total HTTP requests served",
      type: "counter",
      values: [{ value: requestCount }]
    },
    {
      name: "http_request_duration_seconds_sum",
      help: "Total duration of all HTTP requests in seconds",
      type: "counter",
      values: [{ value: requestDurationSum / 1000 }]
    },
    {
      name: "http_connections_active",
      help: "Current active HTTP connections",
      type: "gauge",
      values: [{ value: activeConnections }]
    }
  ];
}

export async function metricsHandler(req: Request): Promise<Response> {
  const now = Date.now();
  if (cachedMetrics && now - lastCollectionTime < CACHE_TTL_MS) {
    return new Response(cachedMetrics, {
      headers: { "Content-Type": "text/plain; version=0.0.4" },
    });
  }

  const allMetrics: Metric[] = [];
  for (const collector of collectors) {
    try {
      const metrics = await collector();
      allMetrics.push(...metrics);
    } catch (error) {
      console.error("Error collecting metrics:", error);
    }
  }

  cachedMetrics = formatPrometheus(allMetrics);
  lastCollectionTime = now;

  return new Response(cachedMetrics, {
    headers: { "Content-Type": "text/plain; version=0.0.4" },
  });
}