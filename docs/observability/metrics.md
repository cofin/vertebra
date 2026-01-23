# Observability & Metrics

Vertebra exposes a unified metrics endpoint compatible with Prometheus.

## Endpoint

**URL**: `GET /__metrics__`

## Key Metrics

### Broker (Connection Pools)
- `pool_connections_total{pool="name", state="idle|in_use"}`: Number of connections.
- `pool_health_checks_total`: Count of health checks performed.
- `pool_errors_total`: Count of pool-related errors.

### Server (HTTP)
- `http_requests_total`: Total HTTP requests served.
- `http_request_duration_seconds_sum`: Cumulative request duration.
- `http_connections_active`: Current active connections.

### MCP (AI Tools)
- `mcp_tool_calls_total{tool="name"}`: Total tool invocations.
- `mcp_tool_duration_seconds_sum`: Total execution time.
- `mcp_tool_errors_total`: Execution errors.

## Grafana Dashboard

A pre-built Grafana dashboard is available at `docs/grafana/vertebra-dashboard.json`.
