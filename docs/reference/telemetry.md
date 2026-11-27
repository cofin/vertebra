# OpenTelemetry Spans, Metrics, SQL Propagation & Logging

`vertebra.telemetry` provides distributed tracing, GenAI token and cost metrics, database pool and event loop telemetry, SQL trace comment propagation, and structured JSON logging.

| Subsystem | Purpose |
| --- | --- |
| **Tracing & Span Export** | Propagate W3C `traceparent` contexts across HTTP, shared-memory rings, and worker lanes, and export OTLP spans. |
| **GenAI & DB Pool Metrics** | Record OpenTelemetry `gen_ai.*` token and cost metrics and `db.client.connections.*` pool gauges. |
| **Event Loop Telemetry** | Track per-shard scheduling lag histograms, active-task counts, `zuvloop` stats, and optional OpenTelemetry meters. |
| **SQL Trace Propagation** | Attach W3C `traceparent` comments or session variables to outbound SQL queries. |
| **Structured Logging** | Emit JSON or console logs enriched with `trace_id` and `span_id`. |

Install the optional `opentelemetry` extra to emit live `opentelemetry.metrics` instruments (`vertebra.loop.lag_ms` and `vertebra.loop.active_tasks`) from `EventLoopRecorder`:

=== "uv"

    ```bash
    uv add "vertebra-core[opentelemetry]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[opentelemetry]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[opentelemetry]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[opentelemetry]"
    ```

## Tracing & Span Export

::: vertebra.telemetry.otel.TraceContext
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.SpanData
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.SpanExporter
    options:
      heading_level: 3

## GenAI, Database Pool & Event Loop Metrics

::: vertebra.telemetry.otel.GenAiUsage
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.ModelPricing
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.GenAiRecorder
    options:
      heading_level: 3

::: vertebra.telemetry.otel.DbPoolMetricSample
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.DbPoolRecorder
    options:
      heading_level: 3

::: vertebra.telemetry.otel.EventLoopMetricSample
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.otel.EventLoopRecorder
    options:
      heading_level: 3
      members:
        - __init__
        - record
        - configure_zuvloop
        - lag_histogram
        - latest_by_shard
        - samples

## SQL Trace Propagation

::: vertebra.telemetry.db_propagation.SqlTracePropagator
    options:
      heading_level: 3

## Structured Logging

::: vertebra.telemetry.logger.LoggingConfig
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.logger.LogHandler
    options:
      heading_level: 3
