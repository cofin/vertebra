# `vertebra.telemetry` — OpenTelemetry Spans, Metrics, SQL Propagation & Logging

Distributed tracing (`TraceContext`, `SpanData`, `SpanExporter`), GenAI token/cost instrumentation (`GenAiUsage`, `ModelPricing`, `GenAiRecorder`), database connection pool telemetry (`DbPoolMetricSample`, `DbPoolRecorder`), SQL `sqlcommenter` trace propagation (`SqlTracePropagator`), and structured JSON logging (`LoggingConfig`, `LogHandler`).

| Subsystem | Classes | Purpose |
| --- | --- | --- |
| **Tracing & Span Export** | `TraceContext`, `SpanData`, `SpanExporter` | Propagate W3C `traceparent` contexts across HTTP, SHM rings, and worker lanes and export OTLP spans |
| **GenAI & DB Pool Metrics** | `GenAiUsage`, `ModelPricing`, `GenAiRecorder`, `DbPoolMetricSample`, `DbPoolRecorder` | Record OpenTelemetry `gen_ai.*` token/cost metrics and `db.client.connections.*` pool utilization |
| **SQL Trace Propagation** | `SqlTracePropagator` | Inject W3C `traceparent` comments (`/*traceparent='...'*/`) or session GUCs into outbound SQL queries |
| **Structured Logging** | `LoggingConfig`, `LogHandler` | Emit JSON or console logs automatically enriched with `trace_id` and `span_id` |

## Tracing & Span Export (`vertebra.telemetry.otel`)

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

## GenAI & Database Pool Metrics (`vertebra.telemetry.otel`)

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

## SQL Trace Propagation (`vertebra.telemetry.db_propagation`)

::: vertebra.telemetry.db_propagation.SqlTracePropagator
    options:
      heading_level: 3

## Structured Logging (`vertebra.telemetry.logger`)

::: vertebra.telemetry.logger.LoggingConfig
    options:
      heading_level: 4
      members: false

::: vertebra.telemetry.logger.LogHandler
    options:
      heading_level: 3
