# `vertebra.telemetry` — OpenTelemetry Spans, Metrics, SQL Propagation & Logging

Distributed tracing (`TraceContext`, `SpanExporter`), GenAI token/cost instrumentation (`GenAiRecorder`), database connection pool telemetry (`DbPoolRecorder`), SQL `sqlcommenter` trace propagation (`SqlTracePropagator`), and structured JSON logging (`LoggingConfig`).

## Tracing & Span Export (`vertebra.telemetry.otel`)

::: vertebra.telemetry.otel.TraceContext
    options:
      heading_level: 3
      members: false

::: vertebra.telemetry.otel.SpanData
    options:
      heading_level: 3
      members: false

::: vertebra.telemetry.otel.SpanExporter
    options:
      heading_level: 3

## GenAI & Database Pool Metrics (`vertebra.telemetry.otel`)

::: vertebra.telemetry.otel.GenAiUsage
    options:
      heading_level: 3
      members: false

::: vertebra.telemetry.otel.ModelPricing
    options:
      heading_level: 3
      members: false

::: vertebra.telemetry.otel.GenAiRecorder
    options:
      heading_level: 3

::: vertebra.telemetry.otel.DbPoolMetricSample
    options:
      heading_level: 3
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
      heading_level: 3
      members: false

::: vertebra.telemetry.logger.LogHandler
    options:
      heading_level: 3
