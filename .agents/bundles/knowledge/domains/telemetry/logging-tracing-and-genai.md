---
type: Guide
title: "Logging, Trace Context, SQL Trace Propagation & GenAI Metrics"
description: "The Rust log queue and trace-context primitives in `vertebra-core`, plus the Python `vertebra.telemetry` package for structured logging, W3C trace context, span buffering, SQL trace propagation, and GenAI metric samples."
status: stable
tags: [domains, telemetry, logging, tracing]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-logger-rs
    resource: src/rs/vertebra-core/src/logger.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/logger.rs)"
  - id: src-vertebra-core-src-telemetry-rs
    resource: src/rs/vertebra-core/src/telemetry.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/telemetry.rs)"
  - id: src-vertebra-py-src-telemetry-rs
    resource: src/rs/vertebra-py/src/telemetry.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/telemetry.rs)"
  - id: src-vertebra-http-src-access-log-rs
    resource: src/rs/vertebra-http/src/access_log.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/access_log.rs)"
  - id: src-telemetry-init-py
    resource: src/py/vertebra/telemetry/__init__.py
    title: "Python implementation module (src/py/vertebra/telemetry/__init__.py)"
  - id: src-telemetry-logger-py
    resource: src/py/vertebra/telemetry/logger.py
    title: "Python implementation module (src/py/vertebra/telemetry/logger.py)"
  - id: src-telemetry-otel-py
    resource: src/py/vertebra/telemetry/otel.py
    title: "Python implementation module (src/py/vertebra/telemetry/otel.py)"
  - id: src-telemetry-db-propagation-py
    resource: src/py/vertebra/telemetry/db_propagation.py
    title: "Python implementation module (src/py/vertebra/telemetry/db_propagation.py)"
  - id: src-src-telemetry-ts
    resource: src/ts/vertebra-client/src/telemetry.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/telemetry.ts)"
  - id: src-tests-unit-telemetry-test-telemetry-and-platform-py
    resource: src/py/tests/unit/telemetry/test_telemetry_and_platform.py
    title: "Python implementation module (src/py/tests/unit/telemetry/test_telemetry_and_platform.py)"
---

# Logging, Trace Context, SQL Trace Propagation & GenAI Metrics

<!-- truth: start -->
- `vertebra-core` provides batched JSONL logging (`AsyncLogCore`), W3C `traceparent` parsing and formatting (`W3cTraceContext`), and SQL literal sanitization (`sanitize_sql_statement`).
- `vertebra.telemetry` exposes `LogSink`, `LogHandler`, `TraceContext`, `SpanExporter`, `SqlTracePropagator` (`sqlcommenter`), and `GenAiRecorder` metrics.
<!-- truth: end -->

Telemetry code lives in separate places that do not call each other: `vertebra-core` Rust modules, the Python [`vertebra.telemetry`](../../../../../src/py/vertebra/telemetry/__init__.py)[^src-telemetry-init-py] package, and the TypeScript `@vertebra/client/telemetry` subpath. Each one implements W3C `traceparent` handling on its own.

> [!NOTE]
> Nothing in production code consumes these modules. `vertebra.telemetry` is imported only by [`test_telemetry_and_platform.py`](../../../../../src/py/tests/unit/telemetry/test_telemetry_and_platform.py)[^src-tests-unit-telemetry-test-telemetry-and-platform-py]. The Rust `logger.rs` and `telemetry.rs` modules are referenced only by their own `#[cfg(test)]` tests. No exporter ships spans or logs anywhere.

## 1. Rust Primitives (`vertebra-core`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`OverflowPolicy`](../../../../../src/rs/vertebra-core/src/logger.rs)[^src-vertebra-core-src-logger-rs] | `logger.rs` | `DropOldest`, `DropNewest`, `BlockTimeoutMs(u64)`. |
| `BoundedLogQueue` / `LoggerMetrics` | `logger.rs` | `Mutex<QueueState>` (`VecDeque<Vec<u8>>` plus monotonic `write_seq`, `evicted_seq`, `flushed_seq`, and `worker_busy`) with `not_empty`, `not_full`, and `flushed_cv` condvars. `push` applies the overflow policy; `drain_batch(max, timeout)` waits once and pops up to `max`; `mark_flushed(count)` advances `flushed_seq` and `metrics.flushed` only after `BufWriter::flush` completes and wakes `wait_flushed(timeout)` callers; `pending_len` reports exact un-flushed accepted records. |
| `AsyncLogCore` | `logger.rs` | `spawn(capacity, policy, flush_interval, sink)` starts a dedicated OS thread that drains batches of 256 into a 64 KiB `BufWriter`, appends a newline when one is missing, flushes the `BufWriter`, and then calls `mark_flushed`. Provides `spawn_stdout`, `write_bytes`, `flush`, `queue`, and `shutdown` (`Drop` calls `shutdown`). |
| [`W3cTraceContext`](../../../../../src/rs/vertebra-core/src/telemetry.rs)[^src-vertebra-core-src-telemetry-rs] | `telemetry.rs` | Byte-level `parse_traceparent` (rejects version `ff` and all-zero trace or span ids), `format_traceparent` (lowercase hex), `is_sampled`. |
| `SpanRecord` | `telemetry.rs` | `#[repr(C)]` fixed-layout span (ids, start, duration, status, kind, flags). No ring or exporter uses it. |
| `sanitize_sql_statement` | `telemetry.rs` | Collapses `IN (...)` to `IN (?)` with paren-depth tracking. Despite its doc comment, it does not touch `VALUES` lists. The `IN ` match is case-insensitive and has no word boundary, so `JOIN (subquery)` is also collapsed. |
| `format_utc_clf_timestamp` | `vertebra-http/src/access_log.rs` | Common Log Format timestamp for the access-log module, which `response::handle` does not call. |

## 2. Python Logging (`logger.py`)

| Symbol | Responsibility |
|---|---|
| `LogOverflowPolicy` | `Literal["drop_oldest", "drop_newest", "block"]`. |
| [`LoggingConfig`](../../../../../src/py/vertebra/telemetry/logger.py)[^src-telemetry-logger-py] | Frozen struct: `level`, `json_format`, `queue_capacity=8192`, `overflow_policy`, `access_log_enabled`, `access_log_format`. `from_pyproject(path)` reads `[tool.vertebra.logging]` and replaces unknown policies with `drop_oldest`. `extract_litestar_formatter` (static) rebuilds a callable formatter from a Litestar `LoggingConfig`: it uses `formatter`, then the `standard` entry of `formatters`, then a default format. |
| `LogSink` | Non-blocking byte sink backed by [`vertebra._vertebra.LogSink`](../../../../../src/rs/vertebra-py/src/telemetry.rs)[^src-vertebra-py-src-telemetry-rs] (`AsyncLogCore`, off-GIL `emit`, Condvar-synchronized `flush` / `drain`, `enqueued_count`, `dropped_count`). `build_structlog_processor` returns a final structlog processor that adds `level`, `timestamp`, `trace_id`, `span_id` and encodes with `encode_json_bytes`. `build_structlog_sink` accepts `bytes`, `str`, or `dict`. |
| `LogHandler` | `logging.Handler` that encodes `{timestamp, level, logger, message, trace_id?, span_id?}` with `encode_json_bytes` into a `LogSink`. Trace ids come from record attributes, then `TraceContext.current()`. |

## 3. Python Trace Context, Spans, GenAI (`otel.py`)

| Symbol | Responsibility |
|---|---|
| [`TraceContext`](../../../../../src/py/vertebra/telemetry/otel.py)[^src-telemetry-otel-py] | `msgspec.Struct(frozen=True, gc=False, kw_only=True, cache_hash=True)`: `trace_id`, `span_id` (lowercase hex), `trace_flags=1`, `version=0`. `parse(header)` validates field lengths and hex and rejects version `ff` and zero ids. Also `generate(sampled=)`, `new_child()`, `format()`, `is_sampled`. |
| `TraceContext.current` / `set_current` (static) | Read and bind the module-level `ContextVar` `_ACTIVE_TRACE_CTX`. `set_current` returns the reset `Token`. |
| `SpanData` / `SpanExporter` | Frozen `msgspec.Struct(gc=False, kw_only=True)` span record. `SpanExporter` is a `deque(maxlen=capacity)` (default 4096) with `record_span` and `drain_spans`. When full, the oldest span is evicted silently. |
| `GenAiMetricSample` / `GenAiRecorder` | Sample struct named after the OpenTelemetry `gen_ai.client.token.usage` / `gen_ai.client.operation.duration` metrics: `model`, `operation`, token counts, `duration_seconds`, `timestamp`. `record_generation(...)` appends to an unbounded list exposed as `samples`. |

## 4. SQL Trace Propagation (`db_propagation.py`)

[`SqlTracePropagator`](../../../../../src/py/vertebra/telemetry/db_propagation.py)[^src-telemetry-db-propagation-py] has only static methods:

- `apply_oracle_trace(conn, trace, *, module, action, client_identifier)` sets `python-oracledb` connection attributes `module` (48 chars), `action` (64), `ecid` (`trace_id:span_id`, 64), and optionally `client_identifier`. These attributes are sent with the next round trip, so tagging costs no extra RTT.
- `append_sqlcommenter(sql, trace, *, db_driver, route, controller)` appends a sorted, URL-quoted sqlcommenter comment (`db_driver`, `traceparent`, optional `controller`/`route`) before a trailing `;`. SQL that already contains `/*traceparent=` is returned unchanged.
- `sanitize_sql(sql)` uses regexes with `\b` boundaries: `IN (...)` → `IN (?)`, and multi-row `VALUES (...), (...)` → `VALUES (?)`. Lists containing nested parentheses are not collapsed. This Python version differs from the Rust `sanitize_sql_statement`.

## 5. TypeScript Client

[`src/ts/vertebra-client/src/telemetry.ts`](../../../../../src/ts/vertebra-client/src/telemetry.ts)[^src-src-telemetry-ts] exports a `TraceContext` class (`parse`, `format`, static `format`, `injectHeader`). It is built as the `./telemetry` package export and is also re-exported from the package index.

Provenance sources: [^src-vertebra-http-src-access-log-rs].

[^src-vertebra-core-src-logger-rs]: `src/rs/vertebra-core/src/logger.rs` — Rust implementation module (src/rs/vertebra-core/src/logger.rs).
[^src-vertebra-core-src-telemetry-rs]: `src/rs/vertebra-core/src/telemetry.rs` — Rust implementation module (src/rs/vertebra-core/src/telemetry.rs).
[^src-vertebra-py-src-telemetry-rs]: `src/rs/vertebra-py/src/telemetry.rs` — Rust implementation module (src/rs/vertebra-py/src/telemetry.rs).
[^src-vertebra-http-src-access-log-rs]: `src/rs/vertebra-http/src/access_log.rs` — Rust implementation module (src/rs/vertebra-http/src/access_log.rs).
[^src-telemetry-init-py]: `src/py/vertebra/telemetry/__init__.py` — Python implementation module (src/py/vertebra/telemetry/__init__.py).
[^src-telemetry-logger-py]: `src/py/vertebra/telemetry/logger.py` — Python implementation module (src/py/vertebra/telemetry/logger.py).
[^src-telemetry-otel-py]: `src/py/vertebra/telemetry/otel.py` — Python implementation module (src/py/vertebra/telemetry/otel.py).
[^src-telemetry-db-propagation-py]: `src/py/vertebra/telemetry/db_propagation.py` — Python implementation module (src/py/vertebra/telemetry/db_propagation.py).
[^src-src-telemetry-ts]: `src/ts/vertebra-client/src/telemetry.ts` — TypeScript client module (src/ts/vertebra-client/src/telemetry.ts).
[^src-tests-unit-telemetry-test-telemetry-and-platform-py]: `src/py/tests/unit/telemetry/test_telemetry_and_platform.py` — Python implementation module (src/py/tests/unit/telemetry/test_telemetry_and_platform.py).
