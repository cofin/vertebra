---
type: Guide
title: "Logging, Trace Context, SQL Trace Propagation & GenAI Metrics"
description: "The Rust log queue and trace-context primitives in vertebra-core, plus the Python vertebra.telemetry package and @vertebra/client/telemetry subpath for structured logging, W3C trace context, span buffering, SQL trace propagation, and GenAI metrics."
status: stable
tags:
  - domains
  - telemetry
  - logging
  - tracing
  - genai
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:02:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
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
  - id: src-telemetry-genai-py
    resource: src/py/vertebra/telemetry/genai.py
    title: "Python implementation module (src/py/vertebra/telemetry/genai.py)"
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
- `vertebra-core` provides batched JSONL logging (`AsyncLogCore`), W3C `traceparent` parsing and formatting (`W3cTraceContext`), and SQL literal sanitization (`sanitize_sql_statement`), exposed to Python through `vertebra._vertebra`.
- `vertebra.telemetry` exposes `LogSink`, `LogHandler`, `TraceContext`, `SpanExporter`, `SqlTracePropagator` (`sqlcommenter`), `DbPoolRecorder`, `GenAiUsage`, and `GenAiRecorder`, consumed across `agent`, `db`, `embeddings`, `extensions`, `framework.queues`, and `server.lanes`.
- `SpanExporter`, `GenAiRecorder`, and `DbPoolRecorder` guard their ring/sample buffers with per-instance `threading.Lock` instances for free-threaded safety, and `telemetry/genai.py` places its tuple `__all__` directly after top-level imports.
<!-- truth: end -->

Telemetry spans `vertebra-core` and `vertebra-py` Rust FFI primitives, the Python [`vertebra.telemetry`](../../../../../src/py/vertebra/telemetry/__init__.py)[^src-telemetry-init-py] package, and the TypeScript `@vertebra/client/telemetry` subpath. In production Python code, `vertebra.telemetry` is consumed across `vertebra.agent` (`AgentTelemetryRecorder`, `AgentRuntime`, `AgentChatController`, `MCPStreamableHttpClient`, and `MCPToolBridge`), `vertebra.db` (`_common.py` and the BigQuery and Spanner adapters via `TraceContext` and `SqlTracePropagator`), `vertebra.embeddings` (`GenAiUsage`), `vertebra.extensions` (`broker/plugin.py` `DbPoolRecorder` and `apex/openapi.py` `inject_w3c_trace_context`), `vertebra.framework.queues` (`TraceContext` propagation), and `vertebra.server.lanes` (`SpanData`, `SpanExporter`, `TraceContext`), with unit coverage in [`test_telemetry_and_platform.py`](../../../../../src/py/tests/unit/telemetry/test_telemetry_and_platform.py)[^src-tests-unit-telemetry-test-telemetry-and-platform-py].

## 1. Rust Primitives (`vertebra-core`, `vertebra-py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`OverflowPolicy`](../../../../../src/rs/vertebra-core/src/logger.rs)[^src-vertebra-core-src-logger-rs] | `logger.rs` | `DropOldest`, `DropNewest`, `BlockTimeoutMs(u64)`. |
| `BoundedLogQueue` / `LoggerMetrics` | `logger.rs` | `Mutex<QueueState>` (`VecDeque<Vec<u8>>` plus monotonic `write_seq`, `evicted_seq`, `flushed_seq`, and `worker_busy`) with `not_empty`, `not_full`, and `flushed_cv` condvars. `push` applies the overflow policy; `drain_batch(max, timeout)` waits once and pops up to `max`; `mark_flushed(count)` advances `flushed_seq` and `metrics.flushed` only after `BufWriter::flush` completes and wakes `wait_flushed(timeout)` callers; `pending_len` reports exact un-flushed accepted records. |
| `AsyncLogCore` | `logger.rs` | `spawn(capacity, policy, flush_interval, sink)` starts a dedicated OS thread that drains batches of 256 into a 64 KiB `BufWriter`, appends a newline when one is missing, flushes the `BufWriter`, and then calls `mark_flushed`. Provides `spawn_stdout`, `write_bytes`, `flush`, `queue`, and `shutdown` (`Drop` calls `shutdown`). |
| [`W3cTraceContext`](../../../../../src/rs/vertebra-core/src/telemetry.rs)[^src-vertebra-core-src-telemetry-rs] | `telemetry.rs` | Byte-level `parse_traceparent` (rejects version `ff` and all-zero trace or span ids), `format_traceparent` (lowercase hex), `is_sampled`; bound in `vertebra._vertebra` (`parse_traceparent`, `format_traceparent`). |
| `SpanRecord` | `telemetry.rs` | `#[repr(C)]` fixed-layout span (ids, start, duration, status, kind, flags). |
| `sanitize_sql_statement` | `telemetry.rs` | Collapses `IN (...)` to `IN (?)` with paren-depth tracking (`vertebra._vertebra.sanitize_sql`). |
| `format_utc_clf_timestamp` | `vertebra-http/src/access_log.rs` | Common Log Format UTC timestamp helper in `vertebra-http`. |

## 2. Python Logging (`logger.py`)

| Symbol | Responsibility |
|---|---|
| `LogOverflowPolicy` | `Literal["drop_oldest", "drop_newest", "block"]`. |
| [`LoggingConfig`](../../../../../src/py/vertebra/telemetry/logger.py)[^src-telemetry-logger-py] | Frozen struct: `level`, `json_format`, `queue_capacity=8192`, `overflow_policy`, `access_log_enabled`, `access_log_format`. `from_pyproject(path)` reads `[tool.vertebra.logging]` and replaces unknown policies with `drop_oldest`. `extract_litestar_formatter` (static) rebuilds a callable formatter from a Litestar `LoggingConfig`: it uses `formatter`, then the `standard` entry of `formatters`, then a default format. |
| `LogSink` | Non-blocking byte sink backed by [`vertebra._vertebra.LogSink`](../../../../../src/rs/vertebra-py/src/telemetry.rs)[^src-vertebra-py-src-telemetry-rs] (`AsyncLogCore`, off-GIL `emit`, Condvar-synchronized `flush` / `drain`, `enqueued_count`, `dropped_count`). `build_structlog_processor` returns a final structlog processor that adds `level`, `timestamp`, `trace_id`, `span_id` and encodes with `encode_json_bytes`. `build_structlog_sink` accepts `bytes`, `str`, or `dict`. |
| `LogHandler` | `logging.Handler` that encodes `{timestamp, level, logger, message, trace_id?, span_id?}` with `encode_json_bytes` into a `LogSink`. Trace ids come from record attributes, then `TraceContext.current()`. |

## 3. Python Trace Context, Spans, Pool & GenAI Metrics (`otel.py`, `genai.py`)

| Symbol | Responsibility |
|---|---|
| [`TraceContext`](../../../../../src/py/vertebra/telemetry/otel.py)[^src-telemetry-otel-py] | `msgspec.Struct(frozen=True, gc=False, kw_only=True, cache_hash=True)`: `trace_id`, `span_id` (lowercase hex), `trace_flags=1`, `version=0`. `parse(header)` validates field lengths and hex and rejects version `ff` and zero ids. Also `generate(sampled=)`, `new_child()`, `format()`, `is_sampled`. |
| `TraceContext.current` / `set_current` (static) | Read and bind the module-level `ContextVar` `_ACTIVE_TRACE_CTX`. `set_current` returns the reset `Token`. |
| `SpanData` / `SpanExporter` | Frozen `msgspec.Struct(gc=False, kw_only=True)` span record. `SpanExporter` wraps a `deque(maxlen=capacity)` (default 4096) guarded by `self._lock = threading.Lock()` across `record_span`, `get_spans`, and `drain_spans`; when full, the oldest span is evicted silently. |
| `DbPoolMetricSample` / `DbPoolRecorder` | Records lease acquisition duration, active leases, and wait queue metrics for `vertebra.extensions.broker`; `samples`, `record_sample`, and `drain_samples` synchronize `self._samples` under `self._lock = threading.Lock()`. |
| `GenAiMetricSample` / `GenAiRecorder` / [`GenAiUsage`](../../../../../src/py/vertebra/telemetry/genai.py)[^src-telemetry-genai-py] | OpenTelemetry `gen_ai.client.token.usage` and `gen_ai.client.operation.duration` sample/histogram types (`GenAiDurationHistogram`, `ModelPricing`, `inject_w3c_trace_context`). `GenAiRecorder` guards its bounded `deque(maxlen=max_samples)` with `self._lock = threading.Lock()` across `samples`, `record_generation`, `duration_histogram`, `total_cost`, `total_tokens`, and `drain`, and `telemetry/genai.py` places its tuple `__all__` at module top immediately after imports. |

## 4. SQL Trace Propagation (`db_propagation.py`)

[`SqlTracePropagator`](../../../../../src/py/vertebra/telemetry/db_propagation.py)[^src-telemetry-db-propagation-py] has only static methods:

- `apply_oracle_trace(conn, trace, *, module, action, client_identifier)` sets `python-oracledb` connection attributes `module` (48 chars), `action` (64), `ecid` (`trace_id:span_id`, 64), and optionally `client_identifier`. These attributes are sent with the next round trip, so tagging costs no extra RTT.
- `append_sqlcommenter(sql, trace, *, db_driver, route, controller)` appends a sorted, URL-quoted sqlcommenter comment (`db_driver`, `traceparent`, optional `controller`/`route`) before a trailing `;`. SQL that already contains `/*traceparent=` is returned unchanged.
- `sanitize_sql(sql)` uses regexes with `\b` boundaries: `IN (...)` → `IN (?)`, and multi-row `VALUES (...), (...)` → `VALUES (?)`. Lists containing nested parentheses are not collapsed. This Python version differs from the Rust `sanitize_sql_statement`.

## 5. TypeScript Client (`@vertebra/client/telemetry`)

[`src/ts/vertebra-client/src/telemetry.ts`](../../../../../src/ts/vertebra-client/src/telemetry.ts)[^src-src-telemetry-ts] exports `TraceContext` (`parse`, `format`, static `format`, `injectHeader`), consumed by `McpClient` and `A2aClient` in `@vertebra/client/agent` and re-exported from both `./telemetry` and the root `@vertebra/client` index.

Provenance sources: [^src-vertebra-http-src-access-log-rs].

[^src-vertebra-core-src-logger-rs]: `src/rs/vertebra-core/src/logger.rs` — Rust implementation module (src/rs/vertebra-core/src/logger.rs).
[^src-vertebra-core-src-telemetry-rs]: `src/rs/vertebra-core/src/telemetry.rs` — Rust implementation module (src/rs/vertebra-core/src/telemetry.rs).
[^src-vertebra-py-src-telemetry-rs]: `src/rs/vertebra-py/src/telemetry.rs` — Rust implementation module (src/rs/vertebra-py/src/telemetry.rs).
[^src-vertebra-http-src-access-log-rs]: `src/rs/vertebra-http/src/access_log.rs` — Rust implementation module (src/rs/vertebra-http/src/access_log.rs).
[^src-telemetry-init-py]: `src/py/vertebra/telemetry/__init__.py` — Python implementation module (src/py/vertebra/telemetry/__init__.py).
[^src-telemetry-logger-py]: `src/py/vertebra/telemetry/logger.py` — Python implementation module (src/py/vertebra/telemetry/logger.py).
[^src-telemetry-otel-py]: `src/py/vertebra/telemetry/otel.py` — Python implementation module (src/py/vertebra/telemetry/otel.py).
[^src-telemetry-genai-py]: `src/py/vertebra/telemetry/genai.py` — Python implementation module (src/py/vertebra/telemetry/genai.py).
[^src-telemetry-db-propagation-py]: `src/py/vertebra/telemetry/db_propagation.py` — Python implementation module (src/py/vertebra/telemetry/db_propagation.py).
[^src-src-telemetry-ts]: `src/ts/vertebra-client/src/telemetry.ts` — TypeScript client module (src/ts/vertebra-client/src/telemetry.ts).
[^src-tests-unit-telemetry-test-telemetry-and-platform-py]: `src/py/tests/unit/telemetry/test_telemetry_and_platform.py` — Python implementation module (src/py/tests/unit/telemetry/test_telemetry_and_platform.py).
