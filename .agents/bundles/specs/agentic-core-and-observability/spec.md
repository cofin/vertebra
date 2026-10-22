---
type: Spec
flow_id: agentic-core-and-observability
title: "Chapter 1: Agentic Core Primitives, Security Context, and Universal GenAI Observability"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Build the shared JSON-RPC 2.0 dispatcher, cursor pagination, signature-to-JSON-Schema 2020-12 builder, SSE frame codec, federated AgentContext and GuardChain, Rust JSON-RPC/schema FFI fastpath, and end-to-end OpenTelemetry gen_ai.* observability harness."
tags: [feature, agent, telemetry, rust]
parent_prd: vertebra-agentic-harness
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 1: Agentic Core Primitives, Security Context, and Universal GenAI Observability

**Flow ID:** `agentic-core-and-observability`

## Specification

### Parent Context

This chapter is Chapter 1 of [`vertebra-agentic-harness`](../vertebra-agentic-harness/spec.md) and has no prior chapter dependencies. It establishes the shared protocol, security, schema, FFI, and telemetry foundation consumed by MCP (`Chapter 2`), A2A (`Chapter 3`), and `AgentRuntime` / `SpecTree` / `DynamicWorkflow` (`Chapter 4`). It owns master requirements **AH-R1** and **AH-R2**.

### Code Analysis Summary

Inspected in the current repository and `litestar-mcp` reference implementation:

- **`src/py/vertebra/telemetry/otel.py:20-168` (168 lines):** Defines `TraceContext`, `SpanData`, `SpanExporter`, `GenAiMetricSample`, and `GenAiRecorder`. Currently `GenAiRecorder.record_generation` (`:150-168`) only records `(model, operation, input_tokens, output_tokens, duration_seconds)` in a Python list without thought/cached token accounting, cost estimation, tool/skill/workflow span helpers, or Rust `SpanRecord` / `W3cTraceContext` integration.
- **`src/py/vertebra/telemetry/logger.py:102-207` (207 lines):** `LogSink` buffers bytes in a Python `collections.deque` with no background writer thread, while `src/rs/vertebra-core/src/logger.rs:131-212` (246 lines) implements the real GIL-free `AsyncLogCore` and `BoundedLogQueue` with background `BufWriter` flushing and `LoggerMetrics` atomic counters (`enqueued`, `flushed`, `dropped`), and `src/rs/vertebra-core/src/telemetry.rs:5-98` (156 lines) implements zero-allocation `W3cTraceContext` and `#[repr(C)] pub struct SpanRecord`. `bridge-and-server-consolidation:4.10` consolidates these into `src/rs/vertebra-py/src/telemetry.rs` (binding `NativeLogSink`/`AsyncLogCore`, `parse_traceparent`, and `format_traceparent`), so `src/rs/vertebra-py/src/agent.rs` focuses strictly on agent-specific acceleration (`classify_jsonrpc_frame` and `SchemaDigestCache`).
- **`src/py/vertebra/framework/mcp/__init__.py:24-174` (232 lines) & `src/py/vertebra/framework/a2a/__init__.py:105-167` (167 lines):** Duplicate ad-hoc `handle_jsonrpc` dicts with no batch support, broken notification handling (returning errors for notifications without `id`), uncaught handler exceptions leaking stack details, and empty default `input_schema` (`{"type": "object", "properties": {}}`).
- **`litestar_mcp/core/{jsonrpc,schema_builder,tools,context,_cursor}.py`:** Provides proven patterns for JSON-RPC 2.0 routing (`JSONRPCRouter`, notification suppression, `-32603` exception type masking), signature-driven JSON Schema 2020-12 generation with `$defs` hoisting and injected parameter stripping, URL-safe opaque cursors, and `ToolContext.owner_key()` tenant isolation.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — Frozen `msgspec.Struct(frozen=True, gc=False)` value objects, centralized `vertebra.utils.serializers` JSON/MessagePack calls, and clean-break domain naming.
- [Python Module and Class Layout](../../knowledge/patterns/python/module-and-class-layout.md) — Top-level imports, `__all__`, class attribute -> `__init__` -> properties -> public methods -> private `_...` helpers, and zero inline `#` comments.
- [PyO3 FFI and GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md) — `#[pymodule(gil_used = false)]` on `_vertebra` and `py.detach(|| ...)` around batch parsing and background flushes.

### Requirements

| ID | Requirement |
| --- | --- |
| C1-R1 | Implement `src/py/vertebra/agent/core/{jsonrpc,cursor,schema,sse}.py` providing `JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`, `JsonRpcErrorException`, `JsonRpcRouter` (single + batch dispatch, notification suppression, `-32603` exception type masking), `CursorPage` / `encode_cursor` / `decode_cursor`, `SchemaBuilder` + `@tool` / `Tool` descriptor (`msgspec.Struct`, dataclasses, `TypedDict`, `Literal`, `Annotated`, union types, `$defs` hoisting, and context/DI parameter stripping), and `SseFrame` / `SseStreamCodec` with bounded memory streams and `Last-Event-ID` replay buffer. |
| C1-R2 | Implement `src/py/vertebra/agent/core/{context,guards}.py` providing `AgentContext` (extracting principal, `tenant_id`, `roles`, `scopes`, `client_cert_cn`, `xfcc_subject`, `iap_jwt_sub`, `session_id`, `turn_id`, `trace_context`, `cancellation_token`, and `progress_reporter` from both native `vertebra._vertebra.Request` / ASGI scope and Litestar `ASGIConnection`), `CancellationToken`, `ProgressReporter`, and hierarchical `GuardChain` (app -> controller/skill -> tool/prompt/agent). |
| C1-R3 | Implement `src/py/vertebra/telemetry/genai.py` and `src/py/vertebra/agent/telemetry.py` (with compatibility re-export in `src/py/vertebra/agent/observability.py`) providing OpenTelemetry `gen_ai.*` semantic convention attributes, `GenAiSpanKind`, `GenAiUsage`, `CostTable`, `AgentTelemetryRecorder`, context-manager/decorator instrumentation for tools, resources, prompts, skills, model calls, agent turns, `SpecTree` nodes, `DynamicWorkflow` steps, and A2A messages, W3C `traceparent` injection/extraction in HTTP headers and JSON-RPC `params._meta`, and structured JSON audit event emission into `LogSink`. |
| C1-R4 | Implement `src/rs/vertebra-py/src/agent.rs` and wire into `src/rs/vertebra-py/src/lib.rs` and `src/py/vertebra/_vertebra.pyi`, exposing `classify_jsonrpc_frame(payload: &[u8]) -> PyResult<(u8, Option<String>, Option<String>)>` and thread-safe `SchemaDigestCache` (64-bit schema hashing and pre-serialized JSON bytes cache). |
| C1-R5 | Validate all core primitives, schema generation edge cases, guard inheritance chains, Rust FFI bindings, and `gen_ai.*` span/metric/log emission in `src/py/tests/unit/agent/test_core_and_observability.py`. |

### Target Layout

```text
src/rs/vertebra-py/src/
  lib.rs                               registers SchemaDigestCache, classify_jsonrpc_frame (alongside telemetry.rs bindings from 4.10)
  agent.rs                             PyO3 bindings for SchemaDigestCache and zero-copy classify_jsonrpc_frame helper
src/py/vertebra/
  _vertebra.pyi                        typed stubs for SchemaDigestCache, classify_jsonrpc_frame
  telemetry/
    __init__.py                        exports GenAiTelemetry, CostEstimate, ModelPricing, and Rust-backed LogSink/TraceContext
    genai.py                           OTel gen_ai.* semantic conventions, token/cost/latency histograms, W3C trace context propagation
    logger.py                          LogSink backed by _vertebra.NativeLogSink with non-blocking background flush (from 4.10)
    otel.py                            TraceContext delegating parse/format to _vertebra.parse_traceparent/_vertebra.format_traceparent (from 4.10)
  agent/
    __init__.py                        PEP 562 lazy exports for core, telemetry, mcp, a2a, bridges, runtime, spec, workflow, sessions
    telemetry.py                       AgentTelemetryRecorder recording spans, metrics, and LogSink audit events for every operation
    observability.py                   Compatibility re-export of AgentTelemetryRecorder as AgentObservabilityHarness
    core/
      __init__.py                      re-exports jsonrpc, cursor, schema, sse, context, guards
      jsonrpc.py                       JsonRpcRequest, JsonRpcResponse, JsonRpcError, JsonRpcRouter (single & batch)
      cursor.py                        encode_cursor, decode_cursor, paginate_sequence
      schema.py                        SchemaBuilder, Tool, ToolCall, ToolResult, ToolArgumentError, @tool
      sse.py                           SseFrame, SseReplayBuffer, SseStreamBroker
      context.py                       AgentContext, CallerIdentity, CancellationToken, ProgressReporter
      guards.py                        GuardProtocol, GuardChain, PermissionDeniedError, RateLimitGuard, ScopeGuard, RoleGuard
src/py/tests/unit/agent/
  __init__.py
  test_core_and_observability.py       unit and property tests for Tasks 1.1–1.4
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Recursive or mutually referential `msgspec.Struct` types in tool signatures cause `$defs` collisions when multiple parameters have nested structs. | `SchemaBuilder.build_parameters_schema` hoists all nested `$defs` into a single top-level `$defs` dictionary and rewrites inner `$ref` pointers consistently. |
| Background thread in `NativeLogSink` blocks interpreter exit if not shut down cleanly. | Implement clean shutdown and `Drop` on native sink, release GIL during background flushes, and register `atexit` cleanup on `LogSink`. |

## Implementation Plan

### Phase 1: Core Protocol Primitives, Context, and Observability
- [ ] 1.1 Core JSON-RPC 2.0 dispatcher, cursor pagination, schema builder, and SSE frame codec ([worksheet](tasks/1.1.md))
- [ ] 1.2 Execution context (`AgentContext`, caller identity, cancellation, progress) and guard inheritance ([worksheet](tasks/1.2.md))
- [ ] 1.3 End-to-end agentic telemetry harness (`agent/telemetry.py` + `telemetry/genai.py`) ([worksheet](tasks/1.3.md))
- [ ] 1.4 Rust-accelerated JSON-RPC frame classification and schema digest caching in `vertebra-py` ([worksheet](tasks/1.4.md))
- [ ] 1.5 Unit and property tests for JSON-RPC, schema builder, guard inheritance, and telemetry emission ([worksheet](tasks/1.5.md))
- [ ] Chapter gate: `make lint-all && make rs-test && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C1-R1 | 1.1, 1.5 | `uv run pytest src/py/tests/unit/agent/test_core_and_observability.py -k "jsonrpc or cursor or schema or sse" -q` |
| C1-R2 | 1.2, 1.5 | `uv run pytest src/py/tests/unit/agent/test_core_and_observability.py -k "context or guard" -q` |
| C1-R3 | 1.3, 1.5 | `uv run pytest src/py/tests/unit/agent/test_core_and_observability.py -k "telemetry or observability" -q` |
| C1-R4 | 1.4, 1.5 | `cd src/rs && cargo test -p vertebra-py && make dev && uv run pytest src/py/tests/unit/agent/test_core_and_observability.py -k "rust_agent" -q` |
| C1-R5 | 1.5 | `uv run pytest src/py/tests/unit/agent/test_core_and_observability.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| MCP-06 | 1.1 (`Tool` and `@tool` descriptor with signature-derived `msgspec` schema) |
| CORE-13 (telemetry/logger binding) | 1.3, 1.4 |

## Continuity Snapshot

- Active flow: `agentic-core-and-observability`, state `planned`. Current task: none. Next step: `/flow:implement agentic-core-and-observability`.
- Plan identity: plan_revision 2, plan_commit: "". State identity: state_revision 0.
- Blockers: none. Decisions: Native telemetry bindings (NativeLogSink, parse_traceparent) consolidated in telemetry.rs from 4.10; agent.rs exclusively accelerates classify_jsonrpc_frame and SchemaDigestCache.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`, `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`.
