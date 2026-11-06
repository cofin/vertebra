---
type: Concept
title: Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols
description: 'The `vertebra.agent` protocol harness: Model Context Protocol (MCP 2026-07-28)
  SSE and Stdio transports, Agent-to-Agent (A2A 1.0) JCS/RFC 8785 Ed25519/ECDSA signed
  agent cards, durable DAG workflows, SpecTree execution, Rust PyO3 cryptographic
  fastpaths, and GenAI telemetry.'
status: stable
tags:
- domains
- agent
- mcp
- a2a
- workflow
- telemetry
generated:
  by: flow/0.26.0
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:flow-review
  at: '2026-11-06T06:10:00Z'
  commit: 7657e8b7c2c96bdda6821aec70a2b603f438a127
- by: agent:codex
  at: '2026-11-05T06:09:26Z'
  commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
- by: agent:flow-setup
  at: '2026-11-04T04:33:00Z'
  commit: f741701
sources:
- id: src-typing-public-py
  resource: src/py/vertebra/typing.py
  title: Public neutral ToolProvider and ToolPolicyError exports
- id: src-typing-internal-py
  resource: src/py/vertebra/_typing.py
  title: Shared neutral callable-map contract and policy exception
- id: src-db-bigquery-tools-py
  resource: src/py/vertebra/db/adapters/bigquery/tools.py
  title: Adapter-owned BigQuery operations and billing policies
- id: src-db-spanner-tools-py
  resource: src/py/vertebra/db/adapters/spanner/tools.py
  title: Adapter-owned Spanner operations and snapshot policies
- id: src-agent-init-py
  resource: src/py/vertebra/agent/__init__.py
  title: Python implementation module (src/py/vertebra/agent/__init__.py)
- id: src-agent-tools-init-py
  resource: src/py/vertebra/agent/tools/__init__.py
  title: Generic provider adaptation and caller-bound confirmation
- id: src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py
  resource: src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  title: GCP adapter and agent tool behavior tests
- id: src-agent-mcp-service-py
  resource: src/py/vertebra/agent/mcp/service.py
  title: Python implementation module (src/py/vertebra/agent/mcp/service.py)
- id: src-agent-mcp-routes-py
  resource: src/py/vertebra/agent/mcp/routes.py
  title: Python implementation module (src/py/vertebra/agent/mcp/routes.py)
- id: src-agent-mcp-stdio-py
  resource: src/py/vertebra/agent/mcp/stdio.py
  title: Python implementation module (src/py/vertebra/agent/mcp/stdio.py)
- id: src-agent-a2a-service-py
  resource: src/py/vertebra/agent/a2a/service.py
  title: Python implementation module (src/py/vertebra/agent/a2a/service.py)
- id: src-agent-a2a-signing-py
  resource: src/py/vertebra/agent/a2a/signing.py
  title: Python implementation module (src/py/vertebra/agent/a2a/signing.py)
- id: src-agent-workflow-py
  resource: src/py/vertebra/agent/workflow.py
  title: Python implementation module (src/py/vertebra/agent/workflow.py)
- id: src-agent-spec-py
  resource: src/py/vertebra/agent/spec.py
  title: Python implementation module (src/py/vertebra/agent/spec.py)
- id: src-agent-runtime-py
  resource: src/py/vertebra/agent/runtime.py
  title: Python implementation module (src/py/vertebra/agent/runtime.py)
- id: src-telemetry-genai-py
  resource: src/py/vertebra/telemetry/genai.py
  title: Python implementation module (src/py/vertebra/telemetry/genai.py)
- id: src-vertebra-py-src-agent-rs
  resource: src/rs/vertebra-py/src/agent.rs
  title: Rust implementation module (src/rs/vertebra-py/src/agent.rs)
- id: src-vertebra-http-src-discovery-rs
  resource: src/rs/vertebra-http/src/discovery.rs
  title: Rust implementation module (src/rs/vertebra-http/src/discovery.rs)
- id: src-src-agent-index-ts
  resource: src/ts/vertebra-client/src/agent/index.ts
  title: TypeScript client module (src/ts/vertebra-client/src/agent/index.ts)
- id: src-tests-unit-agent-test-mcp-conformance-py
  resource: src/py/tests/unit/agent/test_mcp_conformance.py
  title: Python implementation module (src/py/tests/unit/agent/test_mcp_conformance.py)
- id: src-tests-unit-agent-test-a2a-protocol-py
  resource: src/py/tests/unit/agent/test_a2a_protocol.py
  title: Python implementation module (src/py/tests/unit/agent/test_a2a_protocol.py)
- id: src-tests-unit-agent-test-runtime-and-workflows-py
  resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
  title: Python implementation module (src/py/tests/unit/agent/test_runtime_and_workflows.py)
- id: src-tests-unit-agent-test-core-and-observability-py
  resource: src/py/tests/unit/agent/test_core_and_observability.py
  title: Python implementation module (src/py/tests/unit/agent/test_core_and_observability.py)
---

# Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols

<!-- truth: start -->
- `ToolSpec(FrozenBaseStruct, frozen=True, kw_only=True)` and `ToolAnnotations(FrozenBaseStruct, frozen=True, kw_only=True)` derive input and output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerce tool arguments via `msgspec.convert` with zero `eval()` calls; default `exposure` is `frozenset({"mcp", "a2a"})` (excluding `"webmcp"` unless opted in).
- `ToolRegistry` (subclassed by `MCPRegistry`) unifies `vertebra.typing.ToolProvider`, `@tool` callables, `SkillController`s, and opted-in framework routes (`discover_app_routes`), preserving per-call confirmation reasons, isolated canonical arguments, and native error boundaries.
- `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` (subclassed by `MCPService`) and `AsgiMcpApp` / `WsgiMcpApp` (backed by a persistent `PortalProvider.default()` portal) implement MCP `2026-07-28` Streamable HTTP across native `App`, Starlette, FastAPI, Flask, and Django, while Litestar delegates `/mcp` to `litestar-mcp` without duplicate `opt["mcp_tool"]` entries and all mounts share one canonical `/.well-known/mcp.json` discovery schema.
- `build_webmcp_manifest` projects `/.well-known/webmcp.json` strictly from `ToolSpec` entries with `"webmcp" in exposure` (emitting `title`, `outputSchema`, and `annotations.consequentialHint = True` when `requires_confirmation=True`), and `registerWebMCPTools` in `@vertebra/client/webmcp` targets `document.modelContext ?? navigator.modelContext` with `AbortSignal` disposal, HMR idempotency, and per-tool error isolation.
- `vertebra.agent.a2a` implements A2A `1.0` task lifecycles, dynamic request-origin `AgentCard` URLs, `ToolSpec` skill card projection (`"a2a" in exposure`), RFC 8785 JCS Ed25519/ECDSA card signatures, and DAG/`SpecTree` workflow execution.
<!-- truth: end -->

[`vertebra.agent`](../../../../../src/py/vertebra/agent/__init__.py)[^src-agent-init-py] provides an enterprise agentic harness combining Model Context Protocol (`MCP 2026-07-28`), Agent-to-Agent (`A2A 1.0`), durable DAG workflows, SpecTree hierarchical execution, Rust FFI cryptographic acceleration, and OpenTelemetry GenAI semantic conventions.

## 1. Model Context Protocol (MCP 2026-07-28)

[`vertebra.agent.mcp`](../../../../../src/py/vertebra/agent/mcp/service.py)[^src-agent-mcp-service-py] implements the MCP `2026-07-28` specification across Streamable HTTP/SSE and Stdio transports.

| Component | Responsibility |
| :--- | :--- |
| [`ToolSpec` & `SchemaBuilder`](../../../../../src/py/vertebra/agent/core/schema.py) | Compiles input and output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerces arguments via `msgspec.convert` without `eval()`. Carries `exposure: frozenset[Literal["mcp", "webmcp", "a2a"]]`, `ToolAnnotations`, `requires_confirmation`, and `guards`. |
| [`ToolRegistry` (`MCPRegistry`)](../../../../../src/py/vertebra/agent/mcp/registry.py) | Unifies `ToolProvider` instances (`register_provider`), `@tool` callables (`register_tool`), `SkillController`/`PromptController` classes (`register_controller`), and opted-in framework routes (`register_route_handler`, `discover_app_routes`). Filters `list_specs(exposure=...)` by protocol surface (`"mcp"`, `"webmcp"`, `"a2a"`). |
| [`MCPCore` (`MCPService`)](../../../../../src/py/vertebra/agent/mcp/service.py) | Transport-agnostic `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` enforcing MCP `2026-07-28` HTTP rules: `403` on disallowed `Origin`, `405` on `GET /mcp`, `204` on `OPTIONS /mcp`, `202` on JSON-RPC notifications, `400` on JSON-RPC batch arrays or `MCP-Protocol-Version` / `Mcp-Method` / `Mcp-Name` / `Mcp-Param-*` mismatches, `-32601` on removed `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`), `server/discover`, `resultType`, `ttlMs`/`cacheScope`, deterministic `ETag` `304 Not Modified`, `authorize(ctx, spec)` guards, and `ConfirmationGate` (`_meta["vertebra/confirmationToken"]`, error `-32003`). |
| [`AsgiMcpApp`, `WsgiMcpApp` & `MCPHttpHandler`](../../../../../src/py/vertebra/agent/mcp/routes.py)[^src-agent-mcp-routes-py] | Shared ASGI and WSGI HTTP adapters over `MCPCore` (`WsgiMcpApp` reuses `PortalProvider.default()` instead of per-request `asyncio.run`) plus `build_well_known_mcp_document` / `render_well_known_mcp_json` serving one canonical `/.well-known/mcp.json` payload across native `App`, Starlette, FastAPI, Flask, Django, and Litestar. |
| [`McpStdioServer`](../../../../../src/py/vertebra/agent/mcp/stdio.py)[^src-agent-mcp-stdio-py] | Provides local stdio communication for CLI agents and local developer tooling via standard input/output streams. |
| [`McpClient` & `registerWebMCPTools`](../../../../../src/ts/vertebra-client/src/agent/index.ts)[^src-src-agent-index-ts] | TypeScript client SDK sending `Accept: application/json, text/event-stream` and `2026-07-28` `_meta`, parsing SSE JSON-RPC responses, encoding control/non-ASCII header values via `encodeMcpHeaderValue`, and registering browser WebMCP tools on `document.modelContext ?? navigator.modelContext` with `AbortSignal` disposal and HMR idempotency. |

### Adapter-owned tool providers

[`ToolProvider`](../../../../../src/py/vertebra/typing.py)[^src-typing-public-py] exposes
`operations()` and side-effect-free `confirmation_reason(...)`; its definition and explicit
`ToolPolicyError(VertebraError, PermissionError)` live in shared typing. [^src-typing-internal-py]
[`build_tools`](../../../../../src/py/vertebra/agent/tools/__init__.py)[^src-agent-tools-init-py]
accepts any conforming provider, resolves original bound annotations once (stripping `AgentContext` and `AgentContext | None` parameters via `SchemaBuilder.is_stripped_annotation`), and preserves
schemas, defaults, descriptions, and public `Tool.invoke` conversion. No engine imports or
vendor dispatch remain in this generic consumer. Operations accept named positional-or-keyword
and keyword-only parameters; registration rejects positional-only and variadic signatures.

[`BigQueryToolProvider`](../../../../../src/py/vertebra/db/adapters/bigquery/tools.py)[^src-db-bigquery-tools-py]
and [`SpannerToolProvider`](../../../../../src/py/vertebra/db/adapters/spanner/tools.py)[^src-db-spanner-tools-py]
resolve explicit drivers, typed configs, or registry aliases only during execution.
Construction, operation discovery, schema registration, and confirmation classification perform
no connection work. Native async drivers are awaited; synchronous calls run through
`asyncio.to_thread`. Providers own SQL validation, conservative approval classification,
BigQuery request billing caps, and Spanner snapshot staleness.

The bridge validates before policy, materializes defaults, and isolates invocation arguments.
A real approved ticket must match the operation, token-free canonical arguments, and injected
caller owner. Pending ticket arguments are copied separately from invocation state. Ordinary
reads remain ungated; mutations and ambiguous executable SQL including `WITH` require approval
without changing native routing or SQL support. Only `ToolPolicyError` becomes agent
`PermissionDeniedError`; native errors and cancellation preserve identity. Direct tool calls
supply the optional `confirmation_token` argument; MCP transport behavior remains separate.
The [canonical agent/GCP tests](../../../../../src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py)[^src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py]
exercise lazy resolution, native sync/async dispatch, typed invocation, approval, and error paths.

## 2. Agent-to-Agent Protocol (A2A 1.0)

[`vertebra.agent.a2a`](../../../../../src/py/vertebra/agent/a2a/service.py)[^src-agent-a2a-service-py] provides peer-to-peer agent collaboration with verifiable cryptographic identity. All A2A 1.0 data models in [`a2a/models.py`](../../../../../src/py/vertebra/agent/a2a/models.py) subclass [`FrozenCamelizedStruct`](../../../../../src/py/vertebra/_typing.py) (`frozen=True, gc=False, rename="camel", omit_defaults=True, repr_omit_defaults=True`), while MCP `2026-07-28` models in [`mcp/models.py`](../../../../../src/py/vertebra/agent/mcp/models.py) subclass [`FrozenBaseStruct`](../../../../../src/py/vertebra/_typing.py).

- **Cryptographic Card Signing (`signing.py`)**: Signs and verifies `AgentCard` manifests using JSON Canonicalization Scheme (JCS, RFC 8785) with Ed25519 (`ed25519`), ECDSA P-256 (`es256`), or HS256 (`card_secret`).
- **Automatic Interface & Dynamic URL Discovery (`models.py`, `service.py`, `routes.py`)**: Serves `/.well-known/agent-card.json` containing the public `AgentCard`, supported skills, operational modes, and verification keys. When `AgentCard.supported_interfaces` is omitted (or contains a relative URL), `AgentCard.resolve_for_request()` and `A2AService.resolve_agent_card()` synthesize a default `JSONRPC` 1.0 `AgentInterface` and resolve its public URL dynamically from `A2AConfig.base_url` or the incoming `Forwarded`, `X-Forwarded-Proto`, `X-Forwarded-Host`, and `Host` request headers.
- **Task Lifecycle**: Enforces strict state transitions across `submitted` $\to$ `working` $\to$ `completed` / `failed` with incremental progress reporting and cancellation handling.
- **Client SDK (`A2aClient`)**: TypeScript client for querying peer agent capabilities, initiating task handoffs, and validating remote agent card signatures.

## 3. Workflows, DAG Engine & SpecTree

[`vertebra.agent.workflow`](../../../../../src/py/vertebra/agent/workflow.py)[^src-agent-workflow-py] and [`vertebra.agent.spec`](../../../../../src/py/vertebra/agent/spec.py)[^src-agent-spec-py] implement durable dynamic workflows.

- **Dynamic DAG Engine**: `DynamicWorkflow` compiles declarative steps (`StepKind.LLM`, `StepKind.TOOL`, `StepKind.GATE`, `StepKind.PARALLEL`, `StepKind.MAP`) into a directed acyclic graph with topological sorting, cycle detection, and dependency resolution.
- **LiveState Integration**: Workflow execution emits live progress updates and state deltas via `LiveState` channels, synchronizing status to connected clients in real time.
- **SpecTree**: Hierarchical task decomposition tree (`SpecNode`, `SpecTreeExecutor`) with deterministic execution order, boundary rollback, and error isolation.
- **AgentRuntime**: Central coordinator managing tool registries, execution sessions, workflow runtimes, and telemetry exporters.

## 4. Rust PyO3 Acceleration & Ingress Fastpaths

Native FFI acceleration in [`src/rs/vertebra-py/src/agent.rs`](../../../../../src/rs/vertebra-py/src/agent.rs)[^src-vertebra-py-src-agent-rs] and ingress routing in [`src/rs/vertebra-http/src/discovery.rs`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs]:

- **Cryptographic Fastpaths**: `verify_hmac_sha256_token` and `sign_hmac_sha256_token` for zero-allocation token validation and creation.
- **Cursor Tokens**: `encode_cursor_token` and `decode_cursor_token` provide URL-safe, tamper-resistant pagination tokens with embedded timestamps and filter hashes.
- **MCP Header Extraction**: `extract_mcp_headers` normalizes incoming `Mcp-Session-Id`, `Mcp-Method`, `Mcp-Tool-Name`, and trace propagation headers before Python dispatch.
- **Rust Discovery Handler**: Caches `/.well-known/agent.json` and `/.well-known/mcp.json` in memory with atomic reloads.

## 5. GenAI Telemetry & Observability

[`vertebra.telemetry.genai`](../../../../../src/py/vertebra/telemetry/genai.py)[^src-telemetry-genai-py] implements the OpenTelemetry GenAI Semantic Conventions:

- **Usage Tracking**: `GenAiUsage` tracks `input_tokens`, `output_tokens`, `thought_tokens`, `cached_input_tokens`, and calculates USD cost using configurable `ModelPricing` tables.
- **Duration Histograms**: `GenAiDurationHistogram` records latency distributions across operations (`chat`, `embeddings`, `tool_call`, `workflow_step`).
- **Audit Logging**: `LogSink` with `AsyncLogCore` integration flushes newline-delimited JSONL audit records capturing trace IDs, parent span IDs, prompt metadata, tool inputs/outputs, and policy decisions.

Provenance sources: [^src-agent-mcp-routes-py], [^src-agent-a2a-signing-py], [^src-agent-runtime-py], [^src-tests-unit-agent-test-mcp-conformance-py], [^src-tests-unit-agent-test-a2a-protocol-py], [^src-tests-unit-agent-test-runtime-and-workflows-py], [^src-tests-unit-agent-test-core-and-observability-py].

[^src-agent-init-py]: `src/py/vertebra/agent/__init__.py` — Python implementation module (src/py/vertebra/agent/__init__.py).
[^src-agent-mcp-service-py]: `src/py/vertebra/agent/mcp/service.py` — Python implementation module (src/py/vertebra/agent/mcp/service.py).
[^src-agent-mcp-routes-py]: `src/py/vertebra/agent/mcp/routes.py` — Python implementation module (src/py/vertebra/agent/mcp/routes.py).
[^src-agent-mcp-stdio-py]: `src/py/vertebra/agent/mcp/stdio.py` — Python implementation module (src/py/vertebra/agent/mcp/stdio.py).
[^src-agent-a2a-service-py]: `src/py/vertebra/agent/a2a/service.py` — Python implementation module (src/py/vertebra/agent/a2a/service.py).
[^src-agent-a2a-signing-py]: `src/py/vertebra/agent/a2a/signing.py` — Python implementation module (src/py/vertebra/agent/a2a/signing.py).
[^src-agent-workflow-py]: `src/py/vertebra/agent/workflow.py` — Python implementation module (src/py/vertebra/agent/workflow.py).
[^src-agent-spec-py]: `src/py/vertebra/agent/spec.py` — Python implementation module (src/py/vertebra/agent/spec.py).
[^src-agent-runtime-py]: `src/py/vertebra/agent/runtime.py` — Python implementation module (src/py/vertebra/agent/runtime.py).
[^src-telemetry-genai-py]: `src/py/vertebra/telemetry/genai.py` — Python implementation module (src/py/vertebra/telemetry/genai.py).
[^src-vertebra-py-src-agent-rs]: `src/rs/vertebra-py/src/agent.rs` — Rust implementation module (src/rs/vertebra-py/src/agent.rs).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
[^src-src-agent-index-ts]: `src/ts/vertebra-client/src/agent/index.ts` — TypeScript client module (src/ts/vertebra-client/src/agent/index.ts).
[^src-tests-unit-agent-test-mcp-conformance-py]: `src/py/tests/unit/agent/test_mcp_conformance.py` — Python implementation module (src/py/tests/unit/agent/test_mcp_conformance.py).
[^src-tests-unit-agent-test-a2a-protocol-py]: `src/py/tests/unit/agent/test_a2a_protocol.py` — Python implementation module (src/py/tests/unit/agent/test_a2a_protocol.py).
[^src-tests-unit-agent-test-runtime-and-workflows-py]: `src/py/tests/unit/agent/test_runtime_and_workflows.py` — Python implementation module (src/py/tests/unit/agent/test_runtime_and_workflows.py).
[^src-tests-unit-agent-test-core-and-observability-py]: `src/py/tests/unit/agent/test_core_and_observability.py` — Python implementation module (src/py/tests/unit/agent/test_core_and_observability.py).

[^src-agent-tools-init-py]: `src/py/vertebra/agent/tools/__init__.py` — Generic provider adaptation and caller-bound confirmation.
[^src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — GCP adapter and tool behavior tests.

[^src-typing-public-py]: `src/py/vertebra/typing.py` — Public neutral ToolProvider and ToolPolicyError exports.

[^src-typing-internal-py]: `src/py/vertebra/_typing.py` — Shared neutral callable-map contract and policy exception.

[^src-db-bigquery-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery operations and billing policies.

[^src-db-spanner-tools-py]: `src/py/vertebra/db/adapters/spanner/tools.py` — Adapter-owned Spanner operations and snapshot policies.
