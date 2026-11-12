---
type: Concept
title: "Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols"
description: "The vertebra.agent protocol harness covering MCP 2026-07-28 Streamable HTTP, Stdio, and WebMCP surfaces, A2A 1.0 JWS-signed AgentCards, durable DAG and SpecTree workflows, Rust PyO3 JSON-RPC and schema fastpaths, and GenAI telemetry."
status: stable
tags:
  - domains
  - agent
  - mcp
  - a2a
  - webmcp
  - workflow
  - telemetry
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-sync
    at: "2026-11-12T01:15:00Z"
    commit: c8722c76
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-typing-public-py
    resource: src/py/vertebra/typing.py
    title: "Public neutral ToolProvider and ToolPolicyError exports"
  - id: src-db-bigquery-tools-py
    resource: src/py/vertebra/db/adapters/bigquery/tools.py
    title: "Adapter-owned BigQuery operations and billing policies"
  - id: src-db-spanner-tools-py
    resource: src/py/vertebra/db/adapters/spanner/tools.py
    title: "Adapter-owned Spanner operations and snapshot policies"
  - id: src-agent-init-py
    resource: src/py/vertebra/agent/__init__.py
    title: "Python implementation module (src/py/vertebra/agent/__init__.py)"
  - id: src-agent-core-schema-py
    resource: src/py/vertebra/agent/core/schema.py
    title: "Python implementation module (src/py/vertebra/agent/core/schema.py)"
  - id: src-agent-core-cursor-py
    resource: src/py/vertebra/agent/core/cursor.py
    title: "Python implementation module (src/py/vertebra/agent/core/cursor.py)"
  - id: src-agent-tools-init-py
    resource: src/py/vertebra/agent/tools/__init__.py
    title: "Generic provider adaptation and caller-bound confirmation"
  - id: src-agent-mcp-models-py
    resource: src/py/vertebra/agent/mcp/models.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/models.py)"
  - id: src-agent-mcp-registry-py
    resource: src/py/vertebra/agent/mcp/registry.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/registry.py)"
  - id: src-agent-mcp-webmcp-py
    resource: src/py/vertebra/agent/mcp/webmcp.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/webmcp.py)"
  - id: src-agent-mcp-service-py
    resource: src/py/vertebra/agent/mcp/service.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/service.py)"
  - id: src-agent-mcp-routes-py
    resource: src/py/vertebra/agent/mcp/routes.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/routes.py)"
  - id: src-agent-mcp-stdio-py
    resource: src/py/vertebra/agent/mcp/stdio.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/stdio.py)"
  - id: src-agent-mcp-client-py
    resource: src/py/vertebra/agent/mcp/client.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/client.py)"
  - id: src-agent-a2a-models-py
    resource: src/py/vertebra/agent/a2a/models.py
    title: "Python implementation module (src/py/vertebra/agent/a2a/models.py)"
  - id: src-agent-a2a-service-py
    resource: src/py/vertebra/agent/a2a/service.py
    title: "Python implementation module (src/py/vertebra/agent/a2a/service.py)"
  - id: src-agent-a2a-signing-py
    resource: src/py/vertebra/agent/a2a/signing.py
    title: "Python implementation module (src/py/vertebra/agent/a2a/signing.py)"
  - id: src-agent-workflow-py
    resource: src/py/vertebra/agent/workflow.py
    title: "Python implementation module (src/py/vertebra/agent/workflow.py)"
  - id: src-agent-spec-py
    resource: src/py/vertebra/agent/spec.py
    title: "Python implementation module (src/py/vertebra/agent/spec.py)"
  - id: src-agent-runtime-py
    resource: src/py/vertebra/agent/runtime.py
    title: "Python implementation module (src/py/vertebra/agent/runtime.py)"
  - id: src-telemetry-genai-py
    resource: src/py/vertebra/telemetry/genai.py
    title: "Python implementation module (src/py/vertebra/telemetry/genai.py)"
  - id: src-vertebra-py-src-agent-rs
    resource: src/rs/vertebra-py/src/agent.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/agent.rs)"
  - id: src-vertebra-http-src-discovery-rs
    resource: src/rs/vertebra-http/src/discovery.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/discovery.rs)"
  - id: src-src-agent-index-ts
    resource: src/ts/vertebra-client/src/agent/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/agent/index.ts)"
  - id: src-src-webmcp-ts
    resource: src/ts/vertebra-client/src/webmcp.ts
    title: "TypeScript WebMCP client module (src/ts/vertebra-client/src/webmcp.ts)"
  - id: src-tests-unit-agent-test-mcp-conformance-py
    resource: src/py/tests/unit/agent/test_mcp_conformance.py
    title: "Python implementation module (src/py/tests/unit/agent/test_mcp_conformance.py)"
---

# Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols

<!-- truth: start -->
- `ToolSpec(FrozenBaseStruct, frozen=True, kw_only=True)` and `ToolAnnotations(FrozenBaseStruct, frozen=True, kw_only=True)` derive input and output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerce tool arguments via `msgspec.convert` with zero `eval()` calls; default `exposure` is `frozenset({"mcp", "a2a"})` (excluding `"webmcp"` unless opted in).
- `ToolRegistry` unifies `vertebra.typing.ToolProvider`, `@tool` callables, `SkillController`s, and opted-in framework routes (`discover_app_routes`), preserving per-call confirmation reasons, isolated canonical arguments, and native error boundaries; `MCPRegistry(ToolRegistry)` is an alias subclass with a docstring and no added behavior.
- `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` and `AsgiMcpApp` / `WsgiMcpApp` (backed by a persistent `PortalProvider.default()` portal) implement MCP `2026-07-28` Streamable HTTP (`POST /mcp`, `204` on `OPTIONS /mcp`, `405` on `GET /mcp`) across native `App`, Starlette, FastAPI, Flask, and Django, validating `MCP-Protocol-Version`, `Mcp-Method`, and `Mcp-Name` against the JSON-RPC body, echoing `Mcp-Session-Id` without session validation, and sharing one canonical `/.well-known/mcp.json` discovery schema; `MCPService(MCPCore)` is an alias subclass with no added behavior, and `Mcp-Param-*` header validation is not implemented.
- `build_webmcp_manifest` projects `/.well-known/webmcp.json` strictly from `ToolSpec` entries with `"webmcp" in exposure` (emitting `title`, `outputSchema`, and `annotations.consequentialHint = True` when `requires_confirmation=True`), and `registerWebMCPTools` in `@vertebra/client/webmcp` targets `document.modelContext ?? navigator.modelContext` with `AbortSignal` disposal, HMR idempotency, and per-tool error isolation.
- `vertebra.agent.a2a` implements A2A `1.0` task lifecycles, dynamic request-origin `AgentCard` URLs, `ToolSpec` skill card projection (`"a2a" in exposure`), RFC 7515 JWS `HS256` card signatures (`AgentCardSigner`), and DAG/`SpecTree` workflow execution.
<!-- truth: end -->

[`vertebra.agent`](../../../../../src/py/vertebra/agent/__init__.py)[^src-agent-init-py] provides an agentic harness combining Model Context Protocol (`MCP 2026-07-28`), Agent-to-Agent (`A2A 1.0`), browser WebMCP, durable DAG workflows, `SpecTree` hierarchical execution, Rust PyO3 JSON-RPC and schema digest fastpaths, and OpenTelemetry GenAI semantic conventions.

## 1. Model Context Protocol (MCP 2026-07-28)

[`vertebra.agent.mcp`](../../../../../src/py/vertebra/agent/mcp/service.py)[^src-agent-mcp-service-py] implements the MCP `2026-07-28` specification across Streamable HTTP (`POST /mcp`, `204` on `OPTIONS /mcp`, `405` on `GET /mcp`), Stdio (`run_stdio_server`), and browser WebMCP (`WebMCPConfig`, `build_webmcp_manifest`). Protocol gaps that are not implemented yet are recorded in [MCP Implementation Limitations](mcp-limitations.md).

| Component | Responsibility |
| :--- | :--- |
| [`ToolSpec` & `SchemaBuilder`](../../../../../src/py/vertebra/agent/core/schema.py)[^src-agent-core-schema-py] | Compiles input and output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerces arguments via `msgspec.convert` without `eval()`. Carries `exposure: frozenset[Literal["mcp", "webmcp", "a2a"]]`, `ToolAnnotations`, `requires_confirmation`, and `guards`. |
| [`ToolRegistry` and `MCPRegistry`](../../../../../src/py/vertebra/agent/mcp/registry.py)[^src-agent-mcp-registry-py] | `ToolRegistry` unifies `ToolProvider` instances (`register_provider`), `@tool` callables (`register_tool`), `SkillController`/`PromptController` classes (`register_controller`), and opted-in framework routes (`register_route_handler`, `discover_app_routes`). Filters `list_specs(exposure=...)` by protocol surface (`"mcp"`, `"webmcp"`, `"a2a"`). `MCPRegistry(ToolRegistry)` adds no behavior. |
| [`MCPCore` and `MCPService`](../../../../../src/py/vertebra/agent/mcp/service.py) | `MCPService(MCPCore)` adds no behavior. Transport-agnostic `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` enforcing MCP `2026-07-28` Streamable HTTP rules: `403` on disallowed `Origin`, `405` on `GET /mcp`, `204` on `OPTIONS /mcp`, `202` on JSON-RPC notifications, `400` on JSON-RPC batch arrays or `MCP-Protocol-Version` / `Mcp-Method` / `Mcp-Name` mismatches (no `Mcp-Param-*` checks), `-32601` on removed `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`), `server/discover`, `resultType`, `ttlMs`/`cacheScope`, deterministic `ETag` `304 Not Modified`, `authorize(ctx, spec)` guards, and `ConfirmationGate` (`_meta["vertebra/confirmationToken"]`, error `-32003`). |
| [`AsgiMcpApp`, `WsgiMcpApp` & `MCPHttpHandler`](../../../../../src/py/vertebra/agent/mcp/routes.py)[^src-agent-mcp-routes-py] | Shared ASGI and WSGI HTTP adapters over `MCPCore` (`WsgiMcpApp` reuses `PortalProvider.default()` instead of per-request `asyncio.run`) plus `build_well_known_mcp_document` / `render_well_known_mcp_json` serving one canonical `/.well-known/mcp.json` payload across native `App`, Starlette, FastAPI, Flask, Django, and Litestar. |
| [`WebMCPConfig` & `build_webmcp_manifest`](../../../../../src/py/vertebra/agent/mcp/webmcp.py)[^src-agent-mcp-webmcp-py] | Builds and serializes (`render_webmcp_manifest_json`) the `/.well-known/webmcp.json` `WebMCPManifest` (`WebMCPToolDescriptor`) strictly from `ToolSpec` entries with `"webmcp" in exposure`, propagating `title`, `outputSchema`, and `consequentialHint`. |
| [`run_stdio_server`](../../../../../src/py/vertebra/agent/mcp/stdio.py)[^src-agent-mcp-stdio-py] | Hosts an `MCPService` over newline-delimited JSON-RPC 2.0 `stdin`/`stdout` pipes for CLI agents and local developer tooling. |
| [`McpClient`](../../../../../src/ts/vertebra-client/src/agent/index.ts)[^src-src-agent-index-ts] & [`registerWebMCPTools`](../../../../../src/ts/vertebra-client/src/webmcp.ts)[^src-src-webmcp-ts] | TypeScript client SDK sending `Accept: application/json, text/event-stream`, `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`, `Mcp-Session-Id`, and `2026-07-28` `_meta`, parsing JSON or SSE JSON-RPC responses, encoding control/non-ASCII header values via `encodeMcpHeaderValue`, and registering browser WebMCP tools (`WebMCPRegistrationHandle`) on `document.modelContext ?? navigator.modelContext` with `AbortSignal` disposal and HMR idempotency. |

### Adapter-owned tool providers

[`ToolProvider`](../../../../../src/py/vertebra/typing.py)[^src-typing-public-py] exposes `operations()` and side-effect-free `confirmation_reason(...)` alongside `ToolPolicyError(VertebraError, PermissionError)`. [`build_tools`](../../../../../src/py/vertebra/agent/tools/__init__.py)[^src-agent-tools-init-py] accepts any conforming provider, resolves bound annotations once (stripping `AgentContext` and `AgentContext | None` via `SchemaBuilder.is_stripped_annotation`), preserves schemas, defaults, descriptions, and public `Tool.invoke` conversion, and rejects positional-only or variadic signatures with zero vendor dispatch in generic agent code.

[`BigQueryToolProvider`](../../../../../src/py/vertebra/db/adapters/bigquery/tools.py)[^src-db-bigquery-tools-py] and [`SpannerToolProvider`](../../../../../src/py/vertebra/db/adapters/spanner/tools.py)[^src-db-spanner-tools-py] resolve explicit drivers, typed configs, or registry aliases lazily during execution; construction, discovery, schema registration, and confirmation classification perform no connection I/O. Native async drivers are awaited while synchronous calls run via `asyncio.to_thread`, and providers own SQL validation, approval classification, BigQuery billing caps, and Spanner snapshot staleness.

The bridge validates before policy, materializes defaults, and isolates invocation arguments: approved tickets must match the operation, token-free canonical arguments, and caller owner, and pending ticket arguments are copied separately from invocation state. Mutations and ambiguous executable SQL (including `WITH`) require approval; only `ToolPolicyError` becomes agent `PermissionDeniedError` while native errors and cancellation preserve identity.

## 2. Agent-to-Agent Protocol (A2A 1.0)

[`vertebra.agent.a2a`](../../../../../src/py/vertebra/agent/a2a/service.py)[^src-agent-a2a-service-py] provides peer-to-peer agent collaboration with verifiable cryptographic identity. All A2A 1.0 data models in [`a2a/models.py`](../../../../../src/py/vertebra/agent/a2a/models.py)[^src-agent-a2a-models-py] subclass [`FrozenCamelizedStruct`](../../../../../src/py/vertebra/_typing.py) (`frozen=True, gc=False, rename="camel", omit_defaults=True, repr_omit_defaults=True`), while MCP `2026-07-28` models in [`mcp/models.py`](../../../../../src/py/vertebra/agent/mcp/models.py)[^src-agent-mcp-models-py] subclass [`FrozenBaseStruct`](../../../../../src/py/vertebra/_typing.py).

- **Cryptographic Card Signing ([`signing.py`](../../../../../src/py/vertebra/agent/a2a/signing.py)[^src-agent-a2a-signing-py])**: `AgentCardSigner` (`sign`, `verify`, `canonical_payload_bytes`) signs and verifies `AgentCard` manifests with RFC 7515 JSON Web Signature (`HS256`) envelopes over key-sorted canonical JSON using `A2AConfig.card_secret`.
- **Automatic Interface & Dynamic URL Discovery (`models.py`, `service.py`, `routes.py`)**: Serves `/.well-known/agent-card.json` containing the public `AgentCard`, supported skills (including `ToolSpec` entries with `"a2a" in exposure`), operational modes, and JWS signatures. When `AgentCard.supported_interfaces` is omitted (or contains a relative URL), `AgentCard.resolve_for_request()` and `A2AService.resolve_agent_card()` synthesize a default `JSONRPC` 1.0 `AgentInterface` and resolve its public URL dynamically from `A2AConfig.base_url` or the incoming `Forwarded`, `X-Forwarded-Proto`, `X-Forwarded-Host`, and `Host` request headers.
- **Task Lifecycle**: Enforces strict state transitions across `submitted` $\to$ `working` $\to$ `completed` / `failed` / `canceled` with owner-isolated `A2ATaskStore`, SSRF-validated `PushNotificationDispatcher` webhooks, and SSE replay (`SseStreamBroker`).
- **Client SDK (`A2aClient`)**: TypeScript client in [`src/agent/index.ts`](../../../../../src/ts/vertebra-client/src/agent/index.ts) for querying `/.well-known/agent-card.json` with `ETag` `304` caching, sending/streaming messages, resubscribing to tasks, and canceling runs.

## 3. Workflows, DAG Engine & SpecTree

[`vertebra.agent.workflow`](../../../../../src/py/vertebra/agent/workflow.py)[^src-agent-workflow-py], [`vertebra.agent.spec`](../../../../../src/py/vertebra/agent/spec.py)[^src-agent-spec-py], and [`vertebra.agent.runtime`](../../../../../src/py/vertebra/agent/runtime.py)[^src-agent-runtime-py] implement durable dynamic workflows:

- **Dynamic DAG Engine**: `DynamicWorkflow` and `WorkflowEngine` compile declarative `WorkflowNode` steps into a directed acyclic graph with topological sorting, cycle detection, checkpointing (`WorkflowCheckpoint`), and dependency resolution.
- **LiveState Integration**: `WorkflowLiveState` emits live progress updates and state deltas over `LiveState` channels, consumed on the frontend by `WorkflowLiveSubscriber`.
- **SpecTree**: Hierarchical task decomposition tree (`SpecNode`, `SpecTree`, `Agent`, `AgentGroup`) with deterministic execution order, agent transfer (`TRANSFER_TOOL_NAME`), and error isolation.
- **AgentRuntime**: Central coordinator managing `ToolRegistry`, `ConfirmationGate` (`ConfirmationTicket`), multi-turn execution sessions (`AgentSessionStore`), `GoogleGenAIProvider`, and telemetry exporters.

## 4. Rust PyO3 Acceleration, Headers, Pagination & Ingress Discovery

Native FFI acceleration in [`src/rs/vertebra-py/src/agent.rs`](../../../../../src/rs/vertebra-py/src/agent.rs)[^src-vertebra-py-src-agent-rs], Python protocol helpers in [`mcp/client.py`](../../../../../src/py/vertebra/agent/mcp/client.py)[^src-agent-mcp-client-py] and [`core/cursor.py`](../../../../../src/py/vertebra/agent/core/cursor.py)[^src-agent-core-cursor-py], and ingress routing in [`src/rs/vertebra-http/src/discovery.rs`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs]:

- **JSON-RPC Frame Classification (`agent.rs`)**: `classify_jsonrpc_frame` (`FRAME_KIND_NOTIFICATION`, `FRAME_KIND_SINGLE_REQUEST`, `FRAME_KIND_BATCH_REQUEST`, `FRAME_KIND_RESPONSE`) parses raw JSON-RPC bytes via `serde_json::from_slice` without Python dict materialization.
- **Schema Digest Cache (`agent.rs`)**: `SchemaDigestCache` (`compute_schema_digest`, `put`, `get_etag`, `get_bytes`, `combined_etag`) maintains a thread-safe `RwLock<HashMap>` of SHA-256 digests and quoted HTTP `ETag` values for tool schemas and `server/discover` responses.
- **MCP Observability Headers & Pagination (`mcp/client.py`, `mcp/service.py`, `core/cursor.py`)**: `mcp_request_headers`, `encode_mcp_header_value`, and `decode_mcp_header_value` derive and RFC 2047-encode `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`, and `Mcp-Session-Id` headers alongside W3C `traceparent` injection (`inject_w3c_trace_context`), while `encode_cursor`, `decode_cursor`, and `paginate_sequence` (`CursorPage`) provide URL-safe Base64 offset cursors across tools, resources, prompts, skills, and A2A tasks.
- **Rust Well-Known Discovery Cache (`discovery.rs`)**: `WellKnownDiscoveryCache` serves `/.well-known/agent-card.json`, `/.well-known/mcp.json`, and `/.well-known/oauth-protected-resource` directly from `ArcSwapOption<CachedJsonDocument>` memory in `vertebra-http` with `If-None-Match` `304 Not Modified` handling and zero Python round-trips.

## 5. GenAI Telemetry & Observability

[`vertebra.telemetry.genai`](../../../../../src/py/vertebra/telemetry/genai.py)[^src-telemetry-genai-py] implements OpenTelemetry GenAI Semantic Conventions via `GenAiUsage` (`input_tokens`, `output_tokens`, `thought_tokens`, `cached_input_tokens`, and `ModelPricing` USD cost calculation), `GenAiDurationHistogram` (`chat`, `embeddings`, `tool_call`, `workflow_step` latency buckets), and JSONL `LogSink` audit flushing (see [Logging, Tracing & GenAI Metrics](../telemetry/logging-tracing-and-genai.md)).

Provenance sources: [^src-tests-unit-agent-test-mcp-conformance-py].

[^src-typing-public-py]: `src/py/vertebra/typing.py` — Public neutral ToolProvider and ToolPolicyError exports.
[^src-db-bigquery-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery operations and billing policies.
[^src-db-spanner-tools-py]: `src/py/vertebra/db/adapters/spanner/tools.py` — Adapter-owned Spanner operations and snapshot policies.
[^src-agent-init-py]: `src/py/vertebra/agent/__init__.py` — Python implementation module (src/py/vertebra/agent/__init__.py).
[^src-agent-core-schema-py]: `src/py/vertebra/agent/core/schema.py` — Python implementation module (src/py/vertebra/agent/core/schema.py).
[^src-agent-core-cursor-py]: `src/py/vertebra/agent/core/cursor.py` — Python implementation module (src/py/vertebra/agent/core/cursor.py).
[^src-agent-tools-init-py]: `src/py/vertebra/agent/tools/__init__.py` — Generic provider adaptation and caller-bound confirmation.
[^src-agent-mcp-models-py]: `src/py/vertebra/agent/mcp/models.py` — Python implementation module (src/py/vertebra/agent/mcp/models.py).
[^src-agent-mcp-registry-py]: `src/py/vertebra/agent/mcp/registry.py` — Python implementation module (src/py/vertebra/agent/mcp/registry.py).
[^src-agent-mcp-webmcp-py]: `src/py/vertebra/agent/mcp/webmcp.py` — Python implementation module (src/py/vertebra/agent/mcp/webmcp.py).
[^src-agent-mcp-service-py]: `src/py/vertebra/agent/mcp/service.py` — Python implementation module (src/py/vertebra/agent/mcp/service.py).
[^src-agent-mcp-routes-py]: `src/py/vertebra/agent/mcp/routes.py` — Python implementation module (src/py/vertebra/agent/mcp/routes.py).
[^src-agent-mcp-stdio-py]: `src/py/vertebra/agent/mcp/stdio.py` — Python implementation module (src/py/vertebra/agent/mcp/stdio.py).
[^src-agent-mcp-client-py]: `src/py/vertebra/agent/mcp/client.py` — Python implementation module (src/py/vertebra/agent/mcp/client.py).
[^src-agent-a2a-models-py]: `src/py/vertebra/agent/a2a/models.py` — Python implementation module (src/py/vertebra/agent/a2a/models.py).
[^src-agent-a2a-service-py]: `src/py/vertebra/agent/a2a/service.py` — Python implementation module (src/py/vertebra/agent/a2a/service.py).
[^src-agent-a2a-signing-py]: `src/py/vertebra/agent/a2a/signing.py` — Python implementation module (src/py/vertebra/agent/a2a/signing.py).
[^src-agent-workflow-py]: `src/py/vertebra/agent/workflow.py` — Python implementation module (src/py/vertebra/agent/workflow.py).
[^src-agent-spec-py]: `src/py/vertebra/agent/spec.py` — Python implementation module (src/py/vertebra/agent/spec.py).
[^src-agent-runtime-py]: `src/py/vertebra/agent/runtime.py` — Python implementation module (src/py/vertebra/agent/runtime.py).
[^src-telemetry-genai-py]: `src/py/vertebra/telemetry/genai.py` — Python implementation module (src/py/vertebra/telemetry/genai.py).
[^src-vertebra-py-src-agent-rs]: `src/rs/vertebra-py/src/agent.rs` — Rust implementation module (src/rs/vertebra-py/src/agent.rs).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
[^src-src-agent-index-ts]: `src/ts/vertebra-client/src/agent/index.ts` — TypeScript client module (src/ts/vertebra-client/src/agent/index.ts).
[^src-src-webmcp-ts]: `src/ts/vertebra-client/src/webmcp.ts` — TypeScript WebMCP client module (src/ts/vertebra-client/src/webmcp.ts).
[^src-tests-unit-agent-test-mcp-conformance-py]: `src/py/tests/unit/agent/test_mcp_conformance.py` — Python implementation module (src/py/tests/unit/agent/test_mcp_conformance.py).
