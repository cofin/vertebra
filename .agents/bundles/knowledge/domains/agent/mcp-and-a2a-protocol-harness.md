---
type: Concept
title: "Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols"
description: "The `vertebra.agent` protocol harness: Model Context Protocol (MCP 2026-07-28) SSE and Stdio transports, Agent-to-Agent (A2A 1.0) JCS/RFC 8785 Ed25519/ECDSA signed agent cards, durable DAG workflows, SpecTree execution, Rust PyO3 cryptographic fastpaths, and GenAI telemetry."
status: stable
tags: [domains, agent, mcp, a2a, workflow, telemetry]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-agent-init-py
    resource: src/py/vertebra/agent/__init__.py
    title: "Python implementation module (src/py/vertebra/agent/__init__.py)"
  - id: src-agent-tools-init-py
    resource: src/py/vertebra/agent/tools/__init__.py
    title: "Governed GCP tool policy and registration"
  - id: src-agent-tools-bigquery-py
    resource: src/py/vertebra/agent/tools/bigquery.py
    title: "Governed BigQuery agent tools"
  - id: src-agent-tools-spanner-py
    resource: src/py/vertebra/agent/tools/spanner.py
    title: "Governed Spanner agent tools"
  - id: src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py
    resource: src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
    title: "GCP adapter and agent tool behavior tests"
  - id: src-agent-mcp-service-py
    resource: src/py/vertebra/agent/mcp/service.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/service.py)"
  - id: src-agent-mcp-routes-py
    resource: src/py/vertebra/agent/mcp/routes.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/routes.py)"
  - id: src-agent-mcp-stdio-py
    resource: src/py/vertebra/agent/mcp/stdio.py
    title: "Python implementation module (src/py/vertebra/agent/mcp/stdio.py)"
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
  - id: src-tests-unit-agent-test-mcp-conformance-py
    resource: src/py/tests/unit/agent/test_mcp_conformance.py
    title: "Python implementation module (src/py/tests/unit/agent/test_mcp_conformance.py)"
  - id: src-tests-unit-agent-test-a2a-protocol-py
    resource: src/py/tests/unit/agent/test_a2a_protocol.py
    title: "Python implementation module (src/py/tests/unit/agent/test_a2a_protocol.py)"
  - id: src-tests-unit-agent-test-runtime-and-workflows-py
    resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
    title: "Python implementation module (src/py/tests/unit/agent/test_runtime_and_workflows.py)"
  - id: src-tests-unit-agent-test-core-and-observability-py
    resource: src/py/tests/unit/agent/test_core_and_observability.py
    title: "Python implementation module (src/py/tests/unit/agent/test_core_and_observability.py)"
---

# Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols

<!-- truth: start -->
- `MCPService` and `MCPHttpHandler` implement MCP `2026-07-28` JSON-RPC 2.0 over HTTP/SSE and stdio, enforcing `ConfirmationGate` tokens (`-32003`), opt-in `webmcp=True` manifests, and `Mcp-Name`/`Mcp-Method` header binding.
- `vertebra.agent.tools` owns `GCPDatabaseToolPolicy`, `build_bigquery_tools`, `build_spanner_tools`, `is_mutating_sql`, and `register_gcp_database_tools`; `vertebra.agent` exposes them lazily with static type-checking parity. The removed `vertebra.db.mcp_tools` module has no compatibility export.
- `vertebra.agent.a2a` implements A2A `1.0` task lifecycles, dynamic request-origin `AgentCard` URLs, RFC 8785 JCS Ed25519/ECDSA card signatures, and DAG/`SpecTree` workflow execution.
<!-- truth: end -->

[`vertebra.agent`](../../../../../src/py/vertebra/agent/__init__.py)[^src-agent-init-py] provides an enterprise agentic harness combining Model Context Protocol (`MCP 2026-07-28`), Agent-to-Agent (`A2A 1.0`), durable DAG workflows, SpecTree hierarchical execution, Rust FFI cryptographic acceleration, and OpenTelemetry GenAI semantic conventions.

## 1. Model Context Protocol (MCP 2026-07-28)

[`vertebra.agent.mcp`](../../../../../src/py/vertebra/agent/mcp/service.py)[^src-agent-mcp-service-py] implements the MCP `2026-07-28` specification across SSE and Stdio transports.

| Component | Responsibility |
| :--- | :--- |
| [`MCPService`](../../../../../src/py/vertebra/agent/mcp/service.py) | Registers tools (`register_tool`), prompts (`register_prompt`), and resources (`register_resource`). Handles JSON-RPC 2.0 requests (`initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read`, `prompts/list`, `prompts/get`, `ping`). `tools/call` enforces `Tool.requires_confirmation` through a `ConfirmationGate` (`_meta["vertebra/confirmationToken"]`, error `-32003`); approval binds to the tool, caller, and arguments, and `AgentPlugin` shares the runtime gate. WebMCP manifests list only tools opted in with `webmcp=True` (`@mcp_tool(webmcp=True)`, `Tool(webmcp=True)`, route `opt={"mcp_tool": ..., "webmcp": True}`, and guarded `SkillController` tools). `MCPHttpHandler` rejects an `Mcp-Name` header when the body field it names is missing or different, rejects `Mcp-Method`/`Mcp-Name` headers on JSON-RPC batch bodies, and `JSONRPCRouter` passes dict params to `params` handlers without unwrapping nested keys. |
| [`MCPController`](../../../../../src/py/vertebra/agent/mcp/controllers.py) | Exposes SSE endpoints at `/sse` (opening a persistent event stream and returning endpoint URI `/messages?session_id=...`) and handles incoming JSON-RPC POST messages. |
| [`McpStdioServer`](../../../../../src/py/vertebra/agent/mcp/stdio.py)[^src-agent-mcp-stdio-py] | Provides local stdio communication for CLI agents and local developer tooling via standard input/output streams. |
| [`ClientSessionStore`](../../../../../src/py/vertebra/agent/mcp/service.py) | Tracks active sessions, protocol version negotiation (`2026-07-28`), client capabilities, and roots. |
| [`McpClient`](../../../../../src/ts/vertebra-client/src/agent/index.ts)[^src-src-agent-index-ts] | TypeScript client SDK connecting over SSE or WebSockets with automatic reconnection, tool dispatch, and streaming progress callbacks. |

### Governed GCP database tools

[`vertebra.agent.tools`](../../../../../src/py/vertebra/agent/tools/__init__.py)[^src-agent-tools-init-py]
provides the shared policy, SQL mutation classifier, confirmation checks, and registration entry
point. Agent tools use `PermissionDeniedError` from `vertebra.agent.core`; database adapters do
not import the agent or framework layers.

[`build_bigquery_tools`](../../../../../src/py/vertebra/agent/tools/bigquery.py)[^src-agent-tools-bigquery-py]
builds `bigquery_dry_run`, `bigquery_query`, and `bigquery_object_table_inspect`.
[`build_spanner_tools`](../../../../../src/py/vertebra/agent/tools/spanner.py)[^src-agent-tools-spanner-py]
builds `spanner_query` and `spanner_schema_inspect`. Each builder accepts an explicit engine
driver, a typed config, or a registry alias and resolves `provide_driver` at invocation time.
Registration can supply a single engine without resolving an unconfigured default for the other.
Synchronous driver operations run through `asyncio.to_thread`; asynchronous operations dispatch
through `AsyncDatabaseDriver` without inspecting returned awaitables.

BigQuery tools retain dry-run byte estimates and policy ceilings, Object Table metadata and DDL,
and confirmation-gated mutations. Spanner reads use read-only snapshots with optional exact
staleness; schema inspection queries only `information_schema.tables` and
`information_schema.columns`, binding table names as parameters. The
[tool behavior tests](../../../../../src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py)[^src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py]
exercise real tools, registries, confirmation gates, and engine adapters through external client
ports, including sync thread dispatch, async execution, rejected mutations, and driver errors.

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

[^src-agent-tools-init-py]: `src/py/vertebra/agent/tools/__init__.py` — Shared GCP tool policy and registration.
[^src-agent-tools-bigquery-py]: `src/py/vertebra/agent/tools/bigquery.py` — BigQuery agent tool builders.
[^src-agent-tools-spanner-py]: `src/py/vertebra/agent/tools/spanner.py` — Spanner agent tool builders.
[^src-tests-unit-extensions-test-gcp-adapters-and-sqlalchemy-py]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — GCP adapter and tool behavior tests.
