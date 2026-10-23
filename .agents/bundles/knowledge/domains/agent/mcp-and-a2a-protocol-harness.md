---
type: Concept
title: "Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols"
description: "The `vertebra.agent` protocol harness: Model Context Protocol (MCP 2026-07-28) SSE and Stdio transports, Agent-to-Agent (A2A 1.0) JCS/RFC 8785 Ed25519/ECDSA signed agent cards, durable DAG workflows, SpecTree execution, Rust PyO3 cryptographic fastpaths, and GenAI telemetry."
status: stable
tags: [domains, agent, mcp, a2a, workflow, telemetry]
sources:
  - resource: src/py/vertebra/agent/__init__.py
  - resource: src/py/vertebra/agent/mcp/service.py
  - resource: src/py/vertebra/agent/mcp/routes.py
  - resource: src/py/vertebra/agent/mcp/stdio.py
  - resource: src/py/vertebra/agent/a2a/service.py
  - resource: src/py/vertebra/agent/a2a/signing.py
  - resource: src/py/vertebra/agent/workflow.py
  - resource: src/py/vertebra/agent/spec.py
  - resource: src/py/vertebra/agent/runtime.py
  - resource: src/py/vertebra/telemetry/genai.py
  - resource: src/rs/vertebra-py/src/agent.rs
  - resource: src/rs/vertebra-http/src/discovery.rs
  - resource: src/ts/vertebra-client/src/agent/index.ts
  - resource: src/py/tests/unit/agent/test_mcp_conformance.py
  - resource: src/py/tests/unit/agent/test_a2a_protocol.py
  - resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
  - resource: src/py/tests/unit/agent/test_core_and_observability.py
---

# Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols

[`vertebra.agent`](../../../../../src/py/vertebra/agent/__init__.py) provides an enterprise agentic harness combining Model Context Protocol (`MCP 2026-07-28`), Agent-to-Agent (`A2A 1.0`), durable DAG workflows, SpecTree hierarchical execution, Rust FFI cryptographic acceleration, and OpenTelemetry GenAI semantic conventions.

## 1. Model Context Protocol (MCP 2026-07-28)

[`vertebra.agent.mcp`](../../../../../src/py/vertebra/agent/mcp/service.py) implements the MCP `2026-07-28` specification across SSE and Stdio transports.

| Component | Responsibility |
| :--- | :--- |
| [`McpService`](../../../../../src/py/vertebra/agent/mcp/service.py) | Registers tools (`register_tool`), prompts (`register_prompt`), and resources (`register_resource`). Handles JSON-RPC 2.0 requests (`initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read`, `prompts/list`, `prompts/get`, `ping`). |
| [`McpController`](../../../../../src/py/vertebra/agent/mcp/controllers.py) | Exposes SSE endpoints at `/sse` (opening a persistent event stream and returning endpoint URI `/messages?session_id=...`) and handles incoming JSON-RPC POST messages. |
| [`McpStdioServer`](../../../../../src/py/vertebra/agent/mcp/stdio.py) | Provides local stdio communication for CLI agents and local developer tooling via standard input/output streams. |
| [`ClientSessionStore`](../../../../../src/py/vertebra/agent/mcp/service.py) | Tracks active sessions, protocol version negotiation (`2026-07-28`), client capabilities, and roots. |
| [`McpClient`](../../../../../src/ts/vertebra-client/src/agent/index.ts) | TypeScript client SDK connecting over SSE or WebSockets with automatic reconnection, tool dispatch, and streaming progress callbacks. |

## 2. Agent-to-Agent Protocol (A2A 1.0)

[`vertebra.agent.a2a`](../../../../../src/py/vertebra/agent/a2a/service.py) provides peer-to-peer agent collaboration with verifiable cryptographic identity.

- **Cryptographic Card Signing (`signing.py`)**: Signs and verifies `AgentCard` manifests using JSON Canonicalization Scheme (JCS, RFC 8785) with Ed25519 (`ed25519`) or ECDSA P-256 (`es256`).
- **Discovery Endpoint**: Serves `/.well-known/agent.json` containing the public `AgentCard`, supported skills, operational modes, and verification keys.
- **Task Lifecycle**: Enforces strict state transitions across `submitted` $\to$ `working` $\to$ `completed` / `failed` with incremental progress reporting and cancellation handling.
- **Client SDK (`A2aClient`)**: TypeScript client for querying peer agent capabilities, initiating task handoffs, and validating remote agent card signatures.

## 3. Workflows, DAG Engine & SpecTree

[`vertebra.agent.workflow`](../../../../../src/py/vertebra/agent/workflow.py) and [`vertebra.agent.spec`](../../../../../src/py/vertebra/agent/spec.py) implement durable dynamic workflows.

- **Dynamic DAG Engine**: `DynamicWorkflow` compiles declarative steps (`StepKind.LLM`, `StepKind.TOOL`, `StepKind.GATE`, `StepKind.PARALLEL`, `StepKind.MAP`) into a directed acyclic graph with topological sorting, cycle detection, and dependency resolution.
- **LiveState Integration**: Workflow execution emits live progress updates and state deltas via `LiveState` channels, synchronizing status to connected clients in real time.
- **SpecTree**: Hierarchical task decomposition tree (`SpecNode`, `SpecTreeExecutor`) with deterministic execution order, boundary rollback, and error isolation.
- **AgentRuntime**: Central coordinator managing tool registries, execution sessions, workflow runtimes, and telemetry exporters.

## 4. Rust PyO3 Acceleration & Ingress Fastpaths

Native FFI acceleration in [`src/rs/vertebra-py/src/agent.rs`](../../../../../src/rs/vertebra-py/src/agent.rs) and ingress routing in [`src/rs/vertebra-http/src/discovery.rs`](../../../../../src/rs/vertebra-http/src/discovery.rs):

- **Cryptographic Fastpaths**: `verify_hmac_sha256_token` and `sign_hmac_sha256_token` for zero-allocation token validation and creation.
- **Cursor Tokens**: `encode_cursor_token` and `decode_cursor_token` provide URL-safe, tamper-resistant pagination tokens with embedded timestamps and filter hashes.
- **MCP Header Extraction**: `extract_mcp_headers` normalizes incoming `Mcp-Session-Id`, `Mcp-Method`, `Mcp-Tool-Name`, and trace propagation headers before Python dispatch.
- **Rust Discovery Handler**: Caches `/.well-known/agent.json` and `/.well-known/mcp.json` in memory with atomic reloads.

## 5. GenAI Telemetry & Observability

[`vertebra.telemetry.genai`](../../../../../src/py/vertebra/telemetry/genai.py) implements the OpenTelemetry GenAI Semantic Conventions:

- **Usage Tracking**: `GenAiUsage` tracks `input_tokens`, `output_tokens`, `thought_tokens`, `cached_input_tokens`, and calculates USD cost using configurable `ModelPricing` tables.
- **Duration Histograms**: `GenAiDurationHistogram` records latency distributions across operations (`chat`, `embeddings`, `tool_call`, `workflow_step`).
- **Audit Logging**: `LogSink` with `AsyncLogCore` integration flushes newline-delimited JSONL audit records capturing trace IDs, parent span IDs, prompt metadata, tool inputs/outputs, and policy decisions.
