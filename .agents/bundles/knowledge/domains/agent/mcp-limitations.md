---
type: Gotcha
title: MCP Implementation Limitations
description: Residual architectural boundaries across the native vertebra.agent.mcp server and TypeScript client that callers and maintainers must account for.
status: stable
tags:
- agent
- mcp
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-executor
  at: "2026-11-13T20:35:00Z"
  commit: b9429c6d
- by: agent:flow-setup
  at: "2026-11-12T23:30:00Z"
  commit: 18baadcd
sources:
- id: mcp-sessions
  resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore
  title: Per-process in-memory legacy MCP session store
- id: mcp-ts-client
  resource: src/ts/vertebra-client/src/agent/index.ts::McpClient
  title: Stateless MCP 2026-07-28 TypeScript client SDK
- id: mcp-registry
  resource: src/py/vertebra/agent/mcp/registry.py::MCPRegistry
  title: ToolRegistry and host-extension MCPRegistry alias subclass
- id: mcp-identity
  resource: src/py/vertebra/agent/core/identity.py::MtlsIdentitySource
  title: Immediate peer checks for trusted proxy mTLS identity
- id: agent-plugin
  resource: src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native
  title: Header-only native context construction without peer socket address
---

# MCP Implementation Limitations

<!-- truth: start -->
- `MCPSessionStore` stores legacy `initialize` sessions and `resources/subscribe` subscriptions strictly in per-process memory with no cross-worker or external backing store [^mcp-sessions].
- `@vertebra/client` `McpClient` remains a stateless MCP `2026-07-28` HTTP client and does not negotiate legacy `initialize` / `GET /mcp` SSE sessions [^mcp-ts-client].
- Native `App` routes constructed via `AgentPlugin.mount_native` supply headers without socket peer addresses, so `MtlsIdentitySource` refuses header identity on header-only native paths [^mcp-identity] [^agent-plugin].
- `MCPRegistry(ToolRegistry)` remains as a host-extension compatibility shim pending unified extension registry cleanup [^mcp-registry].
<!-- truth: end -->

## Scope & Operational Notes

- Multi-worker deployments running legacy `2025-03-26` / `2025-06-18` / `2025-11-25` clients require sticky single-worker routing or stateless `2026-07-28` clients (`subscriptions/listen`) because `MCPSessionStore` and `SseStreamBroker` replay buffers are process-local [^mcp-sessions].
- For verified Google Cloud proxy authentication across both native `App` and host adapters, configure `IapIdentitySource` (`vertebra-core[iap]`), which validates ES256 JWT assertions without requiring socket peer CIDR matching; plain `x-goog-authenticated-user-email` headers are never trusted [^mcp-identity].

Current protocol capabilities and extensions are documented in [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](mcp-and-a2a-protocol-harness.md).

[^mcp-sessions]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore` — Per-process in-memory legacy MCP session store.
[^mcp-ts-client]: `src/ts/vertebra-client/src/agent/index.ts::McpClient` — Stateless MCP `2026-07-28` TypeScript client SDK.
[^mcp-registry]: `src/py/vertebra/agent/mcp/registry.py::MCPRegistry` — `ToolRegistry` and host-extension `MCPRegistry` alias subclass.
[^mcp-identity]: `src/py/vertebra/agent/core/identity.py::MtlsIdentitySource` — Immediate-peer proxy policy.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Header-only native context forwarding.
