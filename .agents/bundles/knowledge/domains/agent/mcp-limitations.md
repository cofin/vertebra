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
  at: "2026-11-17T05:50:00Z"
  commit: 5a833832
- by: agent:flow-executor
  at: "2026-11-13T20:35:00Z"
  commit: 54a5103d
- by: agent:flow-setup
  at: "2026-11-12T23:30:00Z"
  commit: 18baadcd
sources:
- id: mcp-sessions
  resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore
  title: Per-process and CacheMCPSessionStore legacy MCP session stores
- id: mcp-tasks
  resource: src/py/vertebra/agent/mcp/tasks.py::CacheMCPTaskStore
  title: SyncCacheStore-backed distributed MCP task store
- id: mcp-sse
  resource: src/py/vertebra/agent/core/sse.py::CacheSseStreamBroker
  title: SyncCacheStore-backed SSE replay broker
- id: a2a-service
  resource: src/py/vertebra/agent/a2a/service.py::CacheA2ATaskStore
  title: SyncCacheStore-backed A2A task store and PushNotificationDispatcher
- id: mcp-ts-client
  resource: src/ts/vertebra-client/src/agent/index.ts::McpClient
  title: Stateless MCP 2026-07-28 TypeScript client SDK
- id: mcp-registry
  resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry
  title: ToolRegistry and host-extension ToolRegistry alias subclass
- id: mcp-identity
  resource: src/py/vertebra/agent/core/identity.py::MtlsIdentitySource
  title: Immediate peer checks for trusted proxy mTLS identity
- id: agent-plugin
  resource: src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native
  title: Header-only native context construction without peer socket address
---

# MCP Implementation Limitations

<!-- truth: start -->
- Default `MCPSessionStore`, `MCPTaskStore`, `SseStreamBroker`, and `A2ATaskStore` store state in per-process memory when `cache_url` is unset; multi-worker sharing requires `MCPConfig(cache_url=...)` (`CacheMCPSessionStore` over `AsyncCacheStore`, `CacheMCPTaskStore`, `CacheSseStreamBroker`) or `A2AConfig(cache_url=...)` (`CacheA2ATaskStore`, cache-backed `PushNotificationDispatcher`, `CacheSseStreamBroker`) [^mcp-sessions] [^mcp-tasks] [^a2a-service].
- `CacheSseStreamBroker` persists bounded `Last-Event-ID` replay frames across workers via `SyncCacheStore`, while live push delivery to an already-open SSE stream remains local to the worker holding that subscriber stream unless clients reconnect with `Last-Event-ID` [^mcp-sse].
- Distributed cache-backed stores accept `vertebra.cache` URL schemes (`memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, and object-store schemes `s3://`, `gs://`, `az://`, `r2://`, `oci://`, `file://`); SQL schemes are outside `vertebra.cache` [^mcp-sessions] [^a2a-service].
- `@vertebra/client` `McpClient` operates as a stateless MCP `2026-07-28` HTTP client and does not negotiate legacy `initialize` / `GET /mcp` SSE sessions [^mcp-ts-client].
- Native `App` routes constructed via `AgentPlugin.mount_native` supply headers without socket peer addresses, so `MtlsIdentitySource` refuses header identity on header-only native paths [^mcp-identity] [^agent-plugin].
- `ToolRegistry(ToolRegistry)` acts as a host-extension compatibility alias over `ToolRegistry` [^mcp-registry].
<!-- truth: end -->

## Scope & Operational Notes

- `src/py/vertebra/agent/` passes Pyright with zero errors across `agent/mcp/*`, `agent/a2a/*`, and `agent/plugin.py`, while `src/py/vertebra/agent/__init__.py` emits `reportUnsupportedDunderAll` diagnostics on PEP 562 lazy `__getattr__` exports.
- Multi-worker deployments running legacy `2025-03-26` / `2025-06-18` / `2025-11-25` clients without `cache_url` require sticky single-worker routing or stateless `2026-07-28` clients (`subscriptions/listen`) because default `MCPSessionStore` and `SseStreamBroker` replay buffers are process-local [^mcp-sessions].
- For verified Google Cloud proxy authentication across both native `App` and host adapters, configure `IapIdentitySource` (`vertebra-core[iap]`), which validates ES256 JWT assertions without requiring socket peer CIDR matching; plain `x-goog-authenticated-user-email` headers are never trusted [^mcp-identity].

Current protocol capabilities and extensions are documented in [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](mcp-and-a2a-protocol-harness.md).

[^mcp-sessions]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore` — Per-process and `CacheMCPSessionStore` legacy MCP session stores.
[^mcp-tasks]: `src/py/vertebra/agent/mcp/tasks.py::CacheMCPTaskStore` — `SyncCacheStore`-backed distributed MCP task store.
[^mcp-sse]: `src/py/vertebra/agent/core/sse.py::CacheSseStreamBroker` — `SyncCacheStore`-backed SSE replay broker.
[^a2a-service]: `src/py/vertebra/agent/a2a/service.py::CacheA2ATaskStore` — `SyncCacheStore`-backed A2A task store and `PushNotificationDispatcher`.
[^mcp-ts-client]: `src/ts/vertebra-client/src/agent/index.ts::McpClient` — Stateless MCP `2026-07-28` TypeScript client SDK.
[^mcp-registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry` — `ToolRegistry` and host-extension `ToolRegistry` alias subclass.
[^mcp-identity]: `src/py/vertebra/agent/core/identity.py::MtlsIdentitySource` — Immediate-peer proxy policy.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Header-only native context forwarding.
