---
type: Gotcha
title: MCP Implementation Limitations
description: Verified gaps between the native vertebra.agent.mcp server and the MCP specification surfaces that callers and maintainers must account for.
status: stable
tags:
- agent
- mcp
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-sync
  at: "2026-11-12T03:53:56Z"
  commit: c007f853
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
sources:
- id: mcp-service
  resource: src/py/vertebra/agent/mcp/service.py::MCPCore
  title: Transport-agnostic MCP request handling and the MCPService alias subclass
- id: mcp-registry
  resource: src/py/vertebra/agent/mcp/registry.py::MCPRegistry
  title: ToolRegistry and the MCPRegistry alias subclass
- id: mcp-models
  resource: src/py/vertebra/agent/mcp/models.py
  title: MCP protocol constants and error codes
- id: mcp-stdio
  resource: src/py/vertebra/agent/mcp/stdio.py::run_stdio_server
  title: Stdio session context and buffered input reader
- id: mcp-identity
  resource: src/py/vertebra/agent/core/identity.py::MtlsIdentitySource
  title: Immediate peer checks for trusted proxy identity
- id: agent-plugin
  resource: src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native
  title: Header-only native context construction
- id: cli-ai
  resource: src/py/vertebra/cli/ai.py
  title: vtb ai mcp serve and bridge commands
---

# MCP Implementation Limitations

<!-- truth: start -->
- `MCPCore.handle` accepts only `MCP-Protocol-Version: 2026-07-28`; any other value returns HTTP `400` with JSON-RPC `-32022` (`UNSUPPORTED_PROTOCOL_VERSION`) and no list of supported versions, so `2025-11-25`, `2025-06-18`, and `2025-03-26` clients cannot negotiate down [^mcp-service] [^mcp-models].
- `initialize`, `ping`, `logging/setLevel`, `resources/subscribe`, `resources/unsubscribe`, and `tasks/result` return `-32601`; `GET /mcp` returns `405` with `Allow: POST, OPTIONS`, so there are no server-initiated SSE streams and no `DELETE` session termination [^mcp-service].
- `Mcp-Session-Id` is echoed on the response and never validated or stored; there is no session store [^mcp-service].
- `Mcp-Method` and `Mcp-Name` are compared with the JSON-RPC body; `Mcp-Param-*` headers are not read or validated [^mcp-service].
- `MCPCore.notify_resource_updated` reads `_resource_subscriptions`, which no request path populates, so resource update notifications reach no session [^mcp-service].
- `server/discover` advertises skills under a non-standard `capabilities.skills` key instead of `capabilities.extensions`, and no framework mount wires a skill catalog [^mcp-service].
- `MCPRegistry(ToolRegistry)` and `MCPService(MCPCore)` are alias subclasses with no added behavior [^mcp-registry] [^mcp-service].
- `vtb ai mcp serve` constructs an empty `MCPService()` instead of serving the discovered application's tools [^cli-ai].
<!-- truth: end -->

## Caller Context and Remaining Identity Limits

Adapters carry trusted application identity into bound dispatch. Proxy identity requires an explicit source and opt-in; mTLS policies require a known proxy peer, while custom callable policies own their trust decisions. Plain IAP email headers do not authenticate callers; verified IAP JWT support remains pending. Promoted tasks use independent cancellation and owner-scoped access; anonymous requests cannot access owner-bound tasks [^mcp-service] [^mcp-models].

Native header-only routes do not supply a peer address to identity policies; mTLS policies therefore refuse header identity on those paths. Stdio binds one session context, but its default reader still collects input before dispatching and does not relay server notifications [^mcp-stdio] [^mcp-identity] [^agent-plugin].

## Planned Fixes

The `mcp-protocol-completeness` flow owns these gaps: guard and confirmation correctness, protocol version negotiation with legacy sessions and `GET` SSE streams, resource subscriptions, the skills extension, MCP Apps UI metadata, Litestar delegation, `vtb ai mcp serve` and `bridge`, and the matching docs. Server-initiated SSE depends on incremental mounted streaming from the `server-runtime-correctness` flow ([#81](https://github.com/cofin/vertebra/issues/81)). Tracking issues: [#109](https://github.com/cofin/vertebra/issues/109), [#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111), [#112](https://github.com/cofin/vertebra/issues/112), [#114](https://github.com/cofin/vertebra/issues/114).

Current protocol behavior is described in [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](mcp-and-a2a-protocol-harness.md).

[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — Transport-agnostic MCP request handling and the `MCPService` alias subclass.
[^mcp-registry]: `src/py/vertebra/agent/mcp/registry.py::MCPRegistry` — `ToolRegistry` and the `MCPRegistry` alias subclass.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py` — MCP protocol constants and error codes.
[^cli-ai]: `src/py/vertebra/cli/ai.py` — `vtb ai mcp serve` and `bridge` commands.

[^mcp-stdio]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Shared session context and buffered default input.
[^mcp-identity]: `src/py/vertebra/agent/core/identity.py::MtlsIdentitySource` — Immediate-peer proxy policy.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Header-only native context forwarding.
