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

## Planned Fixes

The `mcp-protocol-completeness` flow owns these gaps: agent context propagation, guard and confirmation correctness, protocol version negotiation with legacy sessions and `GET` SSE streams, resource subscriptions, the skills extension, MCP Apps UI metadata, Litestar delegation, `vtb ai mcp serve` and `bridge`, and the matching docs. Server-initiated SSE depends on incremental mounted streaming from the `server-runtime-correctness` flow ([#81](https://github.com/cofin/vertebra/issues/81)). Tracking issues: [#109](https://github.com/cofin/vertebra/issues/109), [#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111), [#112](https://github.com/cofin/vertebra/issues/112), [#114](https://github.com/cofin/vertebra/issues/114).

Current protocol behavior is described in [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](mcp-and-a2a-protocol-harness.md).

[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — Transport-agnostic MCP request handling and the `MCPService` alias subclass.
[^mcp-registry]: `src/py/vertebra/agent/mcp/registry.py::MCPRegistry` — `ToolRegistry` and the `MCPRegistry` alias subclass.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py` — MCP protocol constants and error codes.
[^cli-ai]: `src/py/vertebra/cli/ai.py` — `vtb ai mcp serve` and `bridge` commands.
