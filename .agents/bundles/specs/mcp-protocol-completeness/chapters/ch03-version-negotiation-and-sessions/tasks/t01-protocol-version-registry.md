---
type: Task
id: "mcp-protocol-completeness:ch03:t01"
chapter_id: ch03-version-negotiation-and-sessions
title: Negotiate protocol versions and method availability
description: Add the supported-version registry and NegotiatedProtocol, classify requests as modern or legacy, return -32022 with supported-version data and -32020 on header/meta conflict, return HTTP 200 with a JSON-RPC -32601 Method not found body for legacy-only methods in modern mode and for unknown methods, and echo the negotiated version.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch02:t02"
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/jsonrpc.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 210
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: mcp-handle
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: Current single-version validation
  - id: mcp-models
    resource: src/py/vertebra/agent/mcp/models.py::UNSUPPORTED_PROTOCOL_VERSION
    title: Protocol error constants
tags:
  - mcp
  - protocol
---

# Task t01: Negotiate Protocol Versions and Method Availability

## Objective & Scope Boundary

Replace the single-version check in `MCPCore.handle` with negotiation across `SUPPORTED_PROTOCOL_VERSIONS` and record the result on the bound context [^mcp-handle] [^mcp-models]. Out of scope: `initialize` and sessions (`t02`, `t03`), capability contents (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/models.py` — constants and `__all__`.
- `src/py/vertebra/agent/mcp/service.py` — `MCPCore.handle`, `_jsonrpc_http_error`, `_with_meta`, `CACHEABLE_METHODS`.
- `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter.has_method`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_core_discover_and_removed_2024_methods`, `test_mcp_http_handler_conformance_and_negative_cases`.

## Symbol & Signature Contracts

- `models.py`: `LEGACY_PROTOCOL_VERSIONS`, `SUPPORTED_PROTOCOL_VERSIONS`, `MCP_FALLBACK_SESSION_VERSION`, `LEGACY_ONLY_METHODS`, and `class NegotiatedProtocol(FrozenBaseStruct, frozen=True, kw_only=True)` with `version: str` and `modern: bool`, all in `__all__` with docstrings.
- `service.py`:
  - Private `MCPCore._negotiate_protocol(self, *, header_version: str | None, meta_version: str | None, session_header: str | None, method: str | None) -> NegotiatedProtocol | McpHttpResponse`: implements the mode rule from the chapter truth block; returns an error response for unsupported or conflicting versions; legacy requests without a version resolve to `MCP_FALLBACK_SESSION_VERSION` (sessions override it in `t03`).
  - Private static `_unsupported_version_response(requested: str, req_id: Any) -> McpHttpResponse`: HTTP `400`, `{"code": -32022, "message": f"Unsupported protocol version: {requested}", "data": {"supported": list(SUPPORTED_PROTOCOL_VERSIONS), "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS), "requested": requested}}`, header `MCP-Protocol-Version: 2026-07-28`.
  - `_jsonrpc_http_error` gains `data: Any = None` and `protocol_version: str = MCP_PROTOCOL_VERSION` keyword parameters and always sets the `MCP-Protocol-Version` header.
  - In `handle`, after negotiation: `ctx.metadata["mcp.protocol"] = negotiated`; modern requests whose method is in `LEGACY_ONLY_METHODS`, and any request (not notification) whose method is not registered, return HTTP `200` with the JSON-RPC body `{"jsonrpc": "2.0", "id": <request id>, "error": {"code": -32601, "message": "Method not found"}}`; HTTP `404` is never used for method errors because legacy clients treat `404` as session expiry and re-initialize.
  - Successful responses set `MCP-Protocol-Version: negotiated.version`.
  - `_with_meta` reads `current_agent_context().metadata["mcp.protocol"]`; legacy results receive no `_meta` key; modern results keep `io.modelcontextprotocol/protocolVersion`, `resultType`, and the cacheable `ttlMs` / `cacheScope` pair. Calls outside `handle` (no negotiated protocol on the context) keep modern `_meta`.

## Edge Cases & Error Semantics

- Header values are stripped before comparison.
- Unknown notifications still return `202` with an empty body.
- The `-32022` response body carries `"id": null` when the frame cannot be classified and the frame id otherwise.
- `ETag` / `304` handling is unchanged in both modes.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (rewrite `test_mcp_core_discover_and_removed_2024_methods` into parametrized `test_mcp_protocol_negotiation`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Unsupported header | `tools/list`, header `MCP-Protocol-Version: 1900-01-01` | `400`; `error.code == -32022`; `error.data == {"supported": ["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"], "supportedVersions": [same], "requested": "1900-01-01"}` |
| Unsupported meta | `tools/list` with `params._meta["io.modelcontextprotocol/protocolVersion"] = "2024-11-05"` | `400`; `-32022`; `data.requested == "2024-11-05"` |
| Header/meta conflict | header `2025-06-18`, meta `2026-07-28` | `400`; `error.code == -32020` |
| Legacy-only in modern | parametrize `initialize`, `ping`, `logging/setLevel`, `resources/subscribe`, `resources/unsubscribe` with header `2026-07-28` | `200`; `-32601` |
| Unknown method | `tasks/result` with header `2026-07-28`, request `id: 9` | `200`; body `== {"jsonrpc": "2.0", "id": 9, "error": {"code": -32601, "message": "Method not found"}}` |
| Unknown method on legacy session | `tasks/result` with header `2025-06-18` and a live `Mcp-Session-Id` | `200`; `-32601`; session still valid on the next `tools/list` |
| Unknown notification | `{"jsonrpc": "2.0", "method": "notifications/unknown"}` header `2026-07-28` | `202`; body `b""` |
| Legacy echo | `tools/list` header `2025-06-18` | `200`; header `MCP-Protocol-Version == "2025-06-18"`; `"_meta" not in result` |
| Modern echo | `tools/list` header `2026-07-28` | `200`; header `"2026-07-28"`; `result["_meta"]["io.modelcontextprotocol/protocolVersion"] == "2026-07-28"` |
| Bare request | `server/discover` without version header | `200`; header `"2026-07-28"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "protocol_negotiation"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/models.py src/py/vertebra/agent/mcp/service.py`.

## Acceptance Criteria

- [ ] R6.1: registry, mode rule, error payloads, HTTP `200` / `-32601` method-not-found handling, and version echo match the matrix.
- [ ] The native `App` route test keeps receiving `2026-07-28` for bare `server/discover`.

## Notes & Discoveries

- [2026-11-12 00:30] At `c8722c76` unknown methods return HTTP `200` with `-32601`; this task keeps that status and fixes the message to `Method not found`. HTTP `404` stays reserved for unknown or expired legacy `Mcp-Session-Id` values (`t03`).

[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — Current single-version validation.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::UNSUPPORTED_PROTOCOL_VERSION` — Protocol error constants.
