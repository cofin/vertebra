---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch02-version-negotiation-and-sessions
title: "Chapter 02: Protocol Version Negotiation, Sessions, and Capabilities"
description: "Implement dual-mode MCP version negotiation, owner-scoped MCPSessionStore, legacy initialize and DELETE termination, 2025-03-26 batching, and a unified capability builder."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-guards-params-and-alias-cleanup
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: d7839d5f
updated_at: "2026-11-13T04:43:42Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T04:43:42Z"
    commit: d7839d5f
tags:
  - mcp
  - protocol
  - sessions
  - jsonrpc
sources:
  - id: mcp-handle
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.handle"
    title: HTTP validation and dispatch
  - id: mcp-discover
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover"
    title: server/discover capability builder
  - id: mcp-models
    resource: "src/py/vertebra/agent/mcp/models.py::MCPConfig"
    title: MCP protocol version constants and config
---

# Chapter 02: Protocol Version Negotiation, Sessions, and Capabilities

<!-- truth: start -->
- `SUPPORTED_PROTOCOL_VERSIONS = ('2026-07-28', '2025-11-25', '2025-06-18', '2025-03-26')`; unsupported versions return HTTP `400` / `-32022` with `data.{supported,supportedVersions,requested}`, header/`_meta` mismatches return `400` / `-32020`, and unknown or modern-disallowed legacy methods return HTTP `200` / `-32601` [^mcp-handle] [^mcp-models].
- Legacy `initialize` creates an owner-scoped `MCPSession` in `MCPSessionStore` (`session_ttl_seconds=3600`, `max_sessions=10_000`, `503` / `-32000` when full) and returns `Mcp-Session-Id`; unknown/expired legacy session headers return HTTP `404` / `-32602`; `DELETE /mcp` terminates sessions; JSON-RPC batch arrays work strictly on `2025-03-26` [^mcp-handle].
- One `_server_capabilities` builder powers `server/discover` and `initialize`, omitting disabled families, capturing client capabilities into context metadata, and handling legacy `logging/setLevel` [^mcp-discover].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`MCPCore.handle` currently accepts only `2026-07-28`, omits `data.supported` on `-32022`, rejects `initialize`/`ping`/`logging/setLevel`, echoes unvalidated `Mcp-Session-Id` headers, and returns `{}` for disabled capability families in `server/discover` ([#109](https://github.com/cofin/vertebra/issues/109)) [^mcp-handle] [^mcp-discover] [^mcp-models].

## 2. Requirements, Concrete Contracts & Architecture

- **`R3a` (`t01`)**: Add `LEGACY_PROTOCOL_VERSIONS`, `SUPPORTED_PROTOCOL_VERSIONS`, `MCP_FALLBACK_SESSION_VERSION = "2025-11-25"`, `LEGACY_ONLY_METHODS`, `SESSION_CAPACITY_EXHAUSTED = -32000`, `McpLogLevel`, `NegotiatedProtocol(version, modern)`, and `MCPConfig.{session_ttl_seconds,max_sessions,logging,default_log_level}` in `models.py`; implement `_negotiate_protocol`, `register_extension`, shared `_server_capabilities`, disabled-family `-32601` gating, and `logging/setLevel` in `service.py`.
- **`R3b` (`t02`)**: Create `src/py/vertebra/agent/mcp/sessions.py` (`MCPSession`, `SessionLimitExceededError`, `MCPSessionStore` with `create`, `get`, `update`, `delete`, `subscribed_session_ids`, `active_session_ids`) and wire legacy `initialize`, `ping`, and `notifications/initialized` in `MCPCore`.
- **`R3c` (`t03`)**: Enforce owner-scoped legacy `Mcp-Session-Id` validation (`404` / `-32602` on unknown, expired, or wrong-owner sessions; never echoing unissued session IDs in `2026-07-28`), `2025-03-26`-only JSON-RPC batch array dispatch (rejecting `initialize` inside batches with `-32600`), `DELETE /mcp` session termination (closing `SseStreamBroker` session channels), and `OPTIONS` (`GET, POST, DELETE, OPTIONS`).

## 3. Documentation & OKF Knowledge Plan

Dual-mode negotiation and session lifecycle semantics are documented in `ch07` (`docs/usage/agent-protocols.md`, `mcp-and-a2a-protocol-harness.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R3a` | `t01` | `agent/mcp/models.py`, `agent/mcp/service.py` | `pytest test_mcp_conformance.py` |
| `R3b` | `t02` | `agent/mcp/sessions.py`, `agent/mcp/models.py`, `agent/mcp/service.py` | `pytest test_mcp_conformance.py` |
| `R3c` | `t03` | `agent/mcp/service.py`, `agent/mcp/sessions.py` | `pytest test_mcp_conformance.py` |

## 5. Implementation Plan

- [x] [5f8fd173] [t01-protocol-version-registry-and-capabilities.md](tasks/t01-protocol-version-registry-and-capabilities.md) — Add protocol version negotiation, shared `_server_capabilities`, and `logging/setLevel`
- [x] [5f8fd173] [t02-session-store-and-initialize.md](tasks/t02-session-store-and-initialize.md) — Create `MCPSessionStore` and implement legacy `initialize`, `ping`, and `notifications/initialized`
- [x] [5f8fd173] [t03-session-validation-batching-and-termination.md](tasks/t03-session-validation-batching-and-termination.md) — Validate legacy sessions (`404`), dispatch `2025-03-26` batches, and handle `DELETE /mcp`

## 6. Current State & Remaining Plan Snapshot

- **Completed (`5f8fd173`)**: All `3` tasks in `ch02-version-negotiation-and-sessions` (`206` unit tests, `ruff`, and `mypy` clean) are complete. Next chapter: `ch03-sse-streams-and-resource-subscriptions`.
- **Fast Chapter Gate (`< 12s`)**: `uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q`.

[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP validation and dispatch.
[^mcp-discover]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover` — server/discover capability builder.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::MCPConfig` — MCP protocol version constants and config.
