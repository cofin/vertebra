---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch03-version-negotiation-and-sessions
title: "Chapter 3: Version Negotiation and Sessions"
description: Add the supported-version registry and negotiation rules, legacy initialize with MCPSessionStore sessions, session validation, batching for 2025-03-26 sessions, DELETE termination, and one capability builder for server/discover and initialize.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-guard-and-confirmation-correctness
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: mcp-handle
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: HTTP validation and dispatch
  - id: mcp-discover
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover
    title: server/discover capability manifest
  - id: mcp-models
    resource: src/py/vertebra/agent/mcp/models.py::MCP_PROTOCOL_VERSION
    title: Protocol constants and MCPConfig
  - id: litestar-mcp-precedent
    resource: .agents/bundles/knowledge/domains/framework/subdomains.md
    title: First-party litestar-mcp integration (dual-mode precedent for Litestar apps)
tags:
  - mcp
  - protocol
  - sessions
---

# Chapter 3: Version Negotiation and Sessions

<!-- truth: start -->
- **Registry**: `SUPPORTED_PROTOCOL_VERSIONS = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26")`, `LEGACY_PROTOCOL_VERSIONS` holds the last three, `MCP_FALLBACK_SESSION_VERSION = "2025-11-25"`, and `LEGACY_ONLY_METHODS = {"initialize", "ping", "resources/subscribe", "resources/unsubscribe", "logging/setLevel"}` [^mcp-models].
- **Mode rule**: a request is modern when its `MCP-Protocol-Version` header or `_meta["io.modelcontextprotocol/protocolVersion"]` equals `2026-07-28`, or when it carries neither plus no `Mcp-Session-Id` and its method is not `initialize`, `ping`, or `notifications/initialized`; every other request is legacy [^mcp-handle].
- **Errors**: unsupported versions return HTTP `400` / `-32022` with `data.supported`, `data.supportedVersions`, `data.requested`; header and `_meta` disagreement returns `400` / `-32020`; legacy-only methods in modern mode and unknown methods return HTTP `200` / `-32601` [^mcp-handle].
- **Sessions**: legacy `initialize` negotiates the requested legacy version (else `2025-11-25`), creates an owner-scoped `MCPSessionStore` session (`session_ttl_seconds=3600`, `max_sessions=10_000`), and returns `Mcp-Session-Id`; legacy requests with unknown sessions return `404` / `-32602`; `DELETE` terminates sessions; JSON-RPC batches are accepted only for `2025-03-26` [^mcp-handle].
- **Capabilities**: one `_server_capabilities` builder serves `server/discover` and `initialize`; disabled families are omitted and their methods return `-32601`; `tasks`, `logging`, `completions`, and registered `extensions` use spec shapes [^mcp-discover].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MCPCore.handle` accepts only `MCP-Protocol-Version: 2026-07-28`. Clients on `2025-11-25`, `2025-06-18`, or `2025-03-26` fail on the first request, receive no `data.supported` list, cannot call `initialize`, and receive echoed `Mcp-Session-Id` values that the server never issued ([#109](https://github.com/cofin/vertebra/issues/109)). `server/discover` advertises `{}` for disabled families and a non-standard `tasks` shape. This chapter adds negotiation, legacy sessions, and honest capabilities while keeping `2026-07-28` stateless. Litestar apps that mount `LitestarMCP` receive the same behaviour from `litestar-mcp>=0.20.0` (`ch08`).

### 1.2 Upstream Execution Baseline

- Depends on `ch02` (bound contexts with owner keys used for session scoping).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| HTTP rules | `src/py/vertebra/agent/mcp/service.py::MCPCore.handle`, `_jsonrpc_http_error`, `_with_meta` [^mcp-handle] | In-process; `McpHttpRequest` in, `McpHttpResponse` out. |
| Discovery | `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover`, `_register_handlers` [^mcp-discover] | In-process. |
| Constants | `src/py/vertebra/agent/mcp/models.py::MCP_PROTOCOL_VERSION`, `UNSUPPORTED_PROTOCOL_VERSION`, `HEADER_MISMATCH` [^mcp-models] | In-process. |
| Router | `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter.has_method`, `dispatch_payload` | In-process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [Framework subdomains](../../../../knowledge/domains/framework/subdomains.md) [^litestar-mcp-precedent].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R6.1**: Version registry, mode rule, `-32022` data payload, `-32020` mismatch, HTTP `200` JSON-RPC `-32601` method-not-found (HTTP `404` only for unknown or expired session ids), negotiated version header and legacy-appropriate result `_meta`.
- **R7.1**: Legacy `initialize`, `ping`, `notifications/initialized`; session creation and `Mcp-Session-Id`; capacity errors.
- **R7.2**: Session validation and sliding expiry, `2025-03-26` batching, `DELETE` termination, `OPTIONS` and `405` method lists.
- **R8.1**: Shared capability builder, omitted disabled families, client capability capture, legacy `logging/setLevel`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# models.py
LEGACY_PROTOCOL_VERSIONS: tuple[str, ...] = ("2025-11-25", "2025-06-18", "2025-03-26")
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = (MCP_PROTOCOL_VERSION, *LEGACY_PROTOCOL_VERSIONS)
MCP_FALLBACK_SESSION_VERSION: str = "2025-11-25"
LEGACY_ONLY_METHODS: frozenset[str]
SESSION_CAPACITY_EXHAUSTED: int = -32000
type McpLogLevel = Literal["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]
class NegotiatedProtocol(FrozenBaseStruct, frozen=True, kw_only=True):
    version: str
    modern: bool
class MCPConfig:  # added
    session_ttl_seconds: int = 3600
    max_sessions: int = 10_000
    logging: bool = False
    default_log_level: McpLogLevel = "info"

# sessions.py
class MCPSession: ...            # fields per root spec
class SessionLimitExceededError(VertebraError, RuntimeError): ...
class MCPSessionStore: ...       # create / get / update / delete / subscribed_session_ids / active_session_ids

# service.py
class MCPCore:
    def __init__(self, config=None, registry=None, telemetry=None, stream_broker=None, task_store=None, skill_catalog=None, confirmation_gate=None, session_store: MCPSessionStore | None = None) -> None: ...
    sessions: MCPSessionStore
    def register_extension(self, name: str, payload: Callable[[], dict[str, Any]]) -> None: ...
```

Context metadata keys written by `MCPCore`: `"mcp.protocol"` (`NegotiatedProtocol`), `"mcp.session"` (`MCPSession | None`), `"mcp.client_capabilities"` (`dict[str, Any]`), `"mcp.created_session_id"` (`str`), `"mcp.http_status"` (`int`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `MCPCore.handle` (`POST`) -> `_negotiate_protocol` -> `_resolve_session` -> `router.dispatch_payload` -> response headers (`MCP-Protocol-Version`, `Mcp-Session-Id`).
- `_handle_initialize` -> `MCPSessionStore.create` -> `_server_capabilities`.
- `MCPCore.handle` (`DELETE`) -> `MCPSessionStore.delete` -> `SseStreamBroker.disconnect`.
- `_handle_discover` -> `_server_capabilities` -> registered extension payload callables.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Unsupported header | `1900-01-01` | `400`, `-32022`, four supported versions | Renegotiation data |
| Header/meta conflict | header `2025-06-18`, meta `2026-07-28` | `400`, `-32020` | One version per request |
| `initialize` in modern mode | `2026-07-28` header | `200`, `-32601` | Stateless profile |
| `initialize` asking for `2026-07-28` bare | no header | Responds `2025-11-25` with session | No session for modern version |
| Unknown method | `tasks/result`, any version | HTTP `200`, body `{"jsonrpc":"2.0","id":<id>,"error":{"code":-32601,"message":"Method not found"}}` | Legacy clients never mistake a method error for session expiry |
| Unknown session id | Legacy POST with `Mcp-Session-Id: nope` | HTTP `404`, `-32602` `Session not found or expired` | `404` is reserved for session expiry |
| Session owner mismatch | Session created by `ana`, used by `bob` | `404`, `-32602` | Owner scope |
| Session capacity | `max_sessions` reached | `503`, `-32000` | Bounded memory |
| Batch on `2025-06-18` | JSON array body | `400`, `-32600` | Batching removed after `2025-03-26` |
| Batch with `initialize` | `2025-03-26` batch containing `initialize` | Entry error `-32600` | `initialize` never batched |
| Disabled tools | `tools_enabled=False`, `tools/list` | `200`, `-32601`; no `tools` capability | Honest capabilities |
| Invalid log level | `logging/setLevel {"level": "loud"}` | `-32602` | Validated input |

### 2.5 Acceptance Scenarios

- Given a `2025-06-18` client, when it calls `initialize` then `tools/list` with the returned session id, then both succeed and carry `MCP-Protocol-Version: 2025-06-18`.
- Given a `2026-07-28` client, when it calls `initialize`, then it receives `200` / `-32601`.
- Given a session, when the client sends `DELETE /mcp`, then later requests with that session id receive `404`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Bare requests | Modern unless handshake methods or a session header | Treating all bare requests as legacy changes `2026-07-28` behaviour for existing callers. |
| Session status transport | Context metadata keys read by `handle` | Return-value wrappers from every handler widen the router contract. |
| Method-not-found status | HTTP `200` with a JSON-RPC `-32601` body | HTTP `404` makes legacy clients treat the session as expired and re-initialize. |
| Batching | Only `2025-03-26` | Allowing batches for later versions violates their specs. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch03]` — transport and session docs land in `ch11:t01`; harness OKF in `ch11:t03`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6.1 | [t01](tasks/t01-protocol-version-registry.md) | `behavior_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R7.1 | [t02](tasks/t02-session-store-and-initialize.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R7.2 | [t03](tasks/t03-session-validation-and-termination.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R8.1 | [t04](tasks/t04-capability-negotiation.md) | `behavior_tdd` | `test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch03:t01 — Negotiate protocol versions and method availability (tasks/t01-protocol-version-registry.md)
- [ ] mcp-protocol-completeness:ch03:t02 — Add MCPSessionStore and legacy initialize (tasks/t02-session-store-and-initialize.md)
- [ ] mcp-protocol-completeness:ch03:t03 — Validate sessions, batch for 2025-03-26, and terminate sessions (tasks/t03-session-validation-and-termination.md)
- [ ] mcp-protocol-completeness:ch03:t04 — Build capabilities once and capture client capabilities (tasks/t04-capability-negotiation.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed.
- **Downstream Dependents**: `ch04` (streams keyed by session ids), `ch05` (session subscriptions), `ch06`/`ch07` (extensions via `register_extension`), `ch09`/`ch10` (stdio and bridge sessions).

[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP validation and dispatch.
[^mcp-discover]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover` — server/discover capability manifest.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::MCP_PROTOCOL_VERSION` — Protocol constants and MCPConfig.
[^litestar-mcp-precedent]: `.agents/bundles/knowledge/domains/framework/subdomains.md` — First-party litestar-mcp integration (dual-mode precedent for Litestar apps).
