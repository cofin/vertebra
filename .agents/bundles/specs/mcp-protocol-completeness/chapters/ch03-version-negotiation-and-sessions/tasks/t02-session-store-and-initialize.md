---
type: Task
id: "mcp-protocol-completeness:ch03:t02"
chapter_id: ch03-version-negotiation-and-sessions
title: Add MCPSessionStore and legacy initialize
description: Add the in-memory owner-scoped MCPSessionStore with TTL and capacity limits, legacy initialize, ping, and notifications/initialized handlers, and Mcp-Session-Id issuance with HTTP 503 on capacity exhaustion.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch03:t01"
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/tasks.py
  - src/py/vertebra/agent/mcp/__init__.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 240
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
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._register_handlers
    title: JSON-RPC handler registration
  - id: task-store
    resource: src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore
    title: In-memory store style precedent (msgspec records, owner scoping)
tags:
  - mcp
  - sessions
---

# Task t02: Add MCPSessionStore and Legacy Initialize

## Objective & Scope Boundary

Legacy clients complete the `initialize` handshake and receive an owner-scoped `Mcp-Session-Id` [^mcp-core]. The store follows the `MCPTaskStore` style (msgspec records, owner checks) [^task-store]. Out of scope: validating session headers on later requests and `DELETE` (`t03`); the full capability builder (`t04`) — `initialize` calls a private `_server_capabilities()` extracted verbatim from `_handle_discover` in this task.

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/models.py` — `MCPConfig`, constants from `t01`.
- `src/py/vertebra/agent/mcp/service.py` — `MCPCore.__init__`, `handle`, `_register_handlers`, `_handle_discover`, `_caller_owner`.
- `src/py/vertebra/agent/mcp/tasks.py` — Store style precedent.
- `src/py/vertebra/agent/mcp/__init__.py` — Lazy export list (read-only; exports added in `ch11` are not needed because `vertebra.agent.mcp.sessions` is imported directly).
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `src/py/vertebra/agent/mcp/sessions.py` (new module; docstring, `__all__ = ("MCPSession", "MCPSessionStore", "SessionLimitExceededError")`):
  - `class MCPSession(msgspec.Struct, frozen=True, gc=False, kw_only=True)` with fields `session_id: str`, `owner_id: str | None`, `protocol_version: str`, `client_capabilities: dict[str, Any]`, `client_info: dict[str, Any]`, `log_level: McpLogLevel`, `subscriptions: frozenset[str] = frozenset()`, `created_at_unix_ms: int`, `expires_at_unix_ms: int` (each with a field docstring). `gc=False` is safe only because the dict fields are never cyclic; use `gc=True` default if the executor finds a cycle risk.
  - `class SessionLimitExceededError(VertebraError, RuntimeError)`.
  - `class MCPSessionStore`: `__init__(self, *, ttl_seconds: int = 3600, max_sessions: int = 10_000, clock: Callable[[], float] = time.monotonic)`; `ttl_seconds < 1` or `max_sessions < 1` raises `ValueError`; state `self._sessions: dict[str, MCPSession]` guarded by `anyio.Lock`.
  - `async create(self, *, owner_id, protocol_version, client_capabilities, client_info, log_level) -> MCPSession`: purges expired sessions, raises `SessionLimitExceededError` at capacity, ids from `secrets.token_urlsafe(24)`.
  - `async get(self, session_id: str, owner_id: str | None) -> MCPSession | None`: `None` for unknown, owner mismatch (`session.owner_id != owner_id`), or expired (expired entries are removed); otherwise slides `expires_at_unix_ms` forward and returns the refreshed record.
  - `async update(self, session_id, owner_id, *, log_level=None, subscriptions=None) -> MCPSession | None`, `async delete(self, session_id, owner_id) -> MCPSession | None`, `async subscribed_session_ids(self, uri: str) -> tuple[str, ...]`, `async active_session_ids(self) -> tuple[str, ...]` (all skip expired sessions).
- `models.py::MCPConfig` additions: `session_ttl_seconds: int = 3600`, `max_sessions: int = 10_000`, `default_log_level: McpLogLevel = "info"`; module constant `SESSION_CAPACITY_EXHAUSTED: int = -32000`; `type McpLogLevel`.
- `service.py`:
  - `MCPCore.__init__(..., session_store: MCPSessionStore | None = None)`; `self.sessions = session_store or MCPSessionStore(ttl_seconds=config.session_ttl_seconds, max_sessions=config.max_sessions)`.
  - Handlers registered as `initialize`, `ping`, `notifications/initialized`.
  - `_handle_initialize(self, params: Any = None, context: Any = None) -> dict[str, Any]`: requires `params["protocolVersion"]` (`str`) else `-32602` `"Missing protocolVersion"`; negotiated version is the requested value when in `LEGACY_PROTOCOL_VERSIONS`, else `MCP_FALLBACK_SESSION_VERSION`; creates the session with the caller owner; writes `context.metadata["mcp.created_session_id"]`; returns `{"protocolVersion": version, "capabilities": self._server_capabilities(), "serverInfo": {"name": config.server_name, "version": config.server_version}, "instructions": config.instructions}`; on `SessionLimitExceededError` sets `context.metadata["mcp.http_status"] = 503` and raises `-32000` `"Session capacity exhausted"`.
  - `_handle_ping` returns `{}`.
  - `handle` copies `mcp.created_session_id` into the `Mcp-Session-Id` response header, sets `MCP-Protocol-Version` to the negotiated `initialize` result version, and applies `mcp.http_status` when present.

## Edge Cases & Error Semantics

- `initialize` sent as a notification is ignored (`202`).
- `initialize` requesting `2026-07-28` without modern headers negotiates `2025-11-25`.
- Expiry uses the injected clock; tests advance a list-backed clock.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new `test_mcp_legacy_initialize_creates_session` parametrized over requested versions, and `test_mcp_session_store_lifecycle`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Echo legacy version | `initialize {"protocolVersion": "2025-06-18", "capabilities": {"roots": {}}, "clientInfo": {"name": "c", "version": "1"}}`, no version header | `200`; `result.protocolVersion == "2025-06-18"`; header `Mcp-Session-Id` non-empty; header `MCP-Protocol-Version == "2025-06-18"`; `result.serverInfo.name == "vertebra-mcp"` |
| Fallback | `initialize {"protocolVersion": "2026-07-28"}` bare | `result.protocolVersion == "2025-11-25"` |
| Missing version | `initialize {}` | `-32602`, message `"Missing protocolVersion"` |
| Ping | `ping {}` bare | `result == {}` |
| Capacity | `MCPCore(session_store=MCPSessionStore(max_sessions=1))`; two `initialize` calls | Second returns HTTP `503`, `error.code == -32000` |
| Store owner scope | `store.create(owner_id="default:ana", ...)`; `store.get(sid, "default:bob")` | `None` |
| Store expiry | `clock = [0.0]`; `MCPSessionStore(ttl_seconds=10, clock=lambda: clock[0])`; create; `clock[0] = 11.0`; `get(sid, owner)` | `None`; `active_session_ids() == ()` |
| Sliding expiry | create at `0`; get at `9`; get at `18` | Both return the session |
| Invalid config | `MCPSessionStore(ttl_seconds=0)` | `ValueError` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "legacy_initialize or session_store"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp`.

## Acceptance Criteria

- [ ] R7.1: `initialize`, `ping`, and `notifications/initialized` work in legacy mode with session issuance.
- [ ] `MCPSessionStore` enforces owner scope, TTL, sliding expiry, and capacity.

## Notes & Discoveries

- [2026-11-12 00:30] Sessions are per-process; multi-worker deployments need sticky routing for legacy clients. Recorded for the limitations chapter in `ch11:t03`.

[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore._register_handlers` — JSON-RPC handler registration.
[^task-store]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — In-memory store style precedent (msgspec records, owner scoping).
