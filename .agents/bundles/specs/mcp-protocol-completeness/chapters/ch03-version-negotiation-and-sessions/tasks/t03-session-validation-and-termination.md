---
type: Task
id: "mcp-protocol-completeness:ch03:t03"
chapter_id: ch03-version-negotiation-and-sessions
title: Validate sessions, batch for 2025-03-26, and terminate sessions
description: Resolve and validate Mcp-Session-Id on legacy requests, stop echoing session ids in modern mode, accept JSON-RPC batches only for 2025-03-26, implement DELETE termination, and advertise GET, POST, DELETE, and OPTIONS consistently.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch03:t02"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 190
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
    title: HTTP method dispatch, batch rejection, and session echo
  - id: sse-disconnect
    resource: src/py/vertebra/agent/core/sse.py::SseStreamBroker.disconnect
    title: Session stream teardown
tags:
  - mcp
  - sessions
---

# Task t03: Validate Sessions, Batch for 2025-03-26, and Terminate Sessions

## Objective & Scope Boundary

Legacy requests carrying `Mcp-Session-Id` are bound to a live, owner-matching session; modern requests ignore the header; `2025-03-26` sessions may batch; `DELETE` ends sessions [^mcp-handle] [^sse-disconnect]. Out of scope: `GET` streams (`ch04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `handle`, batch rejection branch, session echo, `OPTIONS` branch, `_negotiate_protocol` from `t01`.
- `src/py/vertebra/agent/mcp/sessions.py` — `MCPSessionStore.get`, `delete`.
- `src/py/vertebra/agent/core/sse.py::SseStreamBroker.disconnect`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- Private `MCPCore._resolve_session(self, ctx: AgentContext, session_header: str | None, negotiated: NegotiatedProtocol, header_version: str | None) -> MCPSession | McpHttpResponse | None`:
  - Modern or no header -> `None` (no lookup, no echo).
  - Legacy with header -> `await self.sessions.get(sid, owner)`; missing -> HTTP `404`, `-32602` `"Session not found or expired"`; when `header_version` is set and differs from `session.protocol_version` -> `400`, `-32020`.
  - On success sets `ctx.session_id = sid`, `ctx.metadata["mcp.session"] = session`, and the negotiated version to `session.protocol_version`.
- Batch handling in `handle`: a JSON array body is dispatched through `self.router.dispatch_payload` only when the resolved version is `"2025-03-26"`; entries whose method is `initialize` produce entry errors `-32600`; any other version keeps `400` / `-32600`; `Mcp-Method` / `Mcp-Name` headers with a batch keep `400` / `-32020`.
- `DELETE` branch: modern header -> `204` with `MCP-Protocol-Version: 2026-07-28`; legacy without session header -> `204`; legacy with session -> `await self.sessions.delete(sid, owner)`; `None` -> `404` / `-32602`; success -> `self.stream_broker.disconnect(sid)` and `204`.
- Legacy responses with a validated session echo `Mcp-Session-Id`; modern responses never set it.
- `OPTIONS` -> `204` with `Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS`, `Access-Control-Allow-Headers: Content-Type, Accept, Authorization, MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Session-Id, Last-Event-ID, If-None-Match`, `Access-Control-Expose-Headers: Mcp-Session-Id, MCP-Protocol-Version, ETag`.
- Other verbs (including `GET` until `ch04:t01`) -> `405` with `Allow: POST, DELETE, OPTIONS`.

## Edge Cases & Error Semantics

- Session lookups slide the TTL; `DELETE` of an expired session returns `404`.
- A `2026-07-28` request carrying a stale `Mcp-Session-Id` succeeds (header ignored).
- A `2025-03-26` batch of notifications only returns `202`.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_session_validation_and_termination`).
- Setup: `initialize` as `ana` returning `sid`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Valid session | `tools/list`, headers `MCP-Protocol-Version: 2025-06-18`, `Mcp-Session-Id: sid`, context `ana` | `200`; response header `Mcp-Session-Id == sid` |
| Unknown session | same with `Mcp-Session-Id: nope` | `404`; `error.code == -32602`; message `"Session not found or expired"` |
| Wrong owner | `sid` with context `bob` | `404`; `-32602` |
| Version conflict | `sid` negotiated `2025-06-18`, header `2025-11-25` | `400`; `-32020` |
| Modern ignores session | header `2026-07-28`, `Mcp-Session-Id: nope` | `200`; no `Mcp-Session-Id` response header |
| Batch on 2025-03-26 | Session from `initialize {"protocolVersion": "2025-03-26"}`; body `[ping id 1, tools/list id 2]` with `Mcp-Session-Id` and no version header | `200`; JSON array of two results with ids `1` and `2` |
| Batch with initialize | same session; `[initialize id 3]` | Array entry `error.code == -32600` |
| Batch on 2025-06-18 | session `2025-06-18`; `[ping id 1]` | `400`; `-32600` |
| DELETE session | `DELETE` with `Mcp-Session-Id: sid`, header `2025-06-18` | `204`; following `tools/list` with `sid` -> `404` |
| DELETE unknown | `DELETE` with `Mcp-Session-Id: nope`, header `2025-06-18` | `404`; `-32602` |
| DELETE modern | `DELETE` with header `2026-07-28` | `204` |
| OPTIONS | `OPTIONS /mcp` | `204`; `Access-Control-Allow-Methods == "GET, POST, DELETE, OPTIONS"` |
| PUT | `PUT /mcp` | `405`; `Allow == "POST, DELETE, OPTIONS"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "session_validation"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/service.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py`.

## Acceptance Criteria

- [ ] R7.2: session validation, `2025-03-26` batching, `DELETE`, `OPTIONS`, and `405` lists match the matrix.
- [ ] Modern responses never echo `Mcp-Session-Id`.

## Notes & Discoveries

- [2026-11-12 00:30] At `c8722c76` `MCPCore.handle` echoes any client-supplied `Mcp-Session-Id` without issuing or validating sessions.

[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP method dispatch, batch rejection, and session echo.
[^sse-disconnect]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker.disconnect` — Session stream teardown.
