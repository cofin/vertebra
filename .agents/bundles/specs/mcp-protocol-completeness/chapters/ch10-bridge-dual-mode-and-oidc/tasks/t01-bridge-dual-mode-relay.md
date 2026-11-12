---
type: Task
id: "mcp-protocol-completeness:ch10:t01"
chapter_id: ch10-bridge-dual-mode-and-oidc
title: Forward both protocol modes and relay legacy notifications
description: Rebuild run_stdio_http_bridge on an async BridgeTransport port with concurrent frame forwarding, version-aware headers, legacy session tracking, a GET SSE relay with Last-Event-ID reconnection, session termination at EOF, and a StaticTokenSource.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch09:t02"
read_set:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/agent/mcp/client.py
  - src/py/vertebra/cli/ai.py
  - src/py/tests/unit/cli/test_mcp_cli.py
files:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/agent/mcp/client.py
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 250
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
  - id: bridge
    resource: src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge
    title: Current bridge loop
  - id: headers
    resource: src/py/vertebra/agent/mcp/client.py::mcp_request_headers
    title: Header derivation
tags:
  - bridge
  - sse
---

# Task t01: Forward Both Protocol Modes and Relay Legacy Notifications

## Objective & Scope Boundary

The bridge carries any MCP client to any Vertebra or `litestar-mcp` server in either protocol mode, including server-pushed notifications [^bridge] [^headers]. `run_mcp_stdio_bridge` in `cli/ai.py` keeps its `bearer_token` argument and passes `token_provider=StaticTokenSource(bearer_token or os.environ.get("VERTEBRA_MCP_TOKEN"))`. Out of scope: OIDC and the `--audience` option (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/bridge.py` — `run_stdio_http_bridge`, `_parse_sse_events`, `_resolve_awaitable`.
- `src/py/vertebra/agent/mcp/client.py` — `mcp_request_headers`, `_default_http_request`, `MCP_NAME_FIELDS`.
- `src/py/vertebra/cli/ai.py::run_mcp_stdio_bridge` — call site switched to `token_provider=StaticTokenSource(...)` and `transport: BridgeTransport | None`.
- `src/py/tests/unit/cli/test_mcp_cli.py` — existing bridge tests to migrate to the new transport port.

## Symbol & Signature Contracts

- `client.py::mcp_request_headers(..., protocol_version: str | None = MCP_PROTOCOL_VERSION, ...)`: `None` omits `MCP-Protocol-Version`; everything else unchanged.
- `bridge.py`:
  - `BRIDGE_ERROR`, `BridgeResponse`, `BridgeTransport`, `UrllibBridgeTransport`, `BridgeTokenSource`, `StaticTokenSource` per the chapter contracts; `UrllibBridgeTransport` runs `urllib.request` in `anyio.to_thread.run_sync`, returns `lines` (an async iterator fed from a worker thread through an `anyio` memory stream) when the response `Content-Type` contains `text/event-stream`, and maps `HTTPError` to its status, headers, and body.
  - `run_stdio_http_bridge(*, url, token_provider=None, timeout_s=30.0, max_message_bytes=4_194_304, stdin=None, stdout=None, transport=None) -> int` is a sync wrapper around private `async _run_bridge(...)` via `anyio.run`; `token_provider` and `bearer_token` parameters are removed (`StaticTokenSource(os.environ.get("VERTEBRA_MCP_TOKEN"))` is the default source).
  - Private `class _BridgeSession` holds `negotiated_version: str | None`, `session_id: str | None`, `last_event_id: str | None`, an `anyio.Lock` for stdout, and the relay cancel scope.
  - Frame handling (one task per stdin line): `initialize` -> headers with `protocol_version=None`; `ping` / `notifications/initialized` before negotiation -> `None`; others -> `negotiated_version or MCP_PROTOCOL_VERSION`; session header attached when known. On a `200` `initialize` result with `result.protocolVersion` in `LEGACY_PROTOCOL_VERSIONS` and an `Mcp-Session-Id` header, records both and starts the relay.
  - Responses: JSON bodies written as one line; SSE responses (`lines`) written event by event (`data` payload per line); `404` with a session clears the session and stops the relay after writing the body.
  - Relay: `GET` with `Accept: text/event-stream`, negotiated version, session id, and `Last-Event-ID` when known; parses `id:` and `data:` fields; on stream end reconnects up to `5` times with `0.5 * 2**attempt` seconds of `anyio.sleep`; `405` stops silently; other non-`200` statuses stop after writing a message to `sys.stderr`.
  - EOF: waits for in-flight frame tasks, cancels the relay, sends `DELETE` with the session id when present, returns `0`.
  - Oversized input or output lines write the existing `-32600` error frames.

## Edge Cases & Error Semantics

- Empty stdin lines are skipped.
- Transport exceptions for a frame write `{"jsonrpc": "2.0", "id": <frame id>, "error": {"code": -32001, "message": "Bridge transport error: <exc>"}}`.
- Notifications (no `id`) that receive `202` write nothing.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/cli/test_mcp_cli.py` (migrate `test_run_mcp_stdio_bridge_*` to an in-memory `FakeTransport` recording `(method, url, headers, body)` and returning scripted `BridgeResponse` values; new parametrized `test_bridge_dual_mode`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Modern frame | stdin `tools/list` only | One `POST`; headers `MCP-Protocol-Version == "2026-07-28"`, `Mcp-Method == "tools/list"` |
| Legacy handshake | stdin `initialize {"protocolVersion": "2025-06-18"}` | `POST` headers lack `MCP-Protocol-Version` |
| Legacy follow-up | `initialize` returns `{"result": {"protocolVersion": "2025-06-18"}}` with `Mcp-Session-Id: s1`; then `tools/list` | Second `POST` headers `MCP-Protocol-Version == "2025-06-18"`, `Mcp-Session-Id == "s1"` |
| Relay | Same plus `GET` returning `lines` `[b"id: 1", b"data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}", b""]` | stdout contains the notification line; recorded `GET` has `Accept == "text/event-stream"` |
| Reconnect | First `GET` stream ends after `id: 1`; second `GET` scripted | Second `GET` headers `Last-Event-ID == "1"` |
| GET unsupported | `GET` returns `405` | No relay output; no further `GET` |
| EOF termination | Legacy session then EOF | Last recorded call is `("DELETE", url, headers with Mcp-Session-Id "s1", b"")` |
| SSE POST response | `POST` returns `lines` with two `data:` events | Two stdout lines in order |
| Session expired | `POST` with session returns `404` body `{"jsonrpc":"2.0","id":3,"error":{"code":-32602,"message":"Session not found or expired"}}` | Body written; next frame has no `Mcp-Session-Id` |
| Transport error | Transport raises `OSError("down")` for id `9` | stdout `{"jsonrpc":"2.0","id":9,"error":{"code":-32001,"message":"Bridge transport error: down"}}` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q -k "bridge"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/bridge.py src/py/vertebra/agent/mcp/client.py src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/bridge.py src/py/vertebra/agent/mcp/client.py src/py/vertebra/cli/ai.py`.

## Acceptance Criteria

- [ ] R16.1: dual-mode headers, legacy relay with reconnection, session termination, concurrency, and error frames match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] `litestar-mcp`'s bridge skips default `2026-07-28` headers on legacy handshakes for the same reason (`_is_legacy_handshake_or_ping`).

[^bridge]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — Current bridge loop.
[^headers]: `src/py/vertebra/agent/mcp/client.py::mcp_request_headers` — Header derivation.
