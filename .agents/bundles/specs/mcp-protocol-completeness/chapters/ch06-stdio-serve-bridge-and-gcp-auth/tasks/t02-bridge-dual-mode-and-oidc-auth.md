---
type: Task
id: "mcp-protocol-completeness:ch06:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch06-stdio-serve-bridge-and-gcp-auth
title: Implement Dual-Mode Bridge SSE Relay and Refreshed OIDC Token Auth
description: "Upgrade run_stdio_http_bridge with BridgeTransport, legacy handshake header preservation, GET SSE relay, EOF DELETE, and OidcTokenSource (--audience)."
status: stable
state: completed
commit: 7607f4d8
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch06:t01"
files:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/agent/mcp/client.py
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
read_set:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/agent/mcp/client.py
  - src/py/vertebra/cli/ai.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 245
tags:
  - mcp
  - bridge
  - oidc
  - cli
sources:
  - id: bridge-py
    resource: "src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge"
    title: Synchronous POST-only MCP bridge
  - id: headers-py
    resource: "src/py/vertebra/agent/mcp/client.py::mcp_request_headers"
    title: MCP request header builder
---

# Task `t02`: Implement Dual-Mode Bridge SSE Relay and Refreshed OIDC Token Auth

## Objective & Scope Boundary

Complete [#114](https://github.com/cofin/vertebra/issues/114) across `run_stdio_http_bridge` and `vtb ai mcp bridge --audience` per [`../spec.md` §2](../spec.md) [^bridge-py] [^headers-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{bridge,client}.py` & `src/py/vertebra/cli/ai.py`.
- `src/py/vertebra/storage/_store.py` — `AsyncGcpIdTokenCache` re-export from `gcp-embeddings-and-identity`.

## Contracts & Edge Cases

- `mcp_request_headers(..., protocol_version: str | None = MCP_PROTOCOL_VERSION)` omits `MCP-Protocol-Version` when `protocol_version is None` (used on legacy `initialize` and pre-negotiation `ping`/`notifications/initialized`).
- After legacy `initialize` yields `Mcp-Session-Id`, `run_stdio_http_bridge` opens `GET` `Accept: text/event-stream`, writes `data:` payloads to stdout, reconnects with `Last-Event-ID` up to `5` times (`0.5 * 2**n` backoff, stopping cleanly on `405` or `404`), and sends `DELETE` at EOF.
- `OidcTokenSource(audience, *, cache=None)` mints tokens via `AsyncGcpIdTokenCache.get_token(audience)`, invalidates the failed generation + retries once on HTTP `401` (emitting `-32001` `'Bridge authentication failed (HTTP 401)'` on a second `401`), and `vtb ai mcp bridge` rejects `--token` + `--audience` together (`UsageError`, exit `2`).

## Test Matrix

- Parameterized in-memory `BridgeTransport` tests in `test_mcp_cli.py` covering headerless legacy `initialize`, `GET` SSE relay + `Last-Event-ID` reconnect, `405` relay stop, EOF `DELETE`, `401` single refresh retry vs double-`401` `-32001`, and `--token` + `--audience` mutual exclusion.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp/bridge.py src/py/vertebra/agent/mcp/client.py src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py && uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q
```

## Acceptance Criteria

- [x] `vtb ai mcp bridge` forwards both `2026-07-28` and legacy session/SSE protocols cleanly.
- [x] `--audience` authenticates via `AsyncGcpIdTokenCache` with bounded single-retry `401` refresh.

[^bridge-py]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — Synchronous POST-only MCP bridge.
[^headers-py]: `src/py/vertebra/agent/mcp/client.py::mcp_request_headers` — MCP request header builder.
