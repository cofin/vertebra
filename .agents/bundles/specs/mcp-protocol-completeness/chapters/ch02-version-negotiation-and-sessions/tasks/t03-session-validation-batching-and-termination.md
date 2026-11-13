---
type: Task
id: "mcp-protocol-completeness:ch02:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch02-version-negotiation-and-sessions
title: "Validate Legacy Sessions, Support 2025-03-26 Batching, and Handle DELETE /mcp"
description: "Enforce 404 on unknown/expired legacy Mcp-Session-Id, allow JSON-RPC batch arrays only on 2025-03-26, and terminate sessions on DELETE /mcp."
status: stable
state: completed
commit: 5f8fd173
last_verified_commit: 5f8fd173
last_verified_at: "2026-11-13T04:43:05Z"
verification_evidence: "ruff + mypy + pytest (206 agent + flask tests passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch02:t02"
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/core/jsonrpc.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 185
tags:
  - mcp
  - sessions
  - batching
  - http
sources:
  - id: mcp-handle
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.handle"
    title: HTTP method and session header validation
  - id: jsonrpc-py
    resource: "src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter"
    title: JSON-RPC single and batch payload dispatch
---

# Task `t03`: Validate Legacy Sessions, Support 2025-03-26 Batching, and Handle DELETE /mcp

## Objective & Scope Boundary

Complete legacy session validation, `2025-03-26` batching, `DELETE /mcp` termination, and `OPTIONS` Allow headers ([#109](https://github.com/cofin/vertebra/issues/109)) per [`../spec.md` §2](../spec.md) [^mcp-handle] [^jsonrpc-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{service,sessions}.py` & `agent/core/{jsonrpc,sse}.py`.

## Contracts & Edge Cases

- Legacy requests carrying an unknown, expired, or wrong-owner `Mcp-Session-Id` return HTTP `404` with JSON-RPC `-32602` (`'Session not found or expired'`); `2026-07-28` never echoes caller-supplied `Mcp-Session-Id`.
- JSON-RPC array batches are dispatched only when negotiated protocol is `'2025-03-26'` (returning `-32600` per `initialize` entry inside a batch, and HTTP `400` / `-32600` on `2025-06-18`, `2025-11-25`, and `2026-07-28`).
- `DELETE /mcp` deletes the owner's session (`204` or `200`, `404` if unknown/wrong owner) and disconnects session SSE channels; `OPTIONS` returns `Allow: GET, POST, DELETE, OPTIONS`.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering expired/wrong-owner `404`, unissued `Mcp-Session-Id` stripping in `2026-07-28`, `2025-03-26` batch success vs `2025-06-18` batch `400`, `initialize`-in-batch `-32600`, and `DELETE /mcp` lifecycle.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] Unknown or terminated `Mcp-Session-Id` values deterministically return HTTP `404`.
- [x] JSON-RPC batches work only on `2025-03-26` and `DELETE /mcp` cleans up session state.

[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP method and session header validation.
[^jsonrpc-py]: `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter` — JSON-RPC single and batch payload dispatch.
