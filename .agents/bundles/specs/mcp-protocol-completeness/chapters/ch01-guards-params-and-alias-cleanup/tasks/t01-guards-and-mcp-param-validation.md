---
type: Task
id: "mcp-protocol-completeness:ch01:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch01-guards-params-and-alias-cleanup
title: "Run Tool Guards Once with Typed Errors and Validate Mcp-Param-* Headers"
description: "Split Tool.authorize and Tool.call, map guard errors to -32004/-32005, fail closed on 2-arg connection guards, and validate Mcp-Param-* headers in MCPCore.handle."
status: stable
state: completed
commit: 54f39ff4
last_verified_commit: 54f39ff4
last_verified_at: "2026-11-13T01:59:00Z"
verification_evidence: "ruff + mypy + pytest (177 passed) + review-check (exit 0)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/core/guards.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_core_and_observability.py
read_set:
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/core/guards.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/client.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 195
tags:
  - mcp
  - guards
  - validation
  - security
sources:
  - id: tool-schema
    resource: "src/py/vertebra/agent/core/schema.py::Tool.invoke"
    title: Inline guard execution inside Tool.invoke
  - id: mcp-handle
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.handle"
    title: HTTP header validation and tools/call dispatch
---

# Task `t01`: Run Tool Guards Once with Typed Errors and Validate Mcp-Param-* Headers

## Objective & Scope Boundary

Eliminate duplicate guard execution, map guard errors to JSON-RPC `-32004`/`-32005`, and validate `Mcp-Param-*` headers in `MCPCore.handle` per [`../spec.md` §2](../spec.md) [^tool-schema] [^mcp-handle].

## Bounded Read Manifest

- `src/py/vertebra/agent/core/{schema,guards}.py` — `Tool.invoke` and `GuardChain.execute`.
- `src/py/vertebra/agent/mcp/{service,client}.py` — `MCPCore.handle`, `_handle_tools_call`, and `decode_mcp_header_value`.

## Contracts & Edge Cases

- `Tool.authorize(context)` runs guards via `guards.authorize`; `Tool.call(arguments, context)` binds arguments and executes `fn` without re-running guards; `Tool.invoke` awaits `authorize` then `call`.
- `MCPCore._handle_tools_call` calls `await target_tool.authorize(context)` once before confirmation and task promotion, mapping `PermissionDeniedError` -> `PERMISSION_DENIED = -32004` and `RateLimitExceededError` / `BudgetExceededError` -> `RATE_LIMITED = -32005`; 2-arg connection guards without a connection raise `PermissionDeniedError`.
- For every `mcp-param-<slug>` header on `POST`, decode the header value via `decode_mcp_header_value`, match `<slug>` (exact or hyphen/underscore-normalized case-insensitive key) against `params['arguments']` (for `tools/call` / `prompts/get`) or `params`, and return HTTP `400` / `HEADER_MISMATCH = -32020` when missing or unequal.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` and `test_core_and_observability.py` covering `TokenBucketGuard(capacity=1)` single consumption, `-32004` / `-32005` error payloads (sync and promoted task calls), 2-arg connection guard fail-closed denial, and ASCII + RFC 2047 `Mcp-Param-*` match vs mismatch (`400` / `-32020`).

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/core/schema.py src/py/vertebra/agent/core/guards.py src/py/vertebra/agent/mcp/service.py && uv run --no-sync mypy src/py/vertebra/agent/core src/py/vertebra/agent/mcp/service.py && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py -q
```

## Acceptance Criteria

- [x] Guards execute exactly once per `tools/call` and return `-32004` / `-32005` on denial or rate limit.
- [x] Mismatched or missing `Mcp-Param-*` body values return HTTP `400` with JSON-RPC `-32020`.

[^tool-schema]: `src/py/vertebra/agent/core/schema.py::Tool.invoke` — Inline guard execution inside Tool.invoke.
[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP header validation and tools/call dispatch.
