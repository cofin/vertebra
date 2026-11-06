---
type: Task
id: mcp-webmcp-core:t03
title: Implement transport-agnostic MCPCore 2026-07-28 HTTP protocol engine
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t02
files:
- src/py/vertebra/agent/mcp/models.py
- src/py/vertebra/agent/mcp/service.py
- src/py/vertebra/agent/mcp/__init__.py
- src/py/vertebra/agent/core/guards.py
tests:
- src/py/tests/unit/agent/test_mcp_conformance.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 2
claimed_by: agent:flow-implement
claimed_at: '2026-11-06T21:48:00Z'
blocked_reason: null
unblock_condition: null
next_step: Completed
last_verified_at: '2026-11-06T21:58:00Z'
last_verified_commit: "376d3faf71da99aceac4ecde17c690db9a0204b3"
verification_evidence: "uv run --no-sync pytest src/py/tests/unit/agent -q (83 passed) && ruff check && mypy (37 source files)"
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T21:58:00Z'
commit: "376d3faf71da99aceac4ecde17c690db9a0204b3"
tags:
- mcp
- protocol
- security
requirements:
- R4
---

# Implement transport-agnostic MCPCore 2026-07-28 HTTP protocol engine

## Objective

Implement `McpHttpRequest`, `McpHttpResponse`, and `MCPCore.handle(request) -> McpHttpResponse` in `src/py/vertebra/agent/mcp/` enforcing strict MCP `2026-07-28` HTTP transport, `_meta` negotiation, `Origin` validation, `ETag`/`If-None-Match`, guard authorization, argument-bound `ConfirmationGate` challenges, and removal of deprecated `2024-11-05` methods.

## Context

Read `../spec.md`, `t02-tool-registry-and-providers.md`, `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`, and `.agents/skills/litestar-mcp/SKILL.md`. Preserve `_meta["vertebra/confirmationToken"]` argument-digest binding on `ConfirmationGate`.

## Steps

1. Update and expand `test_mcp_conformance.py` with failing tests for `MCPCore.handle`: `_meta` protocol version negotiation (`2026-07-28`), malformed header `400`, cross-origin `Origin` rejection `403`, `GET /mcp` `405`, JSON-RPC batch array rejection `400`, notification `202` empty body, `server/discover`, `resultType` + `ttlMs`/`cacheScope` metadata, deterministic `ETag` / `304`, `authorize(ctx, spec)` guard execution, `ConfirmationGate` `input_required` / token replay checks, and `-32601` on removed `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe`, `resources/unsubscribe`, `tasks/result`).
2. Define `McpHttpRequest` and `McpHttpResponse` (`msgspec.Struct`, `frozen=True`, `gc=False`) in `src/py/vertebra/agent/mcp/models.py` and implement `MCPCore` in `src/py/vertebra/agent/mcp/service.py`.
3. Wire `authorize(ctx, spec)` in `src/py/vertebra/agent/core/guards.py` and export `MCPCore`, `McpHttpRequest`, and `McpHttpResponse` from `vertebra.agent.mcp`.
4. Run verification and commit `feat(mcp): implement transport-agnostic MCPCore 2026-07-28 protocol engine`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_mcp_enterprise_tools.py -q
uv run --no-sync ruff check src/py/vertebra/agent
uv run --no-sync mypy src/py/vertebra/agent
```

## Acceptance Criteria

- [x] R4: `MCPCore.handle` enforces all MCP `2026-07-28` HTTP status codes, `_meta` negotiation, `Origin` validation, `ETag`, guard checks, `ConfirmationGate` binding, and rejects removed `2024-11-05` methods with `-32601`.

## Notes & Discoveries

- `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` centralizes MCP `2026-07-28` HTTP validation (default same-origin `Origin` check `403`, `GET` `405`, `OPTIONS` `204`, batch array `400`, notification `202` empty body, `_meta` protocol version negotiation, `ETag`/`If-None-Match` `304`, `authorize(ctx, spec)`, and removal of `2024-11-05` methods).
