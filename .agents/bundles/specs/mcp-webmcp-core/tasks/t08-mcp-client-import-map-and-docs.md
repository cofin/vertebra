---
type: Task
id: mcp-webmcp-core:t08
title: Update TypeScript McpClient SSE/_meta handling, derive _CLIENT_SUBPATHS from package.json, and sync docs/OKF
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t07
files:
- src/ts/vertebra-client/src/agent/index.ts
- src/py/vertebra/build/_import_map.py
- docs/usage/agent-protocols.md
- docs/usage/a2a-and-workflows.md
- docs/reference/agent.md
- .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
tests:
- src/ts/vertebra-client/tests/agent.test.ts
- src/py/tests/unit/framework/test_import_map.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T06:36:00Z'
commit: null
tags:
- typescript
- mcp
- build
- docs
requirements:
- R8
---

# Update TypeScript McpClient SSE/_meta handling, derive _CLIENT_SUBPATHS from package.json, and sync docs/OKF

## Objective

Update `src/ts/vertebra-client/src/agent/index.ts` (`McpClient`) to send `Accept: application/json, text/event-stream` and `2026-07-28` `_meta`, parse `text/event-stream` JSON-RPC responses, harden `encodeMcpHeaderValue` against control/newline characters, derive `src/py/vertebra/build/_import_map.py::_CLIENT_SUBPATHS` from bundled `package.json` `exports`, and update agent docs and OKF chapters.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/patterns/typescript/client-and-adapter-subpaths.md`, and `.agents/skills/flow-docs/SKILL.md`. Keep public `docs/` Mermaid diagrams conceptual (`4–6` nodes) and omit `frozen=True, gc=False` boilerplate in user-facing doc examples.

## Steps

1. Add failing tests in `src/ts/vertebra-client/tests/agent.test.ts` (SSE `text/event-stream` response parsing, `_meta.protocolVersion` injection, `encodeMcpHeaderValue` CR/LF/CTL rejection) and `src/py/tests/unit/framework/test_import_map.py` (verifying `_CLIENT_SUBPATHS` matches `package.json` `exports` dynamically without manual drift).
2. Implement SSE response parsing, `_meta` negotiation, and `encodeMcpHeaderValue` hardening in `src/ts/vertebra-client/src/agent/index.ts`, rebuild `@vertebra/client` into wheel static assets, and derive `_CLIENT_SUBPATHS` in `src/py/vertebra/build/_import_map.py` from `package.json`.
3. Update `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, `docs/reference/agent.md`, and `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` to document `ToolSpec`, `ToolRegistry`, `MCPCore` (`2026-07-28`), `litestar-mcp` delegation, and `document.modelContext` WebMCP registration.
4. Run full flow verification (`make lint-all`, `make py-test`, `make ts-test`, `make docs`) and commit `feat(agent,client): add McpClient SSE/_meta support, dynamic subpath map, and 2026-07-28 docs`.

## Verification

```bash
make ts-test
uv run --no-sync pytest src/py/tests/unit/framework/test_import_map.py src/py/tests/unit/agent -q
make lint-all
make docs
```

## Acceptance Criteria

- [ ] R8: `McpClient` negotiates `_meta` and parses SSE responses, `_CLIENT_SUBPATHS` derives automatically from `package.json` `exports`, and `make lint-all`, `make py-test`, `make ts-test`, and `make docs` exit 0.

## Notes & Discoveries

None.
