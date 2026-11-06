---
type: Task
id: mcp-webmcp-core:t05
title: Wire native AgentPlugin to MCPCore, delegate Litestar to litestar-mcp, and unify well-known discovery
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t04
files:
- src/py/vertebra/agent/plugin.py
- src/py/vertebra/agent/a2a/routes.py
- src/py/vertebra/extensions/litestar/mcp/plugin.py
- src/py/vertebra/extensions/litestar/mcp/card.py
- src/py/vertebra/extensions/litestar/mcp/accelerator.py
tests:
- src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
- src/py/tests/unit/agent/test_mcp_conformance.py
- src/py/tests/unit/agent/test_a2a_protocol.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 2
claimed_by: executor
claimed_at: '2026-11-06T22:18:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T22:33:00Z'
last_verified_commit: "10143439aba293218a7148a11a2529c877dfe2f1"
verification_evidence: "71 passed in test_litestar_ecosystem_mcp.py, test_mcp_conformance.py, test_a2a_protocol.py, test_runtime_and_workflows.py; ruff, mypy, scan_public_docstrings.py, review_setup.py validate passed."
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T22:34:00Z'
commit: "10143439aba293218a7148a11a2529c877dfe2f1"
tags:
- mcp
- litestar
- agent
requirements:
- R6
---

# Wire native AgentPlugin to MCPCore, delegate Litestar to litestar-mcp, and unify well-known discovery

## Objective

Route native `App` `AgentPlugin` MCP requests through `MCPCore`, delegate Litestar MCP handling cleanly to `litestar-mcp` without double-registering `opt["mcp_tool"]` handlers, and unify `/.well-known/mcp.json` on one canonical discovery payload across native `App`, ASGI/WSGI extensions, and Litestar.

## Context

Read `../spec.md`, `t04-shared-asgi-wsgi-mcp-adapters.md`, and `.agents/skills/litestar-mcp/SKILL.md`.

## Steps

1. Add failing tests in `test_litestar_ecosystem_mcp.py`, `test_mcp_conformance.py`, and `test_a2a_protocol.py` asserting that `opt["mcp_tool"]` routes appear once in Litestar `tools/list`, native `App` + `AgentPlugin` enforces `MCPCore` `2026-07-28` semantics, A2A skill cards project from `ToolSpec` (`"a2a" in exposure`), and `/.well-known/mcp.json` emits identical field names and protocol version (`2026-07-28`) across all mounts.
2. Update `src/py/vertebra/agent/plugin.py` and `src/py/vertebra/agent/a2a/routes.py` to consume `MCPCore` and `ToolSpec`.
3. Update `src/py/vertebra/extensions/litestar/mcp/{plugin.py,card.py,accelerator.py}` to delegate cleanly to `litestar-mcp` without duplicate `mcp_tool` registration and serve the canonical `/.well-known/mcp.json` descriptor.
4. Run verification and commit `feat(mcp): wire AgentPlugin to MCPCore and unify litestar-mcp discovery`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_a2a_protocol.py -q
uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions/litestar/mcp
uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/litestar/mcp
```

## Acceptance Criteria

- [x] R6: Native `AgentPlugin` routes through `MCPCore`, Litestar delegates to `litestar-mcp` without duplicate `opt["mcp_tool"]` entries, and `/.well-known/mcp.json` shares one canonical schema.

## Notes & Discoveries

None.
