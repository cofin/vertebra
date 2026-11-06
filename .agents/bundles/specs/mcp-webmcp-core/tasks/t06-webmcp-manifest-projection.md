---
type: Task
id: mcp-webmcp-core:t06
title: Project WebMCP manifests strictly from ToolSpec exposure metadata
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t05
files:
- src/py/vertebra/agent/mcp/webmcp.py
- src/py/vertebra/agent/mcp/routes.py
- src/py/vertebra/agent/plugin.py
tests:
- src/py/tests/unit/agent/test_mcp_conformance.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 2
claimed_by: executor
claimed_at: '2026-11-06T22:34:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T22:38:00Z'
last_verified_commit: "f4ed07da322704eaea5fb4fd5993a0dbb34caf43"
verification_evidence: "18 passed in test_mcp_conformance.py; ruff, mypy, scan_public_docstrings.py passed."
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T22:38:00Z'
commit: "f4ed07da322704eaea5fb4fd5993a0dbb34caf43"
tags:
- webmcp
- manifest
- security
requirements:
- R7
---

# Project WebMCP manifests strictly from ToolSpec exposure metadata

## Objective

Project `/.well-known/webmcp.json` strictly from `ToolSpec` instances whose `exposure` contains `"webmcp"` (default excluded), emitting `title`, `outputSchema`, and `annotations.consequentialHint` mapped from `requires_confirmation`.

## Context

Read `../spec.md` and `t05-agent-plugin-and-litestar-mcp.md`. Default `@tool` definitions and `ToolProvider` callables must never appear in `/.well-known/webmcp.json` unless explicitly configured with `webmcp=True` / `"webmcp" in exposure`.

## Steps

1. Write failing tests in `test_mcp_conformance.py` verifying that default tools and database `ToolProvider` tools are excluded from `build_webmcp_manifest`, while tools with `"webmcp" in spec.exposure` emit `title`, `inputSchema`, `outputSchema`, and `annotations.consequentialHint = True` when `requires_confirmation=True`.
2. Update `WebMCPToolDescriptor` and `build_webmcp_manifest` in `src/py/vertebra/agent/mcp/webmcp.py` to project directly from `ToolSpec` / `ToolRegistry` and wire the manifest endpoint in `routes.py` and `plugin.py`.
3. Run verification and commit `feat(webmcp): project WebMCP manifests strictly from ToolSpec exposure`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
uv run --no-sync ruff check src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py
uv run --no-sync mypy src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py
```

## Acceptance Criteria

- [x] R7: `/.well-known/webmcp.json` includes only tools with `"webmcp" in spec.exposure` and emits `title`, `outputSchema`, and `consequentialHint`.

## Notes & Discoveries

None.
