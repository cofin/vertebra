---
type: Task
id: mcp-webmcp-core:t02
title: Build ToolRegistry over neutral ToolProvider, SkillControllers, and route discovery
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t01
files:
- src/py/vertebra/agent/mcp/registry.py
- src/py/vertebra/agent/tools/__init__.py
- src/py/vertebra/agent/controller.py
- src/py/vertebra/agent/bridges/mcp.py
- src/py/vertebra/agent/bridges/a2a.py
tests:
- src/py/tests/unit/agent/test_mcp_enterprise_tools.py
- src/py/tests/unit/db/test_bigquery.py
- src/py/tests/unit/db/test_spanner.py
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
- mcp
- registry
- tool-provider
requirements:
- R3
---

# Build ToolRegistry over neutral ToolProvider, SkillControllers, and route discovery

## Objective

Implement `ToolRegistry` (and align `MCPRegistry` / `build_tools`) over the neutral `vertebra.typing.ToolProvider` callable-map protocol, `@tool` functions, `SkillController`s, and route bridges without introducing vendor-specific branches or upward imports in database adapters.

## Context

Read `../spec.md`, `t01-toolspec-and-msgspec-schema.md`, and `src/py/vertebra/typing.py::ToolProvider`. `BigQueryToolProvider` and `SpannerToolProvider` stay in `vertebra.db.adapters.{bigquery,spanner}.tools` with zero imports from `vertebra.agent`.

## Steps

1. Write failing tests in `test_mcp_enterprise_tools.py` registering a custom engine-free `ToolProvider`, `BigQueryToolProvider`, `SpannerToolProvider`, a `SkillController`, and opted-in HTTP routes into `ToolRegistry`, verifying `ToolSpec` projection, bound method owner preservation, per-call confirmation hooks, deterministic digest/ETag invalidation, and zero `vertebra.agent` imports in `vertebra.db.adapters`.
2. Implement `ToolRegistry` in `src/py/vertebra/agent/mcp/registry.py` storing `(ToolSpec, Tool)` entries, supporting `register_provider(provider: ToolProvider, ...)`, `register_controller`, `register_routes`, and deterministic ETag computation across tools, resources, templates, and prompts.
3. Update `src/py/vertebra/agent/tools/__init__.py`, `controller.py`, and `bridges/{mcp,a2a}.py` to project `ToolSpec` cleanly without vendor-specific dispatch.
4. Run verification and commit `feat(agent): unify ToolRegistry discovery over neutral ToolProvider contract`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_enterprise_tools.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py -q
uv run --no-sync ruff check src/py/vertebra/agent
uv run --no-sync mypy src/py/vertebra/agent
```

## Acceptance Criteria

- [ ] R3: `ToolRegistry` ingests any `vertebra.typing.ToolProvider` (including `BigQueryToolProvider` and `SpannerToolProvider`), `@tool` functions, `SkillController`s, and routes with full typed `ToolSpec` metadata and zero upward imports in `vertebra.db`.

## Notes & Discoveries

None.
