---
type: Task
id: "mcp-protocol-completeness:ch05:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch05-framework-mounts-and-litestar-parity
title: "Wire Skills, Well-Known Skill Names, and UI Validation in AgentPlugin and Starlette"
description: "Add AgentPluginConfig.skills, build_well_known_mcp_document skill_names, and mount-time MCPCore.validate() across AgentPlugin and Starlette/FastAPI."
status: stable
state: completed
commit: 8c1a9f30
last_verified_commit: 8c1a9f30
last_verified_at: "2026-11-13T05:28:40Z"
verification_evidence: "uv lock --check --offline + ruff + mypy (48 files) + pytest (279 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/extensions/starlette/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/extensions/fastapi/mcp.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 165
tags:
  - mcp
  - skills
  - ui
  - starlette
sources:
  - id: agent-plugin
    resource: "src/py/vertebra/agent/plugin.py::AgentPluginConfig"
    title: Native and Litestar AgentPlugin configuration
  - id: starlette-mcp
    resource: "src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes"
    title: Starlette and FastAPI MCP mount helper
---

# Task `t01`: Wire Skills, Well-Known Skill Names, and UI Validation in AgentPlugin and Starlette

## Objective & Scope Boundary

Wire `SkillCatalog` and mount-time `MCPCore.validate()` into `AgentPlugin`, `build_well_known_mcp_document`, and `mount_mcp_routes` per [`../spec.md` §2](../spec.md) [^agent-plugin] [^starlette-mcp].

## Bounded Read Manifest

- `src/py/vertebra/agent/{plugin,mcp/routes}.py` & `extensions/{starlette,fastapi}/mcp.py`.

## Contracts & Edge Cases

- `build_well_known_mcp_document(..., skill_names: Iterable[str] | None = None)` always emits `'skills': sorted(set(skill_names or ()))`.
- `AgentPluginConfig.skills: MCPSkillsConfig | SkillCatalog | None = None` and `mount_mcp_routes(..., skills=None)` coerce catalogs via `SkillCatalog.coerce`, populate well-known `skill_names`, and invoke `core.validate()` after route discovery so broken `ui://` links raise `ImproperConfigurationError` at mount.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` verifying `skills/list` and `/.well-known/mcp.json` on `AgentPlugin` and Starlette/FastAPI mounts, plus mount-time `ImproperConfigurationError` on dangling `ui://missing` links.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/plugin.py src/py/vertebra/agent/mcp/routes.py src/py/vertebra/extensions/starlette/mcp.py && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/starlette/mcp.py && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] `AgentPlugin` and Starlette/FastAPI mounts serve configured skills and include them in `/.well-known/mcp.json`.
- [x] Broken `ui://` tool links raise `ImproperConfigurationError` during mount.

[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPluginConfig` — Native and Litestar AgentPlugin configuration.
[^starlette-mcp]: `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — Starlette and FastAPI MCP mount helper.
