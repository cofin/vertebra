---
type: Task
id: "mcp-protocol-completeness:ch06:t03"
chapter_id: ch06-skills-extension
title: Wire skills through AgentPlugin, Starlette, and the well-known document
description: Add AgentPluginConfig.skills and mount_mcp_routes(skills=...), pass the coerced catalog to MCPCore, and add a skills list to build_well_known_mcp_document served by native, Litestar fallback, and Starlette mounts.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch06:t02"
read_set:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/extensions/starlette/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 140
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
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin.__init__
    title: MCPService construction without a skill catalog
  - id: well-known
    resource: src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document
    title: Well-known document keys
  - id: starlette-mount
    resource: src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes
    title: Starlette and FastAPI mount
tags:
  - skills
  - wiring
---

# Task t03: Wire Skills through AgentPlugin, Starlette, and the Well-Known Document

## Objective & Scope Boundary

Applications enable skills with one configuration value on `AgentPlugin` or `mount_mcp_routes`, and discovery documents list skill names [^agent-plugin] [^well-known] [^starlette-mount]. Out of scope: Flask and Django (`t04`), Litestar `LitestarMCP` (`ch08`).

## Bounded Read Manifest

- `src/py/vertebra/agent/plugin.py` — `AgentPluginConfig`, `AgentPlugin.__init__`, native and Litestar well-known handlers.
- `src/py/vertebra/agent/mcp/routes.py` — `build_well_known_mcp_document`, `render_well_known_mcp_json`.
- `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes`.
- `src/py/vertebra/agent/mcp/skills.py::SkillCatalog.coerce`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_native_app_agent_plugin_routes_through_mcp_core_and_unifies_well_known`.

## Symbol & Signature Contracts

- `AgentPluginConfig.skills: MCPSkillsConfig | SkillCatalog | None = None` (field docstring); `AgentPlugin.__init__` sets `self.skill_catalog = SkillCatalog.coerce(self.config.skills)` and passes `skill_catalog=self.skill_catalog` to `MCPService`.
- `build_well_known_mcp_document(config, registry=None, *, tool_names=None, skill_names=None) -> dict[str, Any]` always includes `"skills": list(dict.fromkeys(skill_names or ()))`; `render_well_known_mcp_json` forwards `skill_names`.
- Native, Litestar fallback, and Starlette well-known handlers pass `skill_names=[s.name for s in catalog.list_skills()]` evaluated per request (catalog refreshes stay visible; the Starlette handler stops pre-rendering bytes when a catalog is present).
- `mount_mcp_routes(app, *, registry=None, config=None, webmcp_config=None, skills: MCPSkillsConfig | SkillCatalog | None = None) -> MCPRegistry` passes the coerced catalog to `MCPCore` and stores it as `state.mcp_skill_catalog`.

## Edge Cases & Error Semantics

- `skills=None` keeps every surface free of skills and the well-known `skills` list empty.
- Invalid `skills` values raise `TypeError` at construction (from `coerce`).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (update the expected well-known key set to include `"skills"`; new `test_mcp_skill_wiring_asgi_mounts`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Plugin skills | `AgentPlugin(AgentPluginConfig(skills=MCPSkillsConfig(paths=(tmp_path,))))` with `lint`; `plugin.mcp_service.handle(skills/list)` | One item named `lint` |
| Native well-known | `mount_native(app)`; call the well-known handler | `doc["skills"] == ["lint"]`; key set `{"protocolVersion", "mcp_version", "server", "serverInfo", "endpoint", "tools", "skills"}` |
| Starlette mount | `mount_mcp_routes(SimpleNamespace(routes=[], state=SimpleNamespace()), skills=SkillCatalog([Skill.build("lint", "Lint", body="b")]))`; call `/mcp` with `skills/list` via `state.mcp_app.core.handle` | One item |
| Starlette well-known | Await the well-known endpoint | `skills == ["lint"]` |
| No skills | Default plugin well-known | `skills == []` |
| Bad value | `AgentPluginConfig(skills=123)` then `AgentPlugin(...)` | `TypeError` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "skill_wiring_asgi or unifies_well_known"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions/starlette/mcp.py && uv run --no-sync mypy src/py/vertebra/agent/plugin.py src/py/vertebra/agent/mcp/routes.py src/py/vertebra/extensions/starlette/mcp.py`.

## Acceptance Criteria

- [ ] R12.3: plugin and Starlette/FastAPI skill wiring and well-known `skills` lists match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] FastAPI re-exports `mount_mcp_routes` from the Starlette module, so FastAPI inherits the `skills` parameter without edits.

[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.__init__` — MCPService construction without a skill catalog.
[^well-known]: `src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document` — Well-known document keys.
[^starlette-mount]: `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — Starlette and FastAPI mount.
