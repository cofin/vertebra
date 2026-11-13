---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch05-framework-mounts-and-litestar-parity
title: "Chapter 05: Framework Mount Skill/UI Wiring and Litestar Delegation Parity"
description: "Wire SkillCatalog and MCPCore.validate() across AgentPlugin, Starlette, FastAPI, Flask, Django, and well-known manifests, and pin litestar-mcp>=0.20.0."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch04-skills-and-mcp-apps-core
replan_trigger: "Confirm litestar-mcp>=0.20.0 release on PyPI (SUPPORTED_PROTOCOL_VERSIONS and skill_catalog DI key) before executing t03"
plan_commit: 18baadcd
last_reviewed_commit: 0166aa62
updated_at: "2026-11-13T05:29:35Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:29:35Z"
    commit: 0166aa62
tags:
  - mcp
  - mounts
  - litestar
  - skills
  - ui
sources:
  - id: agent-plugin
    resource: "src/py/vertebra/agent/plugin.py::AgentPluginConfig"
    title: AgentPluginConfig and native/Litestar mount hooks
  - id: well-known
    resource: "src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document"
    title: Well-known /.well-known/mcp.json builder
  - id: starlette-mcp
    resource: "src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes"
    title: Starlette and FastAPI MCP mount helper
  - id: flask-mcp
    resource: "src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes"
    title: Flask MCP mount helper
  - id: litestar-mcp
    resource: "src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin"
    title: LitestarMCP plugin factory
---

# Chapter 05: Framework Mount Skill/UI Wiring and Litestar Delegation Parity

<!-- truth: start -->
- `AgentPluginConfig.skills`, Starlette/FastAPI `mount_mcp_routes(..., skills=...)`, Flask `attach_mcp_routes(..., skills=...)`, and Django `attach_mcp_routes`/`dispatch_mcp_request(..., skills=...)` coerce and pass `SkillCatalog` to `MCPCore` and run `MCPCore.validate()` at mount ([#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111)) [^agent-plugin] [^starlette-mcp] [^flask-mcp].
- `build_well_known_mcp_document(..., skill_names=...)` always emits a `skills` list alongside `tools` [^well-known].
- `pyproject.toml` and `uv.lock` pin `litestar-mcp>=0.20.0`, `create_mcp_plugin` fails loud via `MissingDependencyError` on older installs, and the Litestar well-known handler merges `skill_catalog` skill names [^litestar-mcp].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Even with `SkillCatalog` and `MCPCore.validate()` implemented in `ch04`, `AgentPlugin` and the Starlette, FastAPI, Flask, and Django mount helpers pass `skill_catalog=None` and never validate `ui://` links at startup, while Litestar apps delegate `/mcp` to `litestar-mcp` `0.19.0` lacking dual-mode sessions, SSE, skills, and SEP-1865 UI support [^agent-plugin] [^starlette-mcp] [^flask-mcp] [^litestar-mcp].

## 2. Requirements, Concrete Contracts & Architecture

- **`R5e` (`t01`)**: Add `skills: MCPSkillsConfig | SkillCatalog | None = None` on `AgentPluginConfig`, pass `skill_names` into `build_well_known_mcp_document`, wire `SkillCatalog.coerce(skills)` and `core.validate()` in `AgentPlugin` and `extensions/starlette/mcp.py::mount_mcp_routes` (shared by FastAPI).
- **`R5f` (`t02`)**: Wire `skills: MCPSkillsConfig | SkillCatalog | None = None`, `skill_names` well-known output, and `core.validate()` in `extensions/flask/mcp.py::attach_mcp_routes` and `extensions/django/mcp.py::{attach_mcp_routes,dispatch_mcp_request}`.
- **`R5g` (`t03`)**: Update all three `litestar-mcp` pins in `pyproject.toml` to `litestar-mcp>=0.20.0`, refresh `uv.lock`, guard `create_mcp_plugin` with `MissingDependencyError("litestar-mcp", install_package="litestar-mcp>=0.20.0", extra="mcp")` when `SUPPORTED_PROTOCOL_VERSIONS` lacks `"2025-11-25"`, and include `skill_catalog` skill names in `_vertebra_mcp_well_known`.

## 3. Documentation & OKF Knowledge Plan

Framework mount options and `litestar-mcp>=0.20.0` delegation are documented in `ch07` (`docs/usage/agent-protocols.md`, `domains/extensions/litestar.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R5e` | `t01` | `agent/plugin.py`, `agent/mcp/routes.py`, `extensions/starlette/mcp.py` | `pytest test_mcp_conformance.py` |
| `R5f` | `t02` | `extensions/flask/mcp.py`, `extensions/django/mcp.py` | `pytest test_mcp_conformance.py` |
| `R5g` | `t03` | `pyproject.toml`, `uv.lock`, `extensions/litestar/mcp/plugin.py` | `uv lock --check --offline` + `pytest test_litestar_ecosystem_mcp.py` |

## 5. Implementation Plan

- [x] [8c1a9f30] [t01-plugin-routes-and-starlette-skill-and-ui-wiring.md](tasks/t01-plugin-routes-and-starlette-skill-and-ui-wiring.md) — Wire `skills`, well-known `skill_names`, and `MCPCore.validate()` in `AgentPlugin`, `routes.py`, and Starlette
- [x] [8c1a9f30] [t02-flask-and-django-skill-and-ui-wiring.md](tasks/t02-flask-and-django-skill-and-ui-wiring.md) — Wire `skills`, well-known `skill_names`, and `MCPCore.validate()` in Flask and Django mounts
- [x] [8c1a9f30] [t03-litestar-mcp-dual-mode-pin.md](tasks/t03-litestar-mcp-dual-mode-pin.md) — Pin `litestar-mcp>=0.20.0`, enforce version guard, and merge Litestar skill names in `/.well-known/mcp.json`

## 6. Current State & Remaining Plan Snapshot

- **Status**: `ch05-framework-mounts-and-litestar-parity` is completed at `8c1a9f30` (`sync` `0166aa62`); next frontier is [`ch06-stdio-serve-bridge-and-gcp-auth`](../ch06-stdio-serve-bridge-and-gcp-auth/spec.md).
- **Fast Chapter Gate (`< 15s`)**: `uv lock --check --offline && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions/{starlette,flask,django,litestar} && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/{starlette,flask,django,litestar} && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q`.

[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPluginConfig` — AgentPluginConfig and native/Litestar mount hooks.
[^well-known]: `src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document` — Well-known /.well-known/mcp.json builder.
[^starlette-mcp]: `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — Starlette and FastAPI MCP mount helper.
[^flask-mcp]: `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — Flask MCP mount helper.
[^litestar-mcp]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — LitestarMCP plugin factory.
