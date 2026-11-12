---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch08-litestar-delegation-parity
title: "Chapter 8: Litestar Delegation Parity"
description: Require litestar-mcp>=0.20.0 so Litestar apps that mount LitestarMCP receive dual-mode transport, sessions, GET SSE, subscriptions, skills, and MCP Apps support, fail loud on older installs, and list litestar-mcp skill names in the Vertebra well-known document.
status: draft
state: planned
planning_status: empirical_dependent
depends_on_chapters:
  - ch07-mcp-apps-ui-metadata
replan_trigger: "litestar-mcp 0.20.0 is published to PyPI; confirm the skill_catalog dependency key, the async SkillCatalog.list_skills(request=None) signature, SUPPORTED_PROTOCOL_VERSIONS export path, and litestar_mcp.mcp.stdio.run_stdio signature before executing t01"
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: litestar-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: LitestarMCP factory and well-known handler
  - id: pyproject
    resource: pyproject.toml
    title: Optional dependency pins (litestar-mcp>=0.16.0 in three groups)
  - id: litestar-okf
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: Litestar extension OKF chapter
tags:
  - litestar
  - mcp
  - dependencies
---

# Chapter 8: Litestar Delegation Parity

<!-- truth: start -->
- **Pin**: every `pyproject.toml` group that lists `litestar-mcp` requires `litestar-mcp>=0.20.0`, and `uv.lock` resolves `0.20.0` or later [^pyproject].
- **Fail loud**: `create_mcp_plugin` raises `MissingDependencyError("litestar-mcp", install_package="litestar-mcp>=0.20.0", extra="mcp")` when the installed `litestar_mcp` lacks `SUPPORTED_PROTOCOL_VERSIONS` containing `"2025-11-25"` [^litestar-plugin].
- **Well-known skills**: the Vertebra well-known handler resolves `litestar-mcp`'s `skill_catalog` dependency (optional) and lists its skill names next to merged tool names [^litestar-plugin].
- **Delegation**: Litestar apps with `LitestarMCP` keep delegating `/mcp` to `litestar-mcp`; `AgentPlugin` does not mount its fallback endpoint when `LitestarMCP` is present [^litestar-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Litestar apps delegate MCP to `litestar-mcp`, pinned at `>=0.16.0` and locked at `0.19.0`, which predates dual-mode transport, sessions, `GET` SSE, subscriptions, SEP-2640 skills, and SEP-1865 metadata. `litestar-mcp` `0.20.0` implements those features with the same field names this flow uses. This chapter moves the pin and keeps the Vertebra well-known document complete for Litestar apps.

### 1.2 Upstream Execution Baseline

- Depends on `ch07` (parity target for field names) and on the external `litestar-mcp` `0.20.0` release (`replan_trigger`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Factory | `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` [^litestar-plugin] | Litestar plugin and DI. |
| Pins | `pyproject.toml` (`mcp` extra and two aggregate groups), `uv.lock` (`litestar-mcp` `0.19.0`) [^pyproject] | Packaging. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Litestar extensions](../../../../knowledge/domains/extensions/litestar.md) [^litestar-okf], [`litestar-mcp` skill](../../../../../skills/python-framework/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R14.1**: `litestar-mcp>=0.20.0` pins, lock update, version guard, and well-known skill names.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
def create_mcp_plugin(
    config: MCPConfig | None = None,
    *,
    name: str = "Vertebra MCP API",
    enable_tasks: bool = True,
    registry: Any | None = None,
    expose_well_known: bool = True,
    well_known_path: str = "/.well-known/mcp.json",
    **kwargs: Any,
) -> LitestarMCP: ...
# well-known handler (internal):
async def _vertebra_mcp_well_known(skill_catalog: Any = Dependency(default=None, skip_validation=True)) -> dict[str, Any]: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Litestar app -> `create_mcp_plugin` -> `LitestarMCP(resolved_config)` -> `plugin.register_dynamic_handler(_vertebra_mcp_well_known)`.
- Well-known request -> Litestar DI `skill_catalog` -> `await skill_catalog.list_skills(None)` -> `build_well_known_mcp_document(..., tool_names=..., skill_names=...)`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Old install | `litestar_mcp` without `SUPPORTED_PROTOCOL_VERSIONS` | `MissingDependencyError` naming `litestar-mcp>=0.20.0` | Fail loud |
| No skills configured | Dependency absent | `skills == []` | Optional dependency |
| Skills configured | `MCPConfig(skills=MCPSkillsConfig(paths=[...]))` | Skill names listed | Complete discovery |

### 2.5 Acceptance Scenarios

- Given a Litestar app built with `create_mcp_plugin(MCPConfig(skills=...))`, when a client fetches `/.well-known/mcp.json`, then `skills` lists the configured skills and `tools` lists discovered and registry tools.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Litestar transport | Delegate to `litestar-mcp>=0.20.0` | Mounting Vertebra's core beside `LitestarMCP` serves two MCP implementations on one path. |
| Version check | Attribute probe | Parsing `importlib.metadata` versions adds a packaging dependency to a runtime path. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch08]` — the Litestar OKF chapter update lands in `ch11:t04`; install docs in `ch11:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R14.1 | [t01](tasks/t01-litestar-mcp-dual-mode-pin.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch08:t01 — Require litestar-mcp 0.20.0 and list its skills in the well-known document (tasks/t01-litestar-mcp-dual-mode-pin.md)

**Chapter Recovery Checkpoint:** `uv lock --check && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/litestar/mcp && uv run --no-sync mypy src/py/vertebra/extensions/litestar/mcp` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/1` tasks completed; `planning_status: empirical_dependent` until `litestar-mcp` `0.20.0` is published.
- **Downstream Dependents**: `ch09:t02` delegates `vtb ai mcp serve` for Litestar apps to `litestar_mcp.mcp.stdio.run_stdio`.

[^litestar-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — LitestarMCP factory and well-known handler.
[^pyproject]: `pyproject.toml` — Optional dependency pins (`litestar-mcp>=0.16.0` in three groups).
[^litestar-okf]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar extension OKF chapter.
