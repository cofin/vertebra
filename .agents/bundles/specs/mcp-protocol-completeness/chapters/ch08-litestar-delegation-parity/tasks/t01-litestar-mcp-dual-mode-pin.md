---
type: Task
id: "mcp-protocol-completeness:ch08:t01"
chapter_id: ch08-litestar-delegation-parity
title: Require litestar-mcp 0.20.0 and list its skills in the well-known document
description: Bump every litestar-mcp pin to >=0.20.0, refresh uv.lock, add a fail-loud version guard to create_mcp_plugin, and resolve litestar-mcp's optional skill_catalog dependency in the Vertebra well-known handler.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch07:t04"
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
files:
  - pyproject.toml
  - uv.lock
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
estimated_loc: 90
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
  - id: litestar-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: Factory and well-known handler
  - id: pyproject
    resource: pyproject.toml
    title: Optional dependency groups pinning litestar-mcp
tags:
  - litestar
  - dependencies
---

# Task t01: Require litestar-mcp 0.20.0 and List Its Skills in the Well-Known Document

## Objective & Scope Boundary

Litestar apps get `litestar-mcp`'s dual-mode transport and extensions, old installs fail loud, and the Vertebra well-known document lists Litestar skills [^litestar-plugin] [^pyproject]. Out of scope: `vtb ai mcp serve` delegation (`ch09:t02`).

## Bounded Read Manifest

- `pyproject.toml` — the three `litestar-mcp>=0.16.0` entries (`mcp` extra and two aggregate groups).
- `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin`.
- `src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document` (`skill_names` from `ch06:t03`).
- `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_create_mcp_plugin`, `test_litestar_mcp_delegation_no_duplicate_opt_mcp_tool_and_canonical_well_known`.

## Symbol & Signature Contracts

- `pyproject.toml`: each `"litestar-mcp>=0.16.0"` becomes `"litestar-mcp>=0.20.0"`; `uv.lock` regenerated with `uv lock --upgrade-package litestar-mcp`.
- `plugin.py`:
  - Module-level `try: from litestar_mcp import LitestarMCP, MCPConfig` stays; add a private module function `_require_dual_mode(mcp_module: Any) -> None` placed below `create_mcp_plugin` that raises `MissingDependencyError("litestar-mcp", install_package="litestar-mcp>=0.20.0", extra="mcp")` unless `"2025-11-25" in getattr(mcp_module, "SUPPORTED_PROTOCOL_VERSIONS", ())` (export path confirmed by the chapter `replan_trigger`).
  - `create_mcp_plugin` calls `_require_dual_mode(ensure_module("litestar_mcp", package="litestar-mcp", extra="mcp"))`.
  - Well-known handler signature `async def _vertebra_mcp_well_known(skill_catalog: Any = Dependency(default=None, skip_validation=True)) -> dict[str, Any]` (`from litestar.params import Dependency` at module top); when `skill_catalog` is not `None`, `skill_names = [skill.name for skill in await skill_catalog.list_skills(None)]`; passes `skill_names` to `build_well_known_mcp_document`.

## Edge Cases & Error Semantics

- `skill_catalog` dependency absent -> `skills == []`.
- `list_skills` raising propagates (fail loud; no silent empty list).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py` (extend `test_create_mcp_plugin`; new `test_create_mcp_plugin_requires_dual_mode` and `test_litestar_well_known_lists_skills` via `litestar.testing.create_test_client`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Version guard | `monkeypatch.setattr(litestar_mcp, "SUPPORTED_PROTOCOL_VERSIONS", ("2026-07-28",), raising=False)`; `create_mcp_plugin()` | `MissingDependencyError`; message contains `"litestar-mcp>=0.20.0"` |
| Installed version | Default environment | `create_mcp_plugin()` returns a `LitestarMCP` |
| Skills listed | `create_test_client(plugins=[create_mcp_plugin(MCPConfig(name="x", skills=litestar_mcp.MCPSkillsConfig(paths=[str(tmp_path)])))])` with `tmp_path/"lint"/"SKILL.md"`; `GET /.well-known/mcp.json` | `200`; `skills == ["lint"]` |
| No skills | Same without `skills` | `skills == []` |
| Lock pin | `uv lock --check` after the bump | Exit `0`; lock entry for `litestar-mcp` has version `>= 0.20.0` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q -k "dual_mode or lists_skills"` fails.
- **Green**: `uv lock --upgrade-package litestar-mcp && uv sync --all-extras --dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q && uv lock --check && uv run --no-sync ruff check src/py/vertebra/extensions/litestar/mcp && uv run --no-sync mypy src/py/vertebra/extensions/litestar/mcp`.

## Acceptance Criteria

- [ ] R14.1: pins, lock, version guard, and well-known skill names match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] `litestar-mcp` `0.20.0` exists on its release branch but is not yet published; the lock refresh needs network access and the published wheel.

[^litestar-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — Factory and well-known handler.
[^pyproject]: `pyproject.toml` — Optional dependency groups pinning `litestar-mcp`.
