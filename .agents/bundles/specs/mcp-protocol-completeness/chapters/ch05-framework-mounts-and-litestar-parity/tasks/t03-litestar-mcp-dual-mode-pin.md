---
type: Task
id: "mcp-protocol-completeness:ch05:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch05-framework-mounts-and-litestar-parity
title: "Pin litestar-mcp>=0.20.0 and Merge Litestar Skill Names in Well-Known Document"
description: "Require litestar-mcp>=0.20.0 in pyproject.toml and uv.lock, fail loud on pre-0.20 installs, and merge Litestar skill_catalog names into /.well-known/mcp.json."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch05:t01"
files:
  - pyproject.toml
  - uv.lock
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
estimated_loc: 95
tags:
  - litestar
  - mcp
  - dependencies
  - packaging
sources:
  - id: litestar-mcp
    resource: "src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin"
    title: LitestarMCP factory and well-known handler
  - id: pyproject-toml
    resource: pyproject.toml
    title: Optional dependency groups pinning litestar-mcp
---

# Task `t03`: Pin litestar-mcp>=0.20.0 and Merge Litestar Skill Names in Well-Known Document

## Objective & Scope Boundary

Upgrade `litestar-mcp` pin to `>=0.20.0`, add a runtime capability guard in `create_mcp_plugin`, and include Litestar `skill_catalog` names in `/.well-known/mcp.json` per [`../spec.md` §2](../spec.md) [^litestar-mcp] [^pyproject-toml].

## Bounded Read Manifest

- `pyproject.toml`, `src/py/vertebra/extensions/litestar/mcp/plugin.py`, & `test_litestar_ecosystem_mcp.py`.

## Contracts & Edge Cases

- Bump all three `litestar-mcp>=0.16.0` entries in `pyproject.toml` to `litestar-mcp>=0.20.0` and update `uv.lock`.
- `create_mcp_plugin` raises `MissingDependencyError('litestar-mcp', install_package='litestar-mcp>=0.20.0', extra='mcp')` if `SUPPORTED_PROTOCOL_VERSIONS` lacks `'2025-11-25'`.
- `_vertebra_mcp_well_known` injects optional `skill_catalog` via `Dependency(default=None, skip_validation=True)` and passes its skill names to `build_well_known_mcp_document`.

## Test Matrix

- Unit tests in `test_litestar_ecosystem_mcp.py` covering pre-`0.20.0` `MissingDependencyError`, `/.well-known/mcp.json` with and without skills configured, and dual-mode transport forwarding.

## Verification

```bash
uv lock --check --offline && uv run --no-sync ruff check src/py/vertebra/extensions/litestar/mcp && uv run --no-sync mypy src/py/vertebra/extensions/litestar/mcp && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q
```

## Acceptance Criteria

- [ ] `pyproject.toml` and `uv.lock` resolve `litestar-mcp>=0.20.0`.
- [ ] Litestar well-known handler lists both merged tools and configured skill names.

[^litestar-mcp]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — LitestarMCP factory and well-known handler.
[^pyproject-toml]: `pyproject.toml` — Optional dependency groups pinning litestar-mcp.
