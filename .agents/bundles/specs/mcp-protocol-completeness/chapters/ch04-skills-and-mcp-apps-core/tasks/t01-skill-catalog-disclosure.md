---
type: Task
id: "mcp-protocol-completeness:ch04:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch04-skills-and-mcp-apps-core
title: "Complete SkillCatalog Filtering, URI Lookup, Listeners, and Refresh"
description: "Extend Skill, MCPSkillsConfig, and SkillCatalog in skills.py with coerce, tag/query filtering, skill:// URI resolution, resource_entries, listeners, and refresh."
status: stable
state: completed
commit: a38f6ef7
last_verified_commit: a38f6ef7
last_verified_at: "2026-11-13T05:17:00Z"
verification_evidence: "ruff + mypy (39 files) + pytest (270 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/skills.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 210
tags:
  - mcp
  - skills
  - agent
sources:
  - id: skills-py
    resource: "src/py/vertebra/agent/mcp/skills.py::SkillCatalog"
    title: SkillCatalog and FilesystemSkillSource
---

# Task `t01`: Complete SkillCatalog Filtering, URI Lookup, Listeners, and Refresh

## Objective & Scope Boundary

Complete `SkillCatalog` disclosure primitives for [#111](https://github.com/cofin/vertebra/issues/111) per [`../spec.md` §2](../spec.md) [^skills-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{skills,models}.py` — `Skill`, `MCPSkillsConfig`, `SkillCatalog`, `ResourceEntry`.

## Contracts & Edge Cases

- `SkillCatalog.coerce(MCPSkillsConfig | SkillCatalog | None)` returns a `SkillCatalog` or `None`, raising `TypeError` on invalid types.
- `list_skills(*, tags=None, query=None)` filters by tag intersection and case-insensitive name/title/description substring; `get_skill` resolves `name`, `skill://name`, and `skill://name/SKILL.md`.
- `refresh() -> bool` reloads `sources` and fires `add_listener` callbacks strictly when catalog contents changed.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering `coerce` valid/invalid inputs, tag + query filtering, all 3 `get_skill` URI forms, `unregister_skill`, and `refresh()` change vs no-op listener firing.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp/skills.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/skills.py && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] `SkillCatalog` supports filtering, `skill://` lookup, `resource_entries()`, listeners, and idempotent `refresh()`.
- [x] Invalid `SkillCatalog.coerce` arguments raise `TypeError`.

[^skills-py]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` — SkillCatalog and FilesystemSkillSource.
