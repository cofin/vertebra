---
type: Task
id: "native-sqlite-driver:ch01:t04"
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Register vertebra-sqlite in Review Areas, Checklist, and PyO3 Skill"
description: "Add vertebra-sqlite paths and _sqlite.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch01:t03"
files:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/pyo3-bridge/SKILL.md
tests: []
read_set:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/pyo3-bridge/SKILL.md
estimated_loc: 50
tags:
  - review
  - pyo3
  - skills
sources:
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: "Review router area path ownership manifest"
  - id: review-checklist
    resource: .agents/skills/review-core/references/checklist.md
    title: "Review checklist including FFI-09 stub list"
  - id: pyo3-skill
    resource: .agents/skills/pyo3-bridge/SKILL.md
    title: "PyO3 native extension engineering skill"
---

# Task `t04`: Register vertebra-sqlite in Review Areas, Checklist, and PyO3 Skill

## Objective & Scope Boundary

Register `src/rs/vertebra-sqlite/**` and `src/py/vertebra/_sqlite.pyi` in `areas.toml`, `checklist.md`, and `pyo3-bridge/SKILL.md` per [`../spec.md` §2](../spec.md) [^review-areas] [^review-checklist] [^pyo3-skill].

## Bounded Read Manifest

- `.agents/skills/review-core/references/areas.toml` — `[areas.pyo3-bridge]` `paths` list.
- `.agents/skills/review-core/references/checklist.md` — `FFI-09` `.pyi` stub verification rule.
- `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 extension crate inventory (`<= 200` lines).

## Contracts & Edge Cases

- Add `"src/rs/vertebra-sqlite/**"` and `"src/py/vertebra/_sqlite.pyi"` to `[areas.pyo3-bridge].paths` in `areas.toml`.
- Include `_sqlite` in `FFI-09` in `checklist.md` and `vertebra-sqlite` in `pyo3-bridge/SKILL.md` while keeping `SKILL.md <= 200` lines.
- **Edge Case**: `ReviewSetupValidator` requires every tracked `src/rs/*` and `src/py/vertebra/*` path to match at least one non-cross-cutting area.

## Test Matrix

- `make review-check` exits `0` with zero orphan-source or skill-budget errors.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `make review-check` passes with `vertebra-sqlite` and `_sqlite.pyi` registered.
- [ ] Zero regressions in the chapter fast verification gate.

[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area path ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 native extension engineering skill.
