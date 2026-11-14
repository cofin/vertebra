---
type: Task
id: native-redis-driver:ch01-wheel-scaffold-and-isolation:t04
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Register vertebra-redis in Review Manifests, Checklist, and Skills
description: Add vertebra-redis paths and _redis.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 104fd6fa
depends_on:
- native-redis-driver:ch01-wheel-scaffold-and-isolation:t03
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
  title: Review router area ownership manifest
- id: review-checklist
  resource: .agents/skills/review-core/references/checklist.md
  title: Review checklist including FFI-09 stub list
- id: pyo3-skill
  resource: .agents/skills/pyo3-bridge/SKILL.md
  title: PyO3 native extension engineering skill
---

# Task `t04`: Register vertebra-redis in Review Manifests, Checklist, and Skills

## Objective & Scope Boundary

Register `src/rs/vertebra-redis/**` and `src/py/vertebra/_redis.pyi` in `areas.toml`, `checklist.md`, and `pyo3-bridge/SKILL.md` per [`../spec.md` §2](../spec.md) [^review-areas] [^review-checklist] [^pyo3-skill].

## Bounded Read Manifest

- `.agents/skills/review-core/references/areas.toml` — `[areas.pyo3-bridge]` `paths` list.
- `.agents/skills/review-core/references/checklist.md` — `FFI-09` stub verification rule.
- `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 extension crate inventory (`<= 200` lines).

## Contracts & Edge Cases

- Add `"src/rs/vertebra-redis/**"` and `"src/py/vertebra/_redis.pyi"` to `[areas.pyo3-bridge].paths`.
- Include `_redis` in `FFI-09` (`checklist.md`) and `vertebra-redis` in `pyo3-bridge/SKILL.md` (`<= 200` lines).
- **Edge Case**: If `native-sqlite-driver` landed first, preserve `vertebra-sqlite` entries alongside `vertebra-redis`.

## Test Matrix

- `make review-check` exits `0`.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `make review-check` passes with zero orphan-source or skill-budget errors.
- [ ] Zero regressions in the fast verification gate.

[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 native extension engineering skill.
