---
type: Task
id: native-valkey-driver:ch01-wheel-scaffold-and-isolation:t04
flow_id: native-valkey-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Register vertebra-valkey in Review Capability Manifests and PyO3 Skills
description: Add src/rs/vertebra-valkey/** and src/py/vertebra/_valkey.pyi to .agents/skills/review-core/references/areas.toml, checklist.md (FFI-09), and .agents/skills/pyo3-bridge/SKILL.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch01-wheel-scaffold-and-isolation:t03
files:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/pyo3-bridge/SKILL.md
tests: []
read_set:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/pyo3-bridge/SKILL.md
estimated_loc: 60
tags:
  - review
  - skills
  - pyo3
  - valkey
sources:
  - id: areas-toml
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area capability manifest
  - id: checklist-md
    resource: .agents/skills/review-core/references/checklist.md
    title: Review checklist items
  - id: pyo3-skill
    resource: .agents/skills/pyo3-bridge/SKILL.md
    title: PyO3 bridge engineering skill
verification_strategy: documentation_validation
---

# Task `t04`: Register vertebra-valkey in Review Capability Manifests and PyO3 Skills

## Objective & Scope Boundary

Register `src/rs/vertebra-valkey/**` and `src/py/vertebra/_valkey.pyi` in `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md` (`FFI-09`), and `.agents/skills/pyo3-bridge/SKILL.md` per [`../spec.md` §2](../spec.md) [^areas-toml] [^checklist-md] [^pyo3-skill].

## Bounded Read Manifest

- `.agents/skills/review-core/references/areas.toml` — `[areas.pyo3-bridge].paths` flat path list.
- `.agents/skills/review-core/references/checklist.md` — `FFI-09` stub-parity (`stubtest`) rule.
- `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 bridge skill crate list (`<= 200` lines).

## Contracts & Edge Cases

- Add `src/rs/vertebra-valkey/**` and `src/py/vertebra/_valkey.pyi` to `[areas.pyo3-bridge].paths` in `areas.toml`.
- Add `vertebra-valkey` (`_valkey.pyi`) to `FFI-09` in `checklist.md` and `pyo3-bridge/SKILL.md`.
- **Edge Case**: Keep `checklist.md` and `pyo3-bridge/SKILL.md` `<= 200` lines so `ReviewSetupValidator` passes.

## Test Matrix

- `make review-check` and `make review-route` pass with zero unrouted files.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `areas.toml`, `checklist.md`, and `pyo3-bridge/SKILL.md` include `vertebra-valkey` and pass `make review-check`.
- [ ] Zero regressions in the fast verification gate.

[^areas-toml]: `.agents/skills/review-core/references/areas.toml` — Review area capability manifest.
[^checklist-md]: `.agents/skills/review-core/references/checklist.md` — Review checklist items.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 bridge engineering skill.
