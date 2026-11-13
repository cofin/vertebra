---
type: Task
id: "native-sqlite-driver:ch05:t03"
flow_id: native-sqlite-driver
chapter_id: ch05-docs-and-knowledge-sync
title: "Synchronize review-pyo3-bridge Skill, AGENTS.md, and OKF Workflow"
description: "Add vertebra-sqlite, make sqlite-test, and make sqlite-wheel-check to review-pyo3-bridge/SKILL.md, AGENTS.md, and workflow.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch05:t02"
files:
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
tests: []
read_set:
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
estimated_loc: 70
tags:
  - skills
  - workflow
  - docs
sources:
  - id: review-pyo3-skill
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: "PyO3 bridge review skill"
  - id: agents-md
    resource: AGENTS.md
    title: "Repository agent instructions and quick verification commands"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project Makefile and verification workflow chapter"
---

# Task `t03`: Synchronize review-pyo3-bridge Skill, AGENTS.md, and OKF Workflow

## Objective & Scope Boundary

Synchronize `vertebra-sqlite`, `make sqlite-test`, and `make sqlite-wheel-check` across `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `workflow.md` per [`../spec.md` §2](../spec.md) [^review-pyo3-skill] [^agents-md] [^okf-workflow].

## Bounded Read Manifest

- `.agents/skills/review-pyo3-bridge/SKILL.md` — Review skill crate list (`<= 200` lines).
- `AGENTS.md` — Rust crate list and Quick Verification Commands block.
- `.agents/bundles/knowledge/workflow.md` — Canonical Makefile target catalog (`<= 200` lines).

## Contracts & Edge Cases

- Keep `review-pyo3-bridge/SKILL.md` and `workflow.md` `<= 200` lines each and ensure every command in `AGENTS.md` matches `Makefile` (`DOC-08`).
- **Edge Case**: Preserve `review-pyo3-bridge` `Suppressions Index` and `FP-FFI-*` entries verbatim.

## Test Matrix

- `make review-check` exits `0`.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `AGENTS.md`, `workflow.md`, and `review-pyo3-bridge/SKILL.md` pass `make review-check`.
- [ ] Zero regressions in the chapter fast verification gate.

[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^agents-md]: `AGENTS.md` — Repository agent instructions and quick verification commands.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
