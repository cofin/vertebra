---
type: Task
id: "native-redis-driver:ch08:t03"
flow_id: native-redis-driver
chapter_id: ch08-changelog-and-okf-sync
title: "Synchronize review-pyo3-bridge Skill, Domains Index, and OKF Workflow"
description: "Add vertebra-redis, redis-limitations.md, and make redis-test/redis-wheel-check to review-pyo3-bridge/SKILL.md, domains/index.md, and workflow.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch08:t02"
files:
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/workflow.md
tests: []
read_set:
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/workflow.md
estimated_loc: 75
tags:
  - skills
  - okf
  - workflow
sources:
  - id: review-pyo3-skill
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: "PyO3 bridge review skill"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project Makefile and verification workflow chapter"
---

# Task `t03`: Synchronize review-pyo3-bridge Skill, Domains Index, and OKF Workflow

## Objective & Scope Boundary

Synchronize `vertebra-redis`, `redis-limitations.md`, `make redis-test`, and `make redis-wheel-check` across `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` per [`../spec.md` §2](../spec.md) [^review-pyo3-skill] [^domains-idx] [^okf-workflow].

## Bounded Read Manifest

- `.agents/skills/review-pyo3-bridge/SKILL.md` — Review skill crate list (`<= 200` lines).
- `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
- `.agents/bundles/knowledge/workflow.md` — Canonical Makefile targets (`<= 200` lines).

## Contracts & Edge Cases

- Keep `review-pyo3-bridge/SKILL.md` and `workflow.md` `<= 200` lines each and keep `domains/index.md` frontmatter-free.
- **Edge Case**: Preserve `vertebra-sqlite` entries if `native-sqlite-driver` landed first.

## Test Matrix

- `make review-check` exits `0`.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `make review-check` passes with zero errors.
- [ ] Zero regressions in the chapter fast verification gate.

[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
