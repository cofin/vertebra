---
type: Task
id: "native-postgres-driver:ch07-wheel-ci-and-benchmarks:t04"
flow_id: native-postgres-driver
chapter_id: ch07-wheel-ci-and-benchmarks
title: "Synchronize benchmarks, polyglot-test-matrix, and workflow OKF chapters"
description: "Update benchmarks-and-free-threaded.md, trim polyglot-test-matrix.md (<= 200L), and add postgres-bench targets to workflow.md."
status: stable
state: completed
commit: 9a3631bb
last_verified_at: "2026-11-13T22:11:30Z"
last_verified_commit: 9a3631bb
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch07:t03"
files: 
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
estimated_loc: 155
tags:
  - docs
  - okf
  - testing
  - benchmarks
sources:
  - id: wf-okf
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project workflow and Make targets"
---

# Task `t04`: Synchronize benchmarks, polyglot-test-matrix, and workflow OKF chapters

## Objective & Scope Boundary

Update and trim the 3 testing/workflow OKF chapters (`benchmarks-and-free-threaded.md`: `164L`, `polyglot-test-matrix.md`: `192L`, `workflow.md`: `119L`, all `<= 200L`) [^wf-okf].

## Bounded Read Manifest

- `benchmarks-and-free-threaded.md`, `polyglot-test-matrix.md`, and `workflow.md`.

## Contracts & Edge Cases

- Keep all 3 OKF files `<= 200` lines with `<= 40` truth-block lines and `18baadcd`/current commit provenance.

## Test Matrix

- `make review-check` and line-count check.

## Verification

```bash
make review-check && uv run --no-sync pytest src/py/tests/unit/test_review_setup.py -q
```

## Acceptance Criteria

- [x] All 3 OKF files are `<= 200` lines and pass `make review-check`.
- [x] Zero regressions in `ch07-wheel-ci-and-benchmarks` fast verification gate.

[^wf-okf]: `.agents/bundles/knowledge/workflow.md` — Project workflow and Make targets.
