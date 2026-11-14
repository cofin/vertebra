---
type: Task
id: "driver-conformance-ci:ch04:t03"
flow_id: driver-conformance-ci
chapter_id: ch04-docs-and-okf-sync
title: "Synchronize polyglot-test-matrix.md, benchmarks-and-free-threaded.md, and workflow.md"
description: "Update testing and workflow OKF chapters within <= 200 lines for ConformanceGate, make db-conformance, CI matrix lanes, and bench-only PyPI duckdb."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_revision: 4
plan_commit: 37972f2a
depends_on: []
files:
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/workflow.md
tests: []
read_set:
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/workflow.md
  - Makefile
  - .github/workflows/ci.yml
estimated_loc: 140
tags:
  - okf
  - testing
  - workflow
  - benchmarks
sources:
  - id: okf-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: "Polyglot test matrix and conformance OKF chapter"
  - id: okf-bench
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: "Benchmarks and free-threaded Python OKF chapter"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Canonical Makefile commands and workflow OKF chapter"
---

# Task `t03`: Synchronize `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, and `workflow.md`

## Objective & Scope Boundary

Synchronize `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, and `.agents/bundles/knowledge/workflow.md` (`<= 200` lines each) per [`../spec.md` §2](../spec.md) [^okf-matrix] [^okf-bench] [^okf-workflow]. Out of scope: data-domain OKF chapters (`t04`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/testing/{polyglot-test-matrix,benchmarks-and-free-threaded}.md` and `.agents/bundles/knowledge/workflow.md`.
- `Makefile` and `.github/workflows/ci.yml` — Authoritative targets, dependency groups, and CI job names.

## Contracts & Edge Cases

- **`polyglot-test-matrix.md` (`<= 200L`)**: Document `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`), `ServiceVersionMatrix` (`VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION`), `test_{duckdb,mysql,mssql,oracle}_native.py`, `test-db-conformance`, and non-blocking `conformance-oracle`.
- **`benchmarks-and-free-threaded.md` (`<= 200L`)**: Remove stale claims that `PY_TEST_GROUPS` or `make install` sync `--group duckdb` on GIL builds; document that PyPI `duckdb` is isolated to `--group bench`.
- **`workflow.md` (`<= 200L`)**: Add `make db-conformance` and update `make install` / `make oracle-test` descriptions; stamp `verified[].commit` to a valid `HEAD` ancestor with multi-line YAML frontmatter and zero standalone `---`.

## Test Matrix

- Line-count verification (`<= 200` lines per chapter, truth blocks `<= 40` lines) and `make review-check`.

## Verification

```bash
for f in .agents/bundles/knowledge/testing/polyglot-test-matrix.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md .agents/bundles/knowledge/workflow.md; do test "$(wc -l < "$f")" -le 200; done && make review-check
```

## Acceptance Criteria

- [ ] All three OKF chapters are `<= 200` lines in present tense with zero stale `--group duckdb` claims in `PY_TEST_GROUPS`.
- [ ] `make review-check` exits `0`.

[^okf-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix and conformance OKF chapter.
[^okf-bench]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmarks and free-threaded Python OKF chapter.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical `Makefile` commands and workflow OKF chapter.
