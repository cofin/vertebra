---
type: Task
id: "native-postgres-driver:ch08-user-and-reference-docs:t02"
flow_id: native-postgres-driver
chapter_id: ch08-user-and-reference-docs
title: "Update native-stubs.md, docs/reference/index.md, and installation.md"
description: "Add _mysql, _mssql, and _postgres to native-stubs.md within H2+H3/HTML budgets and sync reference/index.md and installation.md."
status: stable
state: completed
commit: 30c34daf
last_verified_at: "2026-11-13T23:01:00Z"
last_verified_commit: 30c34daf
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch08:t01"
files: 
  - docs/reference/native-stubs.md
  - docs/reference/index.md
  - docs/getting-started/installation.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - docs/reference/native-stubs.md
  - docs/reference/index.md
  - docs/getting-started/installation.md
estimated_loc: 165
tags:
  - docs
  - postgres
  - pyo3
sources:
  - id: doc-stubs
    resource: docs/reference/native-stubs.md
    title: "Native PyO3 stubs reference"
---

# Task `t02`: Update native-stubs.md, docs/reference/index.md, and installation.md

## Objective & Scope Boundary

Synchronize `native-stubs.md`, `docs/reference/index.md`, and `installation.md` for all 10 PyO3 extensions [^doc-stubs].

## Bounded Read Manifest

- `native-stubs.md`, `docs/reference/index.md`, and `installation.md`.

## Contracts & Edge Cases

- Keep `native-stubs.md` `H2+H3 <= 14` and rendered HTML `<= 120,000` bytes; format installation extras with `pymdownx.tabbed`.

## Test Matrix

- `make docs` (`verify_docs_artifacts.py`).

## Verification

```bash
make docs
```

## Acceptance Criteria

- [x] `make docs` passes heading and HTML byte-budget gates.
- [x] Zero regressions in `ch08-user-and-reference-docs` fast verification gate.

[^doc-stubs]: `docs/reference/native-stubs.md` — Native PyO3 stubs reference.
