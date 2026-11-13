---
type: Task
id: "native-postgres-driver:ch08-user-and-reference-docs:t04"
flow_id: native-postgres-driver
chapter_id: ch08-user-and-reference-docs
title: "Fix docs/usage/extensions.md, README.md, and AGENTS.md"
description: "Update +vtb dialect wheel extras in extensions.md, refresh README.md crate/wheel overview, and keep AGENTS.md <= 100 lines."
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
  - "native-postgres-driver:ch08:t03"
files: 
  - docs/usage/extensions.md
  - docs/usage/index.md
  - README.md
  - AGENTS.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - docs/usage/extensions.md
  - README.md
  - AGENTS.md
estimated_loc: 145
tags:
  - docs
  - postgres
sources:
  - id: doc-ext
    resource: docs/usage/extensions.md
    title: "Extensions usage guide"
  - id: readme
    resource: README.md
    title: "Repository README"
---

# Task `t04`: Fix docs/usage/extensions.md, README.md, and AGENTS.md

## Objective & Scope Boundary

Fix stale `oracle+vtb`/`mssql+vtb`/`duckdb+vtb`/`postgresql+vtb` wheel extra notes in `extensions.md:101`, update `README.md`, and finalize `AGENTS.md` (`<= 100L`) [^doc-ext] [^readme].

## Bounded Read Manifest

- `docs/usage/extensions.md`, `README.md`, and `AGENTS.md`.

## Contracts & Edge Cases

- `AGENTS.md` remains `<= 100` lines preserving `<!-- project-customization -->`.

## Test Matrix

- `make docs && make review-check`.

## Verification

```bash
make docs && make review-check
```

## Acceptance Criteria

- [x] `make docs` and `make review-check` both exit `0`.
- [x] Zero regressions in `ch08-user-and-reference-docs` fast verification gate.

[^doc-ext]: `docs/usage/extensions.md` — Extensions usage guide.
[^readme]: `README.md` — Repository README.
