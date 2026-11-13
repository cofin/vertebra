---
type: Task
id: "native-postgres-driver:ch09-data-packaging-okf-and-routing-sync:t02"
flow_id: native-postgres-driver
chapter_id: ch09-data-packaging-okf-and-routing-sync
title: "Synchronize query-pipeline-cdc-and-tenancy.md and knowledge indexes"
description: "Update query-pipeline-cdc-and-tenancy.md for $N PostgreSQL SQLSpec binds and PostgresChangeSource and sync knowledge/domains indexes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch09:t01"
files: 
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
estimated_loc: 135
tags:
  - docs
  - okf
  - postgres
  - cdc
sources:
  - id: cdc-okf
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: "Pipelines, CDC, and tenancy OKF chapter"
---

# Task `t02`: Synchronize query-pipeline-cdc-and-tenancy.md and knowledge indexes

## Objective & Scope Boundary

Update `query-pipeline-cdc-and-tenancy.md` (`<= 200L`) and frontmatter-free `knowledge/index.md` + `domains/index.md` for completed `PostgresChangeSource` and `$N` binds [^cdc-okf].

## Bounded Read Manifest

- `query-pipeline-cdc-and-tenancy.md`, `knowledge/index.md`, and `domains/index.md`.

## Contracts & Edge Cases

- Zero frontmatter in `index.md` files; `query-pipeline-cdc-and-tenancy.md` stays `<= 200` lines.

## Test Matrix

- `make review-check`.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] CDC/tenancy chapter and indexes reflect permanent PostgreSQL state cleanly.
- [ ] Zero regressions in `ch09-data-packaging-okf-and-routing-sync` fast verification gate.

[^cdc-okf]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Pipelines, CDC, and tenancy OKF chapter.
