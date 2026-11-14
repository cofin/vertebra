---
type: Task
id: "native-postgres-driver:ch08-user-and-reference-docs:t01"
flow_id: native-postgres-driver
chapter_id: ch08-user-and-reference-docs
title: "Update databases.md, analytics-and-cdc.md, and database-capabilities.md"
description: "Document vertebra-core[postgres], $N binds, single-Sync pipeline(), Pg* types, PostgresChangeSource, and PostgresEmbeddingPushdown."
status: stable
state: completed
commit: a2fff702
last_verified_at: "2026-11-14T00:42:00Z"
last_verified_commit: a2fff702
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch07:t04"
files: 
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/database-capabilities.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/database-capabilities.md
estimated_loc: 195
tags:
  - docs
  - postgres
sources:
  - id: doc-db
    resource: docs/usage/databases.md
    title: "Database usage guide"
---

# Task `t01`: Update databases.md, analytics-and-cdc.md, and database-capabilities.md

## Objective & Scope Boundary

Synchronize `databases.md`, `analytics-and-cdc.md`, and `database-capabilities.md` for native PostgreSQL [^doc-db].

## Bounded Read Manifest

- `databases.md`, `analytics-and-cdc.md`, and `database-capabilities.md`.

## Contracts & Edge Cases

- Use linked `uv`/`pip`/`pdm`/`poetry` tabs, concise `msgspec.Struct` examples, and zero external library comparisons.

## Test Matrix

- `make docs` offline Zensical build + artifact check.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [x] `make docs` passes with updated PostgreSQL driver, CDC, and capability tables.
- [x] Zero regressions in `ch08-user-and-reference-docs` fast verification gate.

[^doc-db]: `docs/usage/databases.md` — Database usage guide.
