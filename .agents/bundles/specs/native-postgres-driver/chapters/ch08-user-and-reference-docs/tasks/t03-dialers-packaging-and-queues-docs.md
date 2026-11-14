---
type: Task
id: "native-postgres-driver:ch08-user-and-reference-docs:t03"
flow_id: native-postgres-driver
chapter_id: ch08-user-and-reference-docs
title: "Update cloud-dialers.md, packaging-and-deployment.md, and queues-and-realtime.md"
description: "Remove legacy psycopg/asyncpg dialer examples, drop false release-CI PGO prose, and document vertebra-core[postgres] queue guard."
status: stable
state: completed
commit: 1dc528f8
last_verified_at: "2026-11-14T00:20:00Z"
last_verified_commit: 1dc528f8
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch08:t02"
files: 
  - docs/usage/cloud-dialers.md
  - docs/usage/packaging-and-deployment.md
  - docs/usage/queues-and-realtime.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - docs/usage/cloud-dialers.md
  - docs/usage/packaging-and-deployment.md
  - docs/usage/queues-and-realtime.md
estimated_loc: 160
tags:
  - docs
  - postgres
  - dialer
sources:
  - id: doc-dialers
    resource: docs/usage/cloud-dialers.md
    title: "Cloud SQL and AlloyDB dialers guide"
---

# Task `t03`: Update cloud-dialers.md, packaging-and-deployment.md, and queues-and-realtime.md

## Objective & Scope Boundary

Synchronize `cloud-dialers.md`, `packaging-and-deployment.md`, and `queues-and-realtime.md` [^doc-dialers].

## Bounded Read Manifest

- `cloud-dialers.md`, `packaging-and-deployment.md`, and `queues-and-realtime.md`.

## Contracts & Edge Cases

- Show native `PostgreSQLConfig(cloud_sql_instance=...)` / `alloydb_instance_uri=...` usage without deleted `create_psycopg_connect_func` helpers.

## Test Matrix

- `make docs`.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [x] Zero references to deleted `psycopg`/`asyncpg` dialer helpers remain in `docs/`.
- [x] Zero regressions in `ch08-user-and-reference-docs` fast verification gate.

[^doc-dialers]: `docs/usage/cloud-dialers.md` — Cloud SQL and AlloyDB dialers guide.
