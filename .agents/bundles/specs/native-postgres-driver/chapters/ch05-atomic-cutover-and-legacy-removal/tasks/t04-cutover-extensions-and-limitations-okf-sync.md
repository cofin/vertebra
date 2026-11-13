---
type: Task
id: "native-postgres-driver:ch05-atomic-cutover-and-legacy-removal:t04"
flow_id: native-postgres-driver
chapter_id: ch05-atomic-cutover-and-legacy-removal
title: "Synchronize extensions, queues, and database-driver-limitations OKF chapters"
description: "Update sqlspec-alchemy-and-dialects.md, distributed-queues-and-email.md, and database-driver-limitations.md (<= 200L each) for native PostgreSQL."
status: stable
state: completed
commit: c5289d56
last_verified_commit: cffa8feb
last_verified_at: "2026-11-13T15:43:00Z"
verification_evidence: "review_setup.py validate + pytest test_review_setup.py clean"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch05:t03"
files: 
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
estimated_loc: 140
tags:
  - docs
  - okf
  - postgres
sources:
  - id: drv-lim
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Cross-driver limitations record"
---

# Task `t04`: Synchronize extensions, queues, and database-driver-limitations OKF chapters

## Objective & Scope Boundary

Remove stale `psycopg_pool`/unwired-`_postgres` claims from `database-driver-limitations.md` and update `sqlspec-alchemy-and-dialects.md` and `distributed-queues-and-email.md` (`<= 200L` each) [^drv-lim].

## Bounded Read Manifest

- 3 target OKF chapters.

## Contracts & Edge Cases

- Record `$1..$N` numeric compilation across sync/async PostgreSQL SQLSpec and SQLAlchemy dialects, `vertebra-core[postgres]` queue guard, and native wheel status for Oracle/MSSQL/PostgreSQL.

## Test Matrix

- `make review-check` and `pytest src/py/tests/unit/test_review_setup.py`.

## Verification

```bash
make review-check && uv run --no-sync pytest src/py/tests/unit/test_review_setup.py -q
```

## Acceptance Criteria

- [x] All 3 OKF chapters stay `<= 200` lines and pass `make review-check`.
- [x] Zero regressions in `ch05-atomic-cutover-and-legacy-removal` fast verification gate.

[^drv-lim]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Cross-driver limitations record.
