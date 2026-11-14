---
type: Task
id: "native-postgres-driver:ch09-data-packaging-okf-and-routing-sync:t01"
flow_id: native-postgres-driver
chapter_id: ch09-data-packaging-okf-and-routing-sync
title: "Trim database-drivers-and-dialers.md (<= 200L) and finalize postgres-native-driver.md, postgres-limitations.md, and database-driver-limitations.md"
description: "Update database-drivers-and-dialers.md (<= 200L), postgres-native-driver.md, postgres-limitations.md, and database-driver-limitations.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch08:t04"
files: 
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/postgres-native-driver.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/postgres-native-driver.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
estimated_loc: 220
tags:
  - docs
  - okf
  - postgres
sources:
  - id: drivers-okf
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers overview"
  - id: pg-native-okf
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "PostgreSQL native extension chapter"
---

# Task `t01`: Trim database-drivers-and-dialers.md (<= 200L) and finalize postgres-native-driver.md, postgres-limitations.md, and database-driver-limitations.md

## Objective & Scope Boundary

Trim `database-drivers-and-dialers.md` (`260L -> <= 200L`), add `ReplicationSupervisor`/`AsyncPostgresReplication`/`PostgresChangeSource`/native drivers to `postgres-native-driver.md`, and align `postgres-limitations.md` and `database-driver-limitations.md` with permanent runtime boundaries [^drivers-okf] [^pg-native-okf].

## Bounded Read Manifest

- `database-drivers-and-dialers.md`, `postgres-native-driver.md`, `postgres-limitations.md`, and `database-driver-limitations.md`.

## Contracts & Edge Cases

- All 4 files must be `<= 200` lines with `<= 40` truth-block lines and multi-line YAML `sources:`.

## Test Matrix

- `make review-check` + line-count verification.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] All 4 data OKF chapters are `<= 200` lines and pass `make review-check`.
- [ ] Zero regressions in `ch09-data-packaging-okf-and-routing-sync` fast verification gate.

[^drivers-okf]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers overview.
[^pg-native-okf]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — PostgreSQL native extension chapter.
