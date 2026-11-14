---
type: Task
id: "native-postgres-driver:ch09-data-packaging-okf-and-routing-sync:t04"
flow_id: native-postgres-driver
chapter_id: ch09-data-packaging-okf-and-routing-sync
title: "Synchronize adr-004 and verify review-core areas.toml routing"
description: "Update adr-004-optional-driver-wheels-and-shm-ownership.md and verify areas.toml routes all vertebra-postgres and adapter files cleanly."
status: stable
state: completed
commit: 04c0f1ff
last_verified_at: "2026-11-14T03:58:57Z"
last_verified_commit: d63f9ade
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch09:t03"
files: 
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/review-core/references/areas.toml
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/review-core/references/areas.toml
estimated_loc: 95
tags:
  - docs
  - okf
  - review
sources:
  - id: areas-toml
    resource: .agents/skills/review-core/references/areas.toml
    title: "Review areas manifest"
---

# Task `t04`: Synchronize adr-004 and verify review-core areas.toml routing

## Objective & Scope Boundary

Finalize `adr-004` (`<= 200L`) and verify `areas.toml` routing across `vertebra-postgres` benches/tests and Python adapter files [^areas-toml].

## Bounded Read Manifest

- `adr-004-optional-driver-wheels-and-shm-ownership.md` and `areas.toml`.

## Contracts & Edge Cases

- `make review-check` and `make review-route BASE=18baadcd` both exit `0` with zero unowned files.

## Test Matrix

- `make review-check`, `make review-route BASE=18baadcd`, and `pytest src/py/tests/unit/test_review_setup.py`.

## Verification

```bash
make review-check && make review-route BASE=18baadcd && uv run --no-sync pytest src/py/tests/unit/test_review_setup.py -q
```

## Acceptance Criteria

- [x] `make review-check` and `make review-route` pass with `0` issues.
- [x] Zero regressions in `ch09-data-packaging-okf-and-routing-sync` fast verification gate.

[^areas-toml]: `.agents/skills/review-core/references/areas.toml` — Review areas manifest.
