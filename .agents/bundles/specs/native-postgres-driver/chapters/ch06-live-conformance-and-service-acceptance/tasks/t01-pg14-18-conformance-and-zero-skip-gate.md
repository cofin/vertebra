---
type: Task
id: "native-postgres-driver:ch06-live-conformance-and-service-acceptance:t01"
flow_id: native-postgres-driver
chapter_id: ch06-live-conformance-and-service-acceptance
title: "Enable PG14-18 conformance matrix and VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS"
description: "Set supports_pipeline=True and pending_flow_id=None for postgres in test_driver_conformance.py and enforce zero-skip terminal gating."
status: stable
state: completed
commit: 969540de
last_verified_at: "2026-11-13T21:00:57Z"
last_verified_commit: 969540de
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch05:t03"
files: 
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_driver_conformance.py
tests: 
  - src/py/tests/unit/db/test_postgres_zero_skip_gate.py
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 215
tags:
  - python
  - postgres
  - conformance
sources:
  - id: conformance-py
    resource: "src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS"
    title: "Cross-engine conformance specs"
---

# Task `t01`: Enable PG14-18 conformance matrix and VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS

## Objective & Scope Boundary

Enable PostgreSQL `14`–`18` (`supports_pipeline=True`, `pending_flow_id=None`) and add the `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` hook [^conformance-py].

## Bounded Read Manifest

- `integration/conftest.py` and `test_driver_conformance.py`.

## Contracts & Edge Cases

- Fail the test session when `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` and either `0` PostgreSQL tests ran or any PostgreSQL test skipped.

## Test Matrix

- Unit tests in `test_postgres_zero_skip_gate.py` exercising 0-collected, skipped, and all-passed terminal summaries.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_zero_skip_gate.py -q
```

## Acceptance Criteria

- [x] Zero-skip hook fails on skipped/missing PG cases and passes on clean execution.
- [x] Zero regressions in `ch06-live-conformance-and-service-acceptance` fast verification gate.

[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine conformance specs.
