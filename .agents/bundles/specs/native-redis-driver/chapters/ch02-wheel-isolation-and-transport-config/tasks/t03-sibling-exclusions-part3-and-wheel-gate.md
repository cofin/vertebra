---
type: Task
id: "native-redis-driver:ch02:t03"
flow_id: native-redis-driver
chapter_id: ch02-wheel-isolation-and-transport-config
title: "Exclude _redis.pyi from Remaining Sibling Wheels and Expand Wheel Gate"
description: "Exclude _redis.pyi in postgres, spanner, and storage and enforce vertebra-redis + [redis]/[valkey] in test_optional_driver_wheels.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch02:t02"
files:
  - src/rs/vertebra-postgres/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
tests:
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - src/rs/vertebra-postgres/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
  - src/py/tests/unit/db/test_optional_driver_wheels.py
estimated_loc: 95
tags:
  - redis
  - valkey
  - wheel
  - tests
sources:
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: "Sibling PyO3 wheel isolation test gate"
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "Maturin standalone wheel isolation precedent"
---

# Task `t03`: Exclude _redis.pyi from Remaining Sibling Wheels and Expand Wheel Gate

## Objective & Scope Boundary

Exclude `_redis.pyi` in `postgres`, `spanner`, and `storage`, and update `test_optional_driver_wheels.py` to enforce `vertebra-redis`, `_redis.pyi`, and `[redis]`/`[valkey]` extras per [`../spec.md` §2](../spec.md) [^wheel-tests] [^pg-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-postgres/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-spanner/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-storage/pyproject.toml` — Maturin exclude patterns.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and extra assertions.

## Contracts & Edge Cases

- Add `vertebra-redis` and `_redis.pyi` to `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs` and assert both `redis` and `valkey` extras map to `vertebra-redis`.
- **Edge Case**: If `vertebra-sqlite` is already present on branch/main, reconcile reciprocal `_sqlite.pyi` <-> `_redis.pyi` exclusions so all sibling wheels stay pairwise disjoint.

## Test Matrix

- `@pytest.mark.parametrize` verifies every sibling wheel manifest and `make redis-wheel-check`.

## Verification

```bash
make redis-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `make redis-wheel-check` and `test_optional_driver_wheels.py` pass.
- [ ] Zero regressions in the chapter fast verification gate.

[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
