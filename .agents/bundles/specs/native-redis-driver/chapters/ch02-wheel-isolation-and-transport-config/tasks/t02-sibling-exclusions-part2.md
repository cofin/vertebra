---
type: Task
id: "native-redis-driver:ch02:t02"
flow_id: native-redis-driver
chapter_id: ch02-wheel-isolation-and-transport-config
title: "Exclude _redis.pyi from MSSQL, MySQL, and Oracle Wheels"
description: "Add **/py/vertebra/_redis.pyi to [tool.maturin].exclude in mssql, mysql, and oracle pyproject.toml files."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch02:t01"
files:
  - src/rs/vertebra-mssql/pyproject.toml
  - src/rs/vertebra-mysql/pyproject.toml
  - src/rs/vertebra-oracle/pyproject.toml
tests:
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - src/rs/vertebra-mssql/pyproject.toml
  - src/rs/vertebra-mysql/pyproject.toml
  - src/rs/vertebra-oracle/pyproject.toml
estimated_loc: 20
tags:
  - redis
  - wheel
  - packaging
sources:
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "Maturin standalone wheel isolation precedent"
---

# Task `t02`: Exclude _redis.pyi from MSSQL, MySQL, and Oracle Wheels

## Objective & Scope Boundary

Add `"**/py/vertebra/_redis.pyi"` to `[tool.maturin].exclude` in `mssql`, `mysql`, and `oracle` per [`../spec.md` §2](../spec.md) [^pg-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-mssql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-mysql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-oracle/pyproject.toml` — Maturin exclude patterns.

## Contracts & Edge Cases

- Append `"**/py/vertebra/_redis.pyi"` to `[tool.maturin].exclude`.
- **Edge Case**: Preserve existing sibling exclusions.

## Test Matrix

- Verified by `test_optional_driver_wheels.py` in `t03`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `mssql`, `mysql`, and `oracle` exclude `_redis.pyi`.
- [ ] Zero regressions in the chapter fast verification gate.

[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
