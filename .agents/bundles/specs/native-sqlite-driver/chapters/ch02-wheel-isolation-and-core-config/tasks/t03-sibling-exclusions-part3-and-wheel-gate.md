---
type: Task
id: "native-sqlite-driver:ch02:t03"
flow_id: native-sqlite-driver
chapter_id: ch02-wheel-isolation-and-core-config
title: "Exclude _sqlite.pyi from Postgres, Spanner, and Storage and Expand Wheel Manifest Gate"
description: "Exclude _sqlite.pyi in postgres, spanner, and storage and enforce oracle/postgres/sqlite wheel isolation in test_optional_driver_wheels.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch02:t02"
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
  - sqlite
  - wheel
  - tests
sources:
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: "Sibling PyO3 wheel isolation test gate"
  - id: duckdb-pyproj
    resource: src/rs/vertebra-duckdb/pyproject.toml
    title: "Maturin standalone wheel isolation manifest precedent"
---

# Task `t03`: Exclude _sqlite.pyi from Postgres, Spanner, and Storage and Expand Wheel Manifest Gate

## Objective & Scope Boundary

Exclude `_sqlite.pyi` in `postgres`, `spanner`, and `storage`, and expand `test_optional_driver_wheels.py` to assert full reciprocal `.pyi` isolation across `oracle`, `postgres`, and `sqlite` per [`../spec.md` §2](../spec.md) [^wheel-tests] [^duckdb-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-postgres/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-spanner/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-storage/pyproject.toml` — Maturin exclude patterns.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs`.

## Contracts & Edge Cases

- Update `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs` in `test_optional_driver_wheels.py` to cover `_oracle.pyi`, `_postgres.pyi`, and `_sqlite.pyi` plus `[project.optional-dependencies].sqlite`.
- **Edge Case**: Every sibling wheel must exclude every other sibling's `.pyi` stub with zero missing pairs.

## Test Matrix

- `@pytest.mark.parametrize` over all sibling `pyproject.toml` manifests asserts `include` and reciprocal `exclude` completeness.

## Verification

```bash
make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `make sqlite-wheel-check` and `test_optional_driver_wheels.py` pass.
- [ ] Zero regressions in the chapter fast verification gate.

[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
