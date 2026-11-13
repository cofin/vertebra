---
type: Task
id: "native-sqlite-driver:ch02:t02"
flow_id: native-sqlite-driver
chapter_id: ch02-wheel-isolation-and-core-config
title: "Exclude _sqlite.pyi from MSSQL, MySQL, and Oracle Wheels"
description: "Add **/py/vertebra/_sqlite.pyi to [tool.maturin].exclude in mssql, mysql, and oracle pyproject.toml files."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch02:t01"
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
  - sqlite
  - wheel
  - packaging
sources:
  - id: duckdb-pyproj
    resource: src/rs/vertebra-duckdb/pyproject.toml
    title: "Maturin standalone wheel isolation manifest precedent"
---

# Task `t02`: Exclude _sqlite.pyi from MSSQL, MySQL, and Oracle Wheels

## Objective & Scope Boundary

Add `"**/py/vertebra/_sqlite.pyi"` to `[tool.maturin].exclude` in `mssql`, `mysql`, and `oracle` per [`../spec.md` §2](../spec.md) [^duckdb-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-mssql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-mysql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-oracle/pyproject.toml` — Maturin exclude patterns.

## Contracts & Edge Cases

- Preserve existing `include` and `exclude` entries while appending `_sqlite.pyi`.
- **Edge Case**: Ensure TOML syntax validates cleanly via `tomllib`.

## Test Matrix

- Verified by `test_optional_driver_wheels.py` in `t03`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `mssql`, `mysql`, and `oracle` manifests exclude `_sqlite.pyi`.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
