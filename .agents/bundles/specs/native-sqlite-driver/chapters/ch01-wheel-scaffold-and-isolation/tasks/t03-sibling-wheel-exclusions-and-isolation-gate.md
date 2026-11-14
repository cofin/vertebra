---
type: Task
id: native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t03
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Enforce _sqlite Companion Wheel Exclusions Across All Sibling Extensions
description: Exclude _sqlite* and vertebra-sqlite from all 10 sibling extension pyproject.toml manifests and register sqlite in test_optional_driver_wheels.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 1b191388
depends_on:
- native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t02
files:
- src/rs/vertebra-bigquery/pyproject.toml
- src/rs/vertebra-bundler/pyproject.toml
- src/rs/vertebra-duckdb/pyproject.toml
- src/rs/vertebra-mssql/pyproject.toml
- src/rs/vertebra-mysql/pyproject.toml
- src/rs/vertebra-oracle/pyproject.toml
- src/rs/vertebra-postgres/pyproject.toml
- src/rs/vertebra-spanner/pyproject.toml
- src/rs/vertebra-storage/pyproject.toml
tests:
- src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
- src/rs/vertebra-bigquery/pyproject.toml
- src/rs/vertebra-bundler/pyproject.toml
- src/rs/vertebra-duckdb/pyproject.toml
- src/rs/vertebra-mssql/pyproject.toml
- src/rs/vertebra-mysql/pyproject.toml
- src/rs/vertebra-oracle/pyproject.toml
estimated_loc: 220
tags:
- sqlite
- wheel
- packaging
- tests
sources:
- id: duckdb-pyproj
  resource: src/rs/vertebra-duckdb/pyproject.toml
  title: Maturin standalone wheel isolation manifest precedent
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
---

# Task `t03`: Enforce _sqlite Companion Wheel Exclusions Across All Sibling Extensions

## Objective & Scope Boundary

Add `"**/py/vertebra/_sqlite.pyi"` to `[tool.maturin].exclude` in `bigquery`, `bundler`, and `duckdb` per [`../spec.md` §2](../spec.md) [^duckdb-pyproj].

Add `"**/py/vertebra/_sqlite.pyi"` to `[tool.maturin].exclude` in `mssql`, `mysql`, and `oracle` per [`../spec.md` §2](../spec.md) [^duckdb-pyproj].

Exclude `_sqlite.pyi` in `postgres`, `spanner`, and `storage`, and expand `test_optional_driver_wheels.py` to assert full reciprocal `.pyi` isolation across `oracle`, `postgres`, and `sqlite` per [`../spec.md` §2](../spec.md) [^wheel-tests] [^duckdb-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-bundler/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-duckdb/pyproject.toml` — Maturin exclude patterns.

- `src/rs/vertebra-mssql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-mysql/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-oracle/pyproject.toml` — Maturin exclude patterns.

- `src/rs/vertebra-postgres/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-spanner/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-storage/pyproject.toml` — Maturin exclude patterns.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs`.

## Contracts & Edge Cases

- Keep exclusion globs sorted and identical in format to existing sibling `.pyi` entries.
- **Edge Case**: Do not alter `include` globs for each crate's own `.pyi` stub.

- Preserve existing `include` and `exclude` entries while appending `_sqlite.pyi`.
- **Edge Case**: Ensure TOML syntax validates cleanly via `tomllib`.

- Update `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs` in `test_optional_driver_wheels.py` to cover `_oracle.pyi`, `_postgres.pyi`, and `_sqlite.pyi` plus `[project.optional-dependencies].sqlite`.
- **Edge Case**: Every sibling wheel must exclude every other sibling's `.pyi` stub with zero missing pairs.

## Test Matrix

- Verified by `test_optional_driver_wheels.py` in `t03`.

- `@pytest.mark.parametrize` over all sibling `pyproject.toml` manifests asserts `include` and reciprocal `exclude` completeness.

## Verification

```bash
make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `bigquery`, `bundler`, and `duckdb` manifests exclude `_sqlite.pyi`.
- [ ] `mssql`, `mysql`, and `oracle` manifests exclude `_sqlite.pyi`.
- [ ] `make sqlite-wheel-check` and `test_optional_driver_wheels.py` pass.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
