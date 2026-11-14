---
type: Task
id: native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t03
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Enforce _sqlite Companion Wheel Exclusions Across All Sibling Extensions
description: Exclude "**/py/vertebra/_sqlite.pyi" from all 9 sibling extension pyproject.toml manifests and register sqlite in test_optional_driver_wheels.py.
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
- src/rs/vertebra-oracle/pyproject.toml
- src/py/tests/unit/db/test_optional_driver_wheels.py
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

Add `"**/py/vertebra/_sqlite.pyi"` to `[tool.maturin].exclude` across all 9 sibling PyO3 extension manifests (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) and expand `test_optional_driver_wheels.py` to enforce full reciprocal `.pyi` isolation across all extensions plus `[project.optional-dependencies].sqlite` per [`../spec.md` §2](../spec.md) [^duckdb-pyproj] [^wheel-tests].

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — Canonical `[tool.maturin].exclude` ordering precedent.
- `src/rs/vertebra-bundler/pyproject.toml` — Sibling manifest pattern.
- `src/rs/vertebra-duckdb/pyproject.toml` — Sibling manifest pattern (mirrored identically to `mysql`, `postgres`, `spanner`, `storage`).
- `src/rs/vertebra-mssql/pyproject.toml` — Sibling manifest pattern.
- `src/rs/vertebra-oracle/pyproject.toml` — Sibling manifest pattern.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and `[project.optional-dependencies]` checks.

## Contracts & Edge Cases

- Insert `"**/py/vertebra/_sqlite.pyi"` in sorted order inside `[tool.maturin].exclude` across all 9 sibling extension `pyproject.toml` files without altering each crate's own included stub.
- Expand `SIBLING_WHEEL_MANIFESTS` in `test_optional_driver_wheels.py` to cover `vertebra-sqlite` (`_sqlite.pyi`), `vertebra-oracle` (`_oracle.pyi`), and `vertebra-postgres` (`_postgres.pyi`) reciprocally, plus assert `[project.optional-dependencies].sqlite == [f"vertebra-sqlite=={version}"]`.
- **Edge Case**: Every modified TOML manifest validates cleanly with `tomllib`, and zero sibling wheels leak `_sqlite.pyi`.

## Test Matrix

- `@pytest.mark.parametrize` over all sibling `pyproject.toml` manifests asserts reciprocal `include` and `exclude` completeness.
- `make sqlite-wheel-check` verifies the built `vertebra_sqlite-*.whl` contains only `_sqlite.*` + `vertebra/_sqlite.pyi`.

## Verification

```bash
make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] All 9 sibling `pyproject.toml` manifests exclude `"**/py/vertebra/_sqlite.pyi"`.
- [ ] `make sqlite-wheel-check` and `test_optional_driver_wheels.py` pass with full reciprocal `.pyi` coverage.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
