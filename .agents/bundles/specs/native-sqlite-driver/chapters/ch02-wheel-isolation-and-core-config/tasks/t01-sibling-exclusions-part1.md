---
type: Task
id: "native-sqlite-driver:ch02:t01"
flow_id: native-sqlite-driver
chapter_id: ch02-wheel-isolation-and-core-config
title: "Exclude _sqlite.pyi from BigQuery, Bundler, and DuckDB Wheels"
description: "Add **/py/vertebra/_sqlite.pyi to [tool.maturin].exclude in bigquery, bundler, and duckdb pyproject.toml files."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch01:t03"
files:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-duckdb/pyproject.toml
tests:
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-duckdb/pyproject.toml
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

# Task `t01`: Exclude _sqlite.pyi from BigQuery, Bundler, and DuckDB Wheels

## Objective & Scope Boundary

Add `"**/py/vertebra/_sqlite.pyi"` to `[tool.maturin].exclude` in `bigquery`, `bundler`, and `duckdb` per [`../spec.md` §2](../spec.md) [^duckdb-pyproj].

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-bundler/pyproject.toml` — Maturin exclude patterns.
- `src/rs/vertebra-duckdb/pyproject.toml` — Maturin exclude patterns.

## Contracts & Edge Cases

- Keep exclusion globs sorted and identical in format to existing sibling `.pyi` entries.
- **Edge Case**: Do not alter `include` globs for each crate's own `.pyi` stub.

## Test Matrix

- Verified by `test_optional_driver_wheels.py` in `t03`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `bigquery`, `bundler`, and `duckdb` manifests exclude `_sqlite.pyi`.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
