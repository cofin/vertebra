---
type: Task
id: native-redis-driver:ch01-wheel-scaffold-and-isolation:t03
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Enforce _redis Companion Wheel Exclusions Across All Sibling Extensions
description: Exclude "**/py/vertebra/_redis.pyi" from all 9+ sibling extension pyproject.toml manifests and register redis/valkey in test_optional_driver_wheels.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch01-wheel-scaffold-and-isolation:t02
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
- src/rs/vertebra-postgres/pyproject.toml
- src/py/tests/unit/db/test_optional_driver_wheels.py
estimated_loc: 220
tags:
- redis
- wheel
- packaging
- valkey
- tests
sources:
- id: pg-pyproj
  resource: src/rs/vertebra-postgres/pyproject.toml
  title: Maturin standalone wheel isolation precedent
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
---

# Task `t03`: Enforce _redis Companion Wheel Exclusions Across All Sibling Extensions

## Objective & Scope Boundary

Add `"**/py/vertebra/_redis.pyi"` in sorted order to `[tool.maturin].exclude` across all sibling PyO3 extension `pyproject.toml` manifests (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`, plus `sqlite` if present) and expand `test_optional_driver_wheels.py` to enforce `vertebra-redis`, `_redis.pyi`, and `[redis]`/`[valkey]` extras per [`../spec.md` §2](../spec.md) [^pg-pyproj] [^wheel-tests].

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — Sibling `[tool.maturin].exclude` ordering precedent.
- `src/rs/vertebra-bundler/pyproject.toml` — Sibling manifest pattern.
- `src/rs/vertebra-duckdb/pyproject.toml` — Sibling manifest pattern (mirrored identically across `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`).
- `src/rs/vertebra-mssql/pyproject.toml` — Sibling manifest pattern.
- `src/rs/vertebra-postgres/pyproject.toml` — Canonical wheel isolation manifest.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and `[project.optional-dependencies]` checks.

## Contracts & Edge Cases

- Append `"**/py/vertebra/_redis.pyi"` in sorted order inside `[tool.maturin].exclude` without altering any crate's own included stub.
- Add `vertebra-redis` and `_redis.pyi` to `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs` in `test_optional_driver_wheels.py`, asserting both `[project.optional-dependencies].redis` and `[project.optional-dependencies].valkey` resolve to `vertebra-redis=={version}` with zero `.all` extra.
- **Edge Case**: If `vertebra-sqlite` already landed, reconcile reciprocal `_sqlite.pyi` <-> `_redis.pyi` exclusions across both manifests so every sibling wheel stays pairwise disjoint.
- **Edge Case**: Keep every modified TOML file valid under `tomllib`.

## Test Matrix

- `@pytest.mark.parametrize` verifies every sibling wheel manifest is pairwise disjoint on `.pyi` files.
- `make redis-wheel-check` builds `vertebra_redis-*.whl` cleanly with zero foreign stubs or Python modules.

## Verification

```bash
make redis-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] Every sibling PyO3 extension `pyproject.toml` excludes `"**/py/vertebra/_redis.pyi"`.
- [ ] `make redis-wheel-check` and `test_optional_driver_wheels.py` pass cleanly.
- [ ] Zero regressions in the fast verification gate.

[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
