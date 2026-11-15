---
type: Task
id: native-valkey-driver:ch01-wheel-scaffold-and-isolation:t03
flow_id: native-valkey-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Exclude _valkey.pyi from Sibling Wheels and Add Wheel Isolation Gate
description: Add "**/py/vertebra/_valkey.pyi" to every sibling src/rs/vertebra-*/pyproject.toml exclusion list and register valkey in test_optional_driver_wheels.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch01-wheel-scaffold-and-isolation:t02
files:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-duckdb/pyproject.toml
  - src/rs/vertebra-mssql/pyproject.toml
  - src/rs/vertebra-mysql/pyproject.toml
  - src/rs/vertebra-oracle/pyproject.toml
  - src/rs/vertebra-postgres/pyproject.toml
  - src/rs/vertebra-redis/pyproject.toml
  - src/rs/vertebra-sqlite/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
  - src/rs/vertebra-valkey/pyproject.toml
tests:
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-postgres/pyproject.toml
  - src/py/tests/unit/db/test_optional_driver_wheels.py
estimated_loc: 140
tags:
  - packaging
  - wheel
  - isolation
  - valkey
sources:
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: Maturin standalone wheel isolation precedent
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: Sibling PyO3 wheel isolation test gate
verification_strategy: implementation_first
---

# Task `t03`: Exclude _valkey.pyi from Sibling Wheels and Add Wheel Isolation Gate

## Objective & Scope Boundary

Add `"**/py/vertebra/_valkey.pyi"` in sorted order to `[tool.maturin].exclude` across all sibling `src/rs/vertebra-*/pyproject.toml` manifests (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`, plus `redis` and `sqlite`), exclude all sibling `.pyi` files from `src/rs/vertebra-valkey/pyproject.toml`, and expand `test_optional_driver_wheels.py` to enforce `vertebra-valkey`, `_valkey.pyi`, and `[valkey]` extra per [`../spec.md` §2](../spec.md) [^pg-pyproj] [^wheel-tests].

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — Sibling `[tool.maturin].exclude` ordering precedent.
- `src/rs/vertebra-postgres/pyproject.toml` — Canonical wheel isolation manifest.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — `SIBLING_WHEEL_MANIFESTS` and `[project.optional-dependencies]` checks.

## Contracts & Edge Cases

- `vertebra-valkey` wheel contains `== 1` `.pyi` stub (`vertebra/_valkey.pyi`), `== 1` native library (`_valkey.*`), zero `.py` source files, and compressed size `< 15 MiB`.
- Add `vertebra-valkey` and `_valkey.pyi` to `SIBLING_WHEEL_MANIFESTS` and `sibling_stubs` in `test_optional_driver_wheels.py`, asserting `[project.optional-dependencies].valkey` resolves to `vertebra-valkey=={version}` with zero `.all` extra.
- **Edge Case**: Update `src/rs/vertebra-redis/pyproject.toml` and `src/rs/vertebra-sqlite/pyproject.toml` reciprocally so every sibling wheel stays pairwise disjoint.

## Test Matrix

- `make valkey-wheel-check` and `pytest src/py/tests/unit/db/test_optional_driver_wheels.py` verify pairwise disjoint `.pyi` isolation.

## Verification

```bash
make valkey-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `make valkey-wheel-check` and `test_optional_driver_wheels.py` pass cleanly.
- [ ] Every sibling wheel excludes `"**/py/vertebra/_valkey.pyi"`.
- [ ] Zero regressions in the fast verification gate.

[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
