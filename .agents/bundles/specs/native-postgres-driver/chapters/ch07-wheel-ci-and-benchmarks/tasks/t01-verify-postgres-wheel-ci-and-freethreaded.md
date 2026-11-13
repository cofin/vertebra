---
type: Task
id: "native-postgres-driver:ch07-wheel-ci-and-benchmarks:t01"
flow_id: native-postgres-driver
chapter_id: ch07-wheel-ci-and-benchmarks
title: "Add verify_postgres_wheel.py, CI/release lanes, and 3.14t/3.15t pool tests"
description: "Implement two-probe verify_postgres_wheel.py, 14-class release smoke checks, zero-skip CI PG14-18 lanes, and 8-thread free-threaded pool tests."
status: stable
state: completed
commit: 480840a3
last_verified_at: "2026-11-13T22:48:50Z"
last_verified_commit: 480840a3
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch06:t03"
files: 
  - tools/scripts/_wheel_verify.py
  - tools/scripts/verify_postgres_wheel.py
  - tools/scripts/verify_oracle_wheel.py
  - tools/scripts/verify_storage_wheel.py
  - src/rs/vertebra-postgres/pyproject.toml
  - src/py/vertebra/db/dialer/cloudsql.py
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
tests: 
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
  - src/py/tests/unit/db/test_cloud_dialers.py
  - src/py/tests/integration/db/test_driver_conformance.py
read_set:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - src/rs/vertebra-postgres/pyproject.toml
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
  - src/py/vertebra/_postgres.pyi
estimated_loc: 245
tags:
  - postgres
  - wheel
  - ci
  - free-threaded
sources:
  - id: ci-yml
    resource: .github/workflows/ci.yml
    title: "CI workflow"
  - id: release-yml
    resource: .github/workflows/release.yaml
    title: "Release workflow"
---

# Task `t01`: Add verify_postgres_wheel.py, CI/release lanes, and 3.14t/3.15t pool tests

## Objective & Scope Boundary

Create `verify_postgres_wheel.py`, wire PG14–18 zero-skip CI + 14-class release smoke checks, and add `_postgres` immutable-type / 8-thread pool tests in `test_freethreaded_gil.py` [^ci-yml] [^release-yml].

## Bounded Read Manifest

- `ci.yml`, `release.yaml`, `test_freethreaded_gil.py`, and `_postgres.pyi`.

## Contracts & Edge Cases

- Verify all 14 `_postgres` classes reject `setattr` (`Py_TPFLAGS_IMMUTABLETYPE`) and sustain 8 concurrent worker threads on `3.14t`/`3.15t`.
- Align `src/rs/vertebra-postgres/pyproject.toml` `maturin.exclude`/`include` uniformly with sibling extension manifests (`vertebra-oracle`) and remove the legacy `if pkg_slug == "postgres"` special case from `test_packaging_and_pgo.py::test_optional_extension_pyproject_isolation_and_bundler_extra`.
- Fix `cloudsql.py` `ensure_module("vertebra._vertebra", package="vertebra-core")` so `test_optional_dependency_literals_name_declared_extras` passes with zero undeclared `extra="gcp"`.

## Test Matrix

- `test_freethreaded_gil.py` `_postgres` class immutability and concurrent client/pool tests, plus `test_packaging_and_pgo.py -k optional_extension_pyproject_isolation`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/build/test_packaging_and_pgo.py -k "freethreaded or optional_extension_pyproject_isolation" -q && uv run --no-sync ruff check tools/scripts/verify_postgres_wheel.py && uv run --no-sync mypy tools/scripts/verify_postgres_wheel.py
```

## Acceptance Criteria

- [x] `test_freethreaded_gil.py`, Ruff, and Mypy pass cleanly.
- [x] Zero regressions in `ch07-wheel-ci-and-benchmarks` fast verification gate.

[^ci-yml]: `.github/workflows/ci.yml` — CI workflow.
[^release-yml]: `.github/workflows/release.yaml` — Release workflow.
