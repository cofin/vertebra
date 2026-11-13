---
type: Task
id: "driver-conformance-ci:ch03:t01"
flow_id: driver-conformance-ci
chapter_id: ch03-make-lanes-bench-isolation-and-ci
title: "Update Makefile with db-conformance, clean oracle-test, and isolate PyPI duckdb to bench"
description: "Single-pass Makefile and pyproject.toml update adding make db-conformance, dropping the oracle-test fallback, and restricting PyPI duckdb to benchmark runs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - Makefile
  - pyproject.toml
tests:
  - src/py/tests/unit/test_bench_infrastructure.py
read_set:
  - Makefile
  - pyproject.toml
  - src/py/tests/unit/test_bench_infrastructure.py
  - src/py/tests/integration/conformance_gate.py
estimated_loc: 165
tags:
  - makefile
  - duckdb
  - benchmarks
  - conformance
sources:
  - id: makefile
    resource: Makefile
    title: "Makefile PY_TEST_GROUPS, install, oracle-test, and driver test targets"
  - id: bench-infra
    resource: src/py/tests/unit/test_bench_infrastructure.py
    title: "Benchmark and Makefile target structure unit tests"
---

# Task `t01`: Update `Makefile` with `db-conformance`, Clean `oracle-test`, and Isolate PyPI `duckdb` to `bench`

## Objective & Scope Boundary

Perform a single cohesive edit to `Makefile` and `pyproject.toml` implementing `make db-conformance`, fail-loud `oracle-test` (plus `mysql-test`/`mssql-test`/`duckdb-test` integration alignment), and `bench`-only PyPI `duckdb` dependency isolation per [`../spec.md` §2](../spec.md) [^makefile] [^bench-infra]. Out of scope: `.github/workflows/ci.yml` (`t02`).

## Bounded Read Manifest

- `Makefile` and `pyproject.toml` — `PY_TEST_GROUPS`, `install`, `upgrade`, `[dependency-groups]`, and `*-test` targets.
- `src/py/tests/unit/test_bench_infrastructure.py` and `src/py/tests/integration/conformance_gate.py` — Infrastructure assertions and lane env contract.

## Contracts & Edge Cases

- `make db-conformance` without `ENGINE` exits `2` with `"ENGINE is required: duckdb, mysql, mariadb, mssql, oracle"`; with `ENGINE=mysql VERSION=8.4`, exports `VERTEBRA_CONFORMANCE_DOCKER=1`, `VERTEBRA_CONFORMANCE_ENGINES=mysql`, and `VERTEBRA_CONFORMANCE_MYSQL_VERSION=8.4`; with `ENGINE=duckdb`, exports `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` and omits empty `VERSION`.
- `oracle-test` runs `cargo test -p vertebra-oracle`, `mypy.stubtest vertebra._oracle`, and `pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/integration/db/test_oracle_native.py` with zero fallback to `test_freethreaded_gil.py`.
- Remove `--group duckdb` from `PY_TEST_GROUPS`, `make install`, and `make upgrade` in `Makefile`; ensure PyPI `duckdb` is included in `[dependency-groups].bench` (`python_version < '3.14'` marker preserved for free-threaded compatibility) and never required by `py-test` or `coverage`.

## Test Matrix

- Parameterized assertions in `test_bench_infrastructure.py` checking `make -n db-conformance` env expansion across engines/versions, `make db-conformance` missing-`ENGINE` exit code `2`, absence of `--group duckdb` in `Makefile` `install`/`PY_TEST_GROUPS`, and absence of `test_freethreaded_gil.py` inside `oracle-test`.

## Verification

```bash
make -n db-conformance ENGINE=mysql VERSION=8.4 && make -n db-conformance ENGINE=duckdb && make -n oracle-test && uv run --no-sync pytest src/py/tests/unit/test_bench_infrastructure.py -q && uv run --no-sync ruff check src/py/tests/unit/test_bench_infrastructure.py
```

## Acceptance Criteria

- [ ] `Makefile` contains zero `--group duckdb` flags outside benchmark recipes and zero fallback branches in `oracle-test`.
- [ ] `test_bench_infrastructure.py` passes cleanly.

[^makefile]: `Makefile` — `Makefile` `PY_TEST_GROUPS`, `install`, `upgrade`, `oracle-test`, and driver test targets.
[^bench-infra]: `src/py/tests/unit/test_bench_infrastructure.py` — Benchmark and `Makefile` target structure unit tests.
