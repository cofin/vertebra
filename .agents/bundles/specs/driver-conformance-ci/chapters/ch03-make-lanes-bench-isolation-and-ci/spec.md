---
type: Spec
flow_id: driver-conformance-ci
chapter_id: ch03-make-lanes-bench-isolation-and-ci
title: "Chapter 03: Make Conformance Lanes, Bench-Only DuckDB Isolation, and CI Matrix"
description: "Update Makefile in a single pass with make db-conformance, fail-loud oracle-test, and bench-only PyPI duckdb isolation, and add test-db-conformance and conformance-oracle jobs in .github/workflows/ci.yml."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - makefile
  - ci
  - conformance
  - duckdb
  - benchmarks
sources:
  - id: makefile
    resource: Makefile
    title: "PY_TEST_GROUPS, install, upgrade, duckdb-test, mysql-test, mssql-test, and oracle-test targets"
  - id: pyproject
    resource: pyproject.toml
    title: "Dependency groups (duckdb vs bench) in pyproject.toml"
  - id: ci-yml
    resource: .github/workflows/ci.yml
    title: "GitHub Actions CI workflow jobs and path-filter conditions"
  - id: bench-infra-test
    resource: src/py/tests/unit/test_bench_infrastructure.py
    title: "Makefile and dependency-group infrastructure verification tests"
---

# Specification: Chapter 03 — Make Conformance Lanes, Bench-Only DuckDB Isolation, and CI Matrix

<!-- truth: start -->
- **Single-Pass `Makefile` & `pyproject.toml` Hygiene (`t01`)**: `Makefile` adds `db-conformance` (`ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]`), removes the `test_freethreaded_gil.py` fallback from `oracle-test`, and drops `--group duckdb` from `PY_TEST_GROUPS`, `make install`, and `make upgrade` so PyPI `duckdb` is installed exclusively via `--group bench` [^makefile] [^pyproject].
- **Zero-Skip CI Conformance Matrix (`t02`)**: `.github/workflows/ci.yml` adds `test-db-conformance` (`fail-fast: false`, DuckDB `3.12`/`3.14t`, MySQL `8.0`/`8.4`/`9.7` + `8.4` on `3.14t`, MariaDB `11.4`/`11.8`, SQL Server `2022`/`2025`) plus a dedicated `conformance-oracle` (`23ai`, `continue-on-error: true`) job [^ci-yml] [^bench-infra-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`Makefile` currently edits `PY_TEST_GROUPS` and `install` to install PyPI `--group duckdb` during ordinary test runs and masks missing Oracle tests with a `test_freethreaded_gil.py` fallback, while `.github/workflows/ci.yml` runs zero live database lanes for DuckDB, MySQL, MariaDB, SQL Server, or Oracle [^makefile] [^ci-yml]. Consolidating all `Makefile` updates into `t01` avoids double-editing `Makefile`.

## 2. Requirements, Concrete Contracts & Architecture

- **`Makefile` & `pyproject.toml` (`t01`)**:
  - Define `.PHONY: db-conformance`: validate non-empty `ENGINE` (`exit 2` with usage message if missing), export `VERTEBRA_CONFORMANCE_DOCKER=1`, `VERTEBRA_CONFORMANCE_ENGINES=$(ENGINE)`, `VERTEBRA_CONFORMANCE_<UPPER_ENGINE>_VERSION=$(VERSION)` when `VERSION` is non-empty, and `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` when `ENGINE=duckdb`, then run `uv run --no-sync pytest src/py/tests/integration/db -v -rs`.
  - Update `oracle-test` to run `cargo test -p vertebra-oracle`, `mypy.stubtest vertebra._oracle`, and `pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/integration/db/test_oracle_native.py` directly (zero `if [ -f ... ]` `test_freethreaded_gil.py` fallback); align `mysql-test`, `mssql-test`, and `duckdb-test` to include their integration suites.
  - Remove `--group duckdb` from `PY_TEST_GROUPS`, `make install`, and `make upgrade`; fold or retain PyPI `duckdb` strictly for benchmark runs (`--group bench` in `pyproject.toml`, preserving the `python_version < '3.14'` marker for free-threaded Python compatibility) so `make py-test` and `make coverage` never require PyPI `duckdb`.
- **`.github/workflows/ci.yml` (`t02`)**:
  - Add `test-db-conformance` (`runs-on: ubuntu-latest`, `timeout-minutes: 45`, `needs: [changes, lint]`, `fail-fast: false`) with 10 blocking matrix rows (`duckdb` on `3.12` & `3.14t`; `mysql` `8.0`, `8.4`, `9.7` on `3.12` & `8.4` on `3.14t`; `mariadb` `11.4`, `11.8` on `3.12`; `mssql` `2022`, `2025` on `3.12`) invoking `make db-conformance ENGINE=${{ matrix.engine }} VERSION=${{ matrix.version }}`.
  - Add `conformance-oracle` (`runs-on: ubuntu-latest`, `timeout-minutes: 45`, `continue-on-error: true`, `needs: [changes, lint]`) invoking `make db-conformance ENGINE=oracle VERSION=23ai` so Oracle Free container pulls/startup jitter never block unrelated PRs while recording full live Oracle pass/fail status.

## 3. Documentation & OKF Knowledge Plan

Deferred to [`ch04-docs-and-okf-sync`](../ch04-docs-and-okf-sync/spec.md) (`AGENTS.md`, `workflow.md`, `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, `database-driver-limitations.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5` `Makefile` `db-conformance` & `duckdb` isolation | `t01` | `Makefile`, `pyproject.toml` | `pytest src/py/tests/unit/test_bench_infrastructure.py` |
| `R5` CI conformance matrix & Oracle job | `t02` | `.github/workflows/ci.yml` | `pytest src/py/tests/unit/test_bench_infrastructure.py` |

## 5. Implementation Plan

- [ ] [t01-makefile-conformance-and-duckdb-bench-isolation.md](tasks/t01-makefile-conformance-and-duckdb-bench-isolation.md) — Add `make db-conformance`, remove the `oracle-test` fallback, and restrict PyPI `duckdb` to benchmarks in `Makefile` and `pyproject.toml`.
- [ ] [t02-ci-conformance-matrix.md](tasks/t02-ci-conformance-matrix.md) — Add `test-db-conformance` matrix and non-blocking `conformance-oracle` jobs in `.github/workflows/ci.yml`.

## 6. Current State & Remaining Plan Snapshot

- **Fast `< 60s` Offline Chapter Gate**: `make -n db-conformance ENGINE=mysql VERSION=8.4 && make -n oracle-test && uv run --no-sync pytest src/py/tests/unit/test_bench_infrastructure.py -q && uv run --no-sync ruff check src/py/tests/unit/test_bench_infrastructure.py && make review-check`.

[^makefile]: `Makefile` — `PY_TEST_GROUPS`, `install`, `upgrade`, `duckdb-test`, `mysql-test`, `mssql-test`, and `oracle-test` targets.
[^pyproject]: `pyproject.toml` — Dependency groups (`duckdb` vs `bench`) in `pyproject.toml`.
[^ci-yml]: `.github/workflows/ci.yml` — GitHub Actions CI workflow jobs and path-filter conditions.
[^bench-infra-test]: `src/py/tests/unit/test_bench_infrastructure.py` — `Makefile` and dependency-group infrastructure verification tests.
