---
type: Task
id: "driver-conformance-ci:ch03:t02"
flow_id: driver-conformance-ci
chapter_id: ch03-make-lanes-bench-isolation-and-ci
title: "Add test-db-conformance matrix and conformance-oracle jobs to ci.yml"
description: "Wire live DuckDB, MySQL, MariaDB, and SQL Server matrix rows plus a non-blocking Oracle 23ai job in .github/workflows/ci.yml."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_revision: 4
plan_commit: 37972f2a
depends_on:
  - "driver-conformance-ci:ch03:t01"
files:
  - .github/workflows/ci.yml
tests:
  - src/py/tests/unit/test_bench_infrastructure.py
read_set:
  - .github/workflows/ci.yml
  - Makefile
  - src/py/tests/unit/test_bench_infrastructure.py
estimated_loc: 150
tags:
  - ci
  - github-actions
  - conformance
sources:
  - id: ci-yml
    resource: .github/workflows/ci.yml
    title: "GitHub Actions CI workflow definition"
  - id: bench-infra
    resource: src/py/tests/unit/test_bench_infrastructure.py
    title: "CI and Makefile structure assertions"
---

# Task `t02`: Add `test-db-conformance` Matrix and `conformance-oracle` Jobs to `ci.yml`

## Objective & Scope Boundary

Add the blocking `test-db-conformance` matrix job (`duckdb`, `mysql`, `mariadb`, `mssql`) and non-blocking `conformance-oracle` (`oracle` `23ai`, `continue-on-error: true`) job to `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^ci-yml] [^bench-infra]. Out of scope: modifying `test-python` or PostgreSQL CI jobs added by `native-postgres-driver`.

## Bounded Read Manifest

- `.github/workflows/ci.yml` — Existing `changes`, `lint`, `test-python`, and PostgreSQL service jobs.
- `Makefile` and `src/py/tests/unit/test_bench_infrastructure.py` — `make db-conformance` CLI contract and YAML workflow tests.

## Contracts & Edge Cases

- `test-db-conformance`: `needs: [changes, lint]`, `if: needs.changes.outputs.python == 'true' || needs.changes.outputs.rust == 'true' || needs.changes.outputs.ci == 'true'`, `fail-fast: false`, `timeout-minutes: 45`, 10 matrix rows (`duckdb` on `3.12`/`3.14t`; `mysql` `8.0`/`8.4`/`9.7` on `3.12` + `8.4` on `3.14t`; `mariadb` `11.4`/`11.8` on `3.12`; `mssql` `2022`/`2025` on `3.12`), running `make db-conformance ENGINE=${{ matrix.engine }} VERSION=${{ matrix.version }}`.
- `conformance-oracle`: same `needs`/`if` condition, `continue-on-error: true`, `timeout-minutes: 45`, Python `3.12`, running `make db-conformance ENGINE=oracle VERSION=23ai`.
- Free-threaded `3.14t` rows install `uv python install 3.14t` and sync without PyPI `--group duckdb`.

## Test Matrix

- Parsed YAML assertions in `test_bench_infrastructure.py` verifying `test-db-conformance` matrix entries, `fail-fast: false`, `make db-conformance` invocation, and `conformance-oracle` `continue-on-error: true`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/test_bench_infrastructure.py -q && uv run --no-sync ruff check src/py/tests/unit/test_bench_infrastructure.py && make review-check
```

## Acceptance Criteria

- [ ] `.github/workflows/ci.yml` parses cleanly and defines both `test-db-conformance` and `conformance-oracle` invoking `make db-conformance`.
- [ ] `test_bench_infrastructure.py` and `make review-check` exit `0`.

[^ci-yml]: `.github/workflows/ci.yml` — GitHub Actions CI workflow definition.
[^bench-infra]: `src/py/tests/unit/test_bench_infrastructure.py` — CI and `Makefile` structure assertions.
