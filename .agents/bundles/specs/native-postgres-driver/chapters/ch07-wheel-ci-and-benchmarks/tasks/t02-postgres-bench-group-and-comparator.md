---
type: Task
id: "native-postgres-driver:ch07-wheel-ci-and-benchmarks:t02"
flow_id: native-postgres-driver
chapter_id: ch07-wheel-ci-and-benchmarks
title: "Add postgres-bench dependency group and tools/benchmarks/postgres.py"
description: "Declare [dependency-groups].postgres-bench and implement schema_version=1 PostgreSQL benchmark capture and threshold comparator CLI."
status: stable
state: completed
commit: 9a3631bb
last_verified_at: "2026-11-13T22:11:30Z"
last_verified_commit: 9a3631bb
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch07:t01"
files: 
  - tools/benchmarks/postgres.py
  - pyproject.toml
  - uv.lock
tests: 
  - src/py/tests/unit/tools/test_postgres_benchmarks.py
read_set:
  - pyproject.toml
  - tools/benchmarks/runner.py
estimated_loc: 240
tags:
  - postgres
  - benchmarks
  - python
sources:
  - id: pyproject
    resource: pyproject.toml
    title: "Dependency groups configuration"
---

# Task `t02`: Add postgres-bench dependency group and tools/benchmarks/postgres.py

## Objective & Scope Boundary

Add `[dependency-groups].postgres-bench` (`python_version < '3.14'`) and `tools/benchmarks/postgres.py` (`capture`, `compare`) [^pyproject].

## Bounded Read Manifest

- `pyproject.toml` and `tools/benchmarks/runner.py`.

## Contracts & Edge Cases

- Keep `psycopg`/`asyncpg` strictly inside `[dependency-groups].postgres-bench` (never `[project.optional-dependencies]`); comparator enforces throughput/latency ratios deterministically offline.

## Test Matrix

- Offline unit tests in `test_postgres_benchmarks.py` covering JSON schema, ratio calculation, and regression failure exit codes.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q && uv run --no-sync ruff check tools/benchmarks/postgres.py && uv run --no-sync mypy tools/benchmarks/postgres.py
```

## Acceptance Criteria

- [x] Offline benchmark comparator unit tests, Ruff, and Mypy pass.
- [x] Zero regressions in `ch07-wheel-ci-and-benchmarks` fast verification gate.

[^pyproject]: `pyproject.toml` — Dependency groups configuration.
