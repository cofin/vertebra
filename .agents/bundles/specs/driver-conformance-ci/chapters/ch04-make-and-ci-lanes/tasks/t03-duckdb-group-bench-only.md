---
type: Task
id: "driver-conformance-ci:ch04:t03"
chapter_id: ch04-make-and-ci-lanes
title: Restrict the PyPI duckdb group to benchmarks
description: Remove PY_TEST_GROUPS and every --group duckdb use from install, upgrade, py-test, and coverage so the test suite runs without PyPI duckdb, leaving the duckdb dependency group reachable only through the bench group.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "driver-conformance-ci:ch04:t01"
read_set:
  - Makefile
  - pyproject.toml
  - tools/benchmarks/baseline.py
  - tools/benchmarks/store.py
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
files:
  - Makefile
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
tests: []
estimated_loc: 30
plan_revision: 2
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: makefile-groups
    resource: Makefile
    title: PY_TEST_GROUPS, install, upgrade, py-test, coverage
  - id: pyproject-groups
    resource: pyproject.toml
    title: "[dependency-groups] bench includes duckdb"
  - id: bench-store
    resource: tools/benchmarks/store.py
    title: Only importer of PyPI duckdb (with tools/benchmarks/baseline.py)
  - id: bench-okf
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Dependency-group isolation section
tags:
  - makefile
  - duckdb
  - dependencies
---

# Task t03: Restrict the PyPI `duckdb` Group to Benchmarks

## Objective & Scope Boundary

PyPI `duckdb` is imported only by `tools/benchmarks/store.py` and `tools/benchmarks/baseline.py` [^bench-store]; tests use the native `vertebra._duckdb` wheel. The Makefile still installs the group for tests: `PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)` feeds `py-test`, `coverage`, and `upgrade`, and `install` hardcodes `--group duckdb` [^makefile-groups]. A stray PyPI `duckdb` in the test environment can mask a test that accidentally depends on it.

Edit `Makefile`:

1. Delete the `PY_GIL_DISABLED` and `PY_TEST_GROUPS` assignments and the `unexport PY_GIL_DISABLED PY_TEST_GROUPS` line.
2. `install`: `@uv sync --all-extras --dev --group doc`.
3. `upgrade`: `@uv lock --upgrade && uv sync --all-extras --dev --group doc`.
4. `py-test`: `@uv run pytest src/py/tests -v`.
5. `coverage`: `@uv run pytest src/py/tests --cov --quiet`.
6. Leave every `make bench*` target on `--group bench` unchanged; `pyproject.toml` keeps `duckdb = ["duckdb>=1.0"]` included by `bench` only [^pyproject-groups].

Edit `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` section `## 5.` so it states that PyPI `duckdb` installs only through `--group bench`, that no test or install target syncs `--group duckdb`, and that `make py-test` runs against the native `vertebra._duckdb` wheel alone; append a `verified:` entry [^bench-okf].

Out of scope: `ch04 / t01` `oracle-test` and `db-conformance` edits (land first in the same file), `AGENTS.md` (`ch05 / t02`), `workflow.md` and `polyglot-test-matrix.md` (`ch05 / t03`), `pyproject.toml` and `uv.lock` (unchanged).

## Bounded Read Manifest

- `Makefile` — variable block near the top, `install`, `upgrade`, `py-test`, `coverage`, `bench*` [^makefile-groups].
- `pyproject.toml` — `[dependency-groups]` `bench`, `duckdb`, `dev` [^pyproject-groups].
- `tools/benchmarks/store.py`, `tools/benchmarks/baseline.py` — function-scoped `import duckdb` [^bench-store].
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — section 5 [^bench-okf].

## Symbol & Signature Contracts

- Removed Make variables: `PY_GIL_DISABLED`, `PY_TEST_GROUPS`.
- Unchanged: `[dependency-groups].bench` contains `{ include-group = "duckdb" }`; `dev` does not.

## Edge Cases & Error Semantics

- Free-threaded `3.14t`/`3.15t`: behavior is unchanged (the group was already omitted there).
- An existing `.venv` with PyPI `duckdb` installed: `uv run` performs an inexact sync and keeps it; the verification below runs an exact `uv sync` first so the check cannot pass by accident.
- A test that imports PyPI `duckdb`: fails with `ModuleNotFoundError`; fix the test to use `vertebra._duckdb`, never re-add the group.
- `make bench` after `make install`: `uv run --group bench` installs `duckdb` on demand.

## Public-Seam Test Blueprint

- **Canonical gate**: static Makefile checks plus a full test run in an environment without PyPI `duckdb`.

| Case | Concrete Input | Expected Output |
| --- | --- | --- |
| No test-path group | `grep -n -- '--group duckdb' Makefile` | No matches (exit `1`) |
| Variable removed | `grep -n 'PY_TEST_GROUPS' Makefile` | No matches (exit `1`) |
| py-test command | `make -n py-test` | Contains `uv run pytest src/py/tests -v` |
| Bench untouched | `make -n bench-report` | Contains `uv run --group bench` |
| Exact env lacks duckdb | `uv sync --all-extras --dev --group doc && uv run --no-sync python -c "import importlib.util, sys; sys.exit(importlib.util.find_spec('duckdb') is not None)"` | Exit `0` |
| Suite passes without duckdb | `make py-test` after the exact sync | Exit `0` |

## Verification

- `! grep -n -- '--group duckdb' Makefile`
- `! grep -n 'PY_TEST_GROUPS' Makefile`
- `make -n py-test | grep -q 'uv run pytest src/py/tests -v'`
- `make -n bench-report | grep -q -- '--group bench'`
- `uv sync --all-extras --dev --group doc`
- `uv run --no-sync python -c "import importlib.util, sys; sys.exit(importlib.util.find_spec('duckdb') is not None)"`
- `make py-test`

## Acceptance Criteria

- [ ] No install or test target syncs PyPI `duckdb`; only `--group bench` pulls it in (R17).
- [ ] `make py-test` passes in an exactly synced environment without PyPI `duckdb` (R17).
- [ ] The benchmarks OKF chapter describes the bench-only group (R17).

## Notes & Discoveries

- [2026-11-12 01:30] Folding `duckdb>=1.0` into `bench` and deleting the standalone group was considered; it rewrites `uv.lock` for no behavioral gain, so the group stays and only its consumers change.

[^makefile-groups]: `Makefile` — `PY_TEST_GROUPS`, `install`, `upgrade`, `py-test`, `coverage`.
[^pyproject-groups]: `pyproject.toml` — `[dependency-groups]` `bench` includes `duckdb`.
[^bench-store]: `tools/benchmarks/store.py` — Only importers of PyPI `duckdb` (with `tools/benchmarks/baseline.py`).
[^bench-okf]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Dependency-group isolation section.
