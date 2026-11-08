---
type: Task
id: "driver-fail-loud:ch02:t01"
chapter_id: ch02-duckdb-sqlalchemy-queue-fail-loud
title: DuckDB dependency group installed on GIL interpreters
description: Add a dedicated duckdb dependency group, install it from make install and from the py-test and coverage targets only when the interpreter has the GIL, and regenerate uv.lock.
status: draft
state: pending
priority: P1
verification_strategy: refactor_verification
depends_on: []
read_set:
  - pyproject.toml
  - Makefile
  - .github/workflows/ci.yml
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
files:
  - pyproject.toml
  - uv.lock
  - Makefile
tests: []
estimated_loc: 30
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: dep-groups
    resource: pyproject.toml::dependency-groups
    title: dev, test, and bench dependency groups; duckdb only in bench
  - id: make-install
    resource: Makefile::install
    title: Fresh install pinning Python 3.12 and running uv sync --all-extras --dev --group doc
  - id: make-py-test
    resource: Makefile::py-test
    title: Python test target running stubtest and uv run pytest
  - id: make-coverage
    resource: Makefile::coverage
    title: Coverage target running uv run pytest --cov
  - id: ci-test-python
    resource: .github/workflows/ci.yml::test-python
    title: CI Python matrix (3.12, 3.13, 3.14, 3.14t, 3.15, 3.15t) running make py-test
tags:
  - duckdb
  - packaging
  - free-threading
  - ci
---

# Task t01: DuckDB Dependency Group Installed on GIL Interpreters

## Objective & Scope Boundary

`pyproject.toml` gains a `duckdb` dependency group holding `"duckdb>=1.0"`; `bench` includes it through `{ include-group = "duckdb" }`, and `dev` does not include it.[^dep-groups] The `Makefile` installs the group wherever the interpreter has the GIL: `install` syncs it unconditionally (the target pins Python 3.12), and `py-test`, `coverage`, and `upgrade` add `--group duckdb` only when the `PY_GIL_DISABLED` probe prints exactly `0` (a GIL build); output `1` or empty output adds nothing.[^make-install][^make-py-test][^make-coverage] Reason: `duckdb` publishes only `cp312-abi3` wheels, free-threaded CPython cannot load abi3 wheels, and an sdist build on `3.14t` / `3.15t` fails the CI matrix.

The CI `test-python` job runs `make py-test` for every matrix entry, so the `3.12`, `3.13`, `3.14`, and `3.15` jobs install `duckdb` and the `3.14t` and `3.15t` jobs do not; `.github/workflows/ci.yml` needs no edit.[^ci-test-python]

Out of scope: the DuckDB driver change and the `pytest.importorskip` guards (t04); `bench-ft`, which already syncs `--group bench` on a free-threaded interpreter; a cached free-threaded `duckdb` source build (follow-up once DuckDB supports free-threading); a `duckdb` optional-dependency extra (native DuckDB flow).

## Bounded Read Manifest

- `pyproject.toml` — `[dependency-groups]` `dev`, `test`, `bench`.[^dep-groups]
- `Makefile` — `install`, `upgrade`, `py-test`, `coverage` targets and the top-level variable block.[^make-install][^make-py-test][^make-coverage]
- `.github/workflows/ci.yml` — `test-python` job (matrix and `make py-test` step); read only.[^ci-test-python]
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — free-threaded interpreter conventions.

## Symbol & Signature Contracts

`pyproject.toml` `[dependency-groups]`:

```toml
bench = [
    { include-group = "duckdb" },
    "uvicorn[standard]>=0.30",
    "httptools>=0.6",
    "uvloop>=0.23.0; sys_platform != 'win32'",
    "granian>=1.6",
    "spikard>=0.12",
]
duckdb = [
    "duckdb>=1.0",
]
```

`dev` keeps `test`, `lint`, and `build` only. Run `uv lock`; the lock keeps `duckdb` `2.0.0`.

`Makefile` variables (recursively expanded with `=` so each recipe evaluates them when it runs, after its prerequisites; overridable from the command line):

```make
PY_GIL_DISABLED = $(shell uv run --no-sync python -c "import sysconfig; print(int(bool(sysconfig.get_config_var('Py_GIL_DISABLED'))))" 2>/dev/null)
PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)
```

The probe prints `0` on a GIL build and `1` on a free-threaded build. `PY_TEST_GROUPS` is `--group duckdb` only for the exact word `0`; `1`, empty output (probe failed, stderr discarded), or any other text yields an empty string. Reason: installing `duckdb` on an interpreter of unknown build risks an sdist build on `3.14t` / `3.15t`, while skipping it only makes t04's guards skip DuckDB tests.

Recipe lines after the change:

| Target | Line |
| --- | --- |
| `install` | `@uv sync --all-extras --dev --group doc --group duckdb` |
| `upgrade` | `@uv lock --upgrade && uv sync --all-extras --dev --group doc $(PY_TEST_GROUPS)` |
| `py-test` | `@uv run $(PY_TEST_GROUPS) pytest src/py/tests -v` (stubtest line unchanged) |
| `coverage` | `@uv run $(PY_TEST_GROUPS) pytest src/py/tests --cov --quiet` |

## Edge Cases & Error Semantics

- Free-threaded interpreter (`Py_GIL_DISABLED == 1`): `PY_TEST_GROUPS` is empty; `uv run pytest` installs no `duckdb` and t04's guards skip DuckDB tests.
- `uv run` performs an inexact sync, so a `duckdb` already installed by `make install` stays installed when a later `uv run` omits the group.
- `py-test` depends on `dev`, and GNU make expands a recipe only after its prerequisites finish, so the probe runs against the environment `dev` built. `coverage` and `upgrade` have no `dev` prerequisite: when the probe cannot run (no `.venv`, no `uv`, or the interpreter fails to start) it prints nothing, `PY_TEST_GROUPS` is empty, and the target runs without `--group duckdb`.
- `uv lock` resolves the `duckdb` group universally; the lock records the sdist and abi3 wheels, and installation is gated by the Makefile, not by markers. Reason: no PEP 508 marker distinguishes free-threaded builds in the supported uv versions.

## Public-Seam Test Blueprint

- **Canonical gate**: build tooling (`uv lock --check`, `make -n`), no pytest module. Reason: the change is packaging and Makefile wiring with no Python behavior.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output |
| --- | --- | --- |
| Group resolves | `uv export --only-group duckdb --no-hashes \| grep -E '^duckdb=='` | `duckdb==2.0.0` |
| Dev excludes group | `uv export --only-dev --no-hashes \| grep -cE '^duckdb=='` | `0` |
| Bench includes group | `uv export --only-group bench --no-hashes \| grep -cE '^duckdb=='` | `1` |
| GIL interpreter | `make -n py-test PY_GIL_DISABLED=0 \| grep -c 'uv run --group duckdb pytest src/py/tests -v'` | `1` |
| Free-threaded interpreter | `make -n py-test PY_GIL_DISABLED=1 \| grep -c -- '--group duckdb'` | `0` |
| Coverage on GIL | `make -n coverage PY_GIL_DISABLED=0 \| grep -c -- '--group duckdb'` | `1` |
| Probe unavailable | `make -n coverage PY_GIL_DISABLED= \| grep -c -- '--group duckdb'` | `0` |
| Upgrade, probe unavailable | `make -n upgrade PY_GIL_DISABLED= \| grep -c -- '--group duckdb'` | `0` |

## Verification

- `uv lock && uv lock --check`
- `uv export --only-group duckdb --no-hashes | grep -E '^duckdb=='`
- `test "$(uv export --only-dev --no-hashes | grep -cE '^duckdb==')" = 0`
- `make -n py-test PY_GIL_DISABLED=0 | grep -E 'uv run --group duckdb pytest src/py/tests -v'`
- `test "$(make -n py-test PY_GIL_DISABLED=1 | grep -c -- '--group duckdb')" = 0`
- `make -n coverage PY_GIL_DISABLED=0 | grep -E 'uv run --group duckdb pytest src/py/tests --cov'`
- `test "$(make -n coverage PY_GIL_DISABLED= | grep -c -- '--group duckdb')" = 0`
- `uv run --group duckdb python -c "import duckdb; print(duckdb.__version__)"`

## Acceptance Criteria

- [ ] `pyproject.toml` declares `duckdb = ["duckdb>=1.0"]`; `bench` includes it; `dev` does not (R3).
- [ ] `make install` syncs `--group duckdb`; `make py-test`, `make coverage`, and `make upgrade` add it only when the probe prints `0`, and add nothing when the probe prints `1` or nothing (R3).
- [ ] `uv lock --check` exits `0`.
- [ ] `.github/workflows/ci.yml` is unchanged and its non-free-threaded `test-python` jobs install `duckdb` through `make py-test` (R3).

## Notes & Discoveries

- [2026-11-07 00:00] `duckdb` 2.0.0 publishes only `cp312-abi3` wheels; `uv pip install --dry-run --no-build --python 3.14t duckdb==2.0.0` fails. The CI `test-python` matrix includes `3.14t` and `3.15t`.

[^dep-groups]: `pyproject.toml::dependency-groups` — dev, test, and bench dependency groups; `duckdb` only in `bench`.
[^make-install]: `Makefile::install` — Fresh install pinning Python 3.12 and running `uv sync --all-extras --dev --group doc`.
[^make-py-test]: `Makefile::py-test` — Python test target running stubtest and `uv run pytest`.
[^make-coverage]: `Makefile::coverage` — Coverage target running `uv run pytest --cov`.
[^ci-test-python]: `.github/workflows/ci.yml::test-python` — CI Python matrix (3.12, 3.13, 3.14, 3.14t, 3.15, 3.15t) running `make py-test`.
