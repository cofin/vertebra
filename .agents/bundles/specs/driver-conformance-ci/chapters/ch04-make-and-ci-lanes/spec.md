---
type: Spec
parent_flow: driver-conformance-ci
chapter_id: ch04-make-and-ci-lanes
title: "Chapter 4: Make and CI Lanes"
description: make db-conformance, oracle-test cleanup, the test-db-conformance GitHub Actions matrix job running every engine and version lane against pytest-databases containers, and bench-only PyPI duckdb.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-live-conformance-harness
  - ch02-gcs-bearer-secret-rotation
  - ch03-engine-live-suites
replan_trigger: null
current_task: "driver-conformance-ci:ch04:t01"
plan_revision: 2
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T00:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T00:30:00Z"
    commit: c8722c76
sources:
  - id: makefile
    resource: Makefile
    title: py-test, duckdb-test, mysql-test, mssql-test, oracle-test, PY_TEST_GROUPS
  - id: ci
    resource: .github/workflows/ci.yml
    title: changes, lint, test-python, and sibling jobs
tags:
  - ci
  - makefile
  - conformance
---

# Chapter 4: Make and CI Lanes

<!-- truth: start -->
- `make db-conformance ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]` builds extensions (`dev`), then runs `pytest src/py/tests/integration/db` with `VERTEBRA_CONFORMANCE_DOCKER=1`, `VERTEBRA_CONFORMANCE_ENGINES=$(ENGINE)`, the engine's `VERTEBRA_CONFORMANCE_<ENGINE>_VERSION` when `VERSION` is set, and `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` for DuckDB [^makefile].
- `make oracle-test` runs the Oracle Rust tests, stubtest, `test_oracle_adapter.py`, and the Oracle conformance and native suites (offline skips) without a fallback branch [^makefile].
- `.github/workflows/ci.yml` `test-db-conformance` runs one matrix row per engine/version (`fail-fast: false`, `timeout-minutes: 45`) gated like `test-python` [^ci].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

No Make target or CI job runs a live database lane. `make oracle-test` silently swaps in `src/py/tests/unit/test_freethreaded_gil.py` when `test_oracle_adapter.py` is missing and never runs the Oracle conformance or native suites [^makefile]. CI runs `make py-test` only [^ci]. This chapter adds one parameterized Make entry point and a CI matrix that invokes it.

### 1.2 Non-Goals

- PostgreSQL lanes (native-postgres-driver ch29 owns its wheel CI).
- Changing the existing `test-python` matrix.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Make | `Makefile::py-test`, `::duckdb-test`, `::mysql-test`, `::mssql-test`, `::oracle-test`, `::PY_TEST_GROUPS` [^makefile] | Developer and CI entry point. |
| CI | `.github/workflows/ci.yml::changes`, `::lint`, `::test-python` [^ci] | GitHub Actions `ubuntu-latest` with Docker. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Project Workflow](../../../../knowledge/workflow.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R9**: `make db-conformance` and `oracle-test` cleanup (`t01`).
- **R10**: `test-db-conformance` CI job (`t02`).
- **R17**: No install or test target syncs PyPI `duckdb`; only `--group bench` pulls it in, and `make py-test` passes without it (`t03`) [^makefile].

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```make
.PHONY: db-conformance
db-conformance: dev                                 ## Run a live pytest-databases conformance lane: make db-conformance ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]
```

```yaml
test-db-conformance:
  runs-on: ubuntu-latest
  timeout-minutes: 45
  needs: [changes, lint]
  if: needs.changes.outputs.python == 'true' || needs.changes.outputs.rust == 'true' || needs.changes.outputs.ci == 'true'
  strategy:
    fail-fast: false
    matrix:
      include:
        - { engine: duckdb, version: "", python-version: "3.12" }
        - { engine: duckdb, version: "", python-version: "3.14t" }
        - { engine: mysql, version: "8.0", python-version: "3.12" }
        - { engine: mysql, version: "8.4", python-version: "3.12" }
        - { engine: mysql, version: "8.4", python-version: "3.14t" }
        - { engine: mysql, version: "9.7", python-version: "3.12" }
        - { engine: mariadb, version: "11.4", python-version: "3.12" }
        - { engine: mariadb, version: "11.8", python-version: "3.12" }
        - { engine: mssql, version: "2022", python-version: "3.12" }
        - { engine: mssql, version: "2025", python-version: "3.12" }
        - { engine: oracle, version: "23ai", python-version: "3.12" }
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- CI row → `make db-conformance ENGINE=${{ matrix.engine }} VERSION=${{ matrix.version }}` → `make dev` → `uv run $(PY_TEST_GROUPS) pytest src/py/tests/integration/db -v -rs` with lane env → `ConformanceGate` + `pytest-databases`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Missing `ENGINE` | `make db-conformance` | Exit `2`: `ENGINE is required: duckdb, mysql, mariadb, mssql, oracle` | Explicit lanes |
| Unknown engine | `ENGINE=sybase` | Pytest `UsageError` from the gate (exit `4`) | Single validation point |
| Empty `VERSION` | DuckDB rows | No version variable exported | DuckDB is embedded |
| Free-threaded rows | `3.14t` | `PY_TEST_GROUPS` omits the PyPI `duckdb` group; native `vertebra._duckdb` still builds | FT parity |
| One row fails | e.g. `mssql 2025` | Other rows keep running (`fail-fast: false`) | Full matrix evidence per push |
| Missing Oracle unit file | `test_oracle_adapter.py` absent | `pytest` errors on the missing path (no fallback) | Fail loud |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a pull request touching `src/rs/**`, when CI runs, then all eleven `test-db-conformance` rows run after `lint` and each passes with zero skipped items.
- Given `make db-conformance ENGINE=mssql VERSION=2025`, when it runs locally with Docker, then only SQL Server items run against `mssql_2025_service`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Entry point | One `db-conformance` target with `ENGINE`/`VERSION` | Five `*-live` targets: duplicated recipes drift. |
| Matrix failure policy | `fail-fast: false` | `fail-fast: true` (as in `test-python`): one engine failure cancels unrelated engines and hides their status. |
| Version coverage | LTS plus newest per engine; FT rows on DuckDB and MySQL 8.4 | Full Python × version product: 66 rows and multi-hour wall time. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented in this chapter]` (`.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `t03`); the target and job are documented in `ch05 / t02` (`AGENTS.md`) and `ch05 / t03` (`workflow.md`, `polyglot-test-matrix.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R9 | [t01](tasks/t01-db-conformance-make-target.md) | `static_validation` | `make -n db-conformance ENGINE=mysql VERSION=8.0`, `make -n oracle-test` |
| R10 | [t02](tasks/t02-ci-conformance-matrix.md) | `static_validation` | YAML matrix assertion on `.github/workflows/ci.yml` |
| R17 | [t03](tasks/t03-duckdb-group-bench-only.md) | `static_validation` | `! grep -n -- '--group duckdb' Makefile`, `make py-test` without PyPI `duckdb` |

## 5. Implementation Plan

- [ ] driver-conformance-ci:ch04:t01 — Add make db-conformance and remove the oracle-test fallback branch (tasks/t01-db-conformance-make-target.md)
- [ ] driver-conformance-ci:ch04:t02 — Add the test-db-conformance CI matrix job (tasks/t02-ci-conformance-matrix.md)
- [ ] driver-conformance-ci:ch04:t03 — Restrict the PyPI duckdb group to benchmarks (tasks/t03-duckdb-group-bench-only.md)

**Chapter Recovery Checkpoint:** `make -n db-conformance ENGINE=mysql VERSION=8.0 && make -n oracle-test && uv run --no-sync python -c "import yaml; jobs = yaml.safe_load(open('.github/workflows/ci.yml'))['jobs']; assert len(jobs['test-db-conformance']['strategy']['matrix']['include']) == 11"` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 2`, `plan_commit: null`, HEAD `c8722c76`; depends on ch01–ch03.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`0/3` completed).
- **Active Invariants**: CI uses only `pytest-databases` containers; no external database secrets.

[^makefile]: `Makefile` — Test targets and `PY_TEST_GROUPS`.
[^ci]: `.github/workflows/ci.yml` — CI jobs.
