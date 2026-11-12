---
type: Spec
parent_flow: driver-conformance-ci
chapter_id: ch03-engine-live-suites
title: "Chapter 3: Engine Live Suites"
description: Live MySQL/MariaDB and SQL Server engine-specific edge suites, Oracle live suite tagging for lane selection, and the Oracle AQ listener wait budget.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-live-conformance-harness
replan_trigger: null
current_task: "driver-conformance-ci:ch03:t01"
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
  - id: oracle-notify
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: AQ dequeue loop that discards wait_timeout_seconds
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL driver (fetch_all_sets, cancel_query, lastrowid, Arrow load and stream)
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: SQL Server driver (cancel, savepoints, Arrow load and stream)
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: Live Oracle 26ai suite and its gate
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mssql_config
    title: Versioned service fixtures (ch01 t01)
  - id: db-errors
    resource: src/py/vertebra/_vertebra.pyi::DatabaseError
    title: DatabaseError sqlstate, vendor_code, and is_transient
tags:
  - mysql
  - mariadb
  - mssql
  - oracle
  - integration
---

# Chapter 3: Engine Live Suites

<!-- truth: start -->
- `src/py/tests/integration/db/test_mysql_native.py` runs MySQL and MariaDB live edge cases, parametrized by family with per-param `driver_engine` markers [^mysql-driver].
- `src/py/tests/integration/db/test_mssql_native.py` runs SQL Server live edge cases tagged `driver_engine("mssql")` [^mssql-driver].
- `src/py/tests/integration/db/test_oracle_native.py` is tagged `driver_engine("oracle")` and its gate keeps DSN-or-Docker resolution [^oracle-native].
- Every live suite resolves configs from `VERTEBRA_CONFORMANCE_<ENGINE>_DSN` or the versioned container fixture under `VERTEBRA_CONFORMANCE_DOCKER=1`, and skips offline with an explicit reason [^int-conftest].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The shared conformance contract checks portable behavior only; MySQL/MariaDB and SQL Server have no live coverage for engine-specific types, multi-result procedures, server-side cancellation, LOB sizes, or bulk Arrow paths. Their unit tests run against `MySQLWireTestServer` loopbacks or not at all [^mysql-driver] [^mssql-driver]. The Oracle live suite exists but carries no engine tag, so the ch01 gate would deselect it in the Oracle lane [^oracle-native].

### 1.2 Non-Goals

- New driver features; any live failure is recorded as a blocked task with the observed error and routed through `/flow:revise`.
- PostgreSQL suites.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| MySQL | `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver.fetch_all_sets`, `::cancel_query`, `::lastrowid`, `::load_from_arrow`, `::stream_arrow_batches`, `::AsyncMySQLDriver` [^mysql-driver] | Remote-owned server via `pytest-databases`. |
| SQL Server | `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver.cancel`, `::savepoint`, `::rollback_to_savepoint`, `::load_from_arrow`, `::stream_arrow_batches`, `::AsyncMSSQLDriver` [^mssql-driver] | Remote-owned server via `pytest-databases`. |
| Oracle | `src/py/tests/integration/db/test_oracle_native.py::oracle_config` [^oracle-native] | Existing live suite. |
| Errors | `src/py/vertebra/_vertebra.pyi::DatabaseError.vendor_code`, `::IntegrityError` [^db-errors] | Typed error contract. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), `.agents/skills/review-tests/SKILL.md`, `pytest-databases` skill.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R6**: MySQL/MariaDB live suite (`t01`).
- **R7**: SQL Server live suite (`t02`).
- **R8**: Oracle live suite tagging (`t03`).
- **R16**: The Oracle AQ listener uses `wait_timeout_seconds` as its idle dequeue wait budget with 1-second slices and a liveness ping on exhaustion (`t04`) [^oracle-notify].

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# src/py/tests/integration/db/test_mysql_native.py
@pytest.fixture(params=[pytest.param("mysql", marks=pytest.mark.driver_engine("mysql")), pytest.param("mariadb", marks=pytest.mark.driver_engine("mariadb"))])
def mysql_family_config(request: pytest.FixtureRequest) -> MySQLConfig: ...

# src/py/tests/integration/db/test_mssql_native.py
pytestmark = [pytest.mark.driver_engine("mssql")]
@pytest.fixture
def mssql_config(request: pytest.FixtureRequest) -> MSSQLConfig: ...

# src/py/tests/integration/db/test_oracle_native.py
pytestmark = [pytest.mark.driver_engine("oracle")]
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Live test → `mysql_family_config` / `mssql_config` / `oracle_config` → env DSN or `request.getfixturevalue("vtb_<family>_config")` → `ServiceVersionMatrix` → `pytest-databases` service → `SyncMySQLDriver` / `AsyncMySQLDriver` / `SyncMSSQLDriver` / `AsyncMSSQLDriver`.
- `ConformanceGate.item_engines` reads the markers to select and enforce each lane.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Offline | No DSN, Docker unset | Skip: `"<engine> live tests require VERTEBRA_CONFORMANCE_<ENGINE>_DSN or VERTEBRA_CONFORMANCE_DOCKER=1"` | No containers offline |
| Missing wheel | `vertebra._mysql` / `vertebra._mssql` absent | `MissingDependencyError` from `ensure_module` (fail loud) | Live lanes never silently skip a missing wheel |
| Cancellation | `SELECT SLEEP(30)` / `WAITFOR DELAY '00:00:30'` cancelled after the session is observed active | Completes in under `10 s`; MySQL returns `1` (`KILL QUERY` semantics), SQL Server raises `QueryCancelledError` | Server-side cancel works |
| Duplicate key | Insert duplicate PK | `IntegrityError`; MySQL/MariaDB `vendor_code == 1062`, SQL Server `vendor_code == 2627` | Typed errors |
| Table leakage | Each test | Tables named `vtb_live_<case>` dropped in `finally` | Re-runnable on a shared container |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mariadb`, when `test_mysql_native.py` runs, then only `[mariadb-*]` cases run and all pass.
- Given `VERTEBRA_CONFORMANCE_ENGINES=oracle`, when `test_oracle_native.py` runs under Docker, then its cases are selected and the gate counts them as executed.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| MySQL and MariaDB | One family-parametrized suite | Two copied suites: duplicated cases drift. |
| Edge suites vs extending conformance | Engine files for engine-only behavior | Adding engine branches to `test_driver_conformance.py`: dilutes the portable contract. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; suites are listed in `ch05 / t03` (`polyglot-test-matrix.md`) and caveats in `ch05 / t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6 | [t01](tasks/t01-mysql-mariadb-live-suite.md) | `integration_acceptance` | `src/py/tests/integration/db/test_mysql_native.py` |
| R7 | [t02](tasks/t02-mssql-live-suite.md) | `integration_acceptance` | `src/py/tests/integration/db/test_mssql_native.py` |
| R8 | [t03](tasks/t03-oracle-live-suite-tagging.md) | `regression_tdd` | `src/py/tests/integration/db/test_oracle_native.py` |
| R16 | [t04](tasks/t04-oracle-listener-wait-budget.md) | `behavior_tdd` | `src/rs/vertebra-oracle/src/notify.rs` `mod tests` |

## 5. Implementation Plan

- [ ] driver-conformance-ci:ch03:t01 — Add the live MySQL and MariaDB edge suite (tasks/t01-mysql-mariadb-live-suite.md)
- [ ] driver-conformance-ci:ch03:t02 — Add the live SQL Server edge suite (tasks/t02-mssql-live-suite.md)
- [ ] driver-conformance-ci:ch03:t03 — Tag the Oracle live suite for lane selection (tasks/t03-oracle-live-suite-tagging.md)
- [ ] driver-conformance-ci:ch03:t04 — Honor the Oracle AQ listener wait budget (tasks/t04-oracle-listener-wait-budget.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_mssql_native.py src/py/tests/integration/db/test_oracle_native.py -q -rs && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mysql,mariadb,mssql,oracle uv run --no-sync pytest src/py/tests/integration/db -q -rs && uv run --no-sync ruff check src/py/tests/integration/db` exits `0` (the live step needs Docker).

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 2`, `plan_commit: null`, HEAD `c8722c76`; `depends_on_chapters: [ch01-live-conformance-harness]`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`0/4` completed).
- **Active Invariants**: Parametrize only (no loops over cases or sync/async modes); every created table is dropped in `finally`.

[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL driver surface.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — SQL Server driver surface.
[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Live Oracle suite gate.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mssql_config` — Versioned service fixtures.
[^db-errors]: `src/py/vertebra/_vertebra.pyi::DatabaseError` — Typed error attributes.
