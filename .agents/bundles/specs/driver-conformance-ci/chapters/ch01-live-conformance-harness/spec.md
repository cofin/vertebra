---
type: Spec
parent_flow: driver-conformance-ci
chapter_id: ch01-live-conformance-harness
title: "Chapter 1: Live Conformance Harness"
description: Lane-selection and zero-skip conformance gate, driver_engine tagging, versioned pytest-databases service fixtures, a MariaDB conformance engine, and dead conformance surface removal.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "driver-conformance-ci:ch01:t01"
plan_revision: 1
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
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mariadb_config
    title: pytest-databases service fixtures (MySQL, MariaDB, SQL Server, Oracle)
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Live config resolution, native-module gate, and conformance matrix
  - id: pd-mysql
    resource: .venv/lib/python3.12/site-packages/pytest_databases/docker/mysql.py::mysql_84_service
    title: pytest-databases 0.20.0b1 versioned MySQL service fixtures
  - id: pd-mariadb
    resource: .venv/lib/python3.12/site-packages/pytest_databases/docker/mariadb.py::mariadb_114_service
    title: pytest-databases 0.20.0b1 versioned MariaDB service fixtures
  - id: pd-mssql
    resource: .venv/lib/python3.12/site-packages/pytest_databases/docker/mssql.py::mssql_2022_service
    title: pytest-databases 0.20.0b1 SQL Server 2022 and 2025 service fixtures
  - id: pd-oracle
    resource: .venv/lib/python3.12/site-packages/pytest_databases/docker/oracle.py::oracle_23ai_service
    title: pytest-databases 0.20.0b1 Oracle 23ai Free service fixture
tags:
  - conformance
  - pytest-databases
  - harness
---

# Chapter 1: Live Conformance Harness

<!-- truth: start -->
- `src/py/tests/integration/conformance_gate.py::ConformanceGate` reads `VERTEBRA_CONFORMANCE_ENGINES`, deselects integration items not tagged for the listed engines, and fails the session on skipped/xfailed items or engines with zero executed tests [^conformance-suite].
- Engine identity comes from `@pytest.mark.driver_engine("<engine>")` or a parametrized value with a string `engine` attribute.
- `ServiceVersionMatrix.fixture_name` maps `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION` onto `pytest-databases` 0.20.0b1 service fixtures; unknown values raise `pytest.UsageError` [^pd-mysql] [^pd-mariadb] [^pd-mssql] [^pd-oracle].
- `src/py/tests/integration/conftest.py` resolves `vtb_mysql_config`, `vtb_mariadb_config`, `vtb_mssql_config`, and `vtb_oracle_config` through the matrix; PostgreSQL fixtures stay untouched [^int-conftest].
- `ENGINE_SPECS` gains a `mariadb` spec; MariaDB, SQL Server, and Oracle skip offline with explicit reasons; dead conformance surface is removed [^conformance-suite].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/py/tests/integration/conftest.py` binds `vtb_mysql_config` to `mysql_service` (MySQL 8.4), `vtb_mssql_config` to `mssql_service` (2022), and `vtb_oracle_config` to `oracle_23ai_service` with no way to pick another server version, and `vtb_mariadb_config` is defined but no test requests it [^int-conftest]. Nothing fails a live run that silently skips. `_resolve_live_config` duplicates the generic env/container branch for SQL Server, `_DUCKDB_SKIP_REASON` is unused, `_FAIL_LOUD_DRIVER_ENGINES` is always empty, and DuckDB declares a `VERTEBRA_CONFORMANCE_DUCKDB_DSN` variable that is never read [^conformance-suite].

This chapter builds the harness every later live lane depends on: a gate module with its own canonical tests, conftest hooks and versioned fixtures, and a cleaned conformance matrix with MariaDB.

### 1.2 Non-Goals

- PostgreSQL fixtures, `_POSTGRES_TYPES_DDL`, and PostgreSQL zero-skip enforcement (owned by `native-postgres-driver` ch28).
- Engine-specific live suites (ch03) and Make/CI wiring (ch04).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Fixtures | `src/py/tests/integration/conftest.py::vtb_mysql_config`, `::vtb_mariadb_config`, `::vtb_mssql_config`, `::vtb_oracle_config` [^int-conftest] | `pytest-databases` service fixtures (remote-owned Docker containers). |
| Matrix | `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS`, `::_resolve_live_config`, `::test_engine_driver_resolution_and_provide_driver` [^conformance-suite] | Test-owned specs feeding `sync_sql_driver` / `async_sql_driver`. |
| Service versions | `pytest_databases/docker/mysql.py::mysql_8_service`, `mariadb.py::mariadb_118_service`, `mssql.py::mssql_2025_service`, `oracle.py::oracle_23ai_service` [^pd-mysql] [^pd-mariadb] [^pd-mssql] [^pd-oracle] | Versioned fixtures resolved via `request.getfixturevalue`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), `pytest-databases` skill, `.agents/skills/review-tests/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: `ConformanceGate` selects by engine and enforces zero skips/xfails plus at least one executed test per listed engine (`t01`).
- **R2**: `driver_engine(name)` marker registration and `ConformanceGate.item_engines` (`t01`).
- **R3**: `ServiceVersionMatrix` and versioned fixtures (`t01`).
- **R4**: `mariadb` conformance engine; explicit offline skips for MariaDB, SQL Server, and Oracle (`t02`).
- **R5**: Dead conformance surface removed (`t02`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class ConformanceGate:
    ENGINES_ENV: ClassVar[str] = "VERTEBRA_CONFORMANCE_ENGINES"
    ENGINE_MARKER: ClassVar[str] = "driver_engine"
    SUPPORTED_ENGINES: ClassVar[frozenset[str]] = frozenset({"duckdb", "mariadb", "mssql", "mysql", "oracle"})
    def __init__(self, engines: frozenset[str]) -> None: ...
    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> ConformanceGate: ...
    @property
    def engines(self) -> frozenset[str]: ...
    @property
    def active(self) -> bool: ...
    @staticmethod
    def item_engines(item: pytest.Item) -> frozenset[str]: ...
    def selects(self, item: pytest.Item, integration_root: Path) -> bool: ...
    def record(self, nodeid: str, engines: frozenset[str], report: pytest.TestReport) -> None: ...
    def failures(self) -> list[str]: ...

class ServiceVersionMatrix:
    FIXTURES: ClassVar[dict[str, dict[str, str]]]
    DEFAULTS: ClassVar[dict[str, str]] = {"mysql": "8.4", "mariadb": "11.4", "mssql": "2022", "oracle": "23ai"}
    @classmethod
    def fixture_name(cls, family: str, environ: Mapping[str, str]) -> str: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `integration/conftest.py::pytest_configure` → registers `driver_engine` marker → `config.stash[_GATE_KEY] = ConformanceGate.from_environ(os.environ)`.
- `integration/conftest.py::pytest_collection_modifyitems` → `ConformanceGate.selects(item, Path(__file__).parent)` → `config.hook.pytest_deselected(items=...)`.
- `integration/conftest.py::pytest_runtest_makereport` (hookwrapper) → `ConformanceGate.record(report.nodeid, ConformanceGate.item_engines(item), report)`.
- `integration/conftest.py::pytest_sessionfinish` → `ConformanceGate.failures()` → terminal summary lines + `session.exitstatus = pytest.ExitCode.TESTS_FAILED`.
- `vtb_<family>_config` → `ServiceVersionMatrix.fixture_name(family, os.environ)` → `request.getfixturevalue(name)`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Gate unset | `VERTEBRA_CONFORMANCE_ENGINES` absent or `""` | `active is False`; no deselection; `failures() == []` | Offline suite unchanged |
| Whitespace and case | `" MySQL , mssql "` | `engines == frozenset({"mysql", "mssql"})` | Tolerant parsing, strict vocabulary |
| Unknown engine | `"sybase"` | `pytest.UsageError("VERTEBRA_CONFORMANCE_ENGINES contains unsupported engine(s): sybase; supported: duckdb, mariadb, mssql, mysql, oracle")` | No silent no-op |
| PostgreSQL | `"postgres"` | `pytest.UsageError` naming `native-postgres-driver` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS` | Ownership boundary |
| Setup skip | Fixture raises `pytest.skip` for tagged item | Recorded violation `"<nodeid>: skipped"` | Zero-skip evidence |
| Expected failure | Report carries `wasxfail` | Violation `"<nodeid>: xfailed"` (or `"xpassed"`) | No hidden pending engines |
| Empty engine | Engine listed, no `call`-phase pass/fail recorded | Failure `"<engine>: 0 tests executed"` | Lane proves something |
| Untagged integration item | Item under `src/py/tests/integration` with no engine | Deselected when gate active | Lane runs only its engine |
| Items outside integration | Unit tests collected in same session | Never deselected by the gate | Gate scope bounded |
| Unknown version | `VERTEBRA_CONFORMANCE_MARIADB_VERSION=10.6` | `pytest.UsageError` listing `11.3, 11.4, 11.8, 12.2, 12.3, 13.0` | Explicit matrix |
| MariaDB offline | No DSN, Docker unset | `pytest.skip("mariadb conformance requires VERTEBRA_CONFORMANCE_MARIADB_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")` | Never a sample-URL connect |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `VERTEBRA_CONFORMANCE_ENGINES=mssql`, when an `mssql`-tagged item skips during setup, then the session exits `1` and the terminal summary lists that nodeid.
- Given `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_MYSQL_VERSION=9.7`, when `vtb_mysql_config` resolves, then it binds to `mysql_97_service`.
- Given no env vars, when `pytest src/py/tests/integration/db/test_driver_conformance.py -k mariadb` runs, then every MariaDB live case skips with the explicit reason and URL/registry cases pass.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Gate code location | Importable `src/py/tests/integration/conformance_gate.py` with its own canonical test file | Logic inline in `conftest.py`: untestable without `pytester`; importing from `conftest.py` is unsupported. |
| Engine tagging | Marker plus parametrized `engine` attribute | Nodeid substring parsing: `mysql` is a substring of test file names used by MariaDB cases. |
| Report hook | `pytest_runtest_makereport` hookwrapper (has `item`) | `pytest_runtest_logreport`: lacks the `item`, so engine identity is unavailable. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; the gate, variables, and fixtures are documented in `ch05 / t03` (`polyglot-test-matrix.md`, `workflow.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1, R2, R3 | [t01](tasks/t01-conformance-gate-and-versioned-fixtures.md) | `behavior_tdd` | `src/py/tests/integration/test_conformance_gate.py` |
| R4, R5 | [t02](tasks/t02-mariadb-engine-and-matrix-cleanup.md) | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py` |

## 5. Implementation Plan

- [ ] driver-conformance-ci:ch01:t01 — Add the conformance gate, engine tagging, versioned service fixtures, and conftest hooks (tasks/t01-conformance-gate-and-versioned-fixtures.md)
- [ ] driver-conformance-ci:ch01:t02 — Add the MariaDB conformance engine and remove dead conformance surface (tasks/t02-mariadb-engine-and-matrix-cleanup.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/integration/test_conformance_gate.py src/py/tests/integration/db/test_driver_conformance.py -q -rs && uv run --no-sync ruff check src/py/tests/integration && uv run --no-sync ruff format --check src/py/tests/integration` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02` (`0/2` completed).
- **Active Invariants**: Gate inactive without `VERTEBRA_CONFORMANCE_ENGINES`; PostgreSQL fixtures and `_resolve_live_config` PostgreSQL behavior unchanged.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mariadb_config` — `pytest-databases` service fixtures.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Live config resolution and conformance matrix.
[^pd-mysql]: `pytest_databases/docker/mysql.py::mysql_84_service` — Versioned MySQL service fixtures (0.20.0b1).
[^pd-mariadb]: `pytest_databases/docker/mariadb.py::mariadb_114_service` — Versioned MariaDB service fixtures (0.20.0b1).
[^pd-mssql]: `pytest_databases/docker/mssql.py::mssql_2022_service` — SQL Server 2022 and 2025 service fixtures (0.20.0b1).
[^pd-oracle]: `pytest_databases/docker/oracle.py::oracle_23ai_service` — Oracle 23ai Free service fixture (0.20.0b1).
