---
type: Spec
flow_id: driver-conformance-ci
chapter_id: ch02-conformance-harness-and-live-suites
title: "Chapter 02: Conformance Gate, Versioned Fixtures, and Engine Live Suites"
description: "Implement ConformanceGate and ServiceVersionMatrix, clean test_driver_conformance.py with MariaDB support, and add live native suites for DuckDB, MySQL/MariaDB, SQL Server, and Oracle."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_revision: 4
plan_commit: 37972f2a
updated_at: "2026-11-14T17:42:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-14T17:24:00Z"
    commit: 37972f2a
tags:
  - conformance
  - pytest-databases
  - duckdb
  - mysql
  - mariadb
  - mssql
  - oracle
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: "Integration conftest service fixtures and hooks"
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: "Cross-engine driver conformance matrix and _resolve_live_config"
  - id: pyproject-pd
    resource: pyproject.toml
    title: "pytest-databases>=0.20.0b1 dev dependency declaration"
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: "MySQL and MariaDB native driver surface"
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: "SQL Server native TDS driver surface"
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: "Live Oracle native integration suite"
---

# Specification: Chapter 02 — Conformance Gate, Versioned Fixtures, and Engine Live Suites

<!-- truth: start -->
- **Zero-Skip Lane Gate & Version Matrix (`t01`)**: `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`) selects tagged integration tests and fails the session on any skip, xfail, or `0`-test engine; `ServiceVersionMatrix` maps `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION` onto `pytest-databases` `0.20.0b1` fixtures without citing machine-local `.venv` paths [^int-conftest] [^pyproject-pd].
- **Clean Matrix + Embedded/Loopback DuckDB (`t02`)**: `test_driver_conformance.py` adds `mariadb`, removes dead `_DUCKDB_SKIP_REASON` and unused `VERTEBRA_CONFORMANCE_DUCKDB_DSN`, and attaches `driver_engine` markers; `test_duckdb_native.py` covers embedded `:memory:` `VTBARROW`/pipelines/savepoints + offline loopback HTTPS GCS rotation + opt-in `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` [^conformance-suite].
- **Live MySQL/MariaDB, SQL Server & Oracle Suites (`t03`–`t04`)**: Parameterized `test_mysql_native.py`, `test_mssql_native.py`, and tagged `test_oracle_native.py` verify binary prepared statements, LOB/decimal/temporal fidelity, `VTBARROW` `/dev/shm` hygiene, `pipeline()`, active query cancellation, and Oracle AQ `wait_timeout_seconds` liveness [^mysql-driver] [^mssql-driver] [^oracle-native].
<!-- truth: end -->

## 1. Specification, Context & Provenance

After [`ch01-duckdb-gcs-secrets-and-oracle-notify`](../ch01-duckdb-gcs-secrets-and-oracle-notify/spec.md) lands GCS secret rotation and Oracle AQ liveness, Chapter 02 builds the lane-selection and zero-skip conformance gate (`conformance_gate.py` + `conftest.py`), cleans `test_driver_conformance.py`, and adds native edge suites across DuckDB, MySQL/MariaDB, SQL Server, and Oracle while keeping the chapter gate `< 60s` offline [^int-conftest] [^conformance-suite].

## 2. Requirements, Concrete Contracts & Architecture

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
    FIXTURES: ClassVar[dict[str, dict[str, str]]] = {
        "mysql": {"8.0": "mysql_8_service", "8.4": "mysql_84_service", "9.6": "mysql_96_service", "9.7": "mysql_97_service"},
        "mariadb": {
            "11.3": "mariadb_113_service", "11.4": "mariadb_114_service", "11.8": "mariadb_118_service",
            "12.2": "mariadb_122_service", "12.3": "mariadb_123_service", "13.0": "mariadb_130_service",
        },
        "mssql": {"2022": "mssql_2022_service", "2025": "mssql_2025_service"},
        "oracle": {"23ai": "oracle_23ai_service"},
    }
    DEFAULTS: ClassVar[dict[str, str]] = {"mysql": "8.4", "mariadb": "11.4", "mssql": "2022", "oracle": "23ai"}
    @classmethod
    def fixture_name(cls, family: str, environ: Mapping[str, str]) -> str: ...
```

- **Gate Ownership Boundary**: When `"postgres"` is passed in `VERTEBRA_CONFORMANCE_ENGINES` alongside `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` (as in `.github/workflows/ci.yml` `postgres-conformance`), `ConformanceGate.from_environ` delegates `"postgres"` to `evaluate_postgres_zero_skip_gate` in `conftest.py` and excludes `"postgres"` from `self.engines` (remaining inactive when `"postgres"` is the sole engine). Passing `"postgres"` without `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` raises `pytest.UsageError` pointing the caller to `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, and any unknown engine or version raises `pytest.UsageError` listing allowed values.
- **Offline Safety**: When `VERTEBRA_CONFORMANCE_DOCKER != "1"` and `VERTEBRA_CONFORMANCE_<ENGINE>_DSN` is unset, container-backed suites skip cleanly before `request.getfixturevalue(...)` touches Docker; embedded DuckDB and loopback GCS tests run offline in `< 5s`.

## 3. Documentation & OKF Knowledge Plan

Deferred to [`ch04-docs-and-okf-sync`](../ch04-docs-and-okf-sync/spec.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4` Gate & version matrix | `t01` | `tests/integration/{conformance_gate,conftest}.py` | `pytest src/py/tests/unit/db/test_conformance_gate.py` |
| `R4` MariaDB matrix & DuckDB suite | `t02` | `tests/integration/db/{test_driver_conformance,test_duckdb_native}.py` | `pytest test_driver_conformance.py test_duckdb_native.py` |
| `R4` MySQL/MariaDB & MSSQL suites | `t03` | `tests/integration/db/{test_mysql_native,test_mssql_native}.py` | Offline skip collection + Ruff/Mypy (Docker in `/flow:finish`) |
| `R4` Oracle tagging & AQ liveness | `t04` | `tests/integration/db/test_oracle_native.py` | Offline skip collection + `test_conformance_gate.py` |

## 5. Implementation Plan

- [x] [38e02b16] [t01-conformance-gate-and-version-matrix.md](tasks/t01-conformance-gate-and-version-matrix.md) — Implement `ConformanceGate`, `ServiceVersionMatrix`, and `integration/conftest.py` zero-skip hooks and versioned fixtures.
- [x] [38e02b16] [t02-mariadb-matrix-cleanup-and-duckdb-suite.md](tasks/t02-mariadb-matrix-cleanup-and-duckdb-suite.md) — Add `mariadb` to `ENGINE_SPECS`, remove dead conformance surface, and create `test_duckdb_native.py`.
- [x] [38e02b16] [t03-mysql-mariadb-and-mssql-live-suites.md](tasks/t03-mysql-mariadb-and-mssql-live-suites.md) — Create parameterized live `test_mysql_native.py` (MySQL/MariaDB) and `test_mssql_native.py` (SQL Server) suites.
- [x] [38e02b16] [t04-oracle-live-suite-tagging-and-liveness.md](tasks/t04-oracle-live-suite-tagging-and-liveness.md) — Tag `test_oracle_native.py` for `ConformanceGate`, wire `vtb_oracle_config`, and add live AQ `wait_timeout_seconds` liveness coverage.

## 6. Current State & Remaining Plan Snapshot

- **Fast `< 60s` Offline Chapter Gate**: `uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py src/py/tests/integration/db/test_duckdb_native.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_mssql_native.py src/py/tests/integration/db/test_oracle_native.py -q && uv run --no-sync ruff check src/py/tests/integration src/py/tests/unit/db/test_conformance_gate.py && uv run --no-sync mypy src/py/tests/integration` (live Docker & `httpfs` lanes run in `/flow:finish` and CI).

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mysql_config` — Integration `conftest` service fixtures and hooks.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance matrix and `_resolve_live_config`.
[^pyproject-pd]: `pyproject.toml` — `pytest-databases>=0.20.0b1` dev dependency declaration.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL and MariaDB native driver surface.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — SQL Server native TDS driver surface.
[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Live Oracle native integration suite.
