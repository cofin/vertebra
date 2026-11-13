---
type: Task
id: "driver-conformance-ci:ch02:t03"
flow_id: driver-conformance-ci
chapter_id: ch02-conformance-harness-and-live-suites
title: "Create live MySQL/MariaDB and SQL Server native integration suites"
description: "Implement parameterized test_mysql_native.py and test_mssql_native.py covering binary prepared statements, engine types, Arrow SHM, pipelines, and active query cancellation."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "driver-conformance-ci:ch02:t01"
files:
  - src/py/tests/integration/db/test_mysql_native.py
  - src/py/tests/integration/db/test_mssql_native.py
tests:
  - src/py/tests/unit/db/test_conformance_gate.py
read_set:
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/conformance_gate.py
estimated_loc: 245
tags:
  - mysql
  - mariadb
  - mssql
  - integration
sources:
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: "SyncMySQLDriver and AsyncMySQLDriver native protocol methods"
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: "SyncMSSQLDriver and AsyncMSSQLDriver native TDS methods"
---

# Task `t03`: Create Live MySQL/MariaDB and SQL Server Native Integration Suites

## Objective & Scope Boundary

Author `src/py/tests/integration/db/test_mysql_native.py` and `src/py/tests/integration/db/test_mssql_native.py` using `@pytest.mark.parametrize` and `@pytest.fixture(params=...)` (zero `for`-loop case/mode iteration) per [`../spec.md` §2](../spec.md) [^mysql-driver] [^mssql-driver]. Out of scope: Oracle suite tagging (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/{mysql,mssql}/driver.py` — `fetch_all_sets`, `cancel_query` / `cancel`, `lastrowid`, savepoints, `pipeline()`, and Arrow APIs.
- `src/py/tests/integration/{conftest,conformance_gate}.py` — `vtb_mysql_config`, `vtb_mariadb_config`, `vtb_mssql_config`, and `driver_engine` markers.

## Contracts & Edge Cases

- **`test_mysql_native.py`**: Parameterize `mysql_family_config` across `pytest.param("mysql", marks=pytest.mark.driver_engine("mysql"))` and `pytest.param("mariadb", marks=pytest.mark.driver_engine("mariadb"))`; skip offline when neither DSN nor `VERTEBRA_CONFORMANCE_DOCKER == "1"` is set; cover binary `COM_STMT_*` prepared roundtrips, `utf8mb4` 4-byte emoji, `DECIMAL(65,30)`, `DATETIME(6)`, `JSON`, `lastrowid`, multi-result `CALL` (`fetch_all_sets`), `VTBARROW` `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow` (`/dev/shm` clean), `pipeline()`, `SELECT SLEEP(30)` `KILL QUERY` cancellation (`< 10s`), and duplicate-PK `IntegrityError` (`vendor_code == 1062`), dropping `vtb_live_*` tables in `finally`.
- **`test_mssql_native.py` (`pytestmark = [pytest.mark.driver_engine("mssql")]`)**: Skip offline when neither `VERTEBRA_CONFORMANCE_MSSQL_DSN` nor `VERTEBRA_CONFORMANCE_DOCKER == "1"` is set; cover TDS `@P1` `sp_executesql`, `DATETIMEOFFSET` non-UTC offsets, `DECIMAL(38,10)`, `NVARCHAR(MAX)` & `VARBINARY(MAX)` `> 64 KiB` LOBs, `BulkLoad` `load_from_arrow`, `VTBARROW` `select_to_arrow` / `stream_arrow_batches`, `pipeline()`, savepoint rollback, `WAITFOR DELAY '00:00:30'` `ATTENTION` cancellation (`QueryCancelledError` in `< 10s`), and duplicate-PK `IntegrityError` (`vendor_code == 2627`), dropping `vtb_live_*` tables in `finally`.

## Test Matrix

- Verify `ConformanceGate.item_engines` selects `[mysql-*]` items under `VERTEBRA_CONFORMANCE_ENGINES=mysql`, `[mariadb-*]` items under `mariadb`, and `test_mssql_native.py` items under `mssql`, and that offline runs skip cleanly without touching Docker.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_mssql_native.py -q && uv run --no-sync ruff check src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_mssql_native.py && uv run --no-sync mypy src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_mssql_native.py
```

## Acceptance Criteria

- [ ] Both suites use `@pytest.mark.parametrize` / `@pytest.fixture(params=...)` with zero `for`-loop test iteration and clean `finally` table teardown.
- [ ] Offline pytest collection/execution, Ruff, and Mypy pass cleanly in `< 10s`.

[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — `SyncMySQLDriver` and `AsyncMySQLDriver` native protocol methods.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — `SyncMSSQLDriver` and `AsyncMSSQLDriver` native TDS methods.
