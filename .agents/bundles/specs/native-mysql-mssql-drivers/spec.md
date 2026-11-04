---
type: Spec
flow_id: native-mysql-mssql-drivers
title: Standalone Rust MySQL and MSSQL Wheels (vertebra-mysql, vertebra-mssql)
description: Standalone PyO3 crates src/rs/vertebra-mysql (vertebra._mysql + vertebra-core[mysql]) and src/rs/vertebra-mssql (vertebra._mssql + vertebra-core[mssql]) built on mysql_async (rustls+ring, binary protocol, Cloud SQL IAM) and tiberius (rustls, TDS 7.4/8.0, RPC, token stream, bulk load), replacing pymysql and the sqlite3 stand-in.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-04T01:17:00Z
tags: [rust, mysql, mssql, driver, wheel]
---

# Flow Charter: Standalone Rust MySQL and MSSQL Wheels (`vertebra-mysql`, `vertebra-mssql`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **MySQL / MariaDB (`src/rs/vertebra-mysql/` -> `vertebra._mysql` + `vertebra-core[mysql]`).** `mysql_async` 0.37.1 (`default-features = false`, `default-rustls-ring`), exposing `SyncMySQLClient` (`SyncCompletionQueue`) and `AsyncMySQLClient` (`AsyncCompletionQueue` via `vertebra_db::py_async` with `"_vertebra_mysql_completion_drainer"`), binary-protocol row -> `VTBARROW` `ShmCapsule` `ArrowCursor` decoding, Cloud SQL `:3307` mTLS / IAM token authentication (`vertebra_db::connection` / `vertebra_db::dialer`), `KILL QUERY` cancellation, and MariaDB conformance.
- **MSSQL (`src/rs/vertebra-mssql/` -> `vertebra._mssql` + `vertebra-core[mssql]`).** `tiberius` 0.13.0 (`default-features = false`, `tds80`, `rustls`, `chrono`, `rust_decimal`), exposing `SyncMSSQLClient` (`SyncCompletionQueue`) and `AsyncMSSQLClient` (`AsyncCompletionQueue` via `vertebra_db::py_async` with `"_vertebra_mssql_completion_drainer"`), token stream -> `VTBARROW` `ShmCapsule` `ArrowCursor` decoding, Cloud SQL `:3307` mTLS dialer integration, bulk load (`BulkLoadRequest`), and `ATTENTION` cancellation.
- **Per-driver wheel packaging.** Each engine builds as its own workspace PyO3 `cdylib` + `rlib` crate (`#[pymodule(gil_used = false)]`, `src/py/vertebra/_{mysql,mssql}.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].{mysql,mssql}`, `uv lock --offline`, and `areas.toml` registration) so `vertebra-core` stays free of MySQL and TDS wire crates when unused.

## Scope

1. **MySQL crate scaffold, connection & authentication (`src/rs/vertebra-mysql/`).** `SyncMySQLClient` and `AsyncMySQLClient`; Handshake v10, `caching_sha2_password` and `mysql_native_password`, `rustls` + `ring` TLS, Cloud SQL `:3307` mTLS + IAM token refresh (`SyncGcpTokenCache` / `AsyncGcpTokenCache`), and `KILL QUERY <conn_id>` cancellation.
2. **MySQL protocol, codecs & Arrow.**
   - Text (`COM_QUERY`) and binary prepared-statement (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`) execution, multi-result sets, statement caching, `BrokerEngineBackend` registration, and MySQL errno / SQLSTATE -> `DatabaseError` mapping (`vertebra._vertebra`).
   - Codecs for `DECIMAL`, fractional-second `DATETIME`/`TIMESTAMP`/`TIME`, `JSON`, `BLOB`/`TEXT`, `BIT`, and `ENUM`/`SET`, plus `select_to_arrow` / `stream_arrow_batches` (`VTBARROW` `ShmCapsule` `ArrowCursor`) and `load_from_arrow`.
3. **MSSQL crate scaffold, connection & authentication (`src/rs/vertebra-mssql/`).** `SyncMSSQLClient` and `AsyncMSSQLClient`; TDS 7.4 / 8.0 `PRELOGIN`, TLS negotiation, `LOGIN7` SQL authentication, `ENVCHANGE` routing, and Cloud SQL `:3307` mTLS support.
4. **MSSQL protocol, codecs & Arrow.**
   - SQL batch and `sp_executesql` RPC execution with `@P1` parameter rewriting (`vertebra_db::mssql::rewrite_mssql_placeholders`), transaction descriptors, `ATTENTION` cancellation, `BrokerEngineBackend` registration, and TDS error number -> `DatabaseError` mapping (`vertebra._vertebra`).
   - Codecs for `numeric`/`decimal`/`money`, `datetime2`/`datetimeoffset`, `uniqueidentifier`, `(n)varchar(max)` PLP, `varbinary`, `xml`, and `json`, plus `BulkLoad` `load_from_arrow` and `VTBARROW` `ShmCapsule` `select_to_arrow` / `stream_arrow_batches`.
5. **Python driver cutovers.** Cut `vertebra/db/adapters/mysql/driver.py` (`SyncMySQLDriver`, `AsyncMySQLDriver`) over to `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")` and `vertebra/db/adapters/mssql/driver.py` (`SyncMSSQLDriver`, `AsyncMSSQLDriver`) over to `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")`, removing `pymysql`, `memory://`, `_in_memory_rows`, and `sqlite3.connect(":memory:")`, and activating `mysql` and `mssql` in `test_driver_conformance.py`.

## Public Seams

`vertebra._mysql` (`SyncMySQLClient`, `AsyncMySQLClient`), `vertebra._mssql` (`SyncMSSQLClient`, `AsyncMSSQLClient`), `SyncMySQLDriver`, `AsyncMySQLDriver`, `SyncMSSQLDriver`, and `AsyncMSSQLDriver`.

## Dependencies

`db-layout-and-contract` (`refactor/sync-async-names @ 08ac03a`) and `integration-remediation/db-engine-primitives`.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for MySQL 8.4/9, MariaDB, and SQL Server 2022/2025 via `pytest-databases`.
- Golden wire/unit tests cover MySQL handshake + binary rows and TDS `PRELOGIN`/`LOGIN7`/RPC + error mapping.
- `mypy.stubtest vertebra._mysql vertebra._mssql`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-mysql` (`vertebra._mysql`, `_mysql.pyi`, `[mysql]` extra + `uv.lock`), connection, auth, TLS, Cloud SQL IAM, and `KILL QUERY` cancellation.
- t02 MySQL binary prepared protocol, codecs, and `DatabaseError` mapping.
- t03 MySQL `VTBARROW` `ShmCapsule` export/load + Python `SyncMySQLDriver`/`AsyncMySQLDriver` cutover.
- t04 Scaffold `src/rs/vertebra-mssql` (`vertebra._mssql`, `_mssql.pyi`, `[mssql]` extra + `uv.lock`), TDS `PRELOGIN`/TLS/`LOGIN7`, and Cloud SQL mTLS integration.
- t05 MSSQL SQL batch, `sp_executesql` RPC, transactions, `ATTENTION` cancel, and `DatabaseError` mapping.
- t06 MSSQL codecs, `VTBARROW` `ShmCapsule` export, and `BulkLoad` `load_from_arrow`.
- t07 Python `SyncMSSQLDriver`/`AsyncMSSQLDriver` cutover + conformance activation.
