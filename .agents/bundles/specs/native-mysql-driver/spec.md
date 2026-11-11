---
type: Spec
flow_id: native-mysql-driver
title: "Standalone Rust MySQL and MariaDB Wheel (vertebra-mysql)"
description: "Standalone PyO3 crate src/rs/vertebra-mysql (vertebra._mysql + vertebra-core[mysql]) built on mysql_async (rustls+ring, binary protocol, Cloud SQL IAM, KILL QUERY cancellation, VTBARROW ShmCapsule ArrowCursor), replacing pymysql and memory:// execution."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs
    title: MySQL packet framing and caching_sha2 codecs in vertebra-db
  - id: mysql-py
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: Python SyncMySQLDriver and AsyncMySQLDriver target cutover module
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
tags:
  - rust
  - mysql
  - mariadb
  - driver
  - wheel
---

# Flow Charter: Standalone Rust MySQL and MariaDB Wheel (`vertebra-mysql`)

<!-- truth: start -->
- **Standalone Optional Wheel**: `src/rs/vertebra-mysql` (`vertebra._mysql`, `vertebra-core[mysql]`) builds on `mysql_async 0.37.1` (`default-rustls-ring`) with `#[pymodule(gil_used = false)]` [^mysql-rs].
- **Explicit `Sync*` / `Async*` Clients**: `SyncMySQLClient` and `AsyncMySQLClient` support Handshake v10, `caching_sha2_password`, Cloud SQL `:3307` mTLS / IAM tokens, `KILL QUERY` cancellation, binary prepared statements, and `VTBARROW` `ShmCapsule` transfer [^py-async-rs].
- **Clean-Break Python Cutover**: `SyncMySQLDriver` and `AsyncMySQLDriver` remove `pymysql`, `asyncmy`, and `memory://` execution paths [^mysql-py].
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **MySQL / MariaDB (`src/rs/vertebra-mysql/` -> `vertebra._mysql` + `vertebra-core[mysql]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `mysql_async` `0.37.1` (`default-features = false`, `default-rustls-ring`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_mysql.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].mysql`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration so `vertebra-core` remains free of MySQL wire crates when unused.
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Expose `SyncMySQLClient` (`SyncCompletionQueue`) and `AsyncMySQLClient` (`AsyncCompletionQueue` via `vertebra_db::py_async` with `"_vertebra_mysql_completion_drainer"`), binary-protocol row -> `VTBARROW` `ShmCapsule` `ArrowCursor` decoding, Cloud SQL `:3307` mTLS / direct `:3306` IAM token authentication (`SyncGcpTokenCache` / `AsyncGcpTokenCache`), `KILL QUERY <conn_id>` cancellation, and MariaDB conformance.

## Scope

1. **Crate scaffold, connection & authentication (`src/rs/vertebra-mysql/`).**
   - `SyncMySQLClient` and `AsyncMySQLClient` in `vertebra._mysql`; Handshake v10, `caching_sha2_password` (fast path and TLS/RSA full exchange) and `mysql_native_password`, `rustls` + `ring` TLS, Cloud SQL `:3307` mTLS + IAM token refresh (`SyncGcpTokenCache` / `AsyncGcpTokenCache`), and `KILL QUERY <conn_id>` cancellation.
2. **Protocol, codecs & Arrow.**
   - Text (`COM_QUERY`) and binary prepared-statement (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`) execution, multi-result sets, statement caching, `BrokerEngineBackend` registration, and MySQL errno / SQLSTATE -> `DatabaseError` mapping (`vertebra._vertebra`).
   - Codecs for `DECIMAL`, fractional-second `DATETIME`/`TIMESTAMP`/`TIME`, `JSON`, `BLOB`/`TEXT`, `BIT`, and `ENUM`/`SET`, plus `select_to_arrow` / `stream_arrow_batches` (`VTBARROW` `ShmCapsule` `ArrowCursor`) and `load_from_arrow`.
3. **Python driver cutover.**
   - Cut `vertebra/db/adapters/mysql/driver.py` (`SyncMySQLDriver`, `AsyncMySQLDriver`) over to `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")`, removing `pymysql`, `asyncmy`, `memory://`, and `_in_memory_rows`, and activating `mysql` in `src/py/tests/integration/db/test_driver_conformance.py`.

## Public Seams

`vertebra._mysql` (`SyncMySQLClient`, `AsyncMySQLClient`), `SyncMySQLDriver`, and `AsyncMySQLDriver`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`) and `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`).

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for MySQL 8.4/9 and MariaDB via `pytest-databases`.
- Golden wire/unit tests cover MySQL Handshake v10, `caching_sha2_password`, binary prepared rows, `KILL QUERY` cancellation, and errno/SQLSTATE -> `DatabaseError` mapping.
- Wheel isolation check confirms `vertebra-mysql` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._mysql`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-mysql` (`vertebra._mysql`, `_mysql.pyi`, `[mysql]` extra + `uv.lock` + `release.yaml`/`ci.yml`), connection, auth, TLS, Cloud SQL IAM, and `KILL QUERY` cancellation.
- t02 MySQL binary prepared protocol, multi-result sets, statement cache, codecs, and `DatabaseError` mapping.
- t03 MySQL `VTBARROW` `ShmCapsule` export/load and `BrokerEngineBackend` registration.
- t04 Python `SyncMySQLDriver` / `AsyncMySQLDriver` cutover, `pymysql`/`memory://` removal, conformance activation, and docs/OKF update.

[^mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs` — MySQL packet framing and caching_sha2 codecs in vertebra-db.
[^mysql-py]: `src/py/vertebra/db/adapters/mysql/driver.py` — Python SyncMySQLDriver and AsyncMySQLDriver target cutover module.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
