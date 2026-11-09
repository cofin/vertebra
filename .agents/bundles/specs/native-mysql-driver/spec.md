---
type: Spec
flow_id: native-mysql-driver
title: "Standalone Rust MySQL and MariaDB Wheel (vertebra-mysql)"
description: "Standalone PyO3 crate src/rs/vertebra-mysql (vertebra._mysql + vertebra-core[mysql]) built on mysql_async (rustls+ring, binary protocol, Cloud SQL IAM, KILL QUERY cancellation, VTBARROW ShmCapsule ArrowCursor), replacing pymysql and asyncmy execution."
status: stable
state: in_progress
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - driver-fail-loud
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 3
plan_commit: fd571e3a
state_revision: 2
current_task: "native-mysql-driver:t02"
last_verified_checkpoint: 25d0ac25
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-09T03:25:00Z"
generated:
  by: flow-plan
  at: "2026-11-09T03:25:00Z"
verified:
  - by: agent:flow-plan
    at: "2026-11-09T03:25:00Z"
    commit: fd571e3a
sources:
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Rust client crate selection including mysql_async 0.37.1
  - id: fail-loud-baseline
    resource: .agents/bundles/specs/driver-fail-loud/spec.md
    title: Upstream flow that deletes vertebra_db::mysql and MySQL memory:// paths
  - id: mysql-py
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: Python SyncMySQLDriver and AsyncMySQLDriver target cutover module
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: Shared VTBARROW POSIX SHM zero-copy frame writer and reader
tags:
  - rust
  - mysql
  - mariadb
  - driver
  - wheel
---

# Specification: Standalone Rust MySQL and MariaDB Wheel (`vertebra-mysql`)

<!-- truth: start -->
- **Standalone Optional Wheel**: `src/rs/vertebra-mysql` (`vertebra._mysql`, `vertebra-core[mysql]`) builds on `mysql_async 0.37.1` (`default-rustls-ring`) and `arrow 60.1.0` with `#[pymodule(gil_used = false)]`; `vertebra-db` carries no bespoke MySQL module [^crate-research] [^fail-loud-baseline].
- **Explicit `Sync*` / `Async*` Clients**: `SyncMySQLClient` (`SyncCompletionQueue` via `block_on_db`) and `AsyncMySQLClient` (`AsyncCompletionQueue` + `Notifier` with `"_vertebra_mysql_completion_drainer"`) execute MySQL Handshake v10, `caching_sha2_password` (fast path and TLS/RSA full exchange), `mysql_native_password`, Cloud SQL `:3307` mTLS / direct `:3306` IAM token refresh, `KILL QUERY <conn_id>` out-of-band cancellation, binary prepared statements (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`), and `VTBARROW` `ShmCapsule` transfer [^py-async-rs] [^arrow-shm-rs].
- **Clean-Break Python Cutover**: `SyncMySQLDriver` and `AsyncMySQLDriver` delegate to `vertebra._mysql` via `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")`, removing `pymysql`, `asyncmy`, and `memory://` execution paths [^mysql-py] [^fail-loud-baseline].
<!-- truth: end -->

## Overview

`native-mysql-driver` delivers the standalone Rust MySQL 8.4/9 and MariaDB PyO3 native extension crate `src/rs/vertebra-mysql/` (`vertebra._mysql`, packaged as the `vertebra-mysql` wheel and installed via `vertebra-core[mysql]`) and cuts Python `SyncMySQLDriver` and `AsyncMySQLDriver` over to it, removing `pymysql`, `asyncmy`, and `memory://` execution paths [^crate-research] [^fail-loud-baseline] [^mysql-py].

## Requirements & Contracts

### 1. Standalone Crate, Wheel Isolation, and Workspace Registration (`t01`)
- `src/rs/vertebra-mysql/` compiles as `crate-type = ["cdylib", "rlib"]` with `lib.name = "_mysql"` and `#[pymodule(gil_used = false)] fn _mysql` [^crate-research].
- Dependencies pin `mysql_async = { version = "=0.37.1", default-features = false, features = ["default-rustls-ring"] }` and `arrow-{array,buffer,ipc,schema} = { version = "=60.1.0", default-features = false }`, alongside `vertebra-core` and `vertebra-db = { path = "../vertebra-db", features = ["gcp", "pyo3"] }` [^crate-research].
- `src/rs/vertebra-db/src/mysql/mod.rs` and its re-exports in `src/rs/vertebra-db/src/lib.rs` are deleted so `vertebra-db` holds no bespoke MySQL wire framing [^fail-loud-baseline].
- `src/py/vertebra/_mysql.pyi` provides full PEP 484 type stubs verified by `mypy.stubtest vertebra._mysql`, and sibling optional wheel `pyproject.toml` files (`bigquery`, `bundler`, `spanner`, `storage`) exclude `**/py/vertebra/_mysql.pyi`.
- Root `pyproject.toml` exposes `[project.optional-dependencies].mysql = ["vertebra-mysql==0.3.0"]` (and includes `vertebra-mysql==0.3.0` in `all` and `[tool.uv.sources]`), while `Makefile` and `.agents/skills/review-core/references/areas.toml` register `vertebra-mysql`.

### 2. Connection Lifecycle, Authentication, TLS, Cloud SQL IAM, and `KILL QUERY` Cancellation (`t01`)
- `SyncMySQLClient` executes on the shared multi-thread Tokio runtime via `block_on_db` (`py.detach`) and `AsyncMySQLClient` schedules tasks via `spawn_rust_db_task_with_attr` using `MYSQL_COMPLETION_DRAINER_ATTR = "_vertebra_mysql_completion_drainer"` [^py-async-rs].
- Connection setup supports MySQL Handshake v10, `caching_sha2_password` (fast-auth `0x03`, full auth `0x04` cleartext over TLS, or `0x02` RSA OAEP SHA-1 public-key encryption over plaintext with nonce XOR), and `mysql_native_password` (`SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`).
- TLS negotiation uses `rustls` + `ring` across `disable`, `prefer`, `require`, `verify-ca`, and `verify-full` modes.
- Cloud SQL integration supports both the Cloud SQL Admin API `:3307` ephemeral mTLS dialer (`CloudSqlDialer` / `cloudsql://project:region:instance`) and direct `:3306` IAM token refresh via `GcpTokenCache` when `enable_iam_auth=True` [^py-async-rs].
- `cancel()` and `cancel_query(connection_id)` open a short-lived out-of-band control connection to the same MySQL endpoint and issue `KILL QUERY <connection_id>`, aborting the active statement without poisoning the pooled session.

### 3. Binary Prepared Protocol, Codecs, Multi-Result Sets, Statement Cache, and Error Hierarchy (`t02`)
- `SyncMySQLClient` and `AsyncMySQLClient` implement `execute`, `execute_many`, `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value`, `next_set` / multi-result sets, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, `rollback_to_savepoint`, `ping`, and `close`.
- Parameterized statements use the MySQL binary prepared-statement protocol (`COM_STMT_PREPARE` `0x16`, `COM_STMT_EXECUTE` `0x17`, `COM_STMT_RESET` `0x1a`, `COM_STMT_CLOSE` `0x19`) with a per-connection bounded LRU statement cache (`statement_cache_size`), falling back cleanly to `COM_QUERY` (`0x03`) for multi-statement SQL or non-preparable DDL (`CREATE`, `ALTER`, `DROP`, `SET`, `USE`, `SAVEPOINT`, `START TRANSACTION`, `BEGIN`, `COMMIT`, `ROLLBACK`).
- Positional parameter normalization converts `?`, `%s`, `:name`, and `%(name)s` placeholders consistently and preserves string literals, quoted identifiers, and comments.
- Binary and text row decoding and parameter encoding losslessly handle `NULL`, signed and unsigned `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT`, `FLOAT`/`DOUBLE`, `DECIMAL`/`NEWDECIMAL` (`decimal.Decimal`), `DATE` (`datetime.date`), fractional-second `DATETIME`/`TIMESTAMP` (`datetime.datetime`), signed `TIME` (`datetime.time` / `datetime.timedelta`), `YEAR`, `JSON` (decoded via `serde_json` or raw JSON text), `VARCHAR`/`TEXT`/`ENUM`/`SET`, `BINARY`/`VARBINARY`/`BLOB`/MySQL 9 `VECTOR` (`bytes`), and `BIT` (`bytes` / `int` / `bool`).
- MySQL server error packets (`0xFF`, 16-bit `errno`, `#` + 5-byte `SQLSTATE`, message) map deterministically onto `vertebra.db.exceptions`:
  - `1062`, `1216`, `1217`, `1451`, `1452`, `1048`, `3819` or SQLSTATE `23*` -> `IntegrityError`
  - `1213`, `1205` or SQLSTATE `40001` -> `SerializationFailure`
  - `1317`, `3024`, `1969` or SQLSTATE `70100` -> `QueryCancelledError`
  - `1040`, `1042`, `1043`, `1047`, `1053`, `1152`..`1161`, `2002`, `2003`, `2006`, `2013` or SQLSTATE `08*` -> `ConnectionFailure`
  - `1264`, `1265`, `1292`, `1366`, `1406`, `1690` or SQLSTATE `22*` -> `DataError`
  - `1054`, `1064`, `1146`, `1305` or SQLSTATE `42*` -> `ProgrammingError`
  - All other execution errors -> `OperationalError`

### 4. `VTBARROW` POSIX Shared-Memory Export, Bulk Load, and `BrokerEngineBackend` (`t03`)
- `select_to_arrow(sql, parameters, batch_size)` and `stream_arrow_batches(sql, parameters, batch_size)` decode MySQL binary-protocol rows directly into `arrow 60.1.0` builders (`Int64Builder`, `UInt64Builder`, `Float64Builder`, `BooleanBuilder`, `StringBuilder`, `BinaryBuilder`, `Date32Builder`, `TimestampMicrosecondBuilder`, `Time64MicrosecondBuilder`, `Decimal128Builder`), serialize `RecordBatch` streams into POSIX shared memory via `write_shm_arrow_region_unregistered` (`/vtb-my-{pid:x}-{seq:x}` `<= 31` bytes), and return `vertebra.db.arrow.ArrowCursor` backed by a `PyCapsule` (`"vertebra.ShmRegion"`) [^arrow-shm-rs].
- `load_from_arrow(table, source, columns=None, batch_size=1000)` reads `VTBARROW` SHM capsules, `ArrowCursor` instances, Arrow IPC bytes, or tabular mappings and executes chunked multi-row `INSERT INTO <table> (<cols>) VALUES ...` batches inside a transaction, returning total affected rows.
- `vertebra-db` exposes a `BrokerEngineBackend` registration hook for dialect tag `DIALECT_TAG_MYSQL` (`2`), and `vertebra-mysql` registers its native MySQL backend when imported.

### 5. Python `SyncMySQLDriver` / `AsyncMySQLDriver` Cutover and Conformance (`t04`)
- `src/py/vertebra/db/adapters/mysql/driver.py` (`SyncMySQLDriver`, `AsyncMySQLDriver`) loads `vertebra._mysql` via `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")` and delegates all query, script, transaction, savepoint, Arrow (`select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`), schema inspection, and cancellation operations to `SyncMySQLClient` and `AsyncMySQLClient` [^mysql-py].
- All `pymysql`, `asyncmy`, and `memory://` fallback paths are removed from `src/py/vertebra/db/adapters/mysql/`; `memory://` DSNs raise `ImproperConfigurationError` at configuration and connection boundaries [^fail-loud-baseline].
- `src/py/tests/integration/db/test_driver_conformance.py` sets `pending_flow_id=None` for `mysql` and passes all 7 conformance categories (`crud_and_Select`, `arrow_roundtrip`, `transactions_and_savepoints`, `error_mapping`, `cancellation`, `pool_concurrency`, `schema_hydration`) across both sync and async modes.
- Documentation in `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` reflects the standalone `vertebra._mysql` (`vertebra-core[mysql]`) architecture.

## Tasks

- [x] [t01](tasks/t01-vertebra-mysql-crate-auth-and-cancel.md) — Scaffold `src/rs/vertebra-mysql` (`vertebra._mysql`, `_mysql.pyi`, `[mysql]` extra), connection pool, Handshake v10, `caching_sha2_password` / `mysql_native_password`, `rustls` TLS, Cloud SQL `:3307` mTLS & IAM tokens, `KILL QUERY` cancellation, and remove `vertebra_db::mysql`
- [~] [t02](tasks/t02-mysql-binary-protocol-codecs-and-errors.md) — Implement MySQL binary prepared-statement protocol (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`), per-connection LRU statement cache, multi-result sets, binary/text codecs (`DECIMAL`, temporal, `JSON`, `BLOB`/`VECTOR`, `BIT`, `ENUM`/`SET`), and errno/SQLSTATE -> `DatabaseError` hierarchy mapping
- [ ] [t03](tasks/t03-mysql-vtbarrow-shm-and-bulk-load.md) — Implement `VTBARROW` `ShmCapsule` `ArrowCursor` (`select_to_arrow`, `stream_arrow_batches`), batched `load_from_arrow` ingestion, and `BrokerEngineBackend` registration
- [ ] [t04](tasks/t04-python-mysql-driver-cutover-and-docs.md) — Cut `SyncMySQLDriver` and `AsyncMySQLDriver` over to `vertebra._mysql`, remove `pymysql`/`asyncmy`/`memory://` paths, activate `mysql` in `test_driver_conformance.py`, and update docs and OKF knowledge chapters

[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Rust client crate selection including `mysql_async` 0.37.1.
[^fail-loud-baseline]: `.agents/bundles/specs/driver-fail-loud/spec.md` — Upstream flow that deletes `vertebra_db::mysql` and MySQL `memory://` paths.
[^mysql-py]: `src/py/vertebra/db/adapters/mysql/driver.py` — Python SyncMySQLDriver and AsyncMySQLDriver target cutover module.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared VTBARROW POSIX SHM zero-copy frame writer and reader.
