---
type: Spec
flow_id: native-oracle-driver
title: Standalone Rust Oracle Thin Wheel (vertebra-oracle)
description: Standalone PyO3 crate src/rs/vertebra-oracle (vertebra._oracle + vertebra-core[oracle]) built on oracledb =26.0.0-beta.5 with TLS/wallets, binds/defines, LOBs, OSON/VECTOR, DRCP, SyncOraclePipeline / AsyncOraclePipeline, TxEventQ notifications, and Arrow 59 -> VTBARROW ShmCapsule results, replacing the sqlite3 stand-in.
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
updated_at: 2026-11-04T01:15:00Z
tags: [rust, oracle, driver, wheel, pipeline]
---

# Flow Charter: Standalone Rust Oracle Thin Wheel (`vertebra-oracle`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Official pure-Rust crate in its own wheel (`vertebra._oracle` + `vertebra-core[oracle]`).** Build `src/rs/vertebra-oracle` (`#[pymodule(gil_used = false)]`, `src/py/vertebra/_oracle.pyi`, `[project.optional-dependencies].oracle`, `uv lock --offline`) on Oracle's official `oracledb` crate pinned `=26.0.0-beta.5` (`arrow` feature, `UPL-1.0 OR Apache-2.0`, zero Oracle Instant Client / ODPI-C dependency). Isolating `vertebra-oracle` in its own wheel keeps `oracledb` and `arrow 59` completely out of `vertebra-core` and `vertebra-duckdb` (`arrow 60`).
- **Leading `Sync*` / `Async*` clients (`SyncOracleClient` & `AsyncOracleClient`) & `VTBARROW` `ShmCapsule` IPC**. Although `oracledb` `26.0.0-beta.5` uses blocking Rust sockets internally, Python gets full non-blocking `AsyncOracleDriver` (`asyncio` / `oracle+vertebra_async`) alongside `SyncOracleDriver`: each pooled connection runs on a dedicated Rust worker thread bridged synchronously via `SyncCompletionQueue` (`SyncOracleClient`) and asynchronously to `asyncio.Future` via `AsyncCompletionQueue` + `Notifier` (`AsyncOracleClient` via `vertebra_db::py_async` with `"_vertebra_oracle_completion_drainer"`) and `_DbTaskCanceller` calling out-of-band `connection.break_execution()`. `query_arrow` (`arrow 59`) batches serialize directly into `VTBARROW` IPC v5 shared memory and return `vertebra._vertebra.ArrowCursor` via `ShmCapsule` with zero copies.
- **Pipelining (`SyncPipelineCapable` & `AsyncPipelineCapable`)**. `SyncOracleDriver.pipeline()` (`SyncOraclePipeline`) and `AsyncOracleDriver.pipeline()` (`AsyncOraclePipeline`) implement `SyncPipelineCapable` and `AsyncPipelineCapable` using the same API as PostgreSQL, executing queued operations sequentially on the connection with a pinning test that flags upstream `oracledb` 26ai pipelining support when released.
- **Codec consolidation.** Consume `vertebra-db/src/oracle/{mod,tns}.rs` codecs (`OracleWallet`, `parse_tnsnames_ora`, `OracleSessionAttrs`, OSON, 26ai `VECTOR`) where `oracledb` lacks coverage (wallet extraction, token auth, AQ/TxEventQ).

## Scope

1. **Crate scaffold, wallets, and authentication (`src/rs/vertebra-oracle/`).**
   - `SyncOracleClient` and `AsyncOracleClient` in `vertebra._oracle`; TNS connect descriptors and `tnsnames.ora` alias resolution (`vertebra_db::oracle::tns`), TCPS mTLS with in-memory Autonomous Database Wallet (`ewallet.pem` / `cwallet.sso`) extraction, O5LOGON password + IAM/OAuth2 bearer token authentication (`SyncGcpTokenCache` / `AsyncGcpTokenCache`), and piggybacked `CLIENT_IDENTIFIER` / `MODULE` / `ACTION` / `ECID` session attributes.
2. **Execution, PL/SQL, and error mapping.**
   - Execute with positional/named binds, prefetch/array size tuning, `commit`/`rollback`, PL/SQL anonymous blocks and `DBMS_SQL` implicit results, `cancel()` (`connection.break_execution()`), and `ORA-XXXXX` error mapping onto `DatabaseError` subclasses (`vertebra._vertebra`, populating `vendor_code` and `is_transient`).
3. **Types.**
   - `NUMBER` (`Decimal`/`int`/`float`), `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW`, `DATE`/`TIMESTAMP`/`TIMESTAMP WITH [LOCAL] TIME ZONE`, `INTERVAL`, 23ai+ `BOOLEAN`, inline and streaming `CLOB`/`BLOB`/`NCLOB`, `JSON`/OSON, and 26ai `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`).
4. **Pooling & DRCP.**
   - DRCP (`:pooled`, connection class, purity), statement caching, and `BrokerEngineBackend` registration for `broker-pool-sharing`.
5. **Sequential `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`).**
   - `SyncOraclePipeline` (`SyncPipeline`) and `AsyncOraclePipeline` (`AsyncPipeline`) context managers executing queued statements sequentially on the leased connection, returning ordered results and per-operation errors, paired with an upstream capability tracking test.
6. **Bulk & Arrow.**
   - Array DML `execute_many` with batch rowcounts, `select_to_arrow` / `stream_arrow_batches` serializing `arrow 59` `RecordBatch` frames into `VTBARROW` `ShmCapsule` `ArrowCursor`, and `load_from_arrow`.
7. **Notifications (`vertebra/db/adapters/oracle/notify.py`).**
   - Oracle TxEventQ / `DBMS_AQ` enqueue/dequeue implementing `NotificationSource` (complementing `OracleEmbeddingPushdown` from `db-engine-primitives:t04`).
8. **Python driver cutover.**
   - Cut `vertebra/db/adapters/oracle/driver.py` (`SyncOracleDriver`, `AsyncOracleDriver`) over to `ensure_module("vertebra._oracle", package="vertebra-core[oracle]", extra="oracle")`, deleting `sqlite3.connect(":memory:")` and activating `oracle` in `test_driver_conformance.py`.

## Public Seams

`vertebra._oracle` (`SyncOracleClient`, `AsyncOracleClient`), `SyncOracleDriver`, `AsyncOracleDriver`, `driver.pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`), and `OracleNotificationSource` (`NotificationSource`).

## Dependencies

`db-layout-and-contract` (`refactor/sync-async-names @ 08ac03a`) and `integration-remediation/db-engine-primitives`.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively against Oracle 23ai/26ai Free via `pytest-databases`.
- `driver.pipeline()` returns ordered results and raises typed `DatabaseError` with `vendor_code` on `ORA-*` failures.
- `mypy.stubtest vertebra._oracle`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-oracle` (`vertebra._oracle`, `_oracle.pyi`, `[oracle]` extra + `uv.lock`), `oracledb` crate wiring, TNS + wallet mTLS, and session attributes.
- t02 Worker-thread execution bridge (`SyncCompletionQueue` + `AsyncCompletionQueue`), binds/fetch, transactions, `cancel()`, and `ORA-*` `DatabaseError` mapping.
- t03 Scalar, temporal, LOB, OSON, and 26ai `VECTOR` codecs.
- t04 PL/SQL blocks, implicit results, and array DML `execute_many`.
- t05 DRCP, statement caching, and `BrokerEngineBackend` pool integration.
- t06 Sequential `SyncOraclePipeline` / `AsyncOraclePipeline` (`SyncPipelineCapable` / `AsyncPipelineCapable`) + upstream 26ai pipelining tracking test.
- t07 `arrow 59` -> `VTBARROW` `ShmCapsule` `ArrowCursor` + `OracleNotificationSource` (TxEventQ).
- t08 Python `SyncOracleDriver`/`AsyncOracleDriver` cutover + conformance activation.
