---
type: Spec
flow_id: rust-native-enterprise-olap-drivers
title: "Rust-Native Shared-Memory Oracle, SQL Server (MSSQL) & DuckDB Drivers and Adapters (vertebra-db)"
state: planned
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-23T20:44:00Z
updated_at: 2026-10-23T20:44:00Z
description: "Extend src/rs/vertebra-db, vertebra-py, vertebra.db, and vertebra.extensions.{sqlspec,sqlalchemy} with native Rust shared-memory drivers for Oracle (pure-Rust Thin TNS/TTC wire protocol + TNS/Wallet extraction), SQL Server / MSSQL (pure-Rust tiberius TDS wire protocol with rustls and zero msodbcsql18/unixODBC dependency), and DuckDB (feature-gated duckdb / Arrow C Data Interface single-writer SHM broker host with zero-copy ShmRegion Arrow IPC streaming)."
tags: [feature, rust, database, shm, arrow, oracle, mssql, sqlserver, duckdb]
parent_prd: shm-native-drivers-and-broker-proxy
research: []
---

# Flow: Rust-Native Shared-Memory Oracle, SQL Server (`MSSQL`) & DuckDB Drivers and Adapters (`vertebra-db`)

**Flow ID:** `rust-native-enterprise-olap-drivers`

## Specification

### Code Analysis & Feasibility Synthesis

- **Oracle (`src/rs/vertebra-db/src/oracle.rs` + `vertebra-db::tns`) — 100% Pure-Rust Thin TNS/TTC Wire Protocol**:
  - Historical Rust Oracle drivers (`oracle` crate wrapping ODPI-C) required Oracle Instant Client (`libclntsh.so`) on the host. In 2026, two 100% pure-Rust Thin TNS/TTC drivers exist on `crates.io` requiring **zero Oracle Instant Client / OCI libraries**:
    1. **`oracledb`** (`v26.0.0-beta.4`, `oracle/rust-oracledb`): Official Oracle pure-Rust Thin driver speaking TNS/TTC directly over TCP/TLS (`rustls`), supporting Oracle 12c–26ai, `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`), `JSON`/`OSON`, `CLOB`/`BLOB`, DRCP, and optional `features = ["arrow"]` (`arrow-array` / `arrow-schema`) batch extraction.
    2. **`oracle-rs`** (`0.1`, `stiang/oracle-rs` + `deadpool-oracle`): Pure-Rust async Tokio TNS driver with `rustls` TLS, Autonomous Database Wallet (`ewallet.pem` + `tnsnames.ora`), statement caching, `VECTOR`, `JSON`, and `LOB` support.
  - Paired with `vertebra-db`'s existing pure-Rust `parse_tnsnames_ora`, `resolve_tns_alias`, and `OracleConnectDescriptor` (`6737c0c:src/rs/vertebra-db/src/lib.rs:272-422`) plus in-memory Autonomous Database Wallet archive extraction, `vertebra-db` provides a zero-OCI native Rust SHM Oracle driver (`SyncOracleDriver`, `AsyncOracleDriver`, `VertebraOracleSyncConfig`, `VertebraOracleAsyncConfig`, `oracle+vtb`) alongside Tier-2 `ProxySyncConfig[OracleSyncConfig]` / `ProxyAsyncConfig[OracleAsyncConfig]`.
- **SQL Server / MSSQL (`src/rs/vertebra-db/src/mssql.rs`) — 100% Pure-Rust TDS Wire Protocol (`tiberius`)**:
  - Python SQL Server drivers (`pyodbc`, `mssql-python`) require installing Microsoft's proprietary C++ `msodbcsql18` OS driver and `unixODBC`.
  - **`tiberius`** (`0.12`, `default-features = false`, `features = ["rustls", "tds73", "chrono"]`) + `tokio-util` (`compat`) is a **100% pure-Rust asynchronous Tabular Data Stream (TDS) wire-protocol implementation**.
  - Integrating `tiberius` in `src/rs/vertebra-db/src/mssql.rs` gives Vertebra a zero-C-dependency SQL Server / Azure SQL / Cloud SQL for SQL Server driver (`MssqlConfig`, `SyncMssqlDriver`, `AsyncMssqlDriver`, `VertebraMssqlSyncConfig`, `VertebraMssqlAsyncConfig`, `mssql+vtb`) that streams TDS rows directly into SHM ring slots or `ShmRegion` Arrow IPC buffers.
- **DuckDB (`src/rs/vertebra-db/src/duckdb.rs`) — Shared Single-Writer SHM Broker Host + Zero-Copy Arrow IPC**:
  - DuckDB enforces a single-process write lock on database files, which prevents multi-worker Python servers from sharing a read/write DuckDB catalog across worker processes.
  - By hosting the DuckDB catalog inside the Rust SHM broker (using Cargo feature `duckdb` with `duckdb::Connection::query_arrow` or the zero-compile Arrow C Data Interface `FFI_ArrowArrayStream` so default `make dev` builds never pay a 5-minute C++ amalgamation compile), all worker processes query and mutate a single shared DuckDB instance over lock-free SHM rings and receive `RecordBatch` streams zero-copy via `ShmRegion` Arrow IPC (`DuckdbConfig`, `SyncDuckdbDriver`, `AsyncDuckdbDriver`, `VertebraDuckdbSyncConfig`, `VertebraDuckdbAsyncConfig`, `duckdb+vtb`).

### Relevant Patterns

- [PyO3 0.30.1 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md)
- [Lock-Free Shared Memory & Sharded Concurrency](../../knowledge/patterns/rust/lock-free-shm-and-sharding.md)
- [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md)
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)

### Requirements

1. **Native Rust Thin Oracle Driver & Wallet/TNS Engine (`oracle.rs`)**:
   - Implement `src/rs/vertebra-db/src/oracle.rs` combining `parse_tnsnames_ora`, `resolve_tns_alias`, `OracleConnectDescriptor`, in-memory Autonomous Database Wallet (`ewallet.pem` / `tnsnames.ora`) extraction, and pure-Rust TNS/TTC wire execution (`oracledb` / `oracle-rs`) with `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`), `OSON`, zero-RTT TTC `econtext_id`/`module`/`action`/`client_identifier` piggybacking, and `ShmRegion` Arrow IPC result streaming.
   - Expose `OracleConfig`, `SyncOracleDriver`, and `AsyncOracleDriver` in `src/py/vertebra/db/oracle.py` backed by `vertebra-db` and `ShmBrokerTransport`.
2. **Native Pure-Rust TDS SQL Server (`MSSQL`) Driver (`mssql.rs`)**:
   - Implement `src/rs/vertebra-db/src/mssql.rs` using `tiberius` (`rustls`, `tds73`) over Tokio streams (including Cloud SQL for SQL Server `:3307` TLS streams), supporting positional `@P1, @P2` parameter binding, connection pooling, and SHM ring / `ShmRegion` Arrow IPC result framing with zero `msodbcsql18` or `unixODBC` host dependencies.
   - Expose `MssqlConfig`, `SyncMssqlDriver`, and `AsyncMssqlDriver` in `src/py/vertebra/db/mssql.py`.
3. **Native SHM-Brokered DuckDB Driver & Arrow C Data Interface (`duckdb.rs`)**:
   - Implement `src/rs/vertebra-db/src/duckdb.rs` hosting a single-writer / multi-reader DuckDB instance in the SHM broker with session pragma initialization (`memory_limit`, `threads`, `temp_directory`, `httpfs_connection_caching`, `enable_http_metadata_cache`, `parquet_metadata_cache`) and zero-copy Arrow C Data Interface / `ShmRegion` Arrow IPC result streaming, keeping C++ `libduckdb-sys` compilation strictly feature-gated (`--features duckdb`) so default Cargo builds stay fast.
   - Expose `DuckdbConfig`, `SyncDuckdbDriver`, and `AsyncDuckdbDriver` in `src/py/vertebra/db/duckdb.py`.
4. **SQLSpec `Vertebra*Config` and SQLAlchemy `+vtb` Dialects for Oracle, MSSQL & DuckDB**:
   - Register `oracle`, `mssql`/`sqlserver`, and `duckdb` URL schemes in `DatabaseRegistry` (`src/py/vertebra/db/_registry.py`).
   - Expose `VertebraOracleSyncConfig` / `VertebraOracleAsyncConfig`, `VertebraMssqlSyncConfig` / `VertebraMssqlAsyncConfig`, and `VertebraDuckdbSyncConfig` / `VertebraDuckdbAsyncConfig` in `src/py/vertebra/extensions/sqlspec/adapters.py`, plus `OracleVtbDialect`, `MssqlVtbDialect`, and `DuckdbVtbDialect` (`oracle+vtb`, `mssql+vtb`, `duckdb+vtb`) in `src/py/vertebra/extensions/sqlalchemy/dialects.py`.

## Implementation Plan

### Phase 1: Native Rust Thin Oracle & Pure-Rust TDS SQL Server (`MSSQL`) Drivers
- [ ] 1.1 Implement pure-Rust Thin Oracle driver (`src/rs/vertebra-db/src/oracle.rs`) with TNS/Wallet extraction, 26ai `VECTOR`/`OSON`, zero-RTT TTC trace attributes, and SHM Arrow IPC bindings in `src/py/vertebra/db/oracle.py`
- [ ] 1.2 Implement pure-Rust TDS SQL Server (`tiberius`) driver (`src/rs/vertebra-db/src/mssql.rs`) with `rustls`, `@P1` parameter binding, and SHM Arrow IPC bindings in `src/py/vertebra/db/mssql.py`

### Phase 2: SHM-Brokered DuckDB Arrow Driver & Unified SQLSpec / SQLAlchemy `+vtb` Adapters
- [ ] 2.1 Implement SHM-brokered single-writer DuckDB Arrow driver (`src/rs/vertebra-db/src/duckdb.rs` + `src/py/vertebra/db/duckdb.py`) and wire `DatabaseRegistry`, `Vertebra{Oracle,Mssql,Duckdb}*Config`, and `{oracle,mssql,duckdb}+vtb` SQLAlchemy dialects

## Continuity Snapshot

- **Active Flow**: `rust-native-enterprise-olap-drivers` (`planned`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `null`
- **Decisions**:
  - Use pure-Rust Thin TNS/TTC (`oracledb` `v26.0` / `oracle-rs` + `vertebra-db::tns`) for Oracle and pure-Rust TDS (`tiberius` with `rustls`) for SQL Server so neither engine requires proprietary C client libraries (`libclntsh` or `msodbcsql18`/`unixODBC`).
  - Gate `libduckdb-sys` compilation behind optional Cargo feature `duckdb` while streaming Arrow batches via the zero-compile Arrow C Data Interface (`FFI_ArrowArrayStream`) and `ShmRegion` Arrow IPC so multi-worker Python servers share one DuckDB catalog without file-lock contention or slow default Cargo builds.
- **Recent Discoveries**:
  - `6737c0c:src/rs/vertebra-db/src/lib.rs` already contains tested Rust implementations of `parse_tnsnames_ora`, `resolve_tns_alias`, and `OracleConnectDescriptor`.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Activate flow after `rust-native-shm-drivers` Task `1.2` and claim Task `1.1`.
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
  - `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
