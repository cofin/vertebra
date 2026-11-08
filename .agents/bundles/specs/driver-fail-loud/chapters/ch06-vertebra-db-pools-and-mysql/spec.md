---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch06-vertebra-db-pools-and-mysql
title: "Chapter 06: vertebra-db Pools and MySQL Removal"
description: Delete PostgresPool, DuckdbBrokerPool, SqlitePool, MysqlPool, their dialect tags, the OracleWalletMaterial struct, the mysql module, and the sha2 and rmp-serde dependencies from vertebra-db, and rewrite the crate doc, module docs, and Cargo description to describe codecs instead of drivers.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-mssql-oracle-codec-cleanup
replan_trigger: null
current_task: "driver-fail-loud:ch06:t01"
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
verified: []
sources:
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db crate doc, module list, and re-exports
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: vertebra-db package description and dependencies
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Workspace dependency table
  - id: postgres-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: PostgreSQL frame, COPY, JSONB, pgvector codecs, and PostgresPool
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbBrokerPool
    title: DuckDB bootstrap statements and DuckdbBrokerPool
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqlitePool
    title: SQLite pragmas and SqlitePool with msgpack helpers
  - id: tns-rs
    resource: src/rs/vertebra-db/src/oracle/tns.rs::OracleWalletMaterial
    title: tnsnames.ora parser and wallet directory loader
  - id: mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool
    title: MySQL packet framing, caching_sha2 scramble, and MysqlPool
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::parse_cluster_redirect
    title: RESP3 codecs and cluster redirect parser kept for the Python Redis driver
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Python codec bindings that consume SqliteConfig, DuckdbConfig, and RESP3
tags:
  - vertebra-db
  - rust
  - cleanup
  - dead-code
  - dependencies
---

# Chapter 06: vertebra-db Pools and MySQL Removal

<!-- truth: start -->
- **No connectionless pools**: `PostgresPool`, `PostgresPoolConfig`, `DuckdbBrokerPool`, `SqlitePool`, `MysqlPool`, and `MysqlPoolConfig` are deleted with their inline tests and `lib.rs` re-exports; none has a caller outside its own module.[^postgres-rs][^duckdb-rs][^sqlite-rs][^mysql-rs]
- **No dialect tags**: `POSTGRES_DIALECT_TAG`, `SQLITE_DIALECT_TAG`, and `MYSQL_DIALECT_TAG` (pool-only users) are deleted; ch05 removes the MSSQL and Oracle tags.
- **No wallet loader**: `tns::OracleWalletMaterial` is deleted whole with `from_directory`; it has no other constructor and no caller.[^tns-rs]
- **No MySQL module**: `src/rs/vertebra-db/src/mysql/` and `pub mod mysql` are deleted.[^mysql-rs]
- **Dependencies**: `vertebra-db` drops `sha2` (MySQL scramble only) and `rmp-serde` (`SqlitePool` msgpack only); the workspace `rmp-serde = "1.3"` entry in `src/rs/Cargo.toml` goes with it; `Cargo.lock` loses `rmp` and `rmp-serde`.[^db-cargo][^ws-cargo]
- **Kept codecs**: PostgreSQL frame/COPY/JSONB/pgvector, `PoolerMode`, `compute_worker_pool_budget`, `SqliteConfig`, `DuckdbConfig`, `parse_tnsnames_ora`, `resolve_tns_alias`, and all RESP3 functions including `ClusterRedirect`/`RedirectKind`.[^resp3-rs][^db-bindings]
- **Docs say codecs**: the crate doc header, the `postgres`, `sqlite`, `duckdb`, and `oracle::tns` module docs, and the Cargo `description` describe codecs, dialers, transport, and shared-memory Arrow framing.[^db-lib][^db-cargo]
- **Build stays green**: every task commit builds the workspace and passes `make rs-test`; lockfile changes ship in the commit that changes a manifest.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vertebra-db` advertises itself as "native Rust database wire protocols" and carries shared-memory `*Pool` lease tables that never open a connection, a complete MySQL module, and an Oracle wallet directory loader that nothing constructs.[^db-lib][^mysql-rs][^tns-rs] The engine client crates selected by the per-engine driver flows replace that code. Two dependencies, `sha2` and `rmp-serde`, exist in `vertebra-db` only for the MySQL scramble and the `SqlitePool` msgpack helpers.[^db-cargo]

This chapter deletes the PostgreSQL, DuckDB, SQLite, and MySQL pools, the MySQL module, `OracleWalletMaterial`, and the two dependencies, then rewrites the crate doc and Cargo `description` so the crate describes what remains: codecs, dialers, transport, and shared-memory Arrow framing.

**Non-goals**:

- MSSQL and Oracle TDS/TNS framing, `MssqlPool`, `OraclePool`, the `encode_tds_sql_batch` binding, and Python adapter probes (owned by [ch05](../ch05-mssql-oracle-codec-cleanup/spec.md)).
- RESP3 codec removal (owned by `native-redis-driver`).
- Other possibly unused `vertebra-db` dependencies (see Notes in t04); user docs and OKF chapters (owned by ch09).
- `ShmLeaseTable` and `ShmStatementCache` in `vertebra-core` (other consumers decide their fate).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Depends on [ch05-mssql-oracle-codec-cleanup](../ch05-mssql-oracle-codec-cleanup/spec.md): ch05 reduces the `mssql` and `oracle` modules, their `lib.rs` re-exports, and their crate-doc bullets, so this chapter edits a `lib.rs` that no longer carries `MssqlPool`, `OraclePool`, or TDS/TNS framing.
- Precondition check before t01: `grep -rnE "MssqlPool|OraclePool|TdsPacketType|TnsPacketType" src/rs --include=*.rs` prints nothing.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Crate root | `src/rs/vertebra-db/src/lib.rs`[^db-lib], `Cargo.toml`[^db-cargo], workspace `src/rs/Cargo.toml`[^ws-cargo] | In-process Rust; consumers are `vertebra-py`, `vertebra-bigquery`, `vertebra-spanner`; none imports a pool, MySQL, or wallet symbol |
| PostgreSQL / DuckDB | `postgres/mod.rs::PostgresPool`[^postgres-rs], `duckdb/mod.rs::DuckdbBrokerPool`[^duckdb-rs] | `db.rs` and `vector.rs` use codecs and `DuckdbConfig` only |
| SQLite | `sqlite/mod.rs::SqlitePool`[^sqlite-rs] | `db.rs` uses `SqliteConfig` only; `SqlitePool` is the sole `rmp-serde` user |
| Oracle wallet | `oracle/tns.rs::OracleWalletMaterial`[^tns-rs] | No caller; the whole struct, including its only constructor `from_directory`, is deleted. Kept TNS parsing is `parse_tnsnames_ora` and `resolve_tns_alias`. Python wallet handling lives in `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet` and does not call the Rust struct |
| MySQL | `mysql/mod.rs::MysqlPool`[^mysql-rs] | Only `lib.rs` re-exports it; sole `sha2` user in `vertebra-db` |
| RESP3 | `resp3/mod.rs::parse_cluster_redirect`[^resp3-rs] | Kept whole; `db.rs::parse_cluster_redirect` reads `ClusterRedirect.kind.as_str()` |
| PyO3 binding | `vertebra-py/src/db.rs::register_db_bindings`[^db-bindings] | Unchanged by this chapter; `duckdb_bootstrap_statements`, `sqlite_initialization_pragmas`, `parse_tnsnames_ora` keep working |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels and SHM ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md) (`runtime-ipc` owns `src/rs/vertebra-db/**`)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8** (root, pools and MySQL portion): delete the PostgreSQL, DuckDB, SQLite, and MySQL `*Pool` types, the `mysql` module, the remaining dialect tags, the Oracle wallet directory loader, and the `sha2` and `rmp-serde` dependencies from `vertebra-db`; rewrite the crate doc and Cargo `description`. ch05 owns the MSSQL and Oracle portion.
  - **R8.3** (t01): `PostgresPool`, `PostgresPoolConfig`, `POSTGRES_DIALECT_TAG`, and `DuckdbBrokerPool` are deleted.
  - **R8.4** (t02): `SqlitePool`, `SQLITE_DIALECT_TAG`, `OracleWalletMaterial`, and `rmp-serde` (crate and workspace entries) are deleted.
  - **R8.5** (t03): `MysqlPool`, `MysqlPoolConfig`, and `MYSQL_DIALECT_TAG` are deleted.
  - **R8.6** (t04): the `mysql` module and `sha2` are deleted; the crate doc and Cargo `description` describe codecs.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

Final `vertebra_db` module list and complete re-export surface in `src/rs/vertebra-db/src/lib.rs` after this chapter. The `arrow_shm`, `connection`, `dialer`, and `gcp` re-export blocks are retained verbatim from the current file; modules `arrow_shm`, `connection`, `dialer`, `gcp`, and `py_async` are unchanged:

```rust
pub mod arrow_shm;
pub mod connection;
pub mod dialer;
pub mod duckdb;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod mssql;
pub mod oracle;
pub mod postgres;
#[cfg(feature = "pyo3")]
pub mod py_async;
pub mod resp3;
pub mod sqlite;

pub use arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ARROW_IPC_EOS_MARKER, SHM_ARROW_MAGIC, ShmArrowFrameHeader,
    ShmArrowStreamWriter, normalize_shm_name, open_shm_arrow_region, read_shm_arrow_frame,
    read_shm_arrow_frame_with, release_shm_arrow_frame, write_shm_arrow_frame,
    write_shm_arrow_region, write_shm_arrow_region_unregistered,
};
pub use connection::{
    BoxFuture, CancelHandle, ConnectOptions, Connection, EndpointTarget, TlsMode, TransportError,
    TransportStream, install_rustls_ring_provider,
};
pub use dialer::{
    AlloyDbClusterUri, AlloyDbDialer, AuthType, CloudSqlDialer, CloudSqlInstanceUri, IpType,
    decode_alloydb_metadata_response, encode_alloydb_metadata_exchange,
    encode_alloydb_metadata_response, exchange_alloydb_metadata_over_stream,
    generate_ephemeral_keypair_pem,
};
pub use duckdb::DuckdbConfig;
#[cfg(feature = "gcp")]
pub use gcp::{
    GcpErrorInfo, GcpTokenCredentials, code_name, code_number, gax_error_info, http_code,
    official_credentials,
};
pub use mssql::rewrite_mssql_placeholders;
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
pub use postgres::{
    PGCOPY_SIGNATURE, PoolerMode, compute_worker_pool_budget, decode_pgcopy_binary_rows,
    encode_bind_frame, encode_execute_frame, encode_parse_frame, encode_pg_jsonb,
    encode_pgcopy_binary_rows, encode_query_frame, encode_startup_message, encode_sync_frame,
    strip_pg_jsonb_header,
};
pub use resp3::{
    ClusterRedirect, RedirectKind, Resp3Frame, crc16_xmodem, encode_hello3_handshake,
    encode_resp_command, encode_sentinel_get_master_cmd, extract_invalidation_keys,
    parse_cluster_redirect, parse_resp3_frame, redis_hash_slot,
};
pub use sqlite::SqliteConfig;
```

Retained Rust signatures (unchanged): `impl SqliteConfig { pub fn initialization_pragmas(&self) -> Vec<String> }`; `impl DuckdbConfig { pub fn bootstrap_statements(&self) -> Vec<String> }`; `pub fn parse_tnsnames_ora(contents: &str) -> HashMap<String, String>`; `pub fn resolve_tns_alias(contents: &str, alias: &str) -> Option<OracleConnectDescriptor>`; `pub fn compute_worker_pool_budget(total_budget: usize, replica_count: usize, workers_per_replica: usize, reserved_admin_connections: usize) -> usize`; `PoolerMode` and its methods.

`src/rs/vertebra-db/Cargo.toml` after the chapter: `description = "Database wire codecs, Cloud SQL and AlloyDB dialers, shared transport, and shared-memory Arrow framing for Vertebra"`; no `sha2` or `rmp-serde` dependency line; `rsa = { version = "0.9", features = ["sha2"] }` stays. `src/rs/Cargo.toml` `[workspace.dependencies]` has no `rmp-serde` entry and keeps `sha2 = "0.10"`.

No Python surface changes in this chapter; `_vertebra.pyi` is untouched.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `vertebra-py/src/db.rs::{duckdb_bootstrap_statements, sqlite_initialization_pragmas}` → `DuckdbConfig::bootstrap_statements`, `SqliteConfig::initialization_pragmas` (kept).
- `vertebra-py/src/db.rs::parse_tnsnames_ora` → `vertebra_db::oracle::tns::parse_tnsnames_ora` (kept).
- `vertebra-py/src/vector.rs` → `vertebra_db::postgres::{encode_pgvector_*}` (kept).
- `vertebra-py/src/db.rs::parse_cluster_redirect` → `vertebra_db::resp3::parse_cluster_redirect` → `ClusterRedirect { kind: RedirectKind, .. }` (kept).
- `vertebra-core` and `vertebra-storage` → `sha2` (workspace entry kept; only the `vertebra-db` edge goes).

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Pooler rules lose Rust caller | `PostgresPool::new` deleted | `PoolerMode::supports_protocol_prepared_statements` stays public and tested by `test_pooler_mode_rules` | `postgres::tests` count is `5` |
| Leftover import | `Arc`, `Duration`, `vertebra_core::broker`, `ShmRegion` imports remain after a pool delete | Clippy `-D warnings` fails; the task removes them | Clippy exits 0 |
| Lockfile drift | `rmp-serde` or `sha2` removed from a manifest | `cargo build --locked` fails until `Cargo.lock` is regenerated in the same commit | `cargo check --workspace --locked` exits 0 |
| Unused workspace entry | `rmp-serde = "1.3"` left in `src/rs/Cargo.toml` | No warning fires | t02 removes the workspace entry explicitly |
| `sha2` still needed elsewhere | `vertebra-core`, `vertebra-storage` depend on `sha2` | `sha2` package stays in `Cargo.lock`; only the `vertebra-db` edge goes | `cargo tree -p vertebra-db -e normal --depth 1` lists no `sha2` |
| MySQL split | 268-line whole-file delete plus `lib.rs`, Cargo, crate-doc edits exceed 250 lines | t03 removes the pool half; t04 deletes the remaining module | Each task stays within `estimated_loc <= 250` and compiles |
| Unconstructible type | Deleting only `OracleWalletMaterial::from_directory` | Leaves a public struct with no constructor | t02 deletes the struct whole |
| `tempfile` dev-dependency | `SqlitePool` and wallet tests deleted | `tempfile` stays: `connection.rs` tests still use it | Dev-dependencies unchanged |
| Unsafe code | Deleted regions | No `unsafe` block exists in any deleted region | `// SAFETY:` rule unaffected |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the workspace, when `grep -rnE "MssqlPool|OraclePool|PostgresPool|SqlitePool|MysqlPool|DuckdbBrokerPool|TdsPacketType|TnsPacketType|_DIALECT_TAG|OracleWalletMaterial|rmp_serde" src/rs --include=*.rs` runs, then it prints nothing.
- Given the workspace, when `make rs-test` runs, then it exits 0 and `cargo test -p vertebra-db -- --list` lists no `mysql::` test and `17` tests across `mssql`, `oracle`, `postgres`, `sqlite`, `duckdb`, and `resp3`.
- Given `src/rs/vertebra-db/Cargo.toml`, when read, then `description` is `"Database wire codecs, Cloud SQL and AlloyDB dialers, shared transport, and shared-memory Arrow framing for Vertebra"` and neither `sha2` nor `rmp-serde` appears as a dependency line.
- Given `src/rs/Cargo.lock`, when grepped for `name = "rmp-serde"`, then the count is `0`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Gate pools behind a `legacy` Cargo feature | Keeps dead code compiled in CI under `--all-features`; no caller enables it | Rejected: dead code with a feature flag is still dead code |
| Delete only `OracleWalletMaterial::from_directory`, keep the struct | Struct has no other constructor and no caller | Rejected: leaves an unconstructible public type |
| Move pools into `vertebra-core` next to `ShmLeaseTable` | Relocates dead code without a caller | Rejected: no consumer needs a connectionless pool |
| One task per module with whole-file deletes over 250 lines | Simpler review per module | Rejected: `estimated_loc <= 250` per task; MySQL splits into pool and module tasks |
| Leave the workspace `rmp-serde` entry for future use | Unused entries produce no warning and drift | Rejected: removed with its last consumer |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch06]`. In-code docs (crate doc header, `postgres`, `sqlite`, `duckdb`, and `oracle::tns` module `//!` docs, crate-doc bullets, Cargo `description`) are part of each task's production change. User docs and OKF (`database-drivers-and-dialers.md` codec inventory, `product/product.md` vertebra-db row) are owned by ch09.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R8.3 | [t01](tasks/t01-rust-postgres-duckdb-pools.md) | `static_validation` | `make rs-test`, `postgres::tests::*`, `duckdb::tests::*` |
| R8.4 | [t02](tasks/t02-rust-sqlite-pool-and-oracle-wallet.md) | `static_validation` | `make rs-test`, `sqlite::tests::*`, `oracle::tns::tests::*`, `cargo check --workspace --locked` |
| R8.5 | [t03](tasks/t03-rust-mysql-pool.md) | `static_validation` | `make rs-test`, `mysql::tests::*` |
| R8.6 | [t04](tasks/t04-rust-mysql-module-and-crate-docs.md) | `static_validation` | `make rs-test`, `make lint-all` Rust subset, `cargo doc -p vertebra-db --no-deps` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch06:t01 — Delete PostgresPool and DuckdbBrokerPool (tasks/t01-rust-postgres-duckdb-pools.md)
- [ ] driver-fail-loud:ch06:t02 — Delete SqlitePool, OracleWalletMaterial, and rmp-serde (tasks/t02-rust-sqlite-pool-and-oracle-wallet.md)
- [ ] driver-fail-loud:ch06:t03 — Delete MysqlPool and MysqlPoolConfig (tasks/t03-rust-mysql-pool.md)
- [ ] driver-fail-loud:ch06:t04 — Delete the mysql module and sha2 and rewrite the crate doc and Cargo description (tasks/t04-rust-mysql-module-and-crate-docs.md)
- **Chapter Recovery Checkpoint**: `make lint-all && make rs-test && make py-test` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 0acdf576`; `depends_on_chapters: [ch05-mssql-oracle-codec-cleanup]`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01` through `t04` (`0/4` completed). Tasks run in order because each edits `lib.rs`; t04 requires t03 (the pool half leaves before the module).
- **Open Doc/OKF Items**: none in this chapter; ch09 owns user docs and OKF.
- **Active Invariants**: every commit builds the workspace and passes `make rs-test`; lockfile changes ship with manifest changes; RESP3, TNS parsing, VECTOR/OSON, pgvector, PostgreSQL codecs, `SqliteConfig`, and `DuckdbConfig` stay.
- **Sibling flows**: `native-postgres-driver` ch14 cites `PostgresPool` and `broker-pool-sharing` t01 plans the same pool deletion; both need a revise after t01 and t03 land.

[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate doc, module list, and re-exports.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — vertebra-db package description and dependencies.
[^ws-cargo]: `src/rs/Cargo.toml` — Workspace dependency table.
[^postgres-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — PostgreSQL frame, COPY, JSONB, pgvector codecs, and PostgresPool.
[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbBrokerPool` — DuckDB bootstrap statements and DuckdbBrokerPool.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqlitePool` — SQLite pragmas and SqlitePool with msgpack helpers.
[^tns-rs]: `src/rs/vertebra-db/src/oracle/tns.rs::OracleWalletMaterial` — tnsnames.ora parser and wallet directory loader.
[^mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool` — MySQL packet framing, caching_sha2 scramble, and MysqlPool.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::parse_cluster_redirect` — RESP3 codecs and cluster redirect parser kept for the Python Redis driver.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings that consume SqliteConfig, DuckdbConfig, and RESP3.
