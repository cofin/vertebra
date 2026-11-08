---
type: Task
id: "driver-fail-loud:ch06:t01"
chapter_id: ch06-vertebra-db-pools-and-mysql
title: Delete PostgresPool and DuckdbBrokerPool
description: Delete PostgresPool, PostgresPoolConfig, POSTGRES_DIALECT_TAG, the pool SHM test, and DuckdbBrokerPool from vertebra-db, and rewrite the postgres and duckdb module docs and crate-doc bullets.
status: draft
state: pending
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/lib.rs
files:
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/lib.rs
tests:
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
estimated_loc: 210
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: postgres-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: PostgreSQL codecs and PostgresPool
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbBrokerPool
    title: DuckDB bootstrap statements and DuckdbBrokerPool
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Crate doc bullets and re-exports
tags:
  - vertebra-db
  - rust
  - postgres
  - duckdb
  - cleanup
---

# Task t01: Delete `PostgresPool` and `DuckdbBrokerPool`

## Objective & Scope Boundary

Delete `POSTGRES_DIALECT_TAG`, `PostgresPoolConfig`, `PostgresPool`, and `test_postgres_pool_shm_integration` from `postgres/mod.rs`, and `DuckdbBrokerPool` from `duckdb/mod.rs`, with the imports only they use. Rewrite both module `//!` docs, both crate-doc bullets, and the `lib.rs` re-exports.[^postgres-rs][^duckdb-rs][^db-lib]

Out of scope: PostgreSQL frame, COPY, JSONB, and pgvector codecs, `PoolerMode`, `compute_worker_pool_budget` (kept); `DuckdbConfig::bootstrap_statements` and the `arrow_shm` re-exports in `duckdb/mod.rs` (kept); SQLite and MySQL pools (t02, t03).

Precondition: chapter `ch05-mssql-oracle-codec-cleanup` is completed, so `lib.rs` already carries the reduced `mssql` and `oracle` re-exports and crate-doc bullets.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/postgres/mod.rs` — imports, `POSTGRES_DIALECT_TAG`, `PostgresPoolConfig`, `PostgresPool`, `mod tests`.[^postgres-rs]
- `src/rs/vertebra-db/src/duckdb/mod.rs` — whole file (166 lines).[^duckdb-rs]
- `src/rs/vertebra-db/src/lib.rs` — postgres and duckdb crate-doc bullets and `pub use` lines.[^db-lib]

Grep-only verification: `grep -rnE "PostgresPool|POSTGRES_DIALECT_TAG|DuckdbBrokerPool" src/rs --include=*.rs` lists only these three files; `vertebra-py/src/{db,vector}.rs` import `DuckdbConfig`, pgvector encoders, PGCOPY, and JSONB functions only.

## Symbol & Signature Contracts

- `postgres/mod.rs`:
  - Module doc: `//! PostgreSQL v3 frontend frame encoders, pooler topology rules, binary \`JSONB\`, \`PGCOPY\`, and pgvector codecs.`
  - Imports after: `use bytes::{BufMut, Bytes, BytesMut};` and `use serde::{Deserialize, Serialize};` (`PoolerMode` derives serde). Deleted: `std::sync::Arc`, `std::time::Duration`, `vertebra_core::broker::{...}`, `vertebra_core::shm::ShmRegion`.
  - Kept unchanged: `PGCOPY_SIGNATURE`, `PoolerMode` and its methods, `compute_worker_pool_budget`, `strip_pg_jsonb_header`, `encode_pg_jsonb`, `encode_startup_message`, `encode_parse_frame`, `encode_bind_frame`, `encode_execute_frame`, `encode_sync_frame`, `encode_query_frame`, `encode_pgcopy_binary_rows`, `decode_pgcopy_binary_rows`, `encode_pgvector_{f32,f16,bit}`, `decode_pgvector_{f32,f16,bit}`, private f16 helpers, and tests `test_pooler_mode_rules`, `test_pg_jsonb_encode_and_strip`, `test_postgres_wire_frames`, `test_pgcopy_binary_roundtrip`, `test_pgvector_f32_f16_and_bit_codecs`.
- `duckdb/mod.rs`:
  - Module doc: `//! DuckDB 2.0-safe \`SET\` bootstrap statements and \`ShmRegion\` Arrow IPC frame re-exports.`
  - Imports after: `use serde::{Deserialize, Serialize};` plus the existing `pub use crate::arrow_shm::{...}` block. Deleted: `std::sync::Arc`, `std::time::Duration`, `vertebra_core::broker::{LeaseGrant, ShmLeaseTable}`, `vertebra_core::shm::ShmRegion`.
  - Kept: `DuckdbConfig`, `Default for DuckdbConfig`, `DuckdbConfig::bootstrap_statements(&self) -> Vec<String>`, tests `test_duckdb_2_0_safe_bootstrap_statements`, `test_shm_arrow_frame_write_read_release`.
- `lib.rs`:
  - `pub use duckdb::DuckdbConfig;`
  - `pub use postgres::{PGCOPY_SIGNATURE, PoolerMode, compute_worker_pool_budget, decode_pgcopy_binary_rows, encode_bind_frame, encode_execute_frame, encode_parse_frame, encode_pg_jsonb, encode_pgcopy_binary_rows, encode_query_frame, encode_startup_message, encode_sync_frame, strip_pg_jsonb_header};` (rustfmt layout).
  - PostgreSQL bullet: `//! - PostgreSQL v3 frontend frame encoders, pooler topology rules (\`Direct\`, \`PgBouncerTransaction\`,` / `//!   \`PgCat\`, \`Supavisor\`), binary \`JSONB\` header stripping, \`PGCOPY\` binary codec, and pgvector codecs (\`postgres\`).`
  - DuckDB bullet: `//! - DuckDB 2.0-safe \`SET\` bootstrap statements and \`ShmRegion\` Arrow IPC frame re-exports (\`duckdb\`).`
- No `unsafe` code exists in either file.

## Edge Cases & Error Semantics

- `PoolerMode::supports_protocol_prepared_statements` loses its only Rust caller (`PostgresPool::new`) but stays public and tested by `test_pooler_mode_rules`.
- A leftover import fails clippy `-D warnings`.
- `ShmLeaseTable` and `ShmStatementCache` stay in `vertebra-core`; only `vertebra-db` imports go.

## Public-Seam Test Blueprint

- **Canonical test location**: inline `mod tests` in `postgres/mod.rs` and `duckdb/mod.rs`; Python seam `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_duckdb_post_init_bootstrap_and_arrow_shm_streaming` exercises `duckdb_bootstrap_statements`.
- **Deleted test**: `postgres::tests::test_postgres_pool_shm_integration` (subject deleted).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Inventory before | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^postgres::tests::"` | `6` |
| Inventory after | same command | `5` |
| DuckDB inventory | `... \| grep -c "^duckdb::tests::"` before and after | `2` and `2` |
| Bootstrap kept | `DuckdbConfig::default().bootstrap_statements()` | `["SET memory_limit = '2048MB'", "SET threads = 4", "SET parquet_metadata_cache = true", "SET httpfs_connection_caching = true", "SET enable_http_metadata_cache = true"]` |
| Pooler budget kept | `compute_worker_pool_budget(200, 5, 4, 20)` | `9` |
| PGCOPY kept | `decode_pgcopy_binary_rows(&encode_pgcopy_binary_rows(&[vec![Some(b"hello".to_vec()), None]]))` | `Ok(vec![vec![Some(b"hello".to_vec()), None]])` |
| Residue | `grep -rnE "PostgresPool\|POSTGRES_DIALECT_TAG\|DuckdbBrokerPool" src/rs --include=*.rs` | no output, exit 1 |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib -- postgres:: duckdb::` exits 0 with 8 tests.
- Final: `(cd src/rs && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings) && make rs-test`

## Acceptance Criteria

- [ ] `PostgresPool`, `PostgresPoolConfig`, `POSTGRES_DIALECT_TAG`, and `DuckdbBrokerPool` are gone from the workspace (R8.3).
- [ ] PostgreSQL codecs, `PoolerMode`, `DuckdbConfig`, and their tests are unchanged and pass (R8.3).
- [ ] Module docs and crate-doc bullets match the contract text (R8.3).
- [ ] `make rs-test` and workspace clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] `native-postgres-driver` ch14 cites `vertebra_db::postgres::PostgresPool` as a kept helper; that sibling spec needs a revise after this task lands.

[^postgres-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — PostgreSQL codecs and PostgresPool.
[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbBrokerPool` — DuckDB bootstrap statements and DuckdbBrokerPool.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — Crate doc bullets and re-exports.
