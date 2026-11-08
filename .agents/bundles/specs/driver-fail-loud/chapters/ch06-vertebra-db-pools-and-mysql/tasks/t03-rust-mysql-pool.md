---
type: Task
id: "driver-fail-loud:ch06:t03"
chapter_id: ch06-vertebra-db-pools-and-mysql
title: Delete MysqlPool and MysqlPoolConfig
description: Delete MysqlPool, MysqlPoolConfig, MYSQL_DIALECT_TAG, the pool lease test, and their imports and lib.rs re-exports from the vertebra-db mysql module.
status: draft
state: pending
priority: P2
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch06:t02"
read_set:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
files:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
tests:
  - src/rs/vertebra-db/src/mysql/mod.rs
estimated_loc: 130
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
  - id: mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool
    title: MySQL framing, scramble, and MysqlPool
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: mysql re-export
tags:
  - vertebra-db
  - rust
  - mysql
  - cleanup
---

# Task t03: Delete `MysqlPool` and `MysqlPoolConfig`

## Objective & Scope Boundary

Delete `MYSQL_DIALECT_TAG`, `MysqlPoolConfig` (with its `Default` impl), `MysqlPool`, `test_mysql_pool_shm_leasing`, and the imports only they use from `src/rs/vertebra-db/src/mysql/mod.rs`, and drop `MysqlPool` and `MysqlPoolConfig` from the `lib.rs` re-export. This completes the six-pool removal.[^mysql-rs][^db-lib]

The MySQL module is removed in two tasks because its 268-line whole-file deletion plus `lib.rs`, Cargo, and crate-doc edits exceeds the 250-line task budget; this task removes the pool half and leaves a compiling, tested framing module for t04 to delete.

Out of scope: MySQL framing, command encoders, scramble, `sha2`, and `pub mod mysql` (t04).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/mysql/mod.rs` — whole file (268 lines).[^mysql-rs]
- `src/rs/vertebra-db/src/lib.rs` — `pub use mysql::{...}`.[^db-lib]

Grep-only verification: `grep -rnE "MysqlPool|MYSQL_DIALECT_TAG" src/rs --include=*.rs` lists only `mysql/mod.rs` and `lib.rs`.

## Symbol & Signature Contracts

- `mysql/mod.rs` after the change:
  - Module doc: unchanged; t04 deletes the file.
  - Imports: `use bytes::{BufMut, Bytes, BytesMut};` and `use sha2::{Digest, Sha256};`. Deleted: `std::sync::Arc`, `std::time::Duration`, `serde::{Deserialize, Serialize}` (only `MysqlPoolConfig` derived it), `vertebra_core::broker::{...}`, `vertebra_core::shm::ShmRegion`.
  - Kept until t04: `COM_QUERY`, `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, `COM_STMT_CLOSE`, `encode_mysql_packet`, `decode_mysql_packet`, `compute_caching_sha2_scramble`, `encode_com_query`, `encode_com_stmt_prepare`, `encode_com_stmt_close`, tests `test_mysql_packet_framing_and_commands`, `test_caching_sha2_scramble`.
- `lib.rs`: `pub use mysql::{compute_caching_sha2_scramble, decode_mysql_packet, encode_com_query, encode_com_stmt_close, encode_com_stmt_prepare, encode_mysql_packet};` (rustfmt layout).
- No `unsafe` code exists in the file.

## Edge Cases & Error Semantics

- `COM_STMT_EXECUTE` has no caller after this task; it is `pub` so no `dead_code` warning fires, and t04 deletes it.
- A leftover `serde` import fails clippy `-D warnings`.

## Public-Seam Test Blueprint

- **Canonical test location**: inline `mod tests` in `mysql/mod.rs`.
- **Deleted test**: `mysql::tests::test_mysql_pool_shm_leasing` (subject deleted).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Inventory before | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^mysql::tests::"` | `3` |
| Inventory after | same command | `2` |
| Framing still compiles | `decode_mysql_packet(&encode_mysql_packet(3, b"SELECT 1"))` | `Ok((3, b"SELECT 1", 12))` |
| Residue | `grep -rnE "MysqlPool\|MYSQL_DIALECT_TAG" src/rs --include=*.rs` | no output, exit 1 |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib mysql::` exits 0 with 3 tests.
- Final: `(cd src/rs && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings) && make rs-test`

## Acceptance Criteria

- [ ] `MysqlPool`, `MysqlPoolConfig`, and `MYSQL_DIALECT_TAG` are gone from the workspace (R8.5).
- [ ] `grep -rnE "ShmLeaseTable|ShmStatementCache" src/rs/vertebra-db/src` prints nothing: no `vertebra-db` module imports lease or statement-cache types (R8.5).
- [ ] `make rs-test` and workspace clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] `broker-pool-sharing` t01 plans the same `*Pool` deletion; once this task lands, that task has nothing left to delete.

[^mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool` — MySQL framing, scramble, and MysqlPool.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — mysql re-export.
