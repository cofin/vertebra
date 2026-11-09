---
type: Task
id: "driver-fail-loud:ch07:t03"
chapter_id: ch07-vertebra-db-pools-and-mysql
title: Delete MysqlPool, MysqlPoolConfig, and OracleWalletMaterial
description: Delete MysqlPool, MysqlPoolConfig, MYSQL_DIALECT_TAG, the pool lease test, and their lib.rs re-exports from the vertebra-db mysql module, and delete the OracleWalletMaterial struct with from_directory and its test from oracle/tns.rs.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch07:t02"
read_set:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/oracle/tns.rs
files:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/oracle/tns.rs
tests:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/oracle/tns.rs
estimated_loc: 215
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T14:38:00Z"
last_verified_commit: ccd45c42
verification_evidence: "cargo test -p vertebra-db --features gcp passed"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T14:38:00Z"
commit: ccd45c42
sources:
  - id: mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool
    title: MySQL framing, scramble, and MysqlPool
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: mysql re-export
  - id: tns-rs
    resource: src/rs/vertebra-db/src/oracle/tns.rs::OracleWalletMaterial
    title: tnsnames.ora parser and wallet directory loader
tags:
  - vertebra-db
  - rust
  - mysql
  - oracle
  - cleanup
---

# Task t03: Delete `MysqlPool`, `MysqlPoolConfig`, and `OracleWalletMaterial`

## Objective & Scope Boundary

Delete `MYSQL_DIALECT_TAG`, `MysqlPoolConfig` (with its `Default` impl), `MysqlPool`, `test_mysql_pool_shm_leasing`, and the imports only they use from `src/rs/vertebra-db/src/mysql/mod.rs`, and drop `MysqlPool` and `MysqlPoolConfig` from the `lib.rs` re-export. This completes the six-pool removal.[^mysql-rs][^db-lib]

Delete the `OracleWalletMaterial` struct with `from_directory` and `test_oracle_wallet_from_directory` from `src/rs/vertebra-db/src/oracle/tns.rs`, and rewrite the `tns` module `//!` doc. `from_directory` is the struct's only constructor and the struct has no caller anywhere in the workspace, and `lib.rs` does not re-export it. Python wallet handling lives in `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet`, does not call the Rust struct, and stays.[^tns-rs]

The MySQL module is removed in two tasks because its 268-line whole-file deletion plus `lib.rs`, Cargo, and crate-doc edits exceeds the 250-line task budget; this task removes the pool half and leaves a compiling, tested framing module for t04 to delete.

Out of scope: MySQL framing, command encoders, scramble, `sha2`, and `pub mod mysql` (t04); `parse_tnsnames_ora`, `resolve_tns_alias`, and `OracleConnectDescriptor` (kept); the crate doc header (t04).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/mysql/mod.rs` — whole file (268 lines).[^mysql-rs]
- `src/rs/vertebra-db/src/lib.rs` — `pub use mysql::{...}`.[^db-lib]
- `src/rs/vertebra-db/src/oracle/tns.rs` — module doc, imports, `OracleWalletMaterial`, `mod tests`.[^tns-rs]

Grep-only verification: `grep -rnE "MysqlPool|MYSQL_DIALECT_TAG" src/rs --include=*.rs` lists only `mysql/mod.rs` and `lib.rs`; `grep -rn "OracleWalletMaterial" src/rs --include=*.rs` lists only `oracle/tns.rs`.

## Symbol & Signature Contracts

- `mysql/mod.rs` after the change:
  - Module doc: unchanged; t04 deletes the file.
  - Imports: `use bytes::{BufMut, Bytes, BytesMut};` and `use sha2::{Digest, Sha256};`. Deleted: `std::sync::Arc`, `std::time::Duration`, `serde::{Deserialize, Serialize}` (only `MysqlPoolConfig` derived it), `vertebra_core::broker::{...}`, `vertebra_core::shm::ShmRegion`.
  - Kept until t04: `COM_QUERY`, `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, `COM_STMT_CLOSE`, `encode_mysql_packet`, `decode_mysql_packet`, `compute_caching_sha2_scramble`, `encode_com_query`, `encode_com_stmt_prepare`, `encode_com_stmt_close`, tests `test_mysql_packet_framing_and_commands`, `test_caching_sha2_scramble`.
- `lib.rs`: `pub use mysql::{compute_caching_sha2_scramble, decode_mysql_packet, encode_com_query, encode_com_stmt_close, encode_com_stmt_prepare, encode_mysql_packet};` (rustfmt layout).
- `oracle/tns.rs` after the change:
  - Module doc: `//! Oracle \`tnsnames.ora\` parser, connect-descriptor model, and alias resolver.`
  - Delete `use std::path::Path;`; keep `use std::collections::HashMap;` and `use serde::{Deserialize, Serialize};` (`OracleConnectDescriptor` derives serde).
  - Kept: `OracleConnectDescriptor` (`from_descriptor_str`, `to_tns_descriptor`), private `extract_tns_param`, `parse_tnsnames_ora(contents: &str) -> HashMap<String, String>`, `resolve_tns_alias(contents: &str, alias: &str) -> Option<OracleConnectDescriptor>`, test `test_parse_tnsnames_and_resolve_alias`.
- No `unsafe` code exists in either file.

## Edge Cases & Error Semantics

- `COM_STMT_EXECUTE` has no caller after this task; it is `pub`, so no `dead_code` warning fires, and t04 deletes it.
- A leftover `serde` import in `mysql/mod.rs` or `Path` import in `tns.rs` fails clippy `-D warnings`.
- Deleting only `OracleWalletMaterial::from_directory` would leave an unconstructible public struct; the struct goes whole.
- `tempfile` stays in `[dev-dependencies]`: `connection.rs` tests still use it after the wallet test leaves.

## Public-Seam Test Blueprint

- **Canonical test location**: inline `mod tests` in `mysql/mod.rs` and `oracle/tns.rs`; Python seam `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_tns_descriptor_and_wallet_extraction` (uses Python `OracleWallet` and the `parse_tnsnames_ora` binding).
- **Deleted tests**: `mysql::tests::test_mysql_pool_shm_leasing`, `oracle::tns::tests::test_oracle_wallet_from_directory` (subjects deleted).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| MySQL inventory | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^mysql::tests::"` before / after | `3` / `2` |
| TNS inventory | `... \| grep -c "^oracle::tns::tests::"` before / after | `2` / `1` |
| Framing still compiles | `decode_mysql_packet(&encode_mysql_packet(3, b"SELECT 1"))` | `Ok((3, b"SELECT 1", 12))` |
| Alias resolution kept | `resolve_tns_alias(sample, "myatp_tp")` from the kept test | `port == 1522`, `retry_count == Some(20)` |
| Residue | `grep -rnE "MysqlPool\|MYSQL_DIALECT_TAG\|OracleWalletMaterial" src/rs --include=*.rs` | no output, exit 1 |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib -- mysql:: oracle::tns::` exits 0 with 5 tests.
- Final: `(cd src/rs && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings) && make rs-test && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k tns_descriptor -q`

## Acceptance Criteria

- [x] `MysqlPool`, `MysqlPoolConfig`, and `MYSQL_DIALECT_TAG` are gone from the workspace (R8.5).
- [x] The `OracleWalletMaterial` struct, `from_directory`, and its test are gone; TNS parsing and alias resolution tests pass unchanged (R8.5).
- [x] `grep -rnE "ShmLeaseTable|ShmStatementCache" src/rs/vertebra-db/src` prints nothing: no `vertebra-db` module imports lease or statement-cache types (R8.5).
- [x] `make rs-test` and workspace clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] Root R8 names `OracleWalletMaterial::from_directory`; this task deletes the struct too because it has no other constructor or caller.
- [2026-11-08 00:00] `broker-pool-sharing` already assumes this chapter deletes the `vertebra-db` pools.

[^mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs::MysqlPool` — MySQL framing, scramble, and MysqlPool.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — mysql re-export.
[^tns-rs]: `src/rs/vertebra-db/src/oracle/tns.rs::OracleWalletMaterial` — tnsnames.ora parser and wallet directory loader.
