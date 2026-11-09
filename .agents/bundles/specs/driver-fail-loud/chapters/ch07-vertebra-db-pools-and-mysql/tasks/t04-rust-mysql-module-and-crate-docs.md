---
type: Task
id: "driver-fail-loud:ch07:t04"
chapter_id: ch07-vertebra-db-pools-and-mysql
title: Delete the mysql module and sha2 and rewrite the crate doc and Cargo description
description: Delete src/rs/vertebra-db/src/mysql/mod.rs, its module declaration and re-exports, and the sha2 dependency, and rewrite the vertebra-db crate doc and Cargo description to describe codecs, dialers, transport, and shared-memory Arrow framing.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch07:t03"
read_set:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/Cargo.toml
files:
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/Cargo.toml
tests:
  - src/rs/vertebra-db/src/lib.rs
estimated_loc: 185
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
verification_evidence: "cargo test -p vertebra-db --features gcp and cargo doc -p vertebra-db --no-deps passed"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T14:38:00Z"
commit: ccd45c42
sources:
  - id: mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs::compute_caching_sha2_scramble
    title: MySQL framing and caching_sha2 scramble, the sole sha2 user
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Crate doc, module list, and re-exports
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: Package description and dependencies
tags:
  - vertebra-db
  - rust
  - mysql
  - dependencies
  - docs
  - cleanup
---

# Task t04: Delete the `mysql` Module and `sha2` and Rewrite the Crate Doc and Cargo Description

## Objective & Scope Boundary

Delete `src/rs/vertebra-db/src/mysql/mod.rs`, `pub mod mysql;`, and `pub use mysql::{...}` from `lib.rs`, and `sha2 = { workspace = true }` from `vertebra-db/Cargo.toml`; let Cargo rewrite `Cargo.lock` (the `vertebra-db` entry loses `sha2`; the `sha2` package stays for `vertebra-core` and `vertebra-storage`). Rewrite the crate doc header and Cargo `description` so the crate describes codecs, dialers, transport, and shared-memory Arrow framing.[^mysql-rs][^db-lib][^db-cargo]

The hand-edited files are `mysql/mod.rs` (deleted), `lib.rs`, and `vertebra-db/Cargo.toml`; `src/rs/Cargo.lock` is regenerated automatically by `cargo check` and committed with the manifest change.

Out of scope: the workspace `sha2 = "0.10"` entry (still used); `rsa` with its `sha2` feature (dialer dependency); other possibly unused dependencies (see Notes).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/mysql/mod.rs` — confirm it holds only framing, scramble, command encoders, and two tests after t03.[^mysql-rs]
- `src/rs/vertebra-db/src/lib.rs` — whole file (crate doc, module list, re-exports).[^db-lib]
- `src/rs/vertebra-db/Cargo.toml` — `description`, `sha2` line.[^db-cargo]

Grep-only verification: `grep -rn "vertebra_db::mysql\|mysql::" src/rs --include=*.rs` lists only `lib.rs` and `mysql/mod.rs`; `grep -rn "sha2" src/rs/vertebra-db` lists only `Cargo.toml` (`sha2` line and the `rsa` feature), `mysql/mod.rs`, and the `lib.rs` crate-doc bullet; `grep -n "SHM pool" src/rs/vertebra-db/src/lib.rs` lists the SQLite bullet this task rewrites.

## Symbol & Signature Contracts

- Delete the file `src/rs/vertebra-db/src/mysql/mod.rs` (`git rm`).
- `src/rs/vertebra-db/Cargo.toml`:
  - `description = "Database wire codecs, Cloud SQL and AlloyDB dialers, shared transport, and shared-memory Arrow framing for Vertebra"`.
  - Delete `sha2 = { workspace = true }`; keep `rsa = { version = "0.9", features = ["sha2"] }`.
- `src/rs/vertebra-db/src/lib.rs` crate doc after the change (the `mssql` bullet matches `ch05-mssql-codec-cleanup` t03, the `oracle` bullet matches `ch06-oracle-codec-cleanup` t01, and the `postgres` and `duckdb` bullets match this chapter's t01; this task writes the `sqlite` bullet, replacing the one that still names the SHM pool):

```rust
//! `vertebra-db` — Database wire codecs, Cloud SQL and AlloyDB mTLS dialers, shared transport, and
//! shared-memory Arrow framing.
//!
//! Provides:
//! - Zero-sidecar Google Cloud SQL (`:3307`) and AlloyDB (`:5433`) mTLS dialers, ephemeral keypair
//!   generation (`rcgen`), and `MetadataExchangeRequest` / `MetadataExchangeResponse` framing (`dialer`).
//! - Shared async transport stream (TCP, `tokio-rustls`, Unix socket) and the `Connection` /
//!   `CancelHandle` traits (`connection`).
//! - Shared-memory `VTBARROW` Arrow IPC framing and multi-batch stream writer (`arrow_shm`).
//! - PostgreSQL v3 frontend frame encoders, pooler topology rules (`Direct`, `PgBouncerTransaction`,
//!   `PgCat`, `Supavisor`), binary `JSONB` header stripping, `PGCOPY` binary codec, and pgvector codecs (`postgres`).
//! - SQLite 3.45+ WAL / `mmap_size` / `busy_timeout` initialization pragmas (`sqlite`).
//! - Oracle `tnsnames.ora` parser and alias resolver (`oracle::tns`), TTC piggybacked session
//!   attributes, and 26ai `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) and `OSON` codecs (`oracle`).
//! - SQL Server single-pass `@P1` placeholder rewriter (`mssql`).
//! - DuckDB 2.0-safe `SET` bootstrap statements and `ShmRegion` Arrow IPC frame re-exports (`duckdb`).
//! - Redis 7.2+/8.0+ and Valkey 8.0+/9.0+ RESP3 wire framing, recursive parser, `invalidate`
//!   push frame extractor, Sentinel master discovery, and Cluster CRC16-XMODEM slot routing (`resp3`).
//! - Shared credential and TLS wiring for the official Google Cloud Rust clients (`gcp`).
```

- `lib.rs` module list and re-exports match chapter spec §2.2 exactly (no `mysql`). §2.2 lists every retained block, including the unchanged `pub use arrow_shm::{...}`, `pub use connection::{...}`, `pub use dialer::{...}`, and `#[cfg(feature = "gcp")] pub use gcp::{...}` blocks; only `pub mod mysql;` and `pub use mysql::{...}` leave in this task.
- No `unsafe` code exists in the deleted file.

## Edge Cases & Error Semantics

- `cargo build --locked` fails until `Cargo.lock` is regenerated; the lockfile change ships in this commit.
- `sha2` remains in `Cargo.lock` because `vertebra-core` and `vertebra-storage` depend on it; only the `vertebra-db` dependency edge disappears.
- `make lint-all` runs `cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings`; a dangling `mysql` reference fails compilation.

## Public-Seam Test Blueprint

- **Canonical test location**: `vertebra-db` inline tests and integration tests (`tests/arrow_shm_stream_tests.rs`, `tests/gcp_credentials_tests.rs`) via `make rs-test`.
- **Deleted tests**: `mysql::tests::test_mysql_packet_framing_and_commands`, `mysql::tests::test_caching_sha2_scramble` (subjects deleted).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| MySQL inventory | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^mysql::"` | `0` |
| Kept inventory | `... \| grep -cE "^(mssql\|oracle\|postgres\|sqlite\|duckdb\|resp3)::"` | `17` (1 mssql + 3 oracle + 1 oracle::tns + 5 postgres + 1 sqlite + 2 duckdb + 4 resp3) |
| Description | `grep '^description' src/rs/vertebra-db/Cargo.toml` | `description = "Database wire codecs, Cloud SQL and AlloyDB dialers, shared transport, and shared-memory Arrow framing for Vertebra"` |
| sha2 edge | `cd src/rs && cargo tree -p vertebra-db -e normal --depth 1 \| grep -c " sha2 "` | `0` |
| Residue | `grep -rnE "mysql\|Native Rust database wire protocols\|connection pool" src/rs/vertebra-db/src/lib.rs` | no output, exit 1 |
| R8 residue across ch05, ch06, and ch07 | `grep -rnE "MssqlPool\|OraclePool\|PostgresPool\|SqlitePool\|MysqlPool\|DuckdbBrokerPool\|TdsPacketType\|TnsPacketType\|_DIALECT_TAG\|OracleWalletMaterial\|rmp_serde" src/rs --include=*.rs` | no output, exit 1 |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib mysql::` exits 0 with 2 tests.
- Final: `(cd src/rs && cargo check -p vertebra-db && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings && cargo check --workspace --locked && cargo doc -p vertebra-db --no-deps) && make rs-test && make dev && uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner && uv run --no-sync pytest src/py/tests/unit/db -q`

## Acceptance Criteria

- [x] `src/rs/vertebra-db/src/mysql/` no longer exists and `lib.rs` declares no `mysql` module (R8.6).
- [x] `vertebra-db/Cargo.toml` has no `sha2` dependency line and carries the new `description` (R8.6).
- [x] The crate doc matches the contract text; module list and re-exports match chapter spec §2.2 (R8.6).
- [x] Chapter Recovery Checkpoint exits 0.

## Notes & Discoveries

- [2026-11-07 00:00] A grep of `vertebra-db/src` and `tests` finds no direct use of `base64`, `chrono`, `futures`, `hex`, `hmac`, `http-body-util`, `hyper`, `hyper-util`, `lz4_flex`, `reqwest`, `serde_json`, `url`, or `zstd`; they are candidates for a `cargo machete` follow-up outside this chapter.

[^mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs::compute_caching_sha2_scramble` — MySQL framing and caching_sha2 scramble, the sole sha2 user.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — Crate doc, module list, and re-exports.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — Package description and dependencies.
