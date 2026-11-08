---
type: Task
id: "driver-fail-loud:ch05:t03"
chapter_id: ch05-mssql-codec-cleanup
title: Reduce the Rust mssql module to the placeholder rewriter
description: Delete TDS packet and SQLBatch framing, TdsPacketType, MSSQL_DIALECT_TAG, MssqlPool, and the framing test from vertebra-db, leaving rewrite_mssql_placeholders and its test.
status: stable
state: pending
priority: P2
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch05:t02"
read_set:
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-py/src/db.rs
files:
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
tests:
  - src/rs/vertebra-db/src/mssql/mod.rs
estimated_loc: 225
plan_revision: 2
plan_commit: fd571e3a
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
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders
    title: MSSQL module with TDS framing, MssqlPool, and placeholder rewriter
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Crate doc bullet and mssql re-export
tags:
  - vertebra-db
  - rust
  - mssql
  - cleanup
---

# Task t03: Reduce the Rust `mssql` Module to the Placeholder Rewriter

## Objective & Scope Boundary

Delete everything in `src/rs/vertebra-db/src/mssql/mod.rs` except `rewrite_mssql_placeholders` and its test: `MSSQL_DIALECT_TAG`, `TdsPacketType`, `encode_tds_packet`, `decode_tds_packet`, `encode_tds_sql_batch`, `MssqlPool`, `test_tds_packet_and_sql_batch_framing`, and the imports only they use. Update the module `//!` doc, the `lib.rs` crate-doc bullet, and the `lib.rs` re-export.[^mssql-rs][^db-lib]

Out of scope: the Python `MSSQLCore` TDS helpers (pure Python, kept); `vertebra-py` (t02 already removed the binding; `db.rs` imports only `rewrite_mssql_placeholders`).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/mssql/mod.rs` — whole file (383 lines).[^mssql-rs]
- `src/rs/vertebra-db/src/lib.rs` — crate doc bullet `(\`mssql\`)` and `pub use mssql::{...}`.[^db-lib]
- `src/rs/vertebra-py/src/db.rs` — confirm `use vertebra_db::mssql::rewrite_mssql_placeholders as rs_rewrite_mssql_placeholders;` is the only `mssql` import.

Grep-only verification: `grep -rnE "MssqlPool|TdsPacketType|encode_tds_packet|decode_tds_packet|encode_tds_sql_batch|MSSQL_DIALECT_TAG" src/rs --include=*.rs` lists only `mssql/mod.rs` and `lib.rs` lines before the change.

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/mssql/mod.rs` after the change contains, in order:
  1. `//! SQL Server single-pass \`@P1\` placeholder rewriter.`
  2. `pub fn rewrite_mssql_placeholders(sql: &str) -> (String, usize)` with its doc comment and body unchanged.
  3. `#[cfg(test)] mod tests { use super::*; #[test] fn test_rewrite_mssql_placeholders_preserves_quotes_and_comments() { ... } }` unchanged.
- Deleted imports: `std::sync::Arc`, `std::time::Duration`, `bytes::{BufMut, Bytes, BytesMut}`, `serde::{Deserialize, Serialize}`, `vertebra_core::broker::{LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats, StatementPlanEntry}`, `vertebra_core::shm::ShmRegion`. The rewriter uses only `String` and `format!`.
- `src/rs/vertebra-db/src/lib.rs`:
  - `pub use mssql::rewrite_mssql_placeholders;` replaces the five-name `pub use mssql::{...}`.
  - Crate-doc bullet becomes `//! - SQL Server single-pass \`@P1\` placeholder rewriter (\`mssql\`).` (replacing the two-line TDS 7.4 bullet).
- No `unsafe` code exists in the file; no `// SAFETY:` comment changes.

## Edge Cases & Error Semantics

- Rewriter behavior is unchanged; the deleted code shares no helper with it (`LexState` is local to the function).
- A leftover import triggers `unused_imports` under `-D warnings`; clippy fails until removed.
- `vertebra-py` compiles because t02 removed the `rs_encode_tds_sql_batch` import; if `db.rs` still names it, this task is blocked on t02.

## Public-Seam Test Blueprint

- **Canonical test location**: inline `#[cfg(test)] mod tests` in `src/rs/vertebra-db/src/mssql/mod.rs` (in-process).
- **Deleted test**: `mssql::tests::test_tds_packet_and_sql_batch_framing` (its subject is deleted; no surviving code loses coverage).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Test inventory before | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^mssql::tests::"` | `2` |
| Test inventory after | same command | `1` |
| Rewriter kept | `rewrite_mssql_placeholders("SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?")` | `("SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = @P1 AND b = @P2 -- ? $3 comment\n AND /* ? $4 */ c = @P3", 3)` |
| Python binding kept | `uv run --no-sync python -c "import vertebra._vertebra as v; print(v.rewrite_mssql_placeholders('a = ?'))"` after `make dev` | `('a = @P1', 1)` |
| Residue | `grep -rnE "MssqlPool\|TdsPacketType\|encode_tds_packet\|decode_tds_packet\|encode_tds_sql_batch\|MSSQL_DIALECT_TAG" src/rs --include=*.rs` | no output, exit 1 |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib mssql::` exits 0 with 2 tests.
- Final: `(cd src/rs && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings) && make rs-test && make dev && uv run --no-sync python -c "import vertebra._vertebra as v; assert v.rewrite_mssql_placeholders('a = ?') == ('a = @P1', 1)"`

## Acceptance Criteria

- [ ] `mssql/mod.rs` contains only the module doc, `rewrite_mssql_placeholders`, and its test (R8.1).
- [ ] `MSSQL_DIALECT_TAG`, `TdsPacketType`, TDS framing, and `MssqlPool` are gone from the workspace (R8.1).
- [ ] `lib.rs` re-exports only `rewrite_mssql_placeholders` from `mssql` and its doc bullet names the rewriter only (R8.1).
- [ ] `make rs-test` and workspace clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] `MssqlPool::lookup_or_compile_statement` was the only Rust caller of `rewrite_mssql_placeholders`; the PyO3 binding remains its production caller.

[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — MSSQL module with TDS framing, MssqlPool, and placeholder rewriter.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — Crate doc bullet and mssql re-export.
