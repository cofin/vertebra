---
type: Task
id: "driver-fail-loud:ch06:t01"
chapter_id: ch06-oracle-codec-cleanup
title: Reduce the Rust oracle module to TTC, VECTOR, and OSON codecs
description: Delete TNS Connect/Data framing, TnsPacketType, ORACLE_DIALECT_TAG, OraclePool, and the TNS framing test from vertebra-db, keeping OracleSessionAttrs, VECTOR, OSON codecs, and the tns submodule.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/rs/vertebra-db/src/oracle/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-py/src/db.rs
  - src/rs/vertebra-py/src/vector.rs
files:
  - src/rs/vertebra-db/src/oracle/mod.rs
  - src/rs/vertebra-db/src/lib.rs
tests:
  - src/rs/vertebra-db/src/oracle/mod.rs
estimated_loc: 240
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T14:32:00Z"
last_verified_commit: ef6be9a5
verification_evidence: "cargo test -p vertebra-db --lib oracle:: (5 passed)"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T14:32:00Z"
commit: ef6be9a5
sources:
  - id: oracle-rs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Oracle module with TNS framing, OraclePool, and kept codecs
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Crate doc bullet and oracle re-export
  - id: vector-rs
    resource: src/rs/vertebra-py/src/vector.rs
    title: PyO3 vector bindings importing Oracle VECTOR encoders
tags:
  - vertebra-db
  - rust
  - oracle
  - cleanup
---

# Task t01: Reduce the Rust `oracle` Module to TTC, VECTOR, and OSON Codecs

## Objective & Scope Boundary

Delete `ORACLE_DIALECT_TAG`, `TnsPacketType`, `encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet`, `OraclePool`, `test_tns_connect_and_data_framing`, and the imports only they use from `src/rs/vertebra-db/src/oracle/mod.rs`. Update the module `//!` doc, the `lib.rs` crate-doc bullet, and the `lib.rs` re-export.[^oracle-rs][^db-lib]

Out of scope: `oracle/tns.rs` (`OracleWalletMaterial` goes in `ch07-vertebra-db-pools-and-mysql` t03; `parse_tnsnames_ora` and `resolve_tns_alias` stay); Python `OracleCore` TNS helpers (t02 removes only their native probes).

Precondition: chapter `ch05-mssql-codec-cleanup` is completed, so `lib.rs` already carries `pub use mssql::rewrite_mssql_placeholders;` and the reduced `mssql` crate-doc bullet; `grep -rnE "MssqlPool|TdsPacketType" src/rs --include=*.rs` prints nothing.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/oracle/mod.rs` — whole file (502 lines).[^oracle-rs]
- `src/rs/vertebra-db/src/lib.rs` — crate doc bullet `(\`oracle\`)` and `pub use oracle::{...}`.[^db-lib]
- `src/rs/vertebra-py/src/db.rs` — `use vertebra_db::oracle::{...}` imports `OracleSessionAttrs`, VECTOR, and OSON functions only.
- `src/rs/vertebra-py/src/vector.rs` — imports `encode_oracle_vector_{binary,f32,i8}` only.[^vector-rs]

Grep-only verification: `grep -rnE "OraclePool|TnsPacketType|encode_tns_connect_packet|encode_tns_data_packet|decode_tns_packet|ORACLE_DIALECT_TAG" src/rs --include=*.rs` lists only `oracle/mod.rs` and `lib.rs`.

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/oracle/mod.rs` after the change keeps, unchanged: `pub mod tns;`, `OSON_MAGIC`, `ORACLE_VECTOR_MAGIC`, `ORACLE_VECTOR_VERSION`, `OracleSessionAttrs` with `encode_ttc_piggyback_session_attrs(&self) -> Bytes`, `OracleVectorFormat`, private `encode_vector_header` / `decode_vector_header`, `encode_oracle_vector_{f32,f64,i8,binary}`, `decode_oracle_vector_{f32,f64,i8,binary}`, `encode_oson_payload`, `decode_oson_payload`, and tests `test_ttc_piggyback_session_attrs`, `test_oracle_26ai_vector_codecs`, `test_oracle_26ai_oson_codec`.
- Module doc: `//! Oracle TTC piggybacked session attributes and 26ai \`VECTOR\` and \`OSON\` binary codecs.`
- Imports after: `use bytes::{BufMut, Bytes, BytesMut};` and `use serde::{Deserialize, Serialize};` (still used by `OracleSessionAttrs` and `OracleVectorFormat`). Deleted: `std::sync::Arc`, `std::time::Duration`, `vertebra_core::broker::{...}`, `vertebra_core::shm::ShmRegion`.
- `src/rs/vertebra-db/src/lib.rs`:

```rust
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
```

  - Crate-doc bullet becomes:

```rust
//! - Oracle `tnsnames.ora` parser and alias resolver (`oracle::tns`), TTC piggybacked session
//!   attributes, and 26ai `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) and `OSON` codecs (`oracle`).
```

- No `unsafe` code exists in the file.

## Edge Cases & Error Semantics

- `decode_vector_header` and `encode_vector_header` are private and used only by VECTOR codecs; they stay.
- `BufMut` stays in use (`put_u8`, `put_f32`); removing it breaks the build.
- A leftover `Arc`/`Duration`/broker import fails clippy `-D warnings`.

## Public-Seam Test Blueprint

- **Canonical test location**: inline `#[cfg(test)] mod tests` in `src/rs/vertebra-db/src/oracle/mod.rs`; Python seam in `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs`.
- **Deleted test**: `oracle::tests::test_tns_connect_and_data_framing` (subject deleted).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Inventory before | `cd src/rs && cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^oracle::tests::"` | `4` |
| Inventory after | same command | `3` |
| TNS parser kept | `cargo test -p vertebra-db --lib -- --list 2>/dev/null \| grep -c "^oracle::tns::tests::"` | `2` (`ch07-vertebra-db-pools-and-mysql` t03 removes the wallet test) |
| VECTOR roundtrip | `decode_oracle_vector_f32(&encode_oracle_vector_f32(&[0.25, -0.5, 1.0, 3.5]))` | `Ok(vec![0.25, -0.5, 1.0, 3.5])` |
| OSON reject | `decode_oson_payload(b"bad")` | `Err("buffer too short for 8-byte OSON header")` |
| Python VECTOR binding | `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k "vector_oson" -q` after `make dev` | `1 passed` |

## Verification

- Baseline: `cd src/rs && cargo test -p vertebra-db --lib oracle::` exits 0 with 6 tests (4 module + 2 tns).
- Final: `(cd src/rs && cargo fmt --all --check && cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings) && make rs-test && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k "vector_oson or tns_descriptor" -q`
- Residue: `grep -rnE "OraclePool|TnsPacketType|encode_tns_connect_packet|encode_tns_data_packet|decode_tns_packet|ORACLE_DIALECT_TAG" src/rs --include=*.rs` prints nothing.

## Acceptance Criteria

- [x] TNS framing, `TnsPacketType`, `ORACLE_DIALECT_TAG`, and `OraclePool` are gone from `vertebra-db` (R8.2).
- [x] VECTOR, OSON, and TTC codecs and their three tests are unchanged and pass (R8.2).
- [x] `lib.rs` oracle re-export and doc bullet match the contract above (R8.2).
- [x] `make rs-test` and workspace clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] The Python `OracleCore.encode_tns_connect_packet` keeps its pure-Python `struct.pack` body; only Rust framing is deleted here.

[^oracle-rs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Oracle module with TNS framing, OraclePool, and kept codecs.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — Crate doc bullet and oracle re-export.
[^vector-rs]: `src/rs/vertebra-py/src/vector.rs` — PyO3 vector bindings importing Oracle VECTOR encoders.
