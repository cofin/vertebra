---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch06-oracle-codec-cleanup
title: "Chapter 06: Oracle Codec Cleanup"
description: Delete Rust TNS framing, TnsPacketType, ORACLE_DIALECT_TAG, and OraclePool, remove the Oracle TNS and Redis hash-slot probes for natives that vertebra._vertebra never exports, and rewrite Oracle and DuckDB module and adapter docs to describe codecs.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-mssql-codec-cleanup
replan_trigger: null
current_task: "driver-fail-loud:ch06:t01"
plan_revision: 1
plan_commit: 1e1f028a
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
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Python codec bindings on vertebra._vertebra
  - id: oracle-rs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Oracle TTC, VECTOR, OSON codecs, TNS framing, and OraclePool
  - id: tns-rs
    resource: src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora
    title: tnsnames.ora parser and alias resolver kept by this chapter
  - id: vector-rs
    resource: src/rs/vertebra-py/src/vector.rs
    title: PyO3 vector bindings importing Oracle VECTOR encoders
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot
    title: RESP3 codecs and Redis hash slot kept for the Python Redis driver
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Python Oracle TNS helpers and VECTOR/OSON codecs
  - id: oracle-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Oracle configuration, connect descriptors, and wallets
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot
    title: Python Redis hash slot with native probe
  - id: duckdb-init
    resource: src/py/vertebra/db/adapters/duckdb/__init__.py
    title: DuckDB adapter package docstring
tags:
  - vertebra-db
  - rust
  - oracle
  - redis
  - duckdb
  - cleanup
  - dead-code
---

# Chapter 06: Oracle Codec Cleanup

<!-- truth: start -->
- **Oracle keeps codecs**: `vertebra_db::oracle` keeps `OracleSessionAttrs`, `VECTOR`, `OSON`, and the `tns` submodule; `ORACLE_DIALECT_TAG`, `TnsPacketType`, `encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet`, and `OraclePool` are deleted.[^oracle-rs][^tns-rs]
- **Kept bindings**: `vector.rs` and `db.rs` import only VECTOR, OSON, TTC, and `tns::{parse_tnsnames_ora, resolve_tns_alias}`; no binding or stub changes in this chapter.[^vector-rs][^db-bindings]
- **Python probes**: `OracleCore.encode_tns_connect_packet` / `encode_tns_data_packet` stop probing natives that `_vertebra` never exports; `RedisCore.hash_slot` stops probing `redis_hash_slot_native` and keeps the exported `redis_hash_slot`.[^oracle-core][^redis-core][^resp3-rs]
- **Python TNS framing stays**: pure-Python `encode_tns_connect_packet`, `encode_tns_data_packet`, and `decode_tns_packet` bodies and exports are unchanged.
- **Future import**: `oracle/{core,config,__init__}.py` and `duckdb/__init__.py` drop `from __future__ import annotations`; `oracle/config.py` string-quotes its five self-referencing return annotations.[^oracle-config]
- **Docs say codecs**: the `oracle` module `//!` doc, its crate-doc bullet, and the Oracle and DuckDB adapter module and class docstrings describe codecs and helpers, not a native Thin driver or SHM-brokered DuckDB.[^db-lib][^duckdb-init]
- **Build stays green**: every task commit builds the workspace and passes `make rs-test`; stubtest stays at exit 0 because no binding changes.
- **Out of chapter**: `OracleWalletMaterial`, pools, `mysql`, `sha2`, `rmp-serde`, the crate doc header, and the Cargo `description` belong to [ch07](../ch07-vertebra-db-pools-and-mysql/spec.md); RESP3 codec removal belongs to `native-redis-driver`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vertebra-db` carries Oracle TNS Connect/Data packet framing and a connectionless `OraclePool` lease table that no Rust or Python caller uses.[^oracle-rs] Python `OracleCore` probes `_vertebra` for `encode_tns_connect_packet` and `encode_tns_data_packet`, and `RedisCore.hash_slot` falls back to probing `redis_hash_slot_native`; `_vertebra` exports none of the three.[^oracle-core][^redis-core] The Oracle adapter docstrings describe a native Thin TNS/TTC driver, and the DuckDB package docstring describes SHM single-writer/multi-reader coordination that ch02 replaces with a `duckdb`-package driver.[^oracle-config][^duckdb-init] The `oracledb` client selected by `native-oracle-driver` frames TNS itself.

This chapter deletes that Oracle surface and the dead probes, and rewrites the Oracle and DuckDB docs so they describe what remains: TTC session attributes, `VECTOR`/`OSON` codecs, TNS parsing, and pure-Python TNS packet helpers.

**Non-goals**:

- MSSQL framing, the `encode_tds_sql_batch` binding, and MSSQL Python wrappers (owned by [ch05](../ch05-mssql-codec-cleanup/spec.md)).
- `OracleWalletMaterial` in `oracle/tns.rs`, PostgreSQL, SQLite, DuckDB, and MySQL pools, `sha2`, `rmp-serde`, the crate doc header, and the Cargo `description` (owned by [ch07](../ch07-vertebra-db-pools-and-mysql/spec.md)).
- RESP3 codec removal and every other `redis/core.py` line (owned by `native-redis-driver`).
- `oracle/driver.py` and `OracleCore.normalize_sql` (owned by ch01).
- User docs and OKF chapters (owned by ch11).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Depends on [ch05-mssql-codec-cleanup](../ch05-mssql-codec-cleanup/spec.md), which depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md) and transitively ch01, ch02, and ch03:
  - ch05 t03 rewrites the `mssql` re-export and crate-doc bullet in `lib.rs`, which t01 edits next.
  - ch01 removes `OracleCore.normalize_sql` and the `Mapping` import from `oracle/core.py`, which t02 edits next.
  - ch02 t04 makes the DuckDB driver require the `duckdb` package, which t03's `duckdb/__init__.py` docstring describes.
  - ch04 t01 retargets `test_redis_valkey_resp3.py` onto `resp3_server` and ch04 t02 deletes the Redis driver memory emulation; t03 runs that test file after both.
- Precondition check before t01: `grep -rnE "MssqlPool|TdsPacketType" src/rs --include=*.rs` prints nothing.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Rust Oracle | `oracle/mod.rs::OracleSessionAttrs`[^oracle-rs], `oracle/tns.rs::parse_tnsnames_ora`[^tns-rs] | `vector.rs`[^vector-rs] and `db.rs`[^db-bindings] import VECTOR, OSON, TTC, and TNS parsing only; TNS framing and `OraclePool` are reached only through the `lib.rs` re-export, which no crate imports |
| Crate root | `vertebra-db/src/lib.rs`[^db-lib] | Consumers `vertebra-py`, `vertebra-bigquery`, `vertebra-spanner` import no TNS framing or `OraclePool` symbol |
| RESP3 | `resp3/mod.rs::redis_hash_slot`[^resp3-rs] | Kept whole; `RedisCore.hash_slot` reaches it through the exported `redis_hash_slot` binding |
| Python Oracle | `oracle/core.py::OracleCore`[^oracle-core], `oracle/config.py::OracleConfig`[^oracle-config], `oracle/__init__.py` | Lazy exports; tests in `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| Python Redis / DuckDB | `redis/core.py::RedisCore.hash_slot`[^redis-core], `duckdb/__init__.py`[^duckdb-init] | Tests in `test_redis_valkey_resp3.py` and `test_enterprise_olap_drivers.py` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels and SHM ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md) (`runtime-ipc` owns `src/rs/vertebra-db/**`)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8.2** (root R8, Oracle portion; t01): `vertebra_db::oracle` loses TNS framing, `TnsPacketType`, `ORACLE_DIALECT_TAG`, and `OraclePool`; the module doc and crate-doc bullet name TTC, VECTOR, OSON, and TNS parsing.
- **R9.2** (root R9, Oracle, Redis, and DuckDB Python portion; t02, t03): the Oracle TNS probes are deleted and Oracle adapter docstrings describe codecs without the future import (t02); the `redis_hash_slot_native` probe is deleted and the DuckDB package docstring describes the `duckdb`-package driver without the future import (t03).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

`oracle` re-export in `src/rs/vertebra-db/src/lib.rs` after this chapter (other re-exports unchanged until ch07):

```rust
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
```

Retained Rust signatures (unchanged): `impl OracleSessionAttrs { pub fn encode_ttc_piggyback_session_attrs(&self) -> Bytes }`; `pub fn parse_tnsnames_ora(contents: &str) -> HashMap<String, String>`; `pub fn resolve_tns_alias(contents: &str, alias: &str) -> Option<OracleConnectDescriptor>`; `pub fn redis_hash_slot(key: &[u8]) -> u16`.

Retained Python signatures (unchanged; probes removed):

```python
class OracleCore:
    @staticmethod
    def encode_tns_connect_packet(descriptor: str) -> bytes: ...
    @staticmethod
    def encode_tns_data_packet(data_flags: int, payload: bytes) -> bytes: ...

class RedisCore:
    @staticmethod
    def hash_slot(key: str | bytes) -> int: ...
```

`oracle/config.py` return annotations after the future-import removal: `OracleConnectDescriptor.from_tns_descriptor -> "OracleConnectDescriptor"`, `OracleConnectDescriptor.resolve_tns_alias -> "OracleConnectDescriptor | None"`, `OracleWallet.from_zip_bytes -> "OracleWallet"`, `OracleWallet.from_path -> "OracleWallet"`, `OracleConfig.derive_pool -> "OracleConfig"`. No binding or stub changes.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `OracleCore.encode_tns_connect_packet` / `encode_tns_data_packet` → `struct.pack` (pure Python; no native probe).
- `OracleCore.encode_vector_*` / `decode_vector_*` → `vertebra._vertebra.{encode,decode}_oracle_vector_*` → `vertebra_db::oracle::{...}` (kept).
- `vertebra-py/src/vector.rs` → `vertebra_db::oracle::encode_oracle_vector_{binary,f32,i8}` (kept).
- `vertebra-py/src/db.rs` → `vertebra_db::oracle::{OracleSessionAttrs, VECTOR, OSON}` and `oracle::tns::{parse_tnsnames_ora, resolve_tns_alias}` (kept).
- `RedisCore.hash_slot` → `getattr(native, "redis_hash_slot", None)` → `vertebra_db::resp3::redis_hash_slot`, else pure-Python CRC16.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Shared `lib.rs` | ch05 t03 and this chapter's t01 both edit `lib.rs` | t01 runs after ch05 completes | Every commit compiles |
| Oracle imports | `BufMut`, `serde` still used by VECTOR and `OracleSessionAttrs` | Only `Arc`, `Duration`, `vertebra_core::broker`, `ShmRegion` imports go | Clippy `-D warnings` exits 0 |
| Probe removal | `encode_tns_data_packet(0x0040, b"TTC_QUERY")` | `b"\x00\x13\x00\x00\x06\x00\x00\x00\x00\x40TTC_QUERY"` | Byte output identical; `_vertebra` never exported the probed names |
| Redis slot | `hash_slot("123456789")` | `12739` | Exported `redis_hash_slot` still used when built |
| Self-referencing annotation | `oracle/config.py` without the future import | Five return annotations are string-quoted | `import vertebra.db.adapters.oracle.config` raises no `NameError` |
| Runtime-resolved names | `oracle/core.py` annotations `TenantScopeConfig`, `Sequence` | Both are imported at module top | No quoting needed |
| Python TNS framing kept | `decode_tns_packet(encode_tns_connect_packet(desc))` | `(1, 0, desc.encode())` | Exported API unchanged |
| Unsafe code | Deleted Rust regions | No `unsafe` block exists | `// SAFETY:` rule unaffected |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the workspace, when `grep -rnE "OraclePool|TnsPacketType|ORACLE_DIALECT_TAG|encode_tns_connect_packet|encode_tns_data_packet|decode_tns_packet" src/rs --include=*.rs` runs, then it prints nothing.
- Given the workspace, when `make rs-test` runs, then it exits 0 and `cargo test -p vertebra-db --lib -- --list` lists three `oracle::tests::` tests.
- Given `src/py`, when `grep -rn "redis_hash_slot_native\|hasattr(native, \"encode_tns" src/py` runs, then it prints nothing.
- Given the four touched Oracle/DuckDB modules, when grepped for `from __future__`, then nothing prints and `python -c "import vertebra.db.adapters.oracle.config, vertebra.db.adapters.oracle.core, vertebra.db.adapters.duckdb"` exits 0.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Gate TNS framing behind a `legacy` Cargo feature | Keeps dead code compiled under `--all-features`; no caller enables it | Rejected: dead code with a feature flag is still dead code |
| Delete Python TNS pure framing too | Exported package API with tests; outside root R9 | Rejected for this chapter: `native-oracle-driver` decides its fate |
| Delete `OracleWalletMaterial` here with the rest of `oracle` | Lives in `tns.rs`, another file, and pushes t01 past 250 lines | Rejected: ch07 t03 deletes it |
| Keep the Redis probe edit in `native-redis-driver` | One-clause dead probe unrelated to RESP3 removal | Rejected: it is an unexported-native probe, the same class of dead branch as the Oracle probes |
| Combine Oracle, Redis, and DuckDB Python edits in one task | Five production files | Rejected: Oracle in t02, Redis and DuckDB in t03 |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch06]`. In-code docs (the `oracle` module `//!` doc and crate-doc bullet, the Oracle adapter module and class docstrings, the DuckDB package docstring) are part of each task's production change. User docs and OKF (`database-drivers-and-dialers.md` codec inventory) are owned by ch11.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R8.2 | [t01](tasks/t01-rust-oracle-codecs-only.md) | `static_validation` | `make rs-test`, `oracle::tests::*`, `test_enterprise_olap_drivers.py -k vector_oson` |
| R9.2 (Oracle) | [t02](tasks/t02-oracle-python-cleanup.md) | `static_validation` | `test_enterprise_olap_drivers.py`, ruff, mypy |
| R9.2 (Redis, DuckDB) | [t03](tasks/t03-redis-probe-and-duckdb-docstring.md) | `static_validation` | `test_redis_valkey_resp3.py`, `test_enterprise_olap_drivers.py -k duckdb`, ruff, mypy |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch06:t01 — Reduce the Rust oracle module to TTC, VECTOR, and OSON codecs (tasks/t01-rust-oracle-codecs-only.md)
- [ ] driver-fail-loud:ch06:t02 — Remove Oracle TNS probes and rewrite Oracle adapter docstrings (tasks/t02-oracle-python-cleanup.md)
- [ ] driver-fail-loud:ch06:t03 — Remove the redis_hash_slot_native probe and rewrite the DuckDB package docstring (tasks/t03-redis-probe-and-duckdb-docstring.md)
- **Chapter Recovery Checkpoint**: `make lint-all && make rs-test && make py-test` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 0acdf576`; `depends_on_chapters: [ch05-mssql-codec-cleanup]` (transitively ch01 through ch04); `state: planned`.
- **Remaining Tasks in Chapter**: `t01` through `t03` (`0/3` completed). The three tasks touch disjoint files and have no task dependency; they run in listed order.
- **Downstream**: [ch07-vertebra-db-pools-and-mysql](../ch07-vertebra-db-pools-and-mysql/spec.md) depends on this chapter, edits the same `lib.rs`, and deletes `OracleWalletMaterial` from `oracle/tns.rs`.
- **Open Doc/OKF Items**: none in this chapter; ch11 owns user docs and OKF.
- **Active Invariants**: every commit builds the workspace and passes `make rs-test`; RESP3, TNS parsing, VECTOR/OSON, TTC session attributes, and pure-Python TNS framing stay.

[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate doc, module list, and re-exports.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings on vertebra._vertebra.
[^oracle-rs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Oracle TTC, VECTOR, OSON codecs, TNS framing, and OraclePool.
[^tns-rs]: `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` — tnsnames.ora parser and alias resolver kept by this chapter.
[^vector-rs]: `src/rs/vertebra-py/src/vector.rs` — PyO3 vector bindings importing Oracle VECTOR encoders.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot` — RESP3 codecs and Redis hash slot kept for the Python Redis driver.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Python Oracle TNS helpers and VECTOR/OSON codecs.
[^oracle-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Oracle configuration, connect descriptors, and wallets.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot` — Python Redis hash slot with native probe.
[^duckdb-init]: `src/py/vertebra/db/adapters/duckdb/__init__.py` — DuckDB adapter package docstring.
