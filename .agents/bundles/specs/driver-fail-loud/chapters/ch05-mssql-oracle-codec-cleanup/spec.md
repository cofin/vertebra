---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch05-mssql-oracle-codec-cleanup
title: "Chapter 05: MSSQL and Oracle Codec Cleanup"
description: Delete the encode_tds_sql_batch binding and stub, Rust TDS and TNS framing, MssqlPool, OraclePool, the MSSQL and Oracle dialect tags, and Python probes for unexported natives, and rewrite MSSQL, Oracle, and DuckDB module and adapter docs to describe codecs instead of drivers.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-redis-valkey-memory-removal
replan_trigger: null
current_task: "driver-fail-loud:ch05:t01"
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified: []
sources:
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db crate doc, module list, and re-exports
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Python codec bindings on vertebra._vertebra
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Stubtest-gated type stub for vertebra._vertebra
  - id: mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders
    title: MSSQL placeholder rewriter, TDS framing, and MssqlPool
  - id: oracle-rs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Oracle TTC, VECTOR, OSON codecs, TNS framing, and OraclePool
  - id: tns-rs
    resource: src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora
    title: tnsnames.ora parser and alias resolver kept by this chapter
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot
    title: RESP3 codecs and Redis hash slot kept for the Python Redis driver
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore
    title: Python MSSQL TDS helpers and placeholder rewriter
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Python Oracle TNS helpers and VECTOR/OSON codecs
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot
    title: Python Redis hash slot with native probe
  - id: duckdb-init
    resource: src/py/vertebra/db/adapters/duckdb/__init__.py
    title: DuckDB adapter package docstring
tags:
  - vertebra-db
  - rust
  - pyo3
  - mssql
  - oracle
  - cleanup
  - dead-code
---

# Chapter 05: MSSQL and Oracle Codec Cleanup

<!-- truth: start -->
- **Binding surface**: `encode_tds_sql_batch` leaves `register_db_bindings` and `_vertebra.pyi` in one commit; every other codec binding stays.[^db-bindings][^vertebra-pyi]
- **MSSQL keeps only the rewriter**: `vertebra_db::mssql` keeps `rewrite_mssql_placeholders` and its test; `MSSQL_DIALECT_TAG`, `TdsPacketType`, `encode_tds_packet`, `decode_tds_packet`, `encode_tds_sql_batch`, and `MssqlPool` are deleted.[^mssql-rs]
- **Oracle keeps codecs**: `vertebra_db::oracle` keeps `OracleSessionAttrs`, `VECTOR`, `OSON`, and the `tns` submodule; `ORACLE_DIALECT_TAG`, `TnsPacketType`, `encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet`, and `OraclePool` are deleted.[^oracle-rs][^tns-rs]
- **Python probes**: `OracleCore.encode_tns_connect_packet`/`encode_tns_data_packet`, `MSSQLCore.encode_tds_packet`, and `RedisCore.hash_slot` stop probing natives that `_vertebra` never exports; their pure-Python bodies are unchanged.[^oracle-core][^mssql-core][^redis-core]
- **Python API removal**: `vertebra.db.adapters.mssql.encode_tds_sql_batch`, `MSSQLCore.encode_tds_sql_batch`, and `MSSQLConfig.encode_sql_batch` are deleted without aliases.[^mssql-core]
- **Docs say codecs**: the `mssql` and `oracle` module `//!` docs and crate-doc bullets, and the MSSQL, Oracle, and DuckDB adapter docstrings, describe codecs and helpers, not native drivers.[^db-lib][^duckdb-init]
- **Build stays green**: every task commit builds the workspace and keeps `mypy.stubtest vertebra._vertebra` at exit 0; every task commit that touches Rust (t01, t02, t03) passes `make rs-test`.
- **Out of chapter**: `PostgresPool`, `SqlitePool`, `DuckdbBrokerPool`, `MysqlPool`, the `mysql` module, `OracleWalletMaterial`, `sha2`, `rmp-serde`, the crate doc header, and the Cargo `description` belong to [ch06](../ch06-vertebra-db-pools-and-mysql/spec.md).
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The PyO3 layer exposes `encode_tds_sql_batch` solely so the MSSQL SQLite stand-in could record a TDS batch it never sent.[^db-bindings] `vertebra-db` carries MSSQL TDS and Oracle TNS packet framing that no Rust or Python caller uses, plus connectionless `MssqlPool` and `OraclePool` lease tables that never open a connection.[^mssql-rs][^oracle-rs] Three Python helpers probe `_vertebra` for functions it never exports.[^mssql-core][^oracle-core][^redis-core] The engine client crates selected by the per-engine driver flows (`tiberius`, `oracledb`) replace that framing.

This chapter deletes the MSSQL and Oracle portion of that code and rewrites the MSSQL, Oracle, and DuckDB module and adapter docs so they describe what remains: the placeholder rewriter, TTC session attributes, `VECTOR`/`OSON` codecs, and TNS parsing.

**Non-goals**:

- PostgreSQL, SQLite, DuckDB, and MySQL pools, the `mysql` module, `OracleWalletMaterial`, the `sha2` and `rmp-serde` dependencies, the crate doc header, and the Cargo `description` (owned by [ch06](../ch06-vertebra-db-pools-and-mysql/spec.md)).
- RESP3 codec removal and the Python Redis driver (owned by `native-redis-driver`); `redis/core.py` changes by one probe only.
- Python-side TDS/TNS framing (`MSSQLCore.encode_tds_packet`, `decode_tds_packet`, `TDS_PKT_*`, `OracleCore.encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet`): pure-Python bodies stay; only native probes go.
- Oracle and MSSQL driver modules (`driver.py`) and SQLite-only helpers (`normalize_sql`, `normalize_for_sqlite`): owned by ch01.
- User docs and OKF chapters (owned by ch09).
- `from __future__ import annotations` in files this chapter does not otherwise touch.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md), which depends on [ch03-remove-memory-fakes](../ch03-remove-memory-fakes/spec.md), which depends on [ch01-oracle-mssql-fail-loud](../ch01-oracle-mssql-fail-loud/spec.md) and [ch02-duckdb-sqlalchemy-queue-fail-loud](../ch02-duckdb-sqlalchemy-queue-fail-loud/spec.md); this chapter requires all four:
  - ch01 removes `_last_tds_batch` / `last_tds_batch` from `src/py/vertebra/db/adapters/mssql/driver.py` (the last production caller of `MSSQLCore.encode_tds_sql_batch`) and the SQLite stand-in halves of `test_enterprise_olap_drivers.py`.
  - ch02 t04 makes the DuckDB driver require the `duckdb` package, which t04's `duckdb/__init__.py` docstring ("DuckDB driver over the `duckdb` package") describes; ch02 t04 also edits `test_enterprise_olap_drivers.py`.
  - ch03 t01 edits `test_enterprise_olap_drivers.py`, which t01 edits next.
  - ch04 t01 retargets `test_redis_valkey_resp3.py` onto `resp3_server` and ch04 t02 deletes the Redis driver memory emulation; t04 runs `test_redis_valkey_resp3.py` and edits `redis/core.py` after both.
- Precondition check before t01: `grep -n "encode_tds_sql_batch\|_last_tds_batch" src/py/vertebra/db/adapters/mssql/driver.py` prints nothing.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Crate root | `src/rs/vertebra-db/src/lib.rs`[^db-lib] | In-process Rust; consumers are `vertebra-py`, `vertebra-bigquery`, `vertebra-spanner`; none imports `MssqlPool`, `OraclePool`, TDS, or TNS framing symbols |
| MSSQL | `mssql/mod.rs::rewrite_mssql_placeholders`[^mssql-rs] | `encode_tds_sql_batch` reaches Python through `db.rs`; everything else except the rewriter is module-internal |
| Oracle | `oracle/mod.rs::OracleSessionAttrs`[^oracle-rs], `oracle/tns.rs::parse_tnsnames_ora`[^tns-rs] | `vector.rs` and `db.rs` import VECTOR/OSON/TTC and `tns::{parse_tnsnames_ora, resolve_tns_alias}` only |
| RESP3 | `resp3/mod.rs::redis_hash_slot`[^resp3-rs] | Kept whole; `RedisCore.hash_slot` reaches it through the `redis_hash_slot` binding |
| PyO3 binding | `vertebra-py/src/db.rs::register_db_bindings`[^db-bindings], `_vertebra.pyi`[^vertebra-pyi] | `mypy.stubtest vertebra._vertebra` gates parity in `make py-test`, `make lint`, `make lint-all` |
| Python adapters | `mssql/core.py::MSSQLCore`[^mssql-core], `oracle/core.py::OracleCore`[^oracle-core], `redis/core.py::RedisCore.hash_slot`[^redis-core], `duckdb/__init__.py`[^duckdb-init] | Lazy exports via `vertebra.utils.module_loader.lazy_exports`; tests in `src/py/tests/unit/db/` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels and SHM ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md) (`runtime-ipc` owns `src/rs/vertebra-db/**`; `pyo3-bridge` owns `src/rs/vertebra-py/**` and `_vertebra.pyi`)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8** (root, MSSQL and Oracle portion): delete Rust TDS/TNS framing, `MssqlPool`, `OraclePool`, `MSSQL_DIALECT_TAG`, and `ORACLE_DIALECT_TAG` from `vertebra-db`; rewrite the `mssql` and `oracle` module docs and crate-doc bullets. The remaining R8 items belong to ch06.
  - **R8.1** (t02): `mssql` keeps only `rewrite_mssql_placeholders`.
  - **R8.2** (t03): `oracle` loses TNS framing, `TnsPacketType`, `ORACLE_DIALECT_TAG`, and `OraclePool`.
- **R9** (root): delete the `encode_tds_sql_batch` binding and stub and the Python probes for unexported natives; adapter docs describe codecs.
  - **R9.1** (t01): binding, stub, `MSSQLCore.encode_tds_sql_batch`, `MSSQLConfig.encode_sql_batch`, package export, and the `encode_tds_packet` probe are deleted; MSSQL adapter docstrings are rewritten.
  - **R9.2** (t04): Oracle TNS probes and the `redis_hash_slot_native` probe are deleted; Oracle and DuckDB adapter docstrings are rewritten.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

`mssql` and `oracle` re-exports in `src/rs/vertebra-db/src/lib.rs` after this chapter (other modules and re-exports unchanged until ch06):

```rust
pub use mssql::rewrite_mssql_placeholders;
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
```

Retained Rust signatures (unchanged): `pub fn rewrite_mssql_placeholders(sql: &str) -> (String, usize)`; `impl OracleSessionAttrs { pub fn encode_ttc_piggyback_session_attrs(&self) -> Bytes }`; `pub fn parse_tnsnames_ora(contents: &str) -> HashMap<String, String>`; `pub fn resolve_tns_alias(contents: &str, alias: &str) -> Option<OracleConnectDescriptor>`; `pub fn redis_hash_slot(key: &[u8]) -> u16`.

Python surface after the chapter (`vertebra.db.adapters.mssql.__all__` minus `encode_tds_sql_batch`; `MSSQLConfig` minus `encode_sql_batch`):

```python
class MSSQLCore:
    @staticmethod
    def encode_tds_packet(packet_type: int, packet_id: int, is_eom: bool, payload: bytes) -> bytes: ...
    @staticmethod
    def decode_tds_packet(buf: bytes | bytearray | memoryview) -> tuple[int, bool, int, bytes]: ...
    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]: ...
```

`_vertebra.pyi` drops `def encode_tds_sql_batch(sql: str, tx_descriptor: int = 0) -> bytes: ...` and its `__all__` entry; no other stub changes.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `vertebra.db.adapters.mssql.core.MSSQLCore.rewrite_placeholders` → `vertebra._vertebra.rewrite_mssql_placeholders` → `vertebra_db::mssql::rewrite_mssql_placeholders` (kept).
- `MSSQLCore.encode_tds_packet` → `struct.pack(">BBHHBB", ...)` (pure Python; no native probe).
- `OracleCore.encode_tns_connect_packet` / `encode_tns_data_packet` → pure-Python framing (no native probe).
- `vertebra-py/src/vector.rs` → `vertebra_db::oracle::{encode_oracle_vector_*}` (kept).
- `vertebra-py/src/db.rs` → `vertebra_db::oracle::{OracleSessionAttrs, VECTOR, OSON}` and `oracle::tns::{parse_tnsnames_ora, resolve_tns_alias}` (kept).
- `RedisCore.hash_slot` → `getattr(native, "redis_hash_slot", None)` → `vertebra_db::resp3::redis_hash_slot`, else pure-Python CRC16.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Binding before Rust function | t01 removes the pyfunction while `vertebra_db::mssql::encode_tds_sql_batch` still exists | Workspace builds; Rust function removed in t02 | Every task commit compiles and passes `make rs-test` |
| Stub drift | pyfunction removed | `_vertebra.pyi` def and `__all__` entry removed in the same commit | `mypy.stubtest vertebra._vertebra` exits 0 |
| Removed Python export | `from vertebra.db.adapters.mssql import encode_tds_sql_batch` | `ImportError` | No compatibility alias |
| Removed config helper | `MSSQLConfig.encode_sql_batch("SELECT 1")` | `AttributeError` | No replacement method |
| Native probe removal | `encode_tds_packet(0x12, 1, True, b"\x00\x00\x1a\x00\x06\x04")` | `b"\x12\x01\x00\x0e\x00\x00\x01\x00\x00\x00\x1a\x00\x06\x04"` | Byte output identical to pre-change Python path |
| Redis slot | `hash_slot("123456789")` | `12739` | Native `redis_hash_slot` still used when built |
| Rewriter loses Rust caller | `MssqlPool::lookup_or_compile_statement` deleted | `rewrite_mssql_placeholders` stays public; the PyO3 binding is its production caller | `mssql::tests::test_rewrite_mssql_placeholders_preserves_quotes_and_comments` passes |
| Oracle imports | `BufMut`, `serde` still used by VECTOR and `OracleSessionAttrs` | Only `Arc`, `Duration`, `vertebra_core::broker`, `ShmRegion` imports go | Clippy `-D warnings` exits 0 |
| Unsafe code | Deleted regions | No `unsafe` block exists in any deleted region | `// SAFETY:` rule unaffected |
| Future import removal | Oracle/MSSQL/DuckDB module self-referencing annotations | Self-references become string-quoted hints | Module imports at runtime without `NameError` |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the built extension, when `hasattr(vertebra._vertebra, "encode_tds_sql_batch")` runs, then it is `False`, and `vertebra._vertebra.rewrite_mssql_placeholders("a = ?")` returns `("a = @P1", 1)`.
- Given the workspace, when `grep -rnE "MssqlPool|OraclePool|TdsPacketType|TnsPacketType|MSSQL_DIALECT_TAG|ORACLE_DIALECT_TAG|encode_tds_sql_batch|encode_tns_connect_packet" src/rs --include=*.rs` runs, then it prints nothing.
- Given the workspace, when `make rs-test` runs, then it exits 0 and `cargo test -p vertebra-db -- --list` lists `mssql::tests::test_rewrite_mssql_placeholders_preserves_quotes_and_comments` and three `oracle::tests::` tests.
- Given `src/py/vertebra/db/adapters/oracle/core.py` and `mssql/core.py`, when grepped for `getattr(native`, then no probe for `encode_tns_connect_packet`, `encode_tns_data_packet`, or `encode_tds_packet` remains.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Gate TDS/TNS framing behind a `legacy` Cargo feature | Keeps dead code compiled in CI under `--all-features`; no caller enables it | Rejected: dead code with a feature flag is still dead code |
| Keep `encode_tds_sql_batch` binding for `native-mssql-driver` | Binding exists only to feed the stand-in; `tiberius` frames batches itself | Rejected: the native flow builds on `tiberius` |
| Delete Python TDS/TNS pure framing too | Exported package API with tests; outside root R9 | Rejected for this chapter: native Oracle/MSSQL flows decide its fate |
| Remove the Rust function before the binding | Binding would reference a missing symbol | Rejected: t01 removes the binding first so every commit compiles |
| Keep all eight `vertebra-db` cleanup tasks in one chapter | Exceeds the 1–4 task bound | Rejected: MSSQL/Oracle codec surface here; pools and MySQL in ch06 |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch05]`. In-code docs (the `mssql` and `oracle` module `//!` docs and crate-doc bullets, MSSQL, Oracle, and DuckDB adapter module and class docstrings) are part of each task's production change. User docs and OKF (`database-drivers-and-dialers.md` codec inventory, `product/product.md` vertebra-db row) are owned by ch09.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R9.1 | [t01](tasks/t01-tds-batch-binding-and-mssql-python.md) | `static_validation` | `mypy.stubtest vertebra._vertebra`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter` |
| R8.1 | [t02](tasks/t02-rust-mssql-rewriter-only.md) | `static_validation` | `make rs-test`, `mssql::tests::test_rewrite_mssql_placeholders_preserves_quotes_and_comments` |
| R8.2 | [t03](tasks/t03-rust-oracle-codecs-only.md) | `static_validation` | `make rs-test`, `oracle::tests::*` |
| R9.2 | [t04](tasks/t04-oracle-redis-duckdb-python-cleanup.md) | `static_validation` | `test_enterprise_olap_drivers.py`, `test_redis_valkey_resp3.py` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch05:t01 — Remove the encode_tds_sql_batch binding, stub, and MSSQL Python wrappers (tasks/t01-tds-batch-binding-and-mssql-python.md)
- [ ] driver-fail-loud:ch05:t02 — Reduce the Rust mssql module to the placeholder rewriter (tasks/t02-rust-mssql-rewriter-only.md)
- [ ] driver-fail-loud:ch05:t03 — Reduce the Rust oracle module to TTC, VECTOR, and OSON codecs (tasks/t03-rust-oracle-codecs-only.md)
- [ ] driver-fail-loud:ch05:t04 — Remove Oracle and Redis native probes and rewrite Oracle and DuckDB adapter docstrings (tasks/t04-oracle-redis-duckdb-python-cleanup.md)
- **Chapter Recovery Checkpoint**: `make lint-all && make rs-test && make py-test` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 0acdf576`; `depends_on_chapters: [ch03-remove-memory-fakes]` (transitively ch01 and ch02); `state: planned`.
- **Remaining Tasks in Chapter**: `t01` through `t04` (`0/4` completed). t02 requires t01 (the binding leaves before the Rust function); t03 requires t02 (both edit `lib.rs`); t04 touches only Python adapters and has no task dependency.
- **Downstream**: [ch06-vertebra-db-pools-and-mysql](../ch06-vertebra-db-pools-and-mysql/spec.md) depends on this chapter and edits the same `lib.rs`.
- **Open Doc/OKF Items**: none in this chapter; ch09 owns user docs and OKF.
- **Active Invariants**: every commit builds the workspace; every commit that touches Rust passes `make rs-test`; `_vertebra.pyi` changes ship with binding changes; RESP3, TNS parsing, VECTOR/OSON, and TTC session attributes stay.

[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate doc, module list, and re-exports.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Stubtest-gated type stub for vertebra._vertebra.
[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — MSSQL placeholder rewriter, TDS framing, and MssqlPool.
[^oracle-rs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Oracle TTC, VECTOR, OSON codecs, TNS framing, and OraclePool.
[^tns-rs]: `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` — tnsnames.ora parser and alias resolver kept by this chapter.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot` — RESP3 codecs and Redis hash slot kept for the Python Redis driver.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore` — Python MSSQL TDS helpers and placeholder rewriter.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Python Oracle TNS helpers and VECTOR/OSON codecs.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot` — Python Redis hash slot with native probe.
[^duckdb-init]: `src/py/vertebra/db/adapters/duckdb/__init__.py` — DuckDB adapter package docstring.
