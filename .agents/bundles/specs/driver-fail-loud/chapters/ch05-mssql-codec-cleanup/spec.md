---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch05-mssql-codec-cleanup
title: "Chapter 05: MSSQL Codec Cleanup"
description: Delete the MSSQL Python batch wrappers, the encode_tds_sql_batch binding and stub, and Rust TDS framing, MSSQL_DIALECT_TAG, and MssqlPool, leaving the placeholder rewriter and Python TDS packet helpers with docs that describe codecs.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-redis-valkey-memory-removal
replan_trigger: null
current_task: "driver-fail-loud:ch05:t01"
plan_revision: 2
plan_commit: fd571e3a
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
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore
    title: Python MSSQL TDS helpers, batch wrapper, and placeholder rewriter
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch
    title: Config helper wrapping MSSQLCore.encode_tds_sql_batch
  - id: olap-test
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter
    title: TDS framing and placeholder rewriter test
tags:
  - vertebra-db
  - rust
  - pyo3
  - mssql
  - cleanup
  - dead-code
---

# Chapter 05: MSSQL Codec Cleanup

<!-- truth: start -->
- **Python wrappers leave first**: t01 deletes `MSSQLCore.encode_tds_sql_batch`, its module alias, the package lazy export, and `MSSQLConfig.encode_sql_batch` while the binding and stub stay; no Python code reaches the binding after t01.[^mssql-core][^mssql-config]
- **Binding and stub leave together**: t02 deletes `encode_tds_sql_batch` from `register_db_bindings` and `_vertebra.pyi` in one commit; every other codec binding stays.[^db-bindings][^vertebra-pyi]
- **MSSQL keeps only the rewriter**: t03 reduces `vertebra_db::mssql` to `rewrite_mssql_placeholders` and its test; `MSSQL_DIALECT_TAG`, `TdsPacketType`, `encode_tds_packet`, `decode_tds_packet`, `encode_tds_sql_batch`, and `MssqlPool` are deleted.[^mssql-rs]
- **Python TDS helpers stay pure Python**: `MSSQLCore.encode_tds_packet` loses its probe for a native `encode_tds_packet` that `_vertebra` never exports; its `struct.pack` body and `decode_tds_packet` are unchanged.[^mssql-core]
- **No aliases**: `vertebra.db.adapters.mssql.encode_tds_sql_batch`, `MSSQLCore.encode_tds_sql_batch`, and `MSSQLConfig.encode_sql_batch` are deleted without replacements.
- **Docs say codecs**: the `mssql` module `//!` doc, its crate-doc bullet, and the MSSQL adapter module and class docstrings describe the rewriter and packet helpers, not a native TDS driver.[^db-lib]
- **Build stays green**: every task commit builds the workspace, keeps `mypy.stubtest vertebra._vertebra` at exit 0, and passes `make rs-test`.
- **Out of chapter**: Oracle codecs, Oracle/Redis/DuckDB Python probes and docstrings belong to [ch06](../ch06-oracle-codec-cleanup/spec.md); pools, `mysql`, `OracleWalletMaterial`, `sha2`, `rmp-serde`, the crate doc header, and the Cargo `description` belong to [ch07](../ch07-vertebra-db-pools-and-mysql/spec.md).
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The PyO3 layer exposes `encode_tds_sql_batch` solely so the MSSQL SQLite stand-in could record a TDS batch it never sent.[^db-bindings] Python wraps that binding three times (`MSSQLCore.encode_tds_sql_batch`, a module alias re-exported from the package, and `MSSQLConfig.encode_sql_batch`).[^mssql-core][^mssql-config] `vertebra-db` carries TDS packet framing and a connectionless `MssqlPool` lease table that no Rust or Python caller uses.[^mssql-rs] `MSSQLCore.encode_tds_packet` probes `_vertebra` for a function it never exports. The `tiberius` client selected by `native-mssql-driver` frames batches itself.

This chapter deletes that MSSQL surface and rewrites the MSSQL module and adapter docs so they describe what remains: the `@P1` placeholder rewriter (Rust and binding) and the pure-Python TDS packet helpers.

**Non-goals**:

- Oracle TNS framing, `OraclePool`, `ORACLE_DIALECT_TAG`, Oracle/Redis/DuckDB Python probes and docstrings (owned by [ch06](../ch06-oracle-codec-cleanup/spec.md)).
- PostgreSQL, SQLite, DuckDB, and MySQL pools, the `mysql` module, `OracleWalletMaterial`, `sha2`, `rmp-serde`, the crate doc header, and the Cargo `description` (owned by [ch07](../ch07-vertebra-db-pools-and-mysql/spec.md)).
- Python-side TDS framing (`MSSQLCore.encode_tds_packet`, `decode_tds_packet`, `TDS_PKT_*`): pure-Python bodies stay; only the native probe goes.
- `mssql/driver.py` and `MSSQLCore.normalize_for_sqlite` (owned by ch01).
- User docs and OKF chapters (owned by ch11).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md), which depends on [ch03-remove-memory-fakes](../ch03-remove-memory-fakes/spec.md), which depends on [ch01-oracle-mssql-fail-loud](../ch01-oracle-mssql-fail-loud/spec.md) and [ch02-duckdb-sqlalchemy-queue-fail-loud](../ch02-duckdb-sqlalchemy-queue-fail-loud/spec.md):
  - ch01 removes `_last_tds_batch` from `src/py/vertebra/db/adapters/mssql/driver.py` (the last production caller of `MSSQLCore.encode_tds_sql_batch`) and `normalize_for_sqlite` from `mssql/core.py`.
  - ch01, ch02 t04, and ch03 t01 edit `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, which t01 edits next.
- Precondition check before t01: `grep -n "encode_tds_sql_batch\|_last_tds_batch" src/py/vertebra/db/adapters/mssql/driver.py` prints nothing.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Python adapter | `mssql/core.py::MSSQLCore`[^mssql-core], `mssql/config.py::MSSQLConfig.encode_sql_batch`[^mssql-config], `mssql/__init__.py` | Lazy exports via `vertebra.utils.module_loader.lazy_exports`; `MSSQLCore.encode_tds_sql_batch` calls the binding through `import_optional("vertebra._vertebra")` plus `hasattr`, with a pure-Python fallback |
| PyO3 binding | `vertebra-py/src/db.rs::register_db_bindings`[^db-bindings], `_vertebra.pyi`[^vertebra-pyi] | `mypy.stubtest vertebra._vertebra` gates parity in `make py-test`, `make lint`, `make lint-all` |
| Rust MSSQL | `vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders`[^mssql-rs] | `db.rs` imports `rewrite_mssql_placeholders` and `encode_tds_sql_batch`; every other symbol is reached only through the `lib.rs` re-export, which no crate imports |
| Crate root | `vertebra-db/src/lib.rs`[^db-lib] | Consumers `vertebra-py`, `vertebra-bigquery`, `vertebra-spanner` import no TDS or `MssqlPool` symbol |
| Test | `test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter`[^olap-test] | Only test that calls `encode_tds_sql_batch`; it also imports `TDS_PKT_SQL_BATCH` for that block only |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels and SHM ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md) (`runtime-ipc` owns `src/rs/vertebra-db/**`; `pyo3-bridge` owns `src/rs/vertebra-py/**` and `_vertebra.pyi`)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8.1** (root R8, MSSQL portion; t03): `vertebra_db::mssql` keeps only `rewrite_mssql_placeholders`; `MSSQL_DIALECT_TAG`, `TdsPacketType`, TDS framing, and `MssqlPool` are deleted; the module doc and crate-doc bullet name the rewriter.
- **R9.1** (root R9, binding and MSSQL Python portion; t01, t02): `MSSQLCore.encode_tds_sql_batch`, its alias and package export, `MSSQLConfig.encode_sql_batch`, and the `encode_tds_packet` native probe are deleted and MSSQL adapter docstrings describe helpers (t01); the `encode_tds_sql_batch` binding and stub are deleted (t02).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

`mssql` re-export in `src/rs/vertebra-db/src/lib.rs` after this chapter (other re-exports unchanged until ch06 and ch07):

```rust
pub use mssql::rewrite_mssql_placeholders;
```

Retained Rust signature (unchanged): `pub fn rewrite_mssql_placeholders(sql: &str) -> (String, usize)`.

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
- `MSSQLCore.decode_tds_packet` → `struct.unpack` (pure Python; unchanged).
- Before t01: `MSSQLConfig.encode_sql_batch` → `MSSQLCore.encode_tds_sql_batch` → `getattr(native, "encode_tds_sql_batch")` or pure-Python fallback. After t01 this chain has no members; after t02 the binding is gone; after t03 the Rust function is gone.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Task order: Python wrappers first | t01 deletes the wrappers while the binding and stub exist | Binding stays registered with no Python caller; stub unchanged | `mypy.stubtest vertebra._vertebra` exits 0 at t01; no code path changes behavior |
| Binding before Rust function | t02 removes the pyfunction while `vertebra_db::mssql::encode_tds_sql_batch` exists | Workspace builds; t03 deletes the Rust function | Every task commit compiles |
| Stub drift | pyfunction removed | `_vertebra.pyi` def and `__all__` entry removed in the same commit (t02) | stubtest exits 0 |
| Removed Python export | `from vertebra.db.adapters.mssql import encode_tds_sql_batch` | `ImportError` | No compatibility alias |
| Removed config helper | `MSSQLConfig.encode_sql_batch("SELECT 1")` | `AttributeError` | No replacement method |
| Native probe removal | `encode_tds_packet(0x12, 1, True, b"\x00\x00\x1a\x00\x06\x04")` | `b"\x12\x01\x00\x0e\x00\x00\x01\x00\x00\x00\x1a\x00\x06\x04"` | Byte output identical; `_vertebra` never exported `encode_tds_packet` |
| Unused test import | `TDS_PKT_SQL_BATCH` imported only for the deleted batch block | t01 drops the import | ruff `F401` passes |
| Rewriter loses Rust caller | `MssqlPool::lookup_or_compile_statement` deleted in t03 | `rewrite_mssql_placeholders` stays public; the PyO3 binding is its production caller | `mssql::tests::test_rewrite_mssql_placeholders_preserves_quotes_and_comments` passes |
| Leftover Rust import | `Arc`, `Duration`, `bytes`, `serde`, broker, `ShmRegion` imports after t03 | Clippy `-D warnings` fails until removed | Clippy exits 0 |
| Future import removal | `mssql/{core,config,__init__}.py` lose `from __future__ import annotations` | No annotation in those files references an unbound name | Modules import without `NameError` |
| Unsafe code | Deleted regions | No `unsafe` block exists in any deleted region | `// SAFETY:` rule unaffected |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the built extension, when `hasattr(vertebra._vertebra, "encode_tds_sql_batch")` runs, then it is `False`, and `vertebra._vertebra.rewrite_mssql_placeholders("a = ?")` returns `("a = @P1", 1)`.
- Given the workspace, when `grep -rnE "MssqlPool|TdsPacketType|MSSQL_DIALECT_TAG|encode_tds_sql_batch|encode_tds_packet|decode_tds_packet" src/rs --include=*.rs` runs, then it prints nothing.
- Given the workspace, when `grep -rn "encode_tds_sql_batch\|encode_sql_batch" src/py` runs, then it prints nothing.
- Given the workspace, when `make rs-test` runs, then it exits 0 and `cargo test -p vertebra-db --lib -- --list` lists exactly one `mssql::tests::` test.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Remove the binding before the Python wrappers | Also green: the wrapper falls back to a pure-Python encoder with the same `<IIHQI` ALL_HEADERS layout | Rejected: removing wrappers first changes no code path, so no commit relies on fallback equivalence |
| Keep `encode_tds_sql_batch` binding for `native-mssql-driver` | Binding exists only to feed the stand-in; `tiberius` frames batches itself | Rejected |
| Gate TDS framing behind a `legacy` Cargo feature | Keeps dead code compiled under `--all-features`; no caller enables it | Rejected: dead code with a feature flag is still dead code |
| Delete Python TDS pure framing too | Exported package API with tests; outside root R9 | Rejected for this chapter: `native-mssql-driver` decides its fate |
| Keep MSSQL and Oracle cleanup in one chapter | Two tasks exceeded 3 production files | Rejected: MSSQL here, Oracle in ch06 |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch05]`. In-code docs (the `mssql` module `//!` doc and crate-doc bullet, the MSSQL adapter module and class docstrings) are part of each task's production change. User docs and OKF (`database-drivers-and-dialers.md` codec inventory) are owned by ch11.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R9.1 (Python) | [t01](tasks/t01-mssql-python-batch-wrappers.md) | `static_validation` | `test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter`, ruff, mypy, stubtest |
| R9.1 (binding) | [t02](tasks/t02-tds-batch-binding-and-stub.md) | `static_validation` | `mypy.stubtest vertebra._vertebra`, `make rs-test` |
| R8.1 | [t03](tasks/t03-rust-mssql-rewriter-only.md) | `static_validation` | `make rs-test`, `mssql::tests::test_rewrite_mssql_placeholders_preserves_quotes_and_comments` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch05:t01 — Remove the MSSQL Python batch wrappers and encode_tds_packet probe (tasks/t01-mssql-python-batch-wrappers.md)
- [ ] driver-fail-loud:ch05:t02 — Remove the encode_tds_sql_batch binding and stub (tasks/t02-tds-batch-binding-and-stub.md)
- [ ] driver-fail-loud:ch05:t03 — Reduce the Rust mssql module to the placeholder rewriter (tasks/t03-rust-mssql-rewriter-only.md)
- **Chapter Recovery Checkpoint**: `make lint-all && make rs-test && make py-test` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 0acdf576`; `depends_on_chapters: [ch04-redis-valkey-memory-removal]` (transitively ch01, ch02, ch03); `state: planned`.
- **Remaining Tasks in Chapter**: `t01` through `t03` (`0/3` completed). t02 requires t01 (Python stops reaching the binding first); t03 requires t02 (`db.rs` stops importing `vertebra_db::mssql::encode_tds_sql_batch` first).
- **Downstream**: [ch06-oracle-codec-cleanup](../ch06-oracle-codec-cleanup/spec.md) depends on this chapter and edits the same `lib.rs`; [ch07-vertebra-db-pools-and-mysql](../ch07-vertebra-db-pools-and-mysql/spec.md) follows ch06.
- **Open Doc/OKF Items**: none in this chapter; ch11 owns user docs and OKF.
- **Active Invariants**: every commit builds the workspace and passes `make rs-test`; `_vertebra.pyi` changes ship with binding changes; `rewrite_mssql_placeholders` and the pure-Python TDS packet helpers stay.

[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate doc, module list, and re-exports.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Stubtest-gated type stub for vertebra._vertebra.
[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — MSSQL placeholder rewriter, TDS framing, and MssqlPool.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore` — Python MSSQL TDS helpers, batch wrapper, and placeholder rewriter.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch` — Config helper wrapping MSSQLCore.encode_tds_sql_batch.
[^olap-test]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter` — TDS framing and placeholder rewriter test.
