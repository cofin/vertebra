---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch03-engine-capsules-core
title: "Chapter 03: Cross-Extension C-ABI Engine Capsule Registry, MySQL, and DuckDB"
description: "Replace the isolated per-cdylib LazyLock<DashMap> in vertebra-db with a versioned C-ABI BrokerEngineCapsule consumed by vertebra._vertebra.BrokerEngineRegistry, add Python engine discovery, and register MySQL and DuckDB capsules."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - capsule
  - ffi
  - mysql
  - duckdb
  - broker
sources:
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: "Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs"
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Type stubs for vertebra._vertebra"
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
  - id: mysql-rs
    resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
    title: "MySQL PyO3 extension and broker backend"
  - id: duckdb-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
    title: "DuckDB PyO3 extension"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Chapter 03: Cross-Extension C-ABI Engine Capsule Registry, MySQL, and DuckDB

<!-- truth: start -->
- `vertebra_db::BrokerEngineCapsule` (`#[repr(C)]`, `BROKER_ENGINE_ABI_VERSION = 1`, `PyCapsule` name `c"vertebra.BrokerEngineCapsule"`) replaces the isolated per-`cdylib` `BROKER_ENGINE_BACKENDS` static map [^db-registry] [^gcp-capsule].
- `vertebra._vertebra.BrokerEngineRegistry` owns the single process-wide engine handle table (`register`, `registered_tags`, `normalize`, `open_pool`, `execute`, `close_pool`) [^py-broker] [^vertebra-pyi].
- `BrokerEngineDiscovery.register_installed()` probes optional engine wheels through `vertebra.utils.module_loader.import_optional` (`PY-08`) at `BrokerPlugin.on_app_init` [^broker-plugin].
- `vertebra._mysql` (`DIALECT_TAG_MYSQL = 2`) and `vertebra._duckdb` (`DIALECT_TAG_DUCKDB = 6`) export `broker_engine_capsule()` with real `dbrpc` + `VTBARROW` pool execution [^mysql-rs] [^duckdb-rs] [^core-broker].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Each standalone PyO3 wheel (`_mysql`, `_duckdb`, `_oracle`, `_mssql`, `_postgres`) statically links its own copy of `vertebra-db`, so `BROKER_ENGINE_BACKENDS: LazyLock<DashMap<...>>` inside `_mysql` is invisible to `_vertebra` [^db-registry] [^mysql-rs]. Following `GcpTokenCacheCapsule` [^gcp-capsule], this chapter introduces a `#[repr(C)]` `BrokerEngineCapsule` passed via `PyCapsule` into `vertebra._vertebra.BrokerEngineRegistry` and wires MySQL and DuckDB in the same batch so `vertebra-mysql` is edited once.

**Non-Goals:** Oracle, SQL Server, and PostgreSQL capsules (`ch04`); BigQuery and Spanner (stateless/session-managed; excluded per root spec).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Capsule ABI | `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-py/src/broker.rs` [^db-registry] [^py-broker] | `BrokerEngineCapsule`, `BrokerPoolBackend`, `BrokerEngineRegistry` |
| Discovery | `src/py/vertebra/extensions/broker/engines.py`, `plugin.py` [^broker-plugin] | `BrokerEngineDiscovery.register_installed()` via `import_optional` |
| MySQL & DuckDB | `src/rs/vertebra-mysql/src/lib.rs`, `src/rs/vertebra-duckdb/src/lib.rs` [^mysql-rs] [^duckdb-rs] | `broker_engine_capsule()` + `BrokerPoolBackend` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PyO3 FFI and Batch Dispatch](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R8**: Replace `BROKER_ENGINE_BACKENDS` in `vertebra-db` with `#[repr(C)] BrokerEngineCapsule`, `BrokerPoolBackend`, `new_broker_engine_pycapsule`, and `_vertebra.BrokerEngineRegistry` (`t01`).
- **R9**: Implement `BrokerEngineDiscovery.register_installed()` using `import_optional` and wire into `BrokerPlugin.on_app_init` (`t02`).
- **R10 (MySQL)**: Upgrade `MySQLBrokerBackend` (`DIALECT_TAG_MYSQL = 2`) to `BrokerPoolBackend` and export `vertebra._mysql.broker_engine_capsule()` (`t03`).
- **R10 (DuckDB)**: Implement `DuckDBBrokerBackend` (`DIALECT_TAG_DUCKDB = 6`) with `VTBARROW` SHM execution and export `vertebra._duckdb.broker_engine_capsule()` (`t04`).

```rust
pub const BROKER_ENGINE_CAPSULE_NAME: &CStr = c"vertebra.BrokerEngineCapsule";
pub const BROKER_ENGINE_ABI_VERSION: u32 = 1;
pub type BrokerBytesVisitFn = unsafe extern "C" fn(out_ctx: *mut c_void, ptr: *const u8, len: usize);

#[repr(C)]
pub struct BrokerEngineCapsule {
    pub abi_version: u32,
    pub dialect_tag: u16,
    pub name_ptr: *const u8,
    pub name_len: usize,
    pub ctx: *const c_void,
    pub clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void,
    pub drop_ctx_fn: unsafe extern "C" fn(*const c_void),
    pub normalize_fn: unsafe extern "C" fn(*const c_void, *const u8, usize, *mut c_void, BrokerBytesVisitFn) -> u32,
    pub open_pool_fn: unsafe extern "C" fn(*const c_void, *const u8, usize, u32, *mut u64, *mut c_void, BrokerBytesVisitFn) -> i32,
    pub execute_fn: unsafe extern "C" fn(*const c_void, u64, *const u8, usize, *mut c_void, BrokerBytesVisitFn) -> i32,
    pub close_pool_fn: unsafe extern "C" fn(*const c_void, u64),
}

pub trait BrokerPoolBackend: BrokerEngineBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>;
    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>;
    fn close_pool(&self, pool_id: u64);
}
pub fn new_broker_engine_pycapsule(py: Python<'_>, backend: Arc<dyn BrokerPoolBackend>) -> PyResult<Bound<'_, PyCapsule>>;
```

```python
class BrokerEngineRegistry:
    @staticmethod
    def register(capsule: object) -> int: ...
    @staticmethod
    def registered_tags() -> list[int]: ...
    @staticmethod
    def normalize(dialect_tag: int, sql: str) -> tuple[str, int]: ...
    @staticmethod
    def open_pool(dialect_tag: int, dsn: str, max_size: int) -> int: ...
    @staticmethod
    def execute(dialect_tag: int, pool_id: int, frame: bytes) -> bytes: ...
    @staticmethod
    def close_pool(dialect_tag: int, pool_id: int) -> None: ...
```

**Integration Flow:**
- `BrokerPlugin.on_app_init` -> `BrokerEngineDiscovery.register_installed()` -> `import_optional("vertebra._mysql")` -> `mod.broker_engine_capsule()` -> `BrokerEngineRegistry.register(capsule)`.
- `BrokerEngineRegistry.execute(tag, pool_id, frame)` -> `catch_unwind` FFI trampoline -> `<Engine>BrokerBackend::execute` -> `DbRpcResponse` / `VTBARROW` SHM.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid capsule | `register(object())` | `TypeError` naming `vertebra.BrokerEngineCapsule` | Pointer safety |
| ABI mismatch | `abi_version != 1` | `RuntimeError` naming engine and expected ABI `1` | Versioned C-ABI |
| Engine panic | Backend panics inside FFI callback | `catch_unwind` traps panic into error code -> Python `RuntimeError` | No unwind across `extern "C"` |
| Uninstalled wheel | `import_optional("vertebra._oracle")` returns `None` | Silently skipped during discovery | Optional sibling wheels |

## 3. Documentation & OKF Knowledge Plan

Native engine registration matrix is documented in `docs/usage/databases.md` and `broker-and-shm-leases.md` in `ch08`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R8` | `t01` | `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `cargo test -p vertebra-db --test broker_engine_capsule_tests` |
| `R9` | `t02` | `src/py/vertebra/extensions/broker/{engines,plugin}.py` | `pytest src/py/tests/unit/extensions/test_shm_broker.py` |
| `R10` (MySQL) | `t03` | `src/rs/vertebra-mysql/src/lib.rs`, `src/py/vertebra/_mysql.pyi` | `cargo test -p vertebra-mysql && pytest src/py/tests/unit/extensions/test_shm_broker.py` |
| `R10` (DuckDB) | `t04` | `src/rs/vertebra-duckdb/src/lib.rs`, `src/py/vertebra/_duckdb.pyi` | `cargo test -p vertebra-duckdb && pytest src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [ ] [t01-capsule-contract-and-registry.md](tasks/t01-capsule-contract-and-registry.md) — Replace static BROKER_ENGINE_BACKENDS in vertebra-db with BrokerEngineCapsule and _vertebra.BrokerEngineRegistry.
- [ ] [t02-python-engine-discovery.md](tasks/t02-python-engine-discovery.md) — Discover installed vertebra._<engine> wheels via import_optional and register capsules on BrokerPlugin init.
- [ ] [t03-mysql-broker-capsule.md](tasks/t03-mysql-broker-capsule.md) — Implement BrokerPoolBackend for MySQL (tag 2) with real dbrpc/VTBARROW execution and export broker_engine_capsule().
- [ ] [t04-duckdb-broker-capsule.md](tasks/t04-duckdb-broker-capsule.md) — Implement DuckDBBrokerBackend (tag 6) with broker-hosted single-writer pool and VTBARROW responses.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `vertebra-core` and `vertebra-py` never link engine crates; all cross-cdylib calls cross `BrokerEngineCapsule` with `catch_unwind`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-mysql -p vertebra-duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py`

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^mysql-rs]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL PyO3 extension and broker backend.
[^duckdb-rs]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB PyO3 extension.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
