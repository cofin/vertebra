---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: 'Chapter 02: BrokerEngineCapsule ABI v1, Python Discovery, and Relational/OLAP Engine Capsules'
description: Define BrokerEngineCapsule ABI v1 and BrokerEngineRegistry, implement Python BrokerEngineDiscovery, and export broker_engine_capsule() across MySQL, DuckDB, Oracle, SQL Server, and PostgreSQL.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- capsule
- ffi
- mysql
- duckdb
- broker
- oracle
- mssql
- postgres
sources:
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
- id: gcp-capsule
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
  title: Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Type stubs for vertebra._vertebra
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
- id: mysql-rs
  resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
  title: MySQL PyO3 extension and broker backend
- id: duckdb-rs
  resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
  title: DuckDB PyO3 extension
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: oracle-rs
  resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
  title: Oracle native Thin-mode pool
- id: mssql-rs
  resource: src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders
  title: SQL Server native TDS parameter rewriter
- id: postgres-rs
  resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
  title: PostgreSQL native Protocol 3.0 pool
---

# Chapter 02: BrokerEngineCapsule ABI v1, Python Discovery, and Relational/OLAP Engine Capsules

<!-- truth: start -->
- `vertebra_db::BrokerEngineCapsule` (`#[repr(C)]`, `BROKER_ENGINE_ABI_VERSION = 1`, `PyCapsule` name `c"vertebra.BrokerEngineCapsule"`) replaces the isolated per-`cdylib` `BROKER_ENGINE_BACKENDS` static map [^db-registry] [^gcp-capsule].
- `vertebra._vertebra.BrokerEngineRegistry` owns the single process-wide engine handle table (`register`, `registered_tags`, `normalize`, `open_pool`, `execute`, `close_pool`) [^py-broker] [^vertebra-pyi].
- `BrokerEngineDiscovery.register_installed()` probes optional engine wheels through `vertebra.utils.module_loader.import_optional` (`PY-08`) at `BrokerPlugin.on_app_init` [^broker-plugin].
- `vertebra._mysql` (`DIALECT_TAG_MYSQL = 2`) and `vertebra._duckdb` (`DIALECT_TAG_DUCKDB = 6`) export `broker_engine_capsule()` with real `dbrpc` + `VTBARROW` pool execution [^mysql-rs] [^duckdb-rs] [^core-broker].
- `vertebra._oracle` (`DIALECT_TAG_ORACLE = 4`), `vertebra._mssql` (`DIALECT_TAG_MSSQL = 5`), and `vertebra._postgres` (`DIALECT_TAG_POSTGRES = 1`) each export `broker_engine_capsule() -> object` [^oracle-rs] [^mssql-rs] [^postgres-rs] [^core-broker].
- `PostgresBrokerBackend` wraps `vertebra_postgres::PostgresPool` already present on `main @ 18baadcd` [^postgres-rs].
- Every engine crate depends only on `vertebra-db` (`BrokerPoolBackend` + `new_broker_engine_pycapsule`), never on `vertebra-py` [^db-registry].
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

With `BrokerEngineCapsule` and `BrokerEngineRegistry` established in `ch03`, the remaining merged enterprise relational wheels — Oracle (`vertebra-oracle`), SQL Server (`vertebra-mssql`), and PostgreSQL (`vertebra-postgres`) — need `BrokerPoolBackend` implementations so multi-worker deployments share one broker-owned server pool per `pool_key` [^oracle-rs] [^mssql-rs] [^postgres-rs].

**Non-Goals:** SQLite capsule (owned by `native-sqlite-driver`); BigQuery and Spanner (excluded per root spec).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Oracle | `src/rs/vertebra-oracle/src/lib.rs`, `src/py/vertebra/_oracle.pyi` [^oracle-rs] | `OracleBrokerBackend` (`tag=4`) over `SyncOraclePool` |
| SQL Server | `src/rs/vertebra-mssql/src/{broker,lib}.rs`, `src/py/vertebra/_mssql.pyi` [^mssql-rs] | `MssqlBrokerBackend` (`tag=5`) over TDS client pool |
| PostgreSQL | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` [^postgres-rs] | `PostgresBrokerBackend` (`tag=1`) over `PostgresPool` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PostgreSQL Native Driver](../../../../knowledge/domains/data/postgres-native-driver.md).

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

- **R10 (Oracle)**: Implement `OracleBrokerBackend` (`DIALECT_TAG_ORACLE = 4`) and export `vertebra._oracle.broker_engine_capsule()` (`t01`).
- **R10 (SQL Server)**: Wire `MssqlBrokerBackend` (`DIALECT_TAG_MSSQL = 5`, in `src/rs/vertebra-mssql/src/broker.rs`) into `BrokerPoolBackend` and export `vertebra._mssql.broker_engine_capsule()` (`t02`).
- **R10 (PostgreSQL)**: Implement `PostgresBrokerBackend` (`DIALECT_TAG_POSTGRES = 1`) over `PostgresPool` and export `vertebra._postgres.broker_engine_capsule()` (`t03`).

```rust
pub struct OracleBrokerBackend { /* DashMap<u64, Arc<SyncOraclePool>> */ }
pub struct MssqlBrokerBackend { /* RwLock<HashMap<u64, Arc<MssqlPoolEntry>>> */ }
pub struct PostgresBrokerBackend { /* DashMap<u64, Arc<PostgresPool>> */ }

#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>>;
```

```python
# Declared in _oracle.pyi, _mssql.pyi, and _postgres.pyi:
def broker_engine_capsule() -> object: ...
```

**Integration Flow:**
- `BrokerEngineDiscovery.register_installed()` imports `_oracle`, `_mssql`, and `_postgres` and registers tags `4`, `5`, and `1` into `BrokerEngineRegistry`.
- Statement normalization delegates to Oracle `:1`, SQL Server `@P1`, and PostgreSQL `$1` placeholder rewriters.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid DSN | `open_pool("invalid://", 4)` | `Err` mapped to `RuntimeError` | Fail-loud pool open |
| Unknown `pool_id` | `execute(999, frame)` | `Err("unknown pool id 999")` | No default pool fallback |
| Double close | `close_pool(id)` called twice | Idempotent no-op | Clean teardown |

## 3. Documentation & OKF Knowledge Plan

Native engine registration matrix is documented in `docs/usage/databases.md` and `broker-and-shm-leases.md` in `ch04:t04`.

Engine registration matrix updates execute in `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Define BrokerEngineCapsule ABI v1, BrokerEngineRegistry, and Python Discovery | `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` (+2 more) | `src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs`, `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t02` | Implement MySQL and DuckDB BrokerPoolBackend and broker_engine_capsule Exports | `src/rs/vertebra-mysql/src/lib.rs`, `src/py/vertebra/_mysql.pyi`, `src/rs/vertebra-duckdb/src/lib.rs` (+1 more) | `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t03` | Implement Oracle and SQL Server BrokerPoolBackend and broker_engine_capsule Exports | `src/rs/vertebra-oracle/src/lib.rs`, `src/py/vertebra/_oracle.pyi`, `src/rs/vertebra-mssql/src/broker.rs` (+2 more) | `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t04` | Implement PostgreSQL BrokerPoolBackend Over PostgresPool and broker_engine_capsule Export | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` | `src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [ ] [t01-capsule-contract-registry-and-discovery.md](tasks/t01-capsule-contract-registry-and-discovery.md) — Implement BrokerPoolBackend + PyCapsule ABI v1 in vertebra-db/vertebra-py and Python BrokerEngineDiscovery.register_installed().
- [ ] [t02-mysql-and-duckdb-broker-capsules.md](tasks/t02-mysql-and-duckdb-broker-capsules.md) — Implement MySQLBrokerBackend (tag 2) and DuckDBBrokerBackend (tag 6) and export broker_engine_capsule() in _mysql and _duckdb.
- [ ] [t03-oracle-and-mssql-broker-capsules.md](tasks/t03-oracle-and-mssql-broker-capsules.md) — Implement OracleBrokerBackend (tag 4) and wire MssqlBrokerBackend (tag 5) into BrokerPoolBackend, exporting broker_engine_capsule() in _oracle and _mssql.
- [ ] [t04-postgres-broker-capsule.md](tasks/t04-postgres-broker-capsule.md) — Implement PostgresBrokerBackend (tag 1) over PostgresPool and export broker_engine_capsule() in _postgres.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: All three enterprise engine capsules expose ABI v1 and pass `mypy.stubtest`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle -p vertebra-mssql -p vertebra-postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py`

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^mysql-rs]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL PyO3 extension and broker backend.
[^duckdb-rs]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB PyO3 extension.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^oracle-rs]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle native Thin-mode pool.
[^mssql-rs]: `src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders` — SQL Server native TDS parameter rewriter.
[^postgres-rs]: `src/rs/vertebra-postgres/src/pool.rs::PostgresPool` — PostgreSQL native Protocol 3.0 pool.
