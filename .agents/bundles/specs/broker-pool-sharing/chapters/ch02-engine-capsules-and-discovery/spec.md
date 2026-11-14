---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: 'Chapter 02: BrokerEngineCapsule ABI v1, Python Discovery, and Relational/OLAP Engine Capsules'
description: Define BrokerEngineCapsule ABI v1 and BrokerEngineRegistry, implement Python BrokerEngineDiscovery, and export broker_engine_capsule() across MySQL, DuckDB, Oracle, SQL Server, and PostgreSQL.
status: stable
state: completed
commit: 673051f1
last_reviewed_commit: 673051f1
completed_at: '2026-11-14T23:42:28Z'
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
- ch01-shm-ownership-and-broker-wire
plan_commit: 1b191388
updated_at: '2026-11-14T23:05:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:05:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:05:00Z'
  commit: 1b191388
tags:
- capsule
- ffi
- mysql
- duckdb
- oracle
- mssql
- postgres
- broker
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
- `BrokerEngineDiscovery.register_installed()` probes optional engine wheels through `vertebra.utils.module_loader.import_optional` (`PY-08`) during `BrokerPlugin.on_app_init` [^broker-plugin].
- `vertebra._mysql` (`DIALECT_TAG_MYSQL = 2`), `vertebra._duckdb` (`DIALECT_TAG_DUCKDB = 6`), `vertebra._oracle` (`DIALECT_TAG_ORACLE = 4`), `vertebra._mssql` (`DIALECT_TAG_MSSQL = 5`), and `vertebra._postgres` (`DIALECT_TAG_POSTGRES = 1`) each implement `BrokerPoolBackend` and export `broker_engine_capsule() -> object` [^mysql-rs] [^duckdb-rs] [^oracle-rs] [^mssql-rs] [^postgres-rs] [^core-broker].
- Every engine crate depends strictly on `vertebra-db` (`BrokerPoolBackend` + `new_broker_engine_pycapsule`), never on `vertebra-py` [^db-registry].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Each standalone PyO3 wheel (`_mysql`, `_duckdb`, `_oracle`, `_mssql`, `_postgres`) statically links its own copy of `vertebra-db`, so `BROKER_ENGINE_BACKENDS: LazyLock<DashMap<...>>` inside an engine extension is invisible to `_vertebra` [^db-registry] [^mysql-rs]. Following `GcpTokenCacheCapsule` [^gcp-capsule], this chapter introduces the `#[repr(C)]` `BrokerEngineCapsule` C-ABI vtable registered into `vertebra._vertebra.BrokerEngineRegistry` via `BrokerEngineDiscovery`, and wires all five relational and OLAP native wheels (`MySQL`, `DuckDB`, `Oracle`, `SQL Server`, and `PostgreSQL`) onto `BrokerPoolBackend` so multi-worker deployments share one broker-owned server pool per `pool_key` [^oracle-rs] [^mssql-rs] [^postgres-rs].

**Non-Goals:** SQLite capsule (owned by `native-sqlite-driver`); BigQuery and Spanner (stateless REST and session-multiplexed clients; excluded per root spec).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Capsule ABI & Registry | `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` [^db-registry] [^py-broker] [^vertebra-pyi] | `BrokerEngineCapsule`, `BrokerPoolBackend`, `BrokerEngineRegistry` |
| Python Discovery | `src/py/vertebra/extensions/broker/engines.py`, `src/py/vertebra/extensions/broker/plugin.py` [^broker-plugin] | `BrokerEngineDiscovery.register_installed()` via `import_optional` |
| MySQL & DuckDB | `src/rs/vertebra-mysql/src/lib.rs`, `src/rs/vertebra-duckdb/src/lib.rs`, `src/py/vertebra/_{mysql,duckdb}.pyi` [^mysql-rs] [^duckdb-rs] | `MySQLBrokerBackend` (`tag=2`), `DuckDBBrokerBackend` (`tag=6`, `VTBARROW`) |
| Oracle & SQL Server | `src/rs/vertebra-oracle/src/lib.rs`, `src/rs/vertebra-mssql/src/{broker,lib}.rs`, `src/py/vertebra/_{oracle,mssql}.pyi` [^oracle-rs] [^mssql-rs] | `OracleBrokerBackend` (`tag=4`), `MssqlBrokerBackend` (`tag=5`) |
| PostgreSQL | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` [^postgres-rs] | `PostgresBrokerBackend` (`tag=1`) wrapping `PostgresPool` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PyO3 FFI and Batch Dispatch](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md), [PostgreSQL Native Driver](../../../../knowledge/domains/data/postgres-native-driver.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R8**: Replace `BROKER_ENGINE_BACKENDS` in `vertebra-db` with `#[repr(C)] BrokerEngineCapsule`, `BrokerPoolBackend`, `new_broker_engine_pycapsule`, and `_vertebra.BrokerEngineRegistry` (`t01`).
- **R9**: Implement `BrokerEngineDiscovery.register_installed()` using `import_optional` (`PY-08`) and invoke it during `BrokerPlugin.on_app_init` (`t01`).
- **R10a (MySQL & DuckDB)**: Upgrade `MySQLBrokerBackend` (`DIALECT_TAG_MYSQL = 2`) and implement `DuckDBBrokerBackend` (`DIALECT_TAG_DUCKDB = 6`, with `VTBARROW` SHM result frames) over `BrokerPoolBackend` and export `broker_engine_capsule()` in `_mysql` and `_duckdb` (`t02`).
- **R10b (Oracle & SQL Server)**: Implement `OracleBrokerBackend` (`DIALECT_TAG_ORACLE = 4` over `SyncOraclePool`) and wire `MssqlBrokerBackend` (`DIALECT_TAG_MSSQL = 5`, in `src/rs/vertebra-mssql/src/broker.rs`) into `BrokerPoolBackend`, exporting `broker_engine_capsule()` in `_oracle` and `_mssql` (`t03`).
- **R10c (PostgreSQL)**: Implement `PostgresBrokerBackend` (`DIALECT_TAG_POSTGRES = 1`) over `PostgresPool` and export `broker_engine_capsule()` in `_postgres` (`t04`).

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

pub struct OracleBrokerBackend { /* DashMap<u64, Arc<SyncOraclePool>> */ }
pub struct MssqlBrokerBackend { /* RwLock<HashMap<u64, Arc<MssqlPoolEntry>>> */ }
pub struct PostgresBrokerBackend { /* DashMap<u64, Arc<PostgresPool>> */ }

#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>>;
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

Each engine wheel stub (`_mysql.pyi`, `_duckdb.pyi`, `_oracle.pyi`, `_mssql.pyi`, `_postgres.pyi`) declares `def broker_engine_capsule() -> object: ...`.

**Integration Flow:**
- `BrokerPlugin.on_app_init` -> `BrokerEngineDiscovery.register_installed()` -> `import_optional("vertebra._<engine>")` -> `mod.broker_engine_capsule()` -> `BrokerEngineRegistry.register(capsule)`.
- `BrokerEngineRegistry.normalize(tag, sql)` delegates to dialect placeholder rewriters (`$1` PostgreSQL, `?` MySQL/DuckDB, `:1` Oracle, `@P1` SQL Server).
- `BrokerEngineRegistry.execute(tag, pool_id, frame)` -> `catch_unwind` FFI trampoline -> `<Engine>BrokerBackend::execute` -> `DbRpcResponse` / `VTBARROW` SHM.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid capsule | `register(object())` | `TypeError` naming `vertebra.BrokerEngineCapsule` | Pointer safety |
| ABI mismatch | `abi_version != 1` | `RuntimeError` naming engine and expected ABI `1` | Versioned C-ABI |
| Engine panic | Backend panics inside FFI callback | `catch_unwind` traps panic into error code mapped to `RuntimeError` | No unwind across `extern "C"` |
| Uninstalled wheel | `import_optional("vertebra._oracle")` returns `None` | Silently skipped during discovery | Optional sibling wheels |
| Invalid DSN | `open_pool("invalid://", 4)` | `Err` mapped to `RuntimeError` | Fail-loud pool open |
| Unknown `pool_id` | `execute(999, frame)` | `Err("unknown pool id 999")` | No default pool fallback |
| Double close | `close_pool(id)` called twice | Idempotent no-op | Clean teardown |

## 3. Documentation & OKF Knowledge Plan

The native engine capsule registration matrix is documented in `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` in `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Define BrokerEngineCapsule ABI v1, BrokerEngineRegistry, and Python Discovery | `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` (+2 more) | `cargo test -p vertebra-db --test broker_engine_capsule_tests`, `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t02` | Implement MySQL and DuckDB BrokerPoolBackend and broker_engine_capsule Exports | `src/rs/vertebra-mysql/src/lib.rs`, `src/py/vertebra/_mysql.pyi`, `src/rs/vertebra-duckdb/src/lib.rs` (+1 more) | `stubtest vertebra._mysql vertebra._duckdb`, `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t03` | Implement Oracle and SQL Server BrokerPoolBackend and broker_engine_capsule Exports | `src/rs/vertebra-oracle/src/lib.rs`, `src/py/vertebra/_oracle.pyi`, `src/rs/vertebra-mssql/src/broker.rs` (+2 more) | `stubtest vertebra._oracle vertebra._mssql`, `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t04` | Implement PostgreSQL BrokerPoolBackend Over PostgresPool and broker_engine_capsule Export | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` | `stubtest vertebra._postgres`, `src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [x] [673051f1] [t01-capsule-contract-registry-and-discovery.md](tasks/t01-capsule-contract-registry-and-discovery.md) — Implement `BrokerPoolBackend` + `PyCapsule` ABI v1 in `vertebra-db`/`vertebra-py` and Python `BrokerEngineDiscovery.register_installed()`.
- [x] [673051f1] [t02-mysql-and-duckdb-broker-capsules.md](tasks/t02-mysql-and-duckdb-broker-capsules.md) — Implement `MySQLBrokerBackend` (`tag 2`) and `DuckDBBrokerBackend` (`tag 6`) and export `broker_engine_capsule()` in `_mysql` and `_duckdb`.
- [x] [673051f1] [t03-oracle-and-mssql-broker-capsules.md](tasks/t03-oracle-and-mssql-broker-capsules.md) — Implement `OracleBrokerBackend` (`tag 4`) and wire `MssqlBrokerBackend` (`tag 5`) into `BrokerPoolBackend`, exporting `broker_engine_capsule()` in `_oracle` and `_mssql`.
- [x] [673051f1] [t04-postgres-broker-capsule.md](tasks/t04-postgres-broker-capsule.md) — Implement `PostgresBrokerBackend` (`tag 1`) over `PostgresPool` and export `broker_engine_capsule()` in `_postgres`.

## 6. Current State & Remaining Plan Snapshot

- **Completed (`673051f1`)**: `4/4` tasks completed (`ch02` done); `broker_engine_capsule()` exported across PostgreSQL, MySQL, Oracle, SQL Server, and DuckDB, registered via `BrokerEngineDiscovery.register_installed()`, and verified across Rust unit/Clippy, `mypy.stubtest`, and `pytest`.
- **Active Invariants**: All five native relational/OLAP capsules expose ABI v1 and pass `mypy.stubtest`.
- **Fast Chapter Gate (`< 60s`)**: `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-mysql -p vertebra-duckdb -p vertebra-oracle -p vertebra-mssql -p vertebra-postgres --features vertebra-db/gcp && uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._mysql vertebra._duckdb vertebra._oracle vertebra._mssql vertebra._postgres --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q`

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
