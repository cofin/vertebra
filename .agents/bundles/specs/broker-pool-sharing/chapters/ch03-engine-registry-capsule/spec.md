---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch03-engine-registry-capsule
title: "Chapter 3: Engine Registry Capsule"
description: Replace the per-cdylib static BrokerEngineBackend map with a C-ABI BrokerEngineCapsule consumed by vertebra._vertebra.BrokerEngineRegistry, and register installed engine capsules from Python at broker startup.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-pool-partitions-and-dbrpc
replan_trigger: null
current_task: null
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::register_broker_engine_backend
    title: Static LazyLock DashMap registry compiled into each cdylib
  - id: mysql-backend
    resource: src/rs/vertebra-mysql/src/lib.rs::register_mysql_broker_backend
    title: MySQL registration into its own cdylib copy
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: C-ABI capsule precedent with abi_version and ctx clone/drop
  - id: shm-capsule
    resource: src/rs/vertebra-db/src/py_async.rs::SHM_CAPSULE_NAME
    title: PyCapsule naming and destructor precedent
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.on_app_init
    title: Broker plugin startup
tags:
  - registry
  - capsule
  - ffi
---

# Chapter 3: Engine Registry Capsule

<!-- truth: start -->
- `vertebra_db::BrokerEngineCapsule` is a `#[repr(C)]` vtable (`abi_version`, `dialect_tag`, `name_ptr`/`name_len`, `ctx`, `clone_ctx_fn`, `drop_ctx_fn`, `normalize_fn`, `open_pool_fn`, `execute_fn`, `close_pool_fn`) wrapped in a `PyCapsule` named `vertebra.BrokerEngineCapsule` [^gcp-capsule] [^shm-capsule].
- `vertebra_db::BrokerPoolBackend: BrokerEngineBackend` adds pool methods; `BrokerEngineCapsule::from_backend(Arc<dyn BrokerPoolBackend>)` builds the vtable inside each engine cdylib.
- `register_broker_engine_backend`, `lookup_broker_engine_backend`, and `BROKER_ENGINE_BACKENDS` are removed; the only registry lives in `_vertebra` as `BrokerEngineRegistry` [^db-registry].
- `vertebra.extensions.broker.engines.BrokerEngineDiscovery` imports installed `vertebra._<engine>` modules and registers each `broker_engine_capsule()` [^broker-plugin].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`BROKER_ENGINE_BACKENDS` is a `LazyLock<DashMap>` in the `vertebra-db` rlib. Each engine cdylib (`_mysql`, `_oracle`, `_mssql`, `_duckdb`, `_postgres`) statically links its own copy, so `register_mysql_broker_backend()` inside `_mysql` writes to a map `_vertebra` never sees, and `lookup_broker_engine_backend` has no caller [^db-registry] [^mysql-backend]. Rust trait objects have no stable ABI across separately compiled cdylibs. The repository already solves the same problem for GCP token caches with a versioned C-ABI vtable passed through Python [^gcp-capsule].

### 1.2 Non-Goals

- Engine implementations (`ch04`, `ch08 / t01`); broker host dispatch loop (`ch05 / t03`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Registry | `src/rs/vertebra-db/src/lib.rs::{BrokerEngineBackend,register_broker_engine_backend,lookup_broker_engine_backend}` [^db-registry] | rlib linked into every cdylib. |
| Precedent | `src/rs/vertebra-core/src/gcp_auth.rs::{GcpTokenCacheCapsule,SharedGcpTokenCacheHandle}`, `src/rs/vertebra-db/src/py_async.rs::SHM_CAPSULE_NAME` [^gcp-capsule] [^shm-capsule] | C-ABI across cdylibs. |
| Consumer | `src/rs/vertebra-py/src/broker.rs::LeaseBroker` | `_vertebra`. |
| Startup | `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.on_app_init` [^broker-plugin] | Python. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md); `.agents/skills/pyo3-bridge/SKILL.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R8**: Capsule contract and `_vertebra` registry (`t01`). **R9**: Python discovery and registration (`t02`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

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
pub trait BrokerEngineBackend: Send + Sync {
    fn dialect_tag(&self) -> u16;
    fn dialect_name(&self) -> &'static str;
    fn normalize_statement(&self, sql: &str) -> (String, usize);
}
pub trait BrokerPoolBackend: BrokerEngineBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>;
    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>;
    fn close_pool(&self, pool_id: u64);
}
impl BrokerEngineCapsule { pub fn from_backend(backend: Arc<dyn BrokerPoolBackend>) -> Self; }
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
class BrokerEngineDiscovery:
    ENGINE_MODULES: ClassVar[tuple[str, ...]] = ("vertebra._mysql", "vertebra._oracle", "vertebra._mssql", "vertebra._duckdb", "vertebra._postgres")
    @classmethod
    def register_installed(cls) -> list[int]: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `BrokerPlugin.on_app_init` → `BrokerEngineDiscovery.register_installed()` → `importlib.util.find_spec(name)`; when present `importlib.import_module(name).broker_engine_capsule()` → `BrokerEngineRegistry.register(capsule)` → `_vertebra` stores a `ForeignEngineHandle` (clone of ctx) keyed by `dialect_tag`.
- `ShmStatementCache` plan insertion → `BrokerEngineRegistry.normalize(tag, sql)` → engine `normalize_fn`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Wrong capsule name | `register(object())` | `TypeError("expected a vertebra.BrokerEngineCapsule")` | Type safety |
| ABI mismatch | `abi_version = 2` | `RuntimeError("broker engine capsule 'mysql' ABI 2; expected 1")` | Fail loud |
| Duplicate tag | Same tag registered twice | Replaces handle, drops previous ctx once | No leak |
| Module absent | `find_spec` returns `None` | Skipped silently | Optional engines |
| Module present, no capsule fn | `AttributeError` | Propagates (installed wheel must export it) | Fail loud |
| Panic in engine | Engine fn panics | `catch_unwind` in shim returns error code; Python `RuntimeError` | No unwinding across FFI |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `vertebra._mysql` installed, when `BrokerEngineDiscovery.register_installed()` runs, then `2 in BrokerEngineRegistry.registered_tags()` and `normalize(2, "SELECT :a")` returns `("SELECT ?", 1)`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Transport of registration | `PyCapsule` through Python import | Exported C symbol + `dlsym`: depends on wheel layout and loader flags (`RTLD_LOCAL`). |
| Registry location | `_vertebra` only | Keep static map in `vertebra-db`: invisible across cdylibs. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R8 | [t01](tasks/t01-capsule-contract-and-registry.md) | `behavior_tdd` | `src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs` |
| R9 | [t02](tasks/t02-python-engine-discovery.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch03:t01 — Capsule contract and `_vertebra` registry (tasks/t01-capsule-contract-and-registry.md)
- [ ] broker-pool-sharing:ch03:t02 — Python engine discovery at broker startup (tasks/t02-python-engine-discovery.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features tls,compression,gcp --test broker_engine_capsule_tests && make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02` (`0/2` completed).
- **Active Invariants**: `vertebra-core` and `vertebra-py` never depend on engine crates.

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::register_broker_engine_backend` — Static registry.
[^mysql-backend]: `src/rs/vertebra-mysql/src/lib.rs::register_mysql_broker_backend` — MySQL registration.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Capsule precedent.
[^shm-capsule]: `src/rs/vertebra-db/src/py_async.rs::SHM_CAPSULE_NAME` — PyCapsule precedent.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.on_app_init` — Startup.
