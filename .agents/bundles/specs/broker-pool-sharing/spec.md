---
type: Spec
flow_id: broker-pool-sharing
title: "Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config"
description: "Route upstream SQLSpec and Advanced Alchemy/SQLAlchemy configs through the shared-memory ring broker so worker processes share one broker-owned pool per pool_key, with safe segment ownership, authenticated attach, cross-extension native engine registration, quarantine of uncertain connections, session affinity, VTBARROW streaming, and crash reclamation."
status: draft
state: planned
has_chapters: true
chapters:
  - ch01-shm-segment-ownership
  - ch02-pool-partitions-and-dbrpc
  - ch03-engine-registry-capsule
  - ch04-engine-backend-registration
  - ch05-lease-safety-and-host
  - ch06-sqlspec-proxy
  - ch07-dbapi-and-sqlalchemy-pool
  - ch08-reclamation-and-acceptance
  - ch09-docs-and-okf
current_chapter: ch01-shm-segment-ownership
current_task: "broker-pool-sharing:ch01:t01"
parent_prd: native-data-platform
depends_on_flows:
  - native-postgres-driver
research:
  - ../native-data-platform/research/gap-analysis_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_chapter: null
last_reviewed_commit: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: core-shm
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
    title: Named POSIX shared-memory regions (create_named unlinks before O_EXCL)
  - id: core-cleanup
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: Empty panic-hook cleanup function
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Shared-memory lease table, statement cache, dialect tags, reclaim_worker
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: Per-cdylib static BrokerEngineBackend registry (BROKER_ENGINE_BACKENDS)
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: C-ABI vtable capsule precedent for sharing state across PyO3 cdylibs
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 LeaseBroker and StatementCache bindings
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: SHM broker transports, process-seeded hash() keys, open-or-create fallback
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
    title: Broker-side SQLSpec provider with signature duck-typing and suppressed release errors
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: Worker-side upstream SQLSpec proxy drivers
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: SQLAlchemy-embedded DB-API module, connection, and cursor classes
  - id: worker-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: Supervisor worker spawning, panic hook, and stale SHM reaper
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: Broker and SHM leases knowledge chapter
tags:
  - broker
  - shm
  - pool
  - sqlspec
  - advanced-alchemy
  - sqlalchemy
---

# Flow: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

<!-- truth: start -->
- **Target state**: every worker process leases connections over POSIX SHM from one broker-owned pool per `BaseDatabaseConfig.pool_key()`; installed native engine wheels register Rust pool handlers with the broker through a C-ABI capsule [^core-broker] [^gcp-capsule].
- **Segment ownership**: named broker segments are created atomically (open, then `O_CREAT|O_EXCL` only on `ENOENT`, never unlink-then-create) and record owner PID and generation; a live segment is never replaced and only a proven-dead owner's segment is reclaimed ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm].
- **Cleanup**: a process-wide owned-segment registry backs the panic hook and SIGTERM/SIGINT handlers in place of the empty `cleanup_all`; startup reaping is owner-PID-aware ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].
- **Attach authentication**: workers attach through a permission-checked private Unix socket handshake with a constant-time secret compare before receiving the segment ([#97](https://github.com/cofin/vertebra/issues/97)).
- **Uncertain outcomes**: failed rollback/commit, cancel/timeout, or mid-statement disconnect moves a lease to `Quarantined`; the broker closes the physical connection before capacity returns and the worker receives a typed error ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- **Cross-extension registry**: each engine cdylib links its own copy of the `vertebra-db` static `BROKER_ENGINE_BACKENDS` map, so today registrations never reach `_vertebra`; registration moves to `BrokerEngineCapsule` objects consumed by `vertebra._vertebra.BrokerEngineRegistry` [^db-registry].
- **Stable keys**: statement and bind keys use `vertebra._vertebra.stable_hash64` (XXH3-64, seed `0`) instead of process-seeded Python `hash()` [^broker-transport].
- **Fail-loud proxy**: the SQLSpec provider uses typed lease and session contracts; no `inspect.signature` duck-typing and no `contextlib.suppress(Exception)` [^sqlspec-provider].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Users keep configurations such as `sqlspec.adapters.asyncpg.AsyncpgConfig`, `sqlspec.adapters.oracledb.OracleAsyncConfig`, or `advanced_alchemy.config.SQLAlchemyAsyncConfig`, wrap them with Vertebra, and expect one broker-owned connection pool shared across every worker process. At HEAD the broker path has correctness and safety gaps:

- `_BaseSHMBrokerTransport.__init__` falls back to `LeaseBroker.create_named` on any `open_named` exception, and `ShmRegion::create_named` unlinks the name before `O_CREAT|O_EXCL`, so concurrent starters or a validation error replace a live segment; `tests/it/shm.rs::stale_region_replaced_on_create_named` asserts that behavior ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^broker-transport].
- `cleanup::cleanup_all` is empty while `vertebra-http` installs a panic hook that calls it; `cleanup_stale_shm` reaps by name pattern without owner liveness ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].
- Attach is by segment name only; any same-user process that knows the name joins the pool ([#97](https://github.com/cofin/vertebra/issues/97)) [^py-broker].
- `_BaseSQLSpecProvider._release_session_sync` / `_release_session_async` and `_release_broker_lease*` wrap rollback, context exit, and release in `contextlib.suppress(Exception)`, then return the connection normally; `_call_broker_acquire` and `_call_with_params` duck-type through `inspect.signature` ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- `vertebra-mysql` calls `register_broker_engine_backend` inside `_mysql`, but the map is a `LazyLock` static compiled into every cdylib separately; `lookup_broker_engine_backend` has no caller, and Oracle, SQL Server, DuckDB, and PostgreSQL never register [^db-registry].
- `_BaseInMemoryBrokerTransport._get_or_compile_plan`, `_BaseSHMBrokerTransport._get_or_compile_plan`, `_acquire_sync`, and `AsyncSHMBrokerTransport._acquire_async` derive `sql_hash` / `bind_hash` from Python `hash()`, which differs per process under hash randomization [^broker-transport].

This flow delivers safe segment ownership, pool partitions and RPC frames, a cross-extension engine registry with registrations for every merged relational engine, lease safety and broker hosting, a fail-loud SQLSpec proxy, the framework-neutral DB-API package with a SQLAlchemy broker pool, crash reclamation with multi-process acceptance, and synchronized docs and OKF.

### 1.2 Non-Goals

- BigQuery and Spanner native broker backends. BigQuery executes stateless REST jobs with no connection to lease; `vertebra-spanner` manages server-side sessions inside its own client. Both stay on per-worker clients; the exclusion is recorded in the limitations record (`ch09 / t02`).
- A SQLite backend (owned by `native-sqlite-driver`).
- Relocating `vertebra.db.dbapi` after this flow extracts it; `sqlalchemy-and-django-integration` consumes it in place.
- Windows named-segment support.

### 1.3 Upstream Execution Baseline

- Branch `feat/cleanup-2` at `c8722c76` (`origin/main`) after the merged DuckDB, MySQL, SQL Server, Oracle, and PostgreSQL wheels.
- `depends_on_flows: native-postgres-driver` applies only to `ch08 / t01` (PostgreSQL backend registration over the permanent native PostgreSQL driver). Every other task starts from HEAD.
- The completed `driver-fail-loud` baseline removed admission-only `*Pool` structs from `vertebra-db` ([Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md)).

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| SHM regions | `src/rs/vertebra-core/src/shm.rs::ShmRegion::create_named`, `::open_named`; `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` [^core-shm] | Local OS resource (`/dev/shm`); callers in `broker.rs::ShmLeaseTable::create_with_statements`, `vector.rs`, `vertebra-db/src/arrow_shm.rs`. |
| Cleanup | `src/rs/vertebra-core/src/cleanup.rs::cleanup_all`; `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` [^core-cleanup] [^worker-factory] | Supervisor process hooks. |
| Lease table | `src/rs/vertebra-core/src/broker.rs::ShmLeaseHeader`, `::LeaseSlot`, `::ShmLeaseTable::{try_acquire,acquire,release,reclaim_worker}`, `::ShmStatementCache`, `::DIALECT_TAG_*` [^core-broker] | In-process SHM table shared across processes. |
| Engine registry | `src/rs/vertebra-db/src/lib.rs::{BrokerEngineBackend,register_broker_engine_backend,lookup_broker_engine_backend}`; `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` [^db-registry] | Cross-cdylib boundary; precedent `GcpTokenCacheCapsule` [^gcp-capsule]. |
| PyO3 broker | `src/rs/vertebra-py/src/broker.rs::LeaseBroker::{create_named,open_named,from_env}`, `::StatementCache` [^py-broker] | `vertebra._vertebra` extension. |
| Python transport | `src/py/vertebra/extensions/broker/transport.py::{_BaseSHMBrokerTransport,SyncSHMBrokerTransport,AsyncSHMBrokerTransport,_BaseInMemoryBrokerTransport}`; `extensions/broker/plugin.py::BrokerPlugin` [^broker-transport] | Worker and broker host processes. |
| SQLSpec proxy | `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider`; `proxy.py::{ProxySyncDriver,ProxyAsyncDriver}`; `plugin.py::{ProxySyncConfig,ProxyAsyncConfig}` [^sqlspec-provider] [^sqlspec-proxy] | Upstream `sqlspec` adapters (remote-owned databases). |
| DB-API | `src/py/vertebra/extensions/sqlalchemy/dialects.py::{SyncDBAPIModule,SyncDBAPIConnection,SyncDBAPICursor,AsyncDBAPIModule,AsyncDBAPIConnection,AsyncDBAPICursor,_is_row_returning_sql}` [^sa-dialects] | SQLAlchemy dialects and pool creators. |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../knowledge/domains/data/broker-and-shm-leases.md) [^broker-okf], [Ring Buffers and SHM](../../knowledge/architecture/ipc/ring-buffers-and-shm.md), [Transport and Rings](../../knowledge/domains/ipc/transport-and-rings.md), [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../knowledge/workflow.md).
- Skills: `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/review-tests/SKILL.md`, `sqlspec`, `advanced-alchemy`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: `ShmRegion::open_or_create_named` attaches or creates atomically; `create_named` never unlinks a live segment; owner PID and generation live in the region header.
- **R2**: An owned-segment registry backs `cleanup_all`, the panic hook, and SIGTERM/SIGINT; startup reaping removes only dead-owner segments.
- **R3**: Python broker attach raises typed errors for non-`ENOENT` failures and never replaces a live segment; every `create_named` caller is audited.
- **R4**: `stable_hash64` replaces Python `hash()` for every cross-process statement and bind key.
- **R5**: `ShmLeaseTable` partitions per `pool_key` with heartbeats instead of blind TTL reaping.
- **R6**: Versioned `dbrpc` frames cover `execute`, `execute_many`, `begin`, `commit`, `rollback`, `savepoint`, `fetch_chunk`, `cancel`, and `close`.
- **R7**: `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, and `extensions/alchemy/broker.py::StatementCache` are deleted.
- **R8**: `BrokerEngineCapsule` (C-ABI) and `BrokerEngineRegistry` in `_vertebra` replace the per-cdylib static map.
- **R9**: `BrokerPlugin` discovers installed engine modules and registers their capsules at broker startup.
- **R10**: `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, `vertebra._duckdb`, and `vertebra._postgres` export `broker_engine_capsule()`.
- **R11**: Lease slots carry `Quarantined` state; quarantined connections count against admission until disposed.
- **R12**: Authenticated attach over a private Unix socket with constant-time compare and redacted secrets.
- **R13**: Standalone broker host (`vtb broker serve`) and supervisor-spawned host share one lifecycle.
- **R14**: Session affinity pins leases across transactions, temp tables, prepared statements, and session settings; results stream as `VTBARROW` or row frames with cancel propagation.
- **R15**: The SQLSpec provider uses typed lease and session contracts with no signature duck-typing or suppressed errors.
- **R16**: `ProxySyncDriver` / `ProxyAsyncDriver` subclass `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` and preserve upstream compilers.
- **R17**: `vertebra.db.dbapi` exists with no upward or engine-specific imports; SQLAlchemy adaptation lives in `extensions/sqlalchemy/async_adapter.py`.
- **R18**: `extensions/sqlalchemy/pool.py::BrokerPool` shares broker pools for SQLAlchemy and Advanced Alchemy.
- **R19**: Crashed worker leases are reclaimed with server-side `ROLLBACK`; quarantined connections are disposed.
- **R20**: Multi-process acceptance proves one server-side pool for PostgreSQL and MySQL.
- **R21**: Per-pool and quarantine metrics appear in `BrokerPlugin.snapshot_metrics` / ZPages.
- **R22**: Docs, OKF, and a limitations record describe the shipped behavior.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
// vertebra-core/src/shm.rs
pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }
impl ShmRegion {
    pub fn open_or_create_named(name: &str, size: usize, init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>) -> Result<NamedAttach, ShmError>;
    pub fn owner_pid(&self) -> Option<u32>;
}
// vertebra-core/src/cleanup.rs
pub fn register_owned(name: &str);
pub fn unregister_owned(name: &str);
pub fn cleanup_all();
pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32;
// vertebra-core/src/broker.rs
#[repr(u32)] pub enum LeaseState { Free = 0, Leased = 1, Quarantined = 2 }
impl ShmLeaseTable {
    pub fn heartbeat(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn quarantine(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn dispose_quarantined(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
}
pub fn stable_hash64(bytes: &[u8]) -> u64;
// vertebra-db/src/lib.rs
pub const BROKER_ENGINE_CAPSULE_NAME: &CStr = c"vertebra.BrokerEngineCapsule";
pub const BROKER_ENGINE_ABI_VERSION: u32 = 1;
#[repr(C)] pub struct BrokerEngineCapsule { /* abi_version, dialect_tag, name_ptr/len, ctx, normalize_fn, open_pool_fn, execute_fn, close_pool_fn, drop_ctx_fn */ }
```

```python
# vertebra._vertebra (stubs in src/py/vertebra/_vertebra.pyi)
def stable_hash64(data: bytes | str) -> int: ...
class BrokerEngineRegistry:
    @staticmethod
    def register(capsule: object) -> int: ...
    @staticmethod
    def registered_tags() -> list[int]: ...
    @staticmethod
    def normalize(dialect_tag: int, sql: str) -> tuple[str, int]: ...
# vertebra.extensions.broker.errors
class BrokerAttachError(VertebraError): ...
class BrokerSegmentInUseError(BrokerAttachError): ...
class BrokerAuthenticationError(BrokerAttachError): ...
class LeaseQuarantinedError(VertebraError): ...
```

Per-chapter specs carry the remaining signatures (`dbrpc` frames, `BrokerHost`, typed lease protocols, `vertebra.db.dbapi`, `BrokerPool`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncSHMBrokerTransport(config)` → `LeaseBroker.open_or_create_named` → `ShmLeaseTable::open_or_create_with_statements` → `ShmRegion::open_or_create_named` → `cleanup::register_owned` on create.
- Supervisor panic/SIGTERM → `cleanup::cleanup_all` → `shm_unlink` for owned names; startup → `reap_dead_owner_segments`.
- `BrokerPlugin.on_app_init` → `BrokerEngineDiscovery.register_installed()` → `importlib.import_module("vertebra._<engine>").broker_engine_capsule()` → `BrokerEngineRegistry.register(capsule)`.
- Worker `ProxyAsyncDriver.execute` → `AsyncSHMBrokerTransport` dbrpc `execute` frame → `BrokerHost` → `BrokerEngineRegistry` native pool (`execute_fn`) or upstream SQLSpec provider → `VTBARROW` / row frame response.
- Provider release with uncertain outcome → `LeaseQuarantinedError` to worker → `ShmLeaseTable::quarantine` → owning-thread close → `dispose_quarantined`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Concurrent starters | 8 processes call `open_or_create_named` on one name | One `Created`, seven `Attached`; one `/dev/shm` entry | Shared admission limits |
| Live segment, bad ABI | Header ABI differs, owner alive | `BrokerSegmentInUseError`; segment untouched | No live replacement |
| Dead owner | Owner PID not alive | Segment unlinked and recreated | SIGKILL orphans recovered |
| Panic in supervisor | `panic!` after creating segments | Owned names unlinked | No `/dev/shm` leak |
| Wrong attach secret | Handshake secret mismatch | `BrokerAuthenticationError`; no lease granted | Authenticated pool |
| Squatted socket | Socket owned by another UID or mode not `0600` | Refused before secret is written | Secret never leaked |
| Hash randomization | Workers with different `PYTHONHASHSEED` | Identical statement and bind keys | Cross-process cache hits |
| Rollback failure | Rollback raises during release | Lease `Quarantined`; connection closed; `LeaseQuarantinedError` | No dirty connection re-leased |
| Engine wheel missing | `vertebra._oracle` not installed | Discovery skips Oracle; upstream SQLSpec path serves Oracle configs | Optional engines |
| ABI mismatch capsule | `abi_version != 1` | `RuntimeError` naming module and versions | Fail loud |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given an unmodified `AsyncpgConfig` wrapped in `ProxyAsyncConfig` and 4 worker processes, when each opens a transaction, then `pg_stat_activity` shows connections from one pool and one worker `SIGKILL` mid-transaction leaves no open transaction after reclamation.
- Given a MySQL native wheel installed, when the broker starts, then `BrokerEngineRegistry.registered_tags()` contains `2` and 4 workers share one server-side pool.
- Given a forced rollback failure, when the session releases, then the backend PID never appears again and the quarantine counter increments.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Cross-cdylib registration | C-ABI `BrokerEngineCapsule` passed through Python, consumed by `_vertebra` | Shared dynamic `vertebra-db` library: maturin wheels ship self-contained cdylibs; Rust has no stable ABI for `dyn Trait`. |
| Stable hash | XXH3-64 in Rust exposed to Python | `hashlib.blake2b`: slower on the hot path and duplicates a Rust-side key derivation. |
| Attach auth | Private Unix socket + secret | Name-only attach: any same-user process joins; `SO_PEERCRED` alone: does not distinguish co-tenant processes of one UID. |
| Uncertain outcome | Quarantine and close | Suppress and re-lease: leaks open transactions and session state. |
| BigQuery/Spanner | Excluded from native broker pools | Registering them: no leasable connection exists. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/7 files documented]`
- `[ ]` `docs/usage/extensions.md` — broker pools, attach security boundary, quarantine semantics (`ch09 / t01`).
- `[ ]` `docs/usage/databases.md` — native engine broker registration matrix (`ch09 / t01`).
- `[ ]` `docs/architecture/pyo3-and-workers.md` — segment ownership, capsule registry, broker host (`ch09 / t01`).
- `[ ]` `docs/changelog/index.md` — `Unreleased` entries (`ch09 / t01`).
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — current broker architecture (`ch09 / t02`).
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-pool-limitations.md` — new limitations record (`ch09 / t02`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — link to the limitations record (`ch09 / t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 | `ch01 / t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs` |
| R2 | `ch01 / t02` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs` |
| R3 | `ch01 / t03` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| R4 | `ch02 / t01` | `behavior_tdd` | `src/py/tests/unit/extensions/test_broker_transport.py` |
| R5 | `ch02 / t02` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R6 | `ch02 / t03` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R7 | `ch02 / t04` | `regression_tdd` | `src/py/tests/unit/extensions/test_broker_transport.py` |
| R8 | `ch03 / t01` | `behavior_tdd` | `src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs` |
| R9 | `ch03 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R10 | `ch04 / t01`–`t04`, `ch08 / t01` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R11 | `ch05 / t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R12 | `ch05 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| R13 | `ch05 / t03` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R14 | `ch05 / t04` | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |
| R15 | `ch06 / t01`, `ch06 / t02` | `regression_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |
| R16 | `ch06 / t03` | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |
| R17 | `ch07 / t01`, `ch07 / t02` | `behavior_tdd` | `src/py/tests/unit/db/test_dbapi.py` |
| R18 | `ch07 / t03` | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py` |
| R19 | `ch08 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| R20 | `ch08 / t03` | `integration_acceptance` | `src/py/tests/integration/extensions/test_broker_pool_sharing.py` |
| R21 | `ch08 / t04` | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R22 | `ch09 / t01`, `ch09 / t02` | `documentation_validation` | `make docs`, `make review-check` |

## 5. Implementation Plan

- [ ] ch01-shm-segment-ownership — Atomic create-or-attach, owner registry cleanup, fail-loud Python attach (chapters/ch01-shm-segment-ownership/spec.md)
- [ ] ch02-pool-partitions-and-dbrpc — Stable hashing, per-pool partitions with heartbeats, `dbrpc` frames, in-memory transport removal (chapters/ch02-pool-partitions-and-dbrpc/spec.md)
- [ ] ch03-engine-registry-capsule — `BrokerEngineCapsule`, `BrokerEngineRegistry`, and Python discovery (chapters/ch03-engine-registry-capsule/spec.md)
- [ ] ch04-engine-backend-registration — MySQL, Oracle, SQL Server, and DuckDB capsules (chapters/ch04-engine-backend-registration/spec.md)
- [ ] ch05-lease-safety-and-host — Quarantine, authenticated attach, broker host lifecycle, session affinity and streaming (chapters/ch05-lease-safety-and-host/spec.md)
- [ ] ch06-sqlspec-proxy — Typed provider seams and adapter-base proxy drivers (chapters/ch06-sqlspec-proxy/spec.md)
- [ ] ch07-dbapi-and-sqlalchemy-pool — `vertebra.db.dbapi`, SQLAlchemy async adapter, `BrokerPool` (chapters/ch07-dbapi-and-sqlalchemy-pool/spec.md)
- [ ] ch08-reclamation-and-acceptance — PostgreSQL capsule, crash reclamation, multi-process acceptance, metrics (chapters/ch08-reclamation-and-acceptance/spec.md)
- [ ] ch09-docs-and-okf — User docs, changelog, OKF chapter, limitations record (chapters/ch09-docs-and-okf/spec.md)
- **Chapter / Phase Recovery Checkpoint**: `make dev && make rs-test && uv run --no-sync pytest src/py/tests/unit/extensions -q && make lint-all`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `c8722c76` — plan authored; `0/9` chapters and `0/31` tasks completed.
- **Active Chapter**: `ch01-shm-segment-ownership` (`0/3` tasks completed).
- **Remaining Plan**: `ch01` through `ch09`.
- **Documentation & OKF Sync**: `[0/7 files documented]`.
- **Active Invariants**: `vertebra-core` never depends on engine crates; no live segment is unlinked; no `contextlib.suppress(Exception)` in broker or provider paths; Python `hash()` never derives cross-process keys.

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Empty panic-hook cleanup function.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Lease table, statement cache, dialect tags, `reclaim_worker`.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static registry.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI capsule precedent.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 broker bindings.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — SHM broker transports.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side proxy drivers.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded DB-API classes.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor hooks and stale reaper.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker knowledge chapter.
