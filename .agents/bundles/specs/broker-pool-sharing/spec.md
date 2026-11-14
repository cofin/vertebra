---
type: Spec
flow_id: broker-pool-sharing
title: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config
description: Route SQLSpec, Advanced Alchemy, and SQLAlchemy database configurations through the POSIX shared-memory ring broker so worker processes share one broker-owned pool per pool_key with atomic segment ownership, UDS authenticated attach, C-ABI engine capsules, fail-loud quarantine, crash ROLLBACK reclamation, and standalone vertebra.db.dbapi.
status: stable
state: in_progress
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
- ch01-shm-ownership-and-broker-wire
- ch02-engine-capsules-and-discovery
- ch03-lease-safety-and-uds-attach
- ch04-python-pooling-sqlspec-sqlalchemy-and-docs
current_chapter: ch03-lease-safety-and-uds-attach
current_task: broker-pool-sharing:ch03:t01
parent_prd: native-data-platform
depends_on_flows:
- native-postgres-driver
plan_revision: 1
plan_commit: 1b191388
created_at: '2026-11-12T23:45:00Z'
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- broker
- shm
- pool
- sqlspec
- sqlalchemy
- dbapi
sources:
- id: core-shm
  resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
  title: Named POSIX shared-memory regions
- id: core-cleanup
  resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
  title: Process SHM cleanup hook
- id: worker-factory
  resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
  title: Supervisor worker spawning, panic hook, and stale SHM reaper
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
- id: gcp-capsule
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
  title: Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: sqlspec-provider
  resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
  title: Broker-side SQLSpec provider session and lease management
- id: sqlspec-proxy
  resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
  title: Worker-side SQLSpec proxy drivers
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
  title: SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes
- id: postgres-rs
  resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
  title: PostgreSQL native Protocol 3.0 pool
- id: broker-okf
  resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  title: Broker and SHM leases OKF chapter
---

# Specification: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `ShmRegion::create_named` unlinks before `O_EXCL` ([#95](https://github.com/cofin/vertebra/issues/95)), `cleanup_all()` is a no-op ([#96](https://github.com/cofin/vertebra/issues/96)), worker attach is unauthenticated ([#97](https://github.com/cofin/vertebra/issues/97)), `_BaseSQLSpecProvider` suppresses rollback/release errors ([#98](https://github.com/cofin/vertebra/issues/98)), and `BROKER_ENGINE_BACKENDS` is isolated per `cdylib` [^core-shm] [^core-cleanup] [^db-registry] [^sqlspec-provider].
- **Target Architecture**: Atomic `ShmOwnerHeader` create-or-attach + PID-aware signal/startup cleanup, `0600` UDS `SCM_RIGHTS` attach, XXH3-64 `stable_hash64` + 64 `pool_key` partitions, C-ABI `BrokerEngineCapsule` (`MySQL`, `DuckDB`, `Oracle`, `MSSQL`, `PostgreSQL`) in `vertebra._vertebra.BrokerEngineRegistry`, `LeaseState::Quarantined` + server-side crash `ROLLBACK`, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` SQLSpec proxies, standalone `vertebra.db.dbapi`, and SQLAlchemy/Advanced Alchemy `BrokerPool` [^gcp-capsule] [^py-broker] [^postgres-rs].
- **Scope & Boundaries**: `8` chapters and `27` compact tasks; BigQuery and Spanner stay on per-worker clients (recorded in `shm-broker-limitations.md`), and SQLite broker registration lands in `native-sqlite-driver` [^broker-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Multi-worker Vertebra deployments wrapping upstream `sqlspec` configs (`ProxySyncConfig` / `ProxyAsyncConfig`) or SQLAlchemy / Advanced Alchemy engines require a single broker-owned connection pool per `BaseDatabaseConfig.pool_key()` shared safely over POSIX shared memory. At `18baadcd`, named SHM creation pre-unlinks live segments and `_BaseSHMBrokerTransport` falls back to `create_named` on any attach error ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^broker-transport], `cleanup_all()` is empty while startup sweeping ignores owner PID liveness ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory], worker attach lacks UDS authentication ([#97](https://github.com/cofin/vertebra/issues/97)), SQLSpec session release suppresses rollback errors ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider], `get_or_compile_statement` / `_acquire_sync` / `AsyncSHMBrokerTransport.acquire` use process-salted `hash()`, `BROKER_ENGINE_BACKENDS` is duplicated per `cdylib` [^db-registry], and PEP 249 `SyncDBAPI*` / `AsyncDBAPI*` classes are coupled inside `extensions/sqlalchemy/dialects.py` [^sa-dialects].

**Non-Goals:**
- BigQuery and Spanner native broker pools (stateless REST jobs and internal multiplexed gRPC sessions; documented in `shm-broker-limitations.md`).
- SQLite native wheel and broker capsule (owned by downstream flow `native-sqlite-driver`).
- Django database backend rewrites (owned by `sqlalchemy-and-django-integration`, which consumes `vertebra.db.dbapi` in place).
- Windows named POSIX shared-memory segments.

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SHM & cleanup | `vertebra-core/src/{shm,cleanup,broker,broker_attach}.rs`, `vertebra-http/src/workers/factory.rs` [^core-shm] [^core-cleanup] [^worker-factory] | Atomic owner headers, signal/panic cleanup, 64 pool partitions, `Quarantined` slots, `0600` UDS `SCM_RIGHTS` |
| Cross-wheel FFI | `vertebra-db/src/lib.rs`, `vertebra-py/src/broker.rs`, `vertebra-{mysql,duckdb,oracle,mssql,postgres}` [^db-registry] [^gcp-capsule] [^postgres-rs] | `#[repr(C)] BrokerEngineCapsule` -> `vertebra._vertebra.BrokerEngineRegistry` |
| SQLSpec proxy | `extensions/sqlspec/{provider,proxy,plugin}.py` [^sqlspec-provider] [^sqlspec-proxy] | Typed lease/session Protocols, quarantine-on-error, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` |
| DB-API & Pool | `vertebra/db/dbapi/*`, `extensions/sqlalchemy/{async_adapter,dialects,pool}.py` [^sa-dialects] | Framework-neutral PEP 249 package + SQLAlchemy/Advanced Alchemy `BrokerPool` |

**Knowledge Anchors:** [Broker and SHM Leases](../../knowledge/domains/data/broker-and-shm-leases.md), [Ring Buffers and SHM](../../knowledge/architecture/ipc/ring-buffers-and-shm.md), [Transport and Rings](../../knowledge/domains/ipc/transport-and-rings.md), [Database Drivers and Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [SQLSpec, Alchemy, and Dialects](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Project Workflow](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R3**: Atomic `ShmRegion::open_or_create_named` with `ShmOwnerHeader` PID liveness, async-signal-safe `cleanup_all()` + PID-aware `reap_dead_owner_segments`, and explicit Python `owner`/`worker` broker attach (`ch01`).
- **R4–R7**: XXH3-64 `stable_hash64` cross-process keys, up to 64 per-`pool_key` partitions with lease heartbeats, versioned `dbrpc` frames, and removal of in-memory broker transports + `alchemy/broker.py::StatementCache` (`ch02`).
- **R8–R10**: C-ABI `BrokerEngineCapsule` + `_vertebra.BrokerEngineRegistry`, `BrokerEngineDiscovery` via `import_optional`, and capsules across MySQL (`2`), DuckDB (`6`), Oracle (`4`), MSSQL (`5`), and PostgreSQL (`1`) (`ch03`–`ch04`).
- **R11–R14, R19, R21**: `LeaseState::Quarantined`, `0600` UDS `SCM_RIGHTS` authenticated attach, `BrokerHost` (`vtb broker serve`) crash `ROLLBACK` + owning-thread disposal, session affinity + `VTBARROW` streaming, and `PoolMetrics` (`ch03`).
- **R15–R16, R20**: Typed `SQLSpec` provider Protocols with zero `inspect.signature` or `contextlib.suppress(Exception)`, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` proxy drivers, and 4-worker multi-process acceptance (`ch04:t01`–`t02`).
- **R17–R18**: Standalone `vertebra.db.dbapi` (`SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPIModule`) with zero upward imports, `extensions/sqlalchemy/async_adapter.py`, and `BrokerPool`/`AsyncBrokerPool` (`ch04:t03`).
- **R22**: Synchronized `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, and `shm-broker-limitations.md` (`ch04:t04`).

```rust
pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }
#[repr(u32)] pub enum LeaseState { Free = 0, Leased = 1, Quarantined = 2 }
pub const BROKER_ENGINE_CAPSULE_NAME: &CStr = c"vertebra.BrokerEngineCapsule";
pub const BROKER_ENGINE_ABI_VERSION: u32 = 1;
#[repr(C)] pub struct BrokerEngineCapsule { /* abi_version, dialect_tag, ctx, normalize/open_pool/execute/close_pool vtable */ }
```

```python
def stable_hash64(data: bytes | str) -> int: ...
class BrokerEngineRegistry:
    @staticmethod
    def register(capsule: object) -> int: ...
    @staticmethod
    def registered_tags() -> list[int]: ...
class BrokerAttachError(VertebraError): ...
class BrokerSegmentInUseError(BrokerAttachError): ...
class BrokerAuthenticationError(BrokerAttachError): ...
class LeaseQuarantinedError(VertebraError): ...
```

**Integration Flow:**
- Owner `BrokerHost` (`vtb broker serve` or supervisor) creates named SHM atomically, registers installed `BrokerEngineCapsule`s, and serves authenticated `SCM_RIGHTS` UDS attach.
- Workers lease connections per `pool_key` partition via `ProxySyncConfig`/`ProxyAsyncConfig` or SQLAlchemy `BrokerPool`, streaming `dbrpc` + `VTBARROW` results.
- Uncertain rollback/exit quarantines and disposes the physical connection; dead-worker reaping runs server-side `ROLLBACK` before slot reuse.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Live segment race | Concurrent starters or ABI mismatch | 1 `Created` + N `Attached`, or `BrokerSegmentInUseError` if incompatible | Never unlinks live owner ([#95](https://github.com/cofin/vertebra/issues/95)) |
| Panic / orphan SHM | Supervisor panic or `SIGKILL` | `cleanup_all()` on panic/signal; `reap_dead_owner_segments` on `ESRCH` | Clean `/dev/shm` ([#96](https://github.com/cofin/vertebra/issues/96)) |
| Unauthorized attach | Bad secret or permissive UDS dir | `BrokerAuthenticationError` before FD transfer | Authenticated pool ([#97](https://github.com/cofin/vertebra/issues/97)) |
| Rollback failure | Rollback or `__exit__` raises | Slot `Quarantined`, connection closed, `LeaseQuarantinedError` raised | Zero dirty re-lease ([#98](https://github.com/cofin/vertebra/issues/98)) |

## 3. Documentation & OKF Knowledge Plan

- `[ ]` `docs/usage/extensions.md` — shared broker pools, UDS attach boundary, quarantine, `ProxySyncConfig`/`ProxyAsyncConfig`, and `BrokerPool` (`ch04:t04`).
- `[ ]` `docs/usage/databases.md` — native engine `BrokerEngineCapsule` matrix and `vertebra.db.dbapi` (`ch04:t04`).
- `[ ]` `docs/changelog/index.md` — `Unreleased` entries (`ch04:t04`).
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — updated broker architecture (`<= 200` lines, `ch04:t04`).
- `[ ]` `.agents/bundles/knowledge/domains/data/shm-broker-limitations.md` — operational caveats and engine exclusions (`<= 200` lines, `ch04:t04`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — index link to `shm-broker-limitations.md` (`ch04:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: Atomic SHM Ownership, Supervisor Lifecycle, Pool Identity, and DBRPC Wire Protocol | `4` tasks | `pytest shm.rs` |
| `ch02` | Chapter 02: BrokerEngineCapsule ABI v1, Python Discovery, and Relational/OLAP Engine Capsules | `4` tasks | `pytest broker_engine_capsule_tests.rs` |
| `ch03` | Chapter 03: Quarantine Reclamation, Authenticated UDS Broker Attach, Session Affinity, and CLI | `4` tasks | `pytest broker.rs` |
| `ch04` | Chapter 04: Proxy Drivers, Multi-Process SQLSpec & SQLAlchemy Pool Sharing, and Docs/OKF | `4` tasks | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [x] [936110fc] [ch01-shm-ownership-and-broker-wire](chapters/ch01-shm-ownership-and-broker-wire/spec.md) — Enforce O_CREAT | O_EXCL SHM ownership, OwnedShmRegistry crash/shutdown unlink, explicit owner/worker broker roles, cross-process XXH3-64 pool identity, partitioned pools/heartbeats, and binary DBRPC frames.
- [x] [673051f1] [ch02-engine-capsules-and-discovery](chapters/ch02-engine-capsules-and-discovery/spec.md) — Define BrokerEngineCapsule ABI v1 and BrokerEngineRegistry, implement Python BrokerEngineDiscovery, and export broker_engine_capsule() across MySQL, DuckDB, Oracle, SQL Server, and PostgreSQL.
- [ ] [ch03-lease-safety-and-uds-attach](chapters/ch03-lease-safety-and-uds-attach/spec.md) — Implement crashed-worker lease quarantine and reclamation, SCM_RIGHTS/peercred UDS broker attach, BrokerHost reclamation loop, transaction session affinity, streaming Arrow cursors, and vtb broker CLI.
- [ ] [ch04-python-pooling-sqlspec-sqlalchemy-and-docs](chapters/ch04-python-pooling-sqlspec-sqlalchemy-and-docs/spec.md) — Refactor SQLSpec provider seams and ProxySyncDriver/ProxyAsyncDriver, prove 4-worker single-pool sharing, extract standalone vertebra.db.dbapi and wire SQLAlchemy/Advanced Alchemy broker pooling, and sync docs/OKF.

## 6. Current State & Remaining Plan Snapshot

- **Progress (`673051f1`)**: `2/4` chapters and `8/16` tasks completed; frontier at `ch03-lease-safety-and-uds-attach` (`broker-pool-sharing:ch03:t01`).
- **Upstream Flow Dependencies**: `native-postgres-driver`.
- **Active Invariants**: `vertebra-core` never links engine crates; live SHM segments are never unlinked; `extensions/sqlspec` contains zero `inspect.signature` or `contextlib.suppress(Exception)`; `vertebra.db.dbapi` has zero upward imports.
- **Full `/flow:finish` Gate**: `make lint-all && make rs-test && make py-test && make docs && make review-check`

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Process SHM cleanup hook.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor worker spawning, panic hook, and stale SHM reaper.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^postgres-rs]: `src/rs/vertebra-postgres/src/pool.rs::PostgresPool` — PostgreSQL native Protocol 3.0 pool.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
