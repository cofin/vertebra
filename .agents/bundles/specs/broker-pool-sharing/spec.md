---
type: Spec
flow_id: broker-pool-sharing
title: "Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config"
description: "Route SQLSpec, Advanced Alchemy, and SQLAlchemy database configurations through the POSIX shared-memory ring broker so worker processes share one broker-owned pool per pool_key with atomic segment ownership, UDS authenticated attach, C-ABI engine capsules, fail-loud quarantine, crash ROLLBACK reclamation, and standalone vertebra.db.dbapi."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-shm-ownership-and-lifecycle
  - ch02-broker-ipc-identity-and-wire
  - ch03-engine-capsules-core
  - ch04-engine-capsules-enterprise
  - ch05-lease-safety-and-uds-attach
  - ch06-sqlspec-seams-and-multiprocess
  - ch07-standalone-dbapi-and-sqlalchemy
  - ch08-docs-and-knowledge-sync
current_chapter: ch01-shm-ownership-and-lifecycle
current_task: "broker-pool-sharing:ch01:t01"
parent_prd: native-data-platform
depends_on_flows:
  - native-postgres-driver
plan_revision: 1
plan_commit: 18baadcd
created_at: "2026-11-12T23:45:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
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
    title: "Named POSIX shared-memory regions"
  - id: core-cleanup
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: "Process SHM cleanup hook"
  - id: worker-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: "Supervisor worker spawning, panic hook, and stale SHM reaper"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: "Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs"
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
    title: "Broker-side SQLSpec provider session and lease management"
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: "Worker-side SQLSpec proxy drivers"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: "SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes"
  - id: postgres-rs
    resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
    title: "PostgreSQL native Protocol 3.0 pool"
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: "Broker and SHM leases OKF chapter"
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
- **R11–R14, R19, R21**: `LeaseState::Quarantined`, `0600` UDS `SCM_RIGHTS` authenticated attach, `BrokerHost` (`vtb broker serve`) crash `ROLLBACK` + owning-thread disposal, session affinity + `VTBARROW` streaming, and `PoolMetrics` (`ch05`).
- **R15–R16, R20**: Typed `SQLSpec` provider Protocols with zero `inspect.signature` or `contextlib.suppress(Exception)`, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` proxy drivers, and 4-worker multi-process acceptance (`ch06`).
- **R17–R18**: Standalone `vertebra.db.dbapi` (`SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPIModule`) with zero upward imports, `extensions/sqlalchemy/async_adapter.py`, and `BrokerPool`/`AsyncBrokerPool` (`ch07`).
- **R22**: Synchronized `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, and `shm-broker-limitations.md` (`ch08`).

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

- `[ ]` `docs/usage/extensions.md` — shared broker pools, UDS attach boundary, quarantine, `ProxySyncConfig`/`ProxyAsyncConfig`, and `BrokerPool` (`ch08:t01`).
- `[ ]` `docs/usage/databases.md` — native engine `BrokerEngineCapsule` matrix and `vertebra.db.dbapi` (`ch08:t01`).
- `[ ]` `docs/changelog/index.md` — `Unreleased` entries (`ch08:t01`).
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — updated broker architecture (`<= 200` lines, `ch08:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/data/shm-broker-limitations.md` — operational caveats and engine exclusions (`<= 200` lines, `ch08:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — index link to `shm-broker-limitations.md` (`ch08:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R3` ([#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96)) | `ch01` (`t01`–`t04`) | `vertebra-core/src/{shm,cleanup,broker}.rs`, `extensions/broker/{errors,transport,plugin}.py` | `cargo test -p vertebra-core && pytest test_shm_lease_broker.py` |
| `R4`–`R7` | `ch02` (`t01`–`t04`) | `vertebra-core/src/{broker.rs,dbrpc/mod.rs}`, `extensions/broker/{dbrpc,transport,plugin}.py` | `cargo test -p vertebra-core && pytest test_broker_transport.py` |
| `R8`–`R10` (Core) | `ch03` (`t01`–`t04`) | `vertebra-db/src/lib.rs`, `vertebra-{py,mysql,duckdb}`, `extensions/broker/engines.py` | `cargo test -p vertebra-db && pytest test_shm_broker.py` |
| `R10` (Enterprise) | `ch04` (`t01`–`t03`) | `vertebra-{oracle,mssql,postgres}/src/lib.rs`, `_{oracle,mssql,postgres}.pyi` | `cargo test -p vertebra-oracle -p vertebra-mssql -p vertebra-postgres` |
| `R11`–`R14`, `R19`, `R21` ([#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98)) | `ch05` (`t01`–`t04`) | `vertebra-core/src/{broker,broker_attach}.rs`, `extensions/broker/{host,transport,plugin}.py` | `cargo test -p vertebra-core && pytest test_shm_broker.py` |
| `R15`–`R16`, `R20` ([#98](https://github.com/cofin/vertebra/issues/98)) | `ch06` (`t01`–`t03`) | `extensions/sqlspec/{provider,proxy,plugin}.py`, `telemetry/otel.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py test_multiprocess_pool_sharing.py` |
| `R17`–`R18` | `ch07` (`t01`–`t03`) | `vertebra/db/dbapi/*`, `extensions/sqlalchemy/{async_adapter,dialects,pool}.py` | `pytest test_dbapi_standalone.py test_gcp_adapters_and_sqlalchemy.py` |
| `R22` | `ch08` (`t01`–`t02`) | `docs/usage/*.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/*.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [ ] [ch01-shm-ownership-and-lifecycle](chapters/ch01-shm-ownership-and-lifecycle/spec.md) — Atomic `open_or_create_named` owner headers, signal/panic cleanup registry, and fail-loud Python owner/worker attach (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py`)
- [ ] [ch02-broker-ipc-identity-and-wire](chapters/ch02-broker-ipc-identity-and-wire/spec.md) — XXH3-64 `stable_hash64`, per-`pool_key` lease partitions + heartbeats, `dbrpc` frames, and dead transport/cache removal (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-py && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py`)
- [ ] [ch03-engine-capsules-core](chapters/ch03-engine-capsules-core/spec.md) — C-ABI `BrokerEngineCapsule` + `BrokerEngineRegistry`, `BrokerEngineDiscovery`, and MySQL/DuckDB capsules (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-mysql -p vertebra-duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py`)
- [ ] [ch04-engine-capsules-enterprise](chapters/ch04-engine-capsules-enterprise/spec.md) — Oracle (`tag=4`), SQL Server (`tag=5`), and PostgreSQL (`tag=1`) `BrokerPoolBackend` capsules (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle -p vertebra-mssql -p vertebra-postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py`)
- [ ] [ch05-lease-safety-and-uds-attach](chapters/ch05-lease-safety-and-uds-attach/spec.md) — `LeaseState::Quarantined`, `0600` UDS `SCM_RIGHTS` attach, `BrokerHost` (`vtb broker serve`) crash `ROLLBACK`, and pool metrics (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-py && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_shm_lease_broker.py`)
- [ ] [ch06-sqlspec-seams-and-multiprocess](chapters/ch06-sqlspec-seams-and-multiprocess/spec.md) — Typed `SQLSpec` provider protocols + quarantine, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` proxies, and multi-process acceptance (Gate: `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_shm_broker.py`)
- [ ] [ch07-standalone-dbapi-and-sqlalchemy](chapters/ch07-standalone-dbapi-and-sqlalchemy/spec.md) — Standalone `vertebra.db.dbapi` extraction, `extensions/sqlalchemy/async_adapter.py`, and SQLAlchemy/Advanced Alchemy `BrokerPool` (Gate: `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`)
- [ ] [ch08-docs-and-knowledge-sync](chapters/ch08-docs-and-knowledge-sync/spec.md) — Public `docs/` guides + changelog and OKF `broker-and-shm-leases.md` + `shm-broker-limitations.md` (`<= 200` lines) (Gate: `make review-check && uv run --no-sync python -m tools.docs.scan_public_docstrings`)

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `0/8` chapters and `0/27` tasks completed; frontier at `ch01-shm-ownership-and-lifecycle` (`broker-pool-sharing:ch01:t01`).
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
