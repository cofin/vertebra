---
type: Spec
flow_id: native-data-platform
title: "Master PRD: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)"
description: "Master PRD for shared cross-worker connection pools via the shared-memory ring broker and feature-complete per-engine Rust PyO3 driver and object-storage wheels across all supported databases and caches."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
parent_prd: null
research:
  - research/gap-analysis_20261103.md
  - research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 12
plan_commit: 18baadcd
state_revision: 9
current_task: null
last_verified_checkpoint: 28415b3f
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-15T17:48:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:flow-archive
    at: "2026-11-15T17:48:00Z"
    commit: 28415b3f
  - by: agent:flow-completion
    at: "2026-11-14T21:38:00Z"
    commit: 8a836024
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - prd
  - drivers
  - rust
  - broker
  - storage
  - postgres
  - oracle
  - mysql
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Shared-memory ring broker pool admission and worker lease primitives"
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: "VTBARROW shared-memory IPC result allocator"
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: "Polymorphic DatabaseRegistry and per-engine adapter resolution"
  - id: fail-loud
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Completed fail-loud driver, codec, and install-hint baseline"
  - id: pg-native
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline"
  - id: postgres-py
    resource: src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver
    title: "Permanent native SyncPostgreSQLDriver over vertebra-postgres"
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "Notification capability protocol split into SyncNotificationSource and AsyncNotificationSource in native-redis-driver"
  - id: datafusion
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: "Embedded in-process Arrow federation engine exempt from server-client driver metric"
  - id: mysql
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: "Standalone vertebra._mysql PyO3 module"
  - id: mssql
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: "Standalone vertebra._mssql PyO3 module"
  - id: oracle
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: "Standalone vertebra._oracle PyO3 module"
---

# Master PRD: Native Data Platform

<!-- truth: start -->
- **Per-Engine Optional PyO3 Wheels**: Every database and cache engine compiles as an isolated optional PyO3 extension (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`, `vertebra-core[<engine>]`) sharing `vertebra-db` (`rlib`) primitives, keeping `vertebra-core` (`vertebra._vertebra`) and `vertebra-http` free of database and gRPC client SDKs [^db-registry].
- **Merged Engine & Storage Wheels (`main @ 1b191388` + stacked PRs to `HEAD @ 1b191388`)**: BigQuery (`21c99f95`), Spanner (`21c99f95`), Object Storage (`067fa901` + `18baadcd` + archived `storage-integrity-and-streaming`), DuckDB (`14aa83b8` + archived `driver-conformance-ci` GCS bearer rotation `479f8728`/`1b191388`), MySQL (`e86d8daf`), SQL Server (`134546d4`), Oracle (`47da6820` + archived `driver-conformance-ci` notify liveness `479f8728`/`1b191388`), and PostgreSQL (`native-postgres-driver` complete and archived at `7116ba13` / `7116ba13` + remediation `1b191388`) execute natively through standalone wheels [^pg-native] [^postgres-py] [^mysql] [^mssql] [^oracle].
- **Shared Broker Pools Complete; SQLite & Redis Planned**: `broker-pool-sharing` (`C-ABI` `BrokerEngineCapsule`, SHM RPC + `VTBARROW`, `vertebra.db.dbapi`, `SQLAlchemy` `BrokerPool`, `0600` UDS `SCM_RIGHTS`, and crash `ROLLBACK` cleanup) is complete and archived at `28415b3f` alongside `native-adapter-organization` (`8a836024`), `driver-conformance-ci`, and `storage-integrity-and-streaming`; `native-sqlite-driver` (`vertebra-sqlite` bundled `rusqlite`) and `native-redis-driver` (`vertebra-redis` RESP3 + hoisted `SyncNotificationSource`/`AsyncNotificationSource` split) remain planned [^core-broker] [^arrow-shm] [^notification-source].
- **Embedded `DataFusionEngine` Exemption**: `DataFusionEngine` (`datafusion` Python package, `mode="in_process"` only) is an in-process Arrow federation engine holding zero server connections and is exempt from the no-third-party-client driver metric [^datafusion].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Baseline & Problem Statement
Baseline is `main @ 18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)), paired with sibling Master PRD [integration-remediation](../integration-remediation/spec.md). Research dossiers live in [research/gap-analysis_20261103.md](research/gap-analysis_20261103.md) and [research/rust-driver-crates_20261103.md](research/rust-driver-crates_20261103.md).

This initiative delivers two non-negotiable platform commitments:
1. **Cross-Worker Shared Pools via the SHM Ring Broker** [^core-broker]: Any upstream or Vertebra Advanced Alchemy / SQLAlchemy or SQLSpec configuration routes through Vertebra's shared-memory ring broker so worker processes share one physical connection pool with zero-copy `VTBARROW` result transfer [^arrow-shm].
2. **Modular Per-Engine Rust PyO3 Wheels Across Every Engine** [^db-registry]: Standalone `#[pymodule(gil_used = false)]` wheels back PostgreSQL (`vertebra-postgres`), MySQL/MariaDB (`vertebra-mysql`), Oracle 26ai (`vertebra-oracle`), SQL Server (`vertebra-mssql`), SQLite (`vertebra-sqlite`), DuckDB (`vertebra-duckdb`), BigQuery (`vertebra-bigquery`), Spanner (`vertebra-spanner`), Redis/Valkey (`vertebra-redis`), and Object Storage (`vertebra-storage`) [^fail-loud].

Eight companion wheels (`_bigquery`, `_spanner`, `_storage`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`, `_postgres`) are live behind their permanent Python drivers (`native-postgres-driver` completed at `7116ba13`, removing `psycopg`/`asyncpg` and fixing GCP dialer TLS/refresh paths; `native-adapter-organization` archived at `8a836024`, decomposing BigQuery, Spanner, Oracle, MySQL, MSSQL, and DuckDB adapter modules and cleaning `DataFusionConfig`; `broker-pool-sharing` archived at `28415b3f`, delivering `ShmOwnerHeader`, `BrokerEngineCapsule` ABI v1, `BrokerHost` crash `ROLLBACK`, `vertebra.db.dbapi`, and `BrokerPool`) [^pg-native] [^postgres-py]. SQLite (`sqlite3`) and Redis/Valkey (Python sockets + `redis.asyncio`) await `native-sqlite-driver` and `native-redis-driver`.

### 1.2 Non-Goals & Deferred Backlog
- AWS RDS / Aurora DSQL IAM authentication ([#67](https://github.com/cofin/vertebra/issues/67)), Azure Entra ID database/cache authentication ([#68](https://github.com/cofin/vertebra/issues/68)), `SyncRedisQueueStore` / `AsyncRedisQueueStore` (`redis-queue-store`), and CockroachDB/AlloyDB custom wire extensions remain in the backlog until scheduled.

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` Per-Engine Wheel Isolation & `ShmCapsule` Sharing**: Each engine crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, `src/py/vertebra/_<engine>.pyi`, `[project.optional-dependencies].<engine>`) links `vertebra-db` (`rlib`: `connection.rs`, `dialer.rs`, `arrow_shm.rs`, `gcp.rs`, `py_async.rs`), imports `DatabaseError` / `GoogleCloudError` / `TaskPanicError` dynamically from `vertebra._vertebra`, keys completion drainers by `_vertebra_<engine>_completion_drainer`, and crosses `.so` boundaries strictly via C-ABI capsules (`ShmCapsule`, `GcpTokenCacheCapsule`, `BrokerEngineCapsule`).
- **`R2` Leading `Sync*` / `Async*` Driver & Capability Contracts**:
  - `SyncDatabaseDriver` / `AsyncDatabaseDriver` expose `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`), `cancel()`, and `close()` with zero `*_async` methods or aliases.
  - `native-redis-driver` (`ch03`) splits `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across PostgreSQL, Oracle, Redis, and Valkey (`Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource`) ahead of `queues-tasks-and-channels`, while `queues-tasks-and-channels` splits `QueueStore` into `SyncQueueStore` / `AsyncQueueStore` [^notification-source].
- **`R3` Broker Pool Ownership & Neutral `vertebra.db.dbapi`**: Complete (`28415b3f`) — `broker-pool-sharing` hosts physical Rust pools by `config.pool_key()` via `BrokerEngineCapsule`, streams results over `VTBARROW`, extracts framework-neutral PEP 249 `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`), and implements dead-worker lease reclamation (`reclaim_worker_leases`, `BrokerHost` crash `ROLLBACK`, `SLOT_STATE_RECLAIMING`).
- **`R4` Clean Adapter Module Organization & `DataFusionEngine` Boundary**: Complete (`8a836024`) — BigQuery Object Table helpers (`bigquery/object_tables.py`) and Spanner mutation/transaction modules (`spanner/mutations.py`, `spanner/transactions.py`) are separated from connection configs, Oracle/MySQL/MSSQL/DuckDB drivers follow `_sync.py`/`_async.py`/`pipeline.py` boundaries, redundant vendor wrappers are removed from `DataFusionEngine`, and `DataFusionConfig` accepts `mode="in_process"` only while remaining exempt from the database-client metric [^datafusion].
- **`R5` Live Conformance, Integrity & Companion Wheel Gates**: Every engine passes `test_driver_conformance.py` (`pytest-databases`) with zero skips in CI (`driver-conformance-ci`), and every companion wheel (`_storage`, `_bigquery`, `_spanner`, `_bundler`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`, `_postgres`, `_sqlite`, `_redis`) enforces wheel/sdist payload isolation (`storage-integrity-and-streaming` `ch03:t01` + per-driver flows).

## 3. Documentation & OKF Knowledge Plan

Each child flow updates `docs/usage/{databases,analytics-and-cdc,storage,cloud-dialers}.md`, `docs/reference/native-stubs.md`, `.agents/skills/{pyo3-bridge,review-pyo3-bridge,review-core}/`, and `.agents/bundles/knowledge/domains/{data,storage}/` (`<= 200` lines per chapter, `<= 40` truth-block lines), retiring split-state caveats in `postgres-limitations.md`, `database-driver-limitations.md`, and `object-storage-limitations.md` as cutovers land.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Child Flow(s) | Primary Surface | Verification Gate |
| --- | --- | --- | --- |
| `R1` Per-engine wheels & PostgreSQL cutover | `native-postgres-driver` (`7116ba13`), [`native-sqlite-driver`](../native-sqlite-driver/spec.md), [`native-redis-driver`](../native-redis-driver/spec.md) | `src/rs/vertebra-{postgres,sqlite,redis}/**`, `src/py/vertebra/db/adapters/**` | `cargo test`, `mypy.stubtest`, `*-wheel-check`, `pytest` |
| `R2` `SyncNotificationSource` / `AsyncNotificationSource` split | [`native-redis-driver`](../native-redis-driver/spec.md) (`ch03`) | `db/driver/capabilities.py`, `db/adapters/{postgres,oracle,redis,valkey}/notify.py` | Unit & wire notification suites |
| `R3` Shared broker pools & `vertebra.db.dbapi` | `broker-pool-sharing` (`28415b3f`, archived) | `src/rs/vertebra-{core,db,py}/**`, `src/py/vertebra/{db/dbapi,extensions/broker}/**` | Multi-process SHM pool & crash-reclamation tests |
| `R4` Adapter module split & `DataFusionConfig` cleanup | `native-adapter-organization` (`8a836024`) | `src/py/vertebra/db/adapters/{bigquery,spanner,oracle,mysql,mssql,duckdb,datafusion}/**` | `pytest` + `mypy` + docstring leak gate |
| `R5` Live conformance, storage integrity & wheel checks | `driver-conformance-ci` (`1b191388`), `storage-integrity-and-streaming` (`61f0cd0e`) | `src/rs/vertebra-storage/**`, `src/py/tests/integration/**`, `Makefile`, `ci.yml` | `make db-conformance`, `make *-wheel-check`, `make rs-test` |

## 5. Implementation Plan

- [x] [21c99f95] `db-layout-and-contract` — Adapter package layout, standalone GCP wheels, unified driver contract, and conformance harness
- [x] [af1b3d30] `driver-fail-loud` — Removal of `memory://` and `sqlite3` stand-ins, install-hint extras gate, and explicit conformance states
- [x] [067fa901] `storage-completeness` & `storage-remediation` — Multicloud `vertebra-storage`, `GcpTokenCacheCapsule`, zero-copy `PyBackedBytes`, bulk transfers, and readers/writers
- [x] [14aa83b8] `native-duckdb-driver` — Standalone `vertebra-duckdb` wheel, `VTBARROW`, `Appender`, and `DuckDBSecretConfig`
- [x] [e86d8daf] `native-mysql-driver` — Standalone `vertebra-mysql` wheel, binary prepared statements, Cloud SQL IAM, and `VTBARROW`
- [x] [134546d4] `native-mssql-driver` — Standalone `vertebra-mssql` wheel, TDS 7.4/8.0 `sp_executesql`, `BulkLoad`, and `VTBARROW`
- [x] [47da6820] `native-oracle-driver` — Standalone `vertebra-oracle` Thin 26ai wheel, wallets/DRCP, `VECTOR`/`OSON`, and TxEventQ notifications
- [x] [7116ba13] `native-postgres-driver` — Complete standalone `vertebra-postgres` wheel (`ch01`–`ch09`, `32` tasks): verified reconnect, `AsyncPostgresReplication`, CAS `PostgresChangeSource`, permanent `_sync.py`/`_async.py` drivers, `psycopg`/`asyncpg` removal + GCP dialer fixes, PG14–18 conformance, wheel/benchmark gates, and docs
- [x] [61f0cd0e] `storage-integrity-and-streaming` — CRC32C/MD5/SHA-256 content-integrity receipts and verified streaming reads ([#99](https://github.com/cofin/vertebra/issues/99)), streaming Arrow/Parquet multipart writes ([#100](https://github.com/cofin/vertebra/issues/100)), `vertebra.typing.IntegrityError`, and parameterized `{storage,bigquery,spanner,bundler}-wheel-check` isolation gates
- [x] [1b191388] `driver-conformance-ci` — DuckDB GCS bearer secret rotation (`SyncGcsBearerSecretProvider`/`AsyncGcsBearerSecretProvider`, `(HTTP 401)`/`(HTTP 403)` invalidation, startup `_client` cleanup), Oracle `wait_timeout_seconds` `SELECT 1 FROM DUAL` liveness, `ConformanceGate` + `ServiceVersionMatrix`, live DuckDB/MySQL/MariaDB/MSSQL/Oracle suites, `make db-conformance`, `bench`-only PyPI `duckdb` isolation, and CI conformance lanes
- [x] [8a836024] `native-adapter-organization` — BigQuery `object_tables.py`, Spanner `mutations.py`/`transactions.py`, Oracle/MySQL/MSSQL/DuckDB driver splits, and `DataFusionEngine` / `DataFusionConfig` (`pgwire_fdw` removal) cleanup (`t01`–`t04`)
- [x] [28415b3f] `broker-pool-sharing` — Cross-`.so` `BrokerEngineCapsule`, SHM RPC + `VTBARROW` lease execution, `vertebra.db.dbapi`, SQLAlchemy `BrokerPool`, and crash reclamation ([#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98); `ch01`–`ch04`, `16/16` tasks)
- [ ] [`native-sqlite-driver`](../native-sqlite-driver/spec.md) — Standalone `vertebra-sqlite` (`rusqlite` bundled WAL multi-reader pool, `JSONB`, `VTBARROW`, delayed-commit pipelines, stdlib `sqlite3` removal; `ch01`–`ch03`, `12` tasks)
- [ ] [`native-redis-driver`](../native-redis-driver/spec.md) — Standalone `vertebra-redis` RESP3 wheel (Cluster, Sentinel, client tracking, GCP Memorystore IAM, Streams, Pub/Sub) plus hoisted `SyncNotificationSource`/`AsyncNotificationSource` split (`ch01`–`ch04`, `16` tasks)

## 6. Current State & Remaining Plan Snapshot

- **Immediate Wave (Ready)**: `native-sqlite-driver` (`ch01`–`ch03`) and `native-redis-driver` (`ch01`–`ch04`), unlocking downstream [integration-remediation](../integration-remediation/spec.md) flows (`native-postgres-driver`, `storage-integrity-and-streaming`, `driver-conformance-ci`, `native-adapter-organization`, and `broker-pool-sharing` archived).

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory ring broker pool admission and worker leases.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named shared-memory allocator for `VTBARROW` IPC frames.
[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Polymorphic database registry and driver loader.
[^fail-loud]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud driver, codec, and install-hint baseline.
[^pg-native]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — Merged standalone `vertebra-postgres` wheel and bounded `ReplicationSession` baseline.
[^postgres-py]: `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` — Permanent native PostgreSQL driver over `vertebra._postgres`.
[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — Notification capability protocol split into `SyncNotificationSource` and `AsyncNotificationSource` in `native-redis-driver`.
[^datafusion]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — Embedded in-process Arrow federation engine exempt from server-client driver metric.
[^mysql]: `src/rs/vertebra-mysql/src/lib.rs` — Standalone `vertebra._mysql` PyO3 module.
[^mssql]: `src/rs/vertebra-mssql/src/lib.rs` — Standalone `vertebra._mssql` PyO3 module.
[^oracle]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` PyO3 module.
